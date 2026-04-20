"""GraphRunnerController — runs validated pydantic-graph code per bundle.

Lifecycle per cycle (level-triggered, like other controllers):

    1. List bundles where graph_code IS NOT NULL and status='open'.
    2. For each, if not already in flight and capacity allows, spawn an
       asyncio.task that:
         a. Execs the graph code via the sandbox to get a Graph object.
         b. Builds task_id_map = {graph_node_id: task_id} from the bundle.
         c. Constructs OrchestratorState/Deps with the live db/http/watch.
         d. Drives graph.iter() to completion, letting dispatch_cycle do
            the per-step A2A retry loops.
         e. Updates the bundle row to COOKED on success or BLOCKED on
            any unhandled failure.

The controller does *not* await the per-bundle tasks inside reconcile() —
that would block the reconcile loop and prevent other bundles from being
picked up. Instead it tracks _in_flight bundle ids and lets each runner
task complete independently.

Crash recovery is intentionally minimal in this slice: if krewhub
restarts mid-run, the bundle is still status='open' (we only mark it
terminal on completion), so the next reconcile picks it up and starts
over from the beginning. The cycle's already-DONE short-circuit handles
the case where individual tasks completed before the crash.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import aiosqlite
import httpx

from krewhub.controllers.base import BaseController
from krewhub.models import BundleStatus, WatchEventType
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.services.digest_service import DigestService
from krewhub.services.graph_runtime import (
    OrchestratorDeps,
    OrchestratorState,
    dispatch_cycle,
)
from krewhub.services.graph_sandbox import (
    GraphExecError,
    GraphValidationError,
    execute_graph_code,
)
from krewhub.watch.service import WatchService

logger = logging.getLogger(__name__)

_DEFAULT_HTTP_TIMEOUT = 30.0


def _dedupe_facts(facts: list[dict]) -> list[dict]:
    """Remove duplicate facts by (claim, source_url, captured_by) key."""
    seen: set[str] = set()
    unique: list[dict] = []
    for f in facts:
        key = f"{f.get('claim', '')}::{f.get('source_url', '')}::{f.get('captured_by', '')}"
        if key not in seen:
            seen.add(key)
            unique.append(f)
    return unique


def _dedupe_code_refs(code_refs: list[dict]) -> list[dict]:
    """Remove duplicate code_refs by (repo_url, branch, commit_sha) key."""
    seen: set[str] = set()
    unique: list[dict] = []
    for c in code_refs:
        paths = "::".join(sorted(c.get("paths", [])))
        key = f"{c.get('repo_url', '')}::{c.get('branch', '')}::{c.get('commit_sha', '')}::{paths}"
        if key not in seen:
            seen.add(key)
            unique.append(c)
    return unique


class GraphRunnerController(BaseController):
    """Picks up bundles with attached graph code and runs them.

    Parameters:
        db, watch:        usual controller deps.
        interval:         reconcile cadence (seconds).
        max_concurrent:   max bundles running graphs simultaneously. Each
                          one consumes one asyncio task plus an httpx
                          connection pool slot, so cap defensively.
        poll_interval:    forwarded into dispatch_cycle for task-status polls.
        task_timeout:     forwarded into dispatch_cycle as the per-attempt ceiling.
    """

    def __init__(
        self,
        db: aiosqlite.Connection,
        watch: WatchService,
        *,
        interval: float = 2.0,
        max_concurrent: int = 4,
        poll_interval: float = 2.0,
        task_timeout: float = 300.0,
    ) -> None:
        super().__init__(db, watch, interval=interval)
        self._max_concurrent = max_concurrent
        self._poll_interval = poll_interval
        self._task_timeout = task_timeout
        self._http = httpx.AsyncClient(
            timeout=_DEFAULT_HTTP_TIMEOUT, follow_redirects=True,
        )
        self._in_flight: set[str] = set()
        self._runner_tasks: set[asyncio.Task] = set()
        # Skip first 10 reconcile cycles (20s) to let agents heartbeat
        self._startup_grace = 10

    async def stop(self) -> None:
        await super().stop()
        # Wait briefly for in-flight runs to wind down before closing http.
        if self._runner_tasks:
            await asyncio.gather(*self._runner_tasks, return_exceptions=True)
        await self._http.aclose()

    # ------------------------------------------------------------------
    # Reconcile
    # ------------------------------------------------------------------

    async def reconcile(self) -> None:
        # Wait for agents to come online after startup before dispatching.
        # Without this, the runner fires within 2s of startup but agents
        # need ~15s to send their first heartbeat, exhausting all attempts
        # against an empty agent pool.
        if self._startup_grace > 0:
            self._startup_grace -= 1
            return  # Wait for agents to heartbeat after restart

        if len(self._in_flight) >= self._max_concurrent:
            return

        bundle_repo = BundleRepo(self._db)
        candidates = await bundle_repo.list_runnable()

        for bundle in candidates:
            if bundle.id in self._in_flight:
                continue
            if len(self._in_flight) >= self._max_concurrent:
                break
            self._spawn_runner(bundle.id)

    def _spawn_runner(self, bundle_id: str) -> None:
        """Reserve the bundle and kick off its runner task."""
        self._in_flight.add(bundle_id)
        task = asyncio.create_task(
            self._run_one_bundle(bundle_id),
            name=f"graphrun:{bundle_id}",
        )
        self._runner_tasks.add(task)
        task.add_done_callback(self._on_runner_done)

    def _on_runner_done(self, task: asyncio.Task) -> None:
        self._runner_tasks.discard(task)
        # _in_flight is cleared inside _run_one_bundle's finally, but the
        # task may have been cancelled before that block ran — defensive cleanup.
        bundle_id = (task.get_name() or "").removeprefix("graphrun:")
        self._in_flight.discard(bundle_id)

    # ------------------------------------------------------------------
    # Per-bundle execution
    # ------------------------------------------------------------------

    async def _run_one_bundle(self, bundle_id: str) -> None:
        try:
            await self._execute_bundle(bundle_id)
        except asyncio.CancelledError:
            logger.info("graph runner: bundle %s cancelled", bundle_id)
            raise
        except Exception as exc:
            # Last-resort safety net: any unhandled exception inside the
            # runner must mark the bundle BLOCKED so it doesn't loop.
            logger.exception("graph runner: unhandled error for bundle %s", bundle_id)
            await self._mark_blocked(bundle_id, f"runner crash: {exc}")
        finally:
            self._in_flight.discard(bundle_id)

    async def _execute_bundle(self, bundle_id: str) -> None:
        bundle_repo = BundleRepo(self._db)
        task_repo = TaskRepo(self._db)
        recipe_repo = RecipeRepo(self._db)

        bundle = await bundle_repo.get(bundle_id)
        if bundle is None or bundle.graph_code is None:
            logger.warning("graph runner: bundle %s missing or no graph_code", bundle_id)
            return

        recipe = await recipe_repo.get(bundle.recipe_id)
        if recipe is None or recipe.cookbook_id is None:
            await self._mark_blocked(
                bundle_id,
                f"bundle's recipe {bundle.recipe_id} has no cookbook",
            )
            return

        # 1. Validate + exec the graph code (sandboxed).
        try:
            graph = execute_graph_code(
                bundle.graph_code,
                orchestrator_state_cls=OrchestratorState,
                orchestrator_deps_cls=OrchestratorDeps,
                dispatch_cycle=dispatch_cycle,
            )
        except (GraphValidationError, GraphExecError) as exc:
            await self._mark_blocked(bundle_id, f"graph compile failed: {exc}")
            return

        # 2. Build the node_id → task_id map from the bundle's tasks.
        task_id_map = await task_repo.build_node_id_map(bundle_id)
        if not task_id_map:
            await self._mark_blocked(
                bundle_id,
                "no graph-bound tasks found for bundle (graph_node_id is NULL on all rows)",
            )
            return

        # 3. Construct runtime state + deps.
        state = OrchestratorState(
            prompt=bundle.prompt,
            bundle_id=bundle.id,
            recipe_id=bundle.recipe_id,
        )
        deps = OrchestratorDeps(
            db=self._db,
            http=self._http,
            watch=self._watch,
            task_id_map=task_id_map,
            cookbook_id=recipe.cookbook_id,
            recipe_meta={
                "recipe_id": recipe.id,
                "recipe_name": recipe.name,
                "repo_url": recipe.repo_url,
                "branch": recipe.default_branch,
            },
            poll_interval=self._poll_interval,
            task_timeout=self._task_timeout,
        )

        logger.info(
            "graph runner: starting bundle %s with %d graph nodes",
            bundle_id, len(task_id_map),
        )

        # 4. Drive graph.iter() to completion.
        try:
            async with graph.iter(state=state, deps=deps) as run:
                async for _node in run:
                    # Per-node events surface through the watch channel via
                    # dispatch_cycle's task row updates; nothing more to do here.
                    pass
        except Exception as exc:
            await self._mark_blocked(bundle_id, f"graph.iter failed: {exc}")
            return

        # 5. Aggregate results and finalize the bundle row.
        all_success = bool(state.task_results) and all(
            r.success for r in state.task_results.values()
        )
        if all_success:
            updated = await bundle_repo.update_status(
                bundle_id,
                BundleStatus.COOKED,
                cooked_at=datetime.now(timezone.utc),
            )
            logger.info(
                "graph runner: bundle %s COOKED (%d nodes)",
                bundle_id, len(state.task_results),
            )
            if updated is not None:
                await self._watch.record_resource(
                    "bundle", bundle_id, WatchEventType.MODIFIED, updated,
                    recipe_id=updated.recipe_id,
                )

            # Auto-submit digest: aggregate facts/code_refs collected by
            # dispatch_cycle across all graph nodes.
            await self._auto_submit_digest(bundle_id, state)
        else:
            failures = [
                f"{r.node_id}: {r.summary}"
                for r in state.task_results.values()
                if not r.success
            ]
            reason = "; ".join(failures) or "no task results recorded"
            await self._mark_blocked(bundle_id, reason)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _auto_submit_digest(
        self, bundle_id: str, state: OrchestratorState,
    ) -> None:
        """Aggregate facts/code_refs from graph state + events and submit a digest."""
        all_facts: list[dict] = []
        all_code_refs: list[dict] = []
        task_results: list[dict] = []

        for result in state.task_results.values():
            task_results.append({
                "task_id": result.task_id,
                "outcome": result.summary,
            })
            all_facts.extend(result.facts)
            all_code_refs.extend(result.code_refs)

        # Fallback: also collect from events table (covers facts posted
        # via POST /tasks/{id}/events that dispatch_cycle didn't capture).
        # Also collect event-sink telemetry so we can warn on data loss.
        total_dropped = 0
        try:
            from krewhub.repositories.event_repo import EventRepo
            event_repo = EventRepo(self._db)
            for result in state.task_results.values():
                events = await event_repo.list_by_task(result.task_id)
                for evt in events:
                    all_facts.extend(f.model_dump() for f in evt.facts)
                    all_code_refs.extend(c.model_dump() for c in evt.code_refs)
                    # Check for event-sink drop telemetry
                    payload = evt.payload or {}
                    if payload.get("_telemetry") == "event_sink":
                        total_dropped += int(payload.get("dropped_count") or 0)
        except Exception:
            logger.debug("graph runner: event-based fact collection skipped", exc_info=True)

        all_facts = _dedupe_facts(all_facts)
        all_code_refs = _dedupe_code_refs(all_code_refs)

        node_count = len(state.task_results)
        fact_count = len(all_facts)
        code_ref_count = len(all_code_refs)
        summary = (
            f"Completed {node_count} task{'s' if node_count != 1 else ''}"
            f" — {fact_count} fact{'s' if fact_count != 1 else ''},"
            f" {code_ref_count} code ref{'s' if code_ref_count != 1 else ''}"
        )
        if total_dropped > 0:
            summary += f" ⚠ {total_dropped} event(s) dropped under back-pressure"

        try:
            digest_svc = DigestService(self._db, self._watch)
            digest = await digest_svc.submit_digest(
                bundle_id=bundle_id,
                submitted_by="graph-runner",
                summary=summary,
                task_results=task_results,
                facts=all_facts,
                code_refs=all_code_refs,
            )
            if digest is not None:
                logger.info(
                    "graph runner: auto-submitted digest %s for bundle %s",
                    digest.id, bundle_id,
                )
            else:
                logger.warning(
                    "graph runner: digest submission returned None for bundle %s "
                    "(digest may already exist or bundle not in expected state)",
                    bundle_id,
                )
        except Exception:
            logger.exception(
                "graph runner: failed to auto-submit digest for bundle %s",
                bundle_id,
            )

    async def _mark_blocked(self, bundle_id: str, reason: str) -> None:
        """Mark a bundle BLOCKED and surface the change over the watch bus.

        Emitting a MODIFIED watch event is the only way cookrew's SSE
        feed learns the bundle flipped — without it the UI keeps
        showing the last-loaded status (usually ``open``) and the user
        has no visible signal that the graph run ended, nor access to
        the Re-Run affordance that's gated on ``bundle.status``.
        """
        try:
            updated = await BundleRepo(self._db).update_status(
                bundle_id,
                BundleStatus.BLOCKED,
                blocked_reason=reason[:500],
            )
        except Exception:
            logger.exception(
                "graph runner: failed to mark bundle %s blocked", bundle_id,
            )
            return

        logger.info(
            "graph runner: bundle %s BLOCKED — %s", bundle_id, reason,
        )

        if updated is not None:
            try:
                await self._watch.record_resource(
                    "bundle", bundle_id, WatchEventType.MODIFIED, updated,
                    recipe_id=updated.recipe_id,
                )
            except Exception:
                # A watch-bus failure must not mask the DB update;
                # the runner already owns the terminal state.
                logger.exception(
                    "graph runner: failed to emit watch event for blocked "
                    "bundle %s", bundle_id,
                )
            # Write error to tape so the blocked reason is part of the
            # tape history — without this, graph failures leave no trace.
            try:
                from krewhub.tape.manager import TapeManager
                from republic import TapeEntry
                tape = TapeManager(self._db, updated.recipe_id)
                await tape._store.append(
                    tape._tape_name,
                    TapeEntry(
                        id=0, kind="event",
                        payload={
                            "bundle_id": bundle_id,
                            "body": f"Graph blocked: {reason[:400]}",
                            "phase": "blocked",
                        },
                        meta={"actor_type": "system", "event_type": "graph_blocked"},
                    ),
                )
            except Exception:
                logger.debug("graph runner: failed to write tape entry for %s", bundle_id)
