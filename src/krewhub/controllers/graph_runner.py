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
from krewhub.models import BundleStatus, TaskStatus, WatchEventType
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.task_repo import TaskRepo
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
            await self._emit_graph_milestone(bundle_id, f"runner crash: {exc}", success=False)
        finally:
            self._in_flight.discard(bundle_id)

    async def _execute_bundle(self, bundle_id: str) -> None:
        bundle_repo = BundleRepo(self._db)
        task_repo = TaskRepo(self._db)

        bundle = await bundle_repo.get(bundle_id)
        if bundle is None or bundle.graph_code is None:
            logger.warning("graph runner: bundle %s missing or no graph_code", bundle_id)
            return

        if bundle.cookbook_id is None:
            await self._emit_graph_milestone(
                bundle_id,
                "bundle has no cookbook_id",
                success=False,
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
            await self._emit_graph_milestone(bundle_id, f"graph compile failed: {exc}", success=False)
            return

        # 2. Build the node_id → task_id map from the bundle's tasks.
        task_id_map = await task_repo.build_node_id_map(bundle_id)
        if not task_id_map:
            await self._emit_graph_milestone(
                bundle_id,
                "no graph-bound tasks found for bundle (graph_node_id is NULL on all rows)",
                success=False,
            )
            return

        # 3. Construct runtime state + deps.
        # Step (e): recipe is gone; repo coords now come from bundle.repo_spec.
        recipe_meta: dict = {}
        if bundle.repo_spec:
            spec = bundle.repo_spec
            owner_p = spec.get("owner", "")
            repo_p = spec.get("repo", "")
            ref = spec.get("ref") or spec.get("branch") or "main"
            if owner_p and repo_p:
                recipe_meta["repo_url"] = (
                    f"git@{spec.get('provider', 'github')}.com:"
                    f"{owner_p}/{repo_p}.git"
                )
            recipe_meta["branch"] = ref

        state = OrchestratorState(
            prompt=bundle.prompt,
            bundle_id=bundle.id,
        )
        deps = OrchestratorDeps(
            db=self._db,
            http=self._http,
            watch=self._watch,
            task_id_map=task_id_map,
            cookbook_id=bundle.cookbook_id,
            recipe_meta=recipe_meta,
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
            await self._emit_graph_milestone(bundle_id, f"graph.iter failed: {exc}", success=False)
            return

        # 5. Aggregate results and finalize the bundle row.
        all_success = bool(state.task_results) and all(
            r.success for r in state.task_results.values()
        )
        if all_success:
            # Re-verify every task row is in a terminal state. graph.iter()
            # returning doesn't prove this — an async branch could have
            # recorded success in state while the task row is still WORKING
            # (race against a sibling dispatch_cycle still polling).
            bundle_tasks = await task_repo.list_by_bundle(bundle_id)
            _TERMINAL = (TaskStatus.DONE, TaskStatus.BLOCKED, TaskStatus.CANCELLED)
            non_terminal = [t for t in bundle_tasks if t.status not in _TERMINAL]
            if non_terminal:
                await self._emit_graph_milestone(
                    bundle_id,
                    "graph finished but {n} task(s) non-terminal: {ids}".format(
                        n=len(non_terminal),
                        ids=", ".join(f"{t.id}={t.status}" for t in non_terminal[:5]),
                    ),
                    success=False,
                )
                return

            logger.info(
                "graph runner: bundle %s graph completed (%d nodes)",
                bundle_id, len(state.task_results),
            )
            await self._emit_graph_milestone(
                bundle_id,
                f"Graph completed: {len(state.task_results)} nodes succeeded",
                success=True,
            )
        else:
            failures = [
                f"{r.node_id}: {r.summary}"
                for r in state.task_results.values()
                if not r.success
            ]
            reason = "; ".join(failures) or "no task results recorded"
            await self._emit_graph_milestone(bundle_id, reason, success=False)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _emit_graph_milestone(
        self, bundle_id: str, body: str, *, success: bool,
    ) -> None:
        """Step (d.1): graph completion no longer writes bundle status.

        Bundle is a dumb container — we just emit a MILESTONE event and
        let downstream observers (UI, reapers, caller's close logic)
        decide what to do. Tape entry preserves the trace for replay.
        """
        bundle = await BundleRepo(self._db).get(bundle_id)
        if bundle is None:
            return

        from krewhub.repositories.event_repo import EventRepo
        from krewhub.models import ActorType, EventType, Event
        import uuid

        evt_body = body[:500]
        try:
            event = Event(
                id=f"evt_{uuid.uuid4().hex[:8]}",
                cookbook_id=bundle.cookbook_id,
                bundle_id=bundle_id,
                type=EventType.MILESTONE,
                actor_id="graph-runner",
                actor_type=ActorType.SYSTEM,
                body=evt_body,
                payload={"graph_success": success},
                created_at=datetime.now(timezone.utc),
            )
            await EventRepo(self._db).create(event)
        except Exception:
            logger.exception(
                "graph runner: failed to emit milestone event for %s", bundle_id,
            )

        try:
            await self._watch.record_resource(
                "bundle", bundle_id, WatchEventType.MODIFIED, bundle,
                cookbook_id=bundle.cookbook_id,
            )
        except Exception:
            logger.exception(
                "graph runner: failed to emit watch event for %s", bundle_id,
            )
