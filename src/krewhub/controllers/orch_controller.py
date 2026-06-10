"""Orch mode (O2): the minimal orchestration loop.

Supervises Brief-managed tasks (tasks with brief_json set) through the
control-loop contract from docs/orch-mode-design.html §2.1:

    dispatch(worker, Brief)   -> task-create with brief (O1) + the existing
                                 TaskDispatchController push
    observe(events|heartbeat) -> in-process WatchService subscription
                                 (event-driven fast path) + this reconciler
                                 (level-triggered fallback — never bare-polls
                                 workers, it reads krewhub's own state)
    on completion             -> validate(Report) -> accept ("retire") or
                                 escalate needs_human when the Report is
                                 missing/invalid
    on liveness_lost          -> respawn: reopen the task with claim cleared.
                                 The Brief lives on the task row, so the
                                 redispatch replays it by construction
                                 (Brief 重放 = 幂等下发). Capped at
                                 max_respawns, then parked BLOCKED with a
                                 blocker event (failure discipline: root-
                                 cause review, not retry-bombing).

Scope guard: tasks WITHOUT a brief are never touched — legacy behavior is
byte-identical. Liveness is judged on agent_runtimes (the daemon heartbeat,
~15s cadence); tasks without an assigned runtime are skipped (they belong
to the legacy claim flow that PresenceController already supervises).

Bookkeeping lives in tasks.orch_json:
    {respawns, last_respawn_at, accepted_at, report_invalid, halted}
All transitions are idempotent — re-running reconcile() converges.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

import aiosqlite
from pydantic import ValidationError

from krewhub.controllers.base import BaseController
from krewhub.models import ActorType, EventType, TaskStatus, WatchEventType
from krewhub.repositories.task_repo import TaskRepo
from krewhub.watch.service import WatchService
from krewhub.watch.types import WatchOptions

logger = logging.getLogger(__name__)

_ORCH_ACTOR = "orch"


class OrchController(BaseController):
    """Level-triggered reconciler + event-driven fast path for Brief-managed
    tasks. Safe to restart at any time (state is re-read from the DB)."""

    def __init__(
        self,
        db: aiosqlite.Connection,
        watch: WatchService,
        *,
        interval: float = 5.0,
        liveness_timeout: float = 60.0,
        max_respawns: int = 3,
    ) -> None:
        super().__init__(db, watch, interval=interval)
        self._liveness_timeout = liveness_timeout
        self._max_respawns = max_respawns
        self._watch_queue: asyncio.Queue | None = None
        self._watch_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Lifecycle: base reconcile loop + a watch consumer for the fast path
    # ------------------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        self._watch_queue = self._watch.subscribe(
            WatchOptions(resource_type="task"),
        )
        self._watch_task = asyncio.create_task(
            self._consume_watch(), name="controller:OrchController:watch",
        )

    async def stop(self) -> None:
        if self._watch_task is not None:
            self._watch_task.cancel()
            try:
                await self._watch_task
            except asyncio.CancelledError:
                pass
            self._watch_task = None
        if self._watch_queue is not None:
            self._watch.unsubscribe(self._watch_queue)
            self._watch_queue = None
        await super().stop()

    async def _consume_watch(self) -> None:
        """Event-driven fast path: a task change triggers an immediate
        targeted reconcile instead of waiting for the next tick. Purely an
        accelerator — reconcile() remains the correctness backstop."""
        assert self._watch_queue is not None
        while True:
            try:
                event = await self._watch_queue.get()
            except asyncio.CancelledError:
                break
            try:
                payload = event.object or {}
                # Only Brief-managed tasks are ours.
                if isinstance(payload, dict) and payload.get("brief"):
                    await self._reconcile_task_id(event.resource_id)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception(
                    "OrchController: watch fast-path failed for %s",
                    getattr(event, "resource_id", "?"),
                )

    # ------------------------------------------------------------------
    # Reconcile
    # ------------------------------------------------------------------

    async def reconcile(self) -> None:
        cursor = await self._db.execute(
            """SELECT id FROM tasks
               WHERE brief_json IS NOT NULL
                 AND status NOT IN ('cancelled')""",
        )
        rows = await cursor.fetchall()
        for row in rows:
            await self._reconcile_task_id(row["id"])

    async def _reconcile_task_id(self, task_id: str) -> None:
        task = await TaskRepo(self._db).get(task_id)
        if task is None or task.brief is None:
            return

        orch = dict(task.orch or {})
        if orch.get("accepted_at") or orch.get("halted"):
            return  # Retired or escalated — nothing left to converge.

        if task.status == TaskStatus.DONE:
            await self._handle_done(task, orch)
        elif task.status in (TaskStatus.CLAIMED, TaskStatus.WORKING):
            await self._check_liveness(task, orch)
        # open: TaskDispatchController owns dispatch. blocked /
        # blocked_on_review: owned by HITL / the review gate.

    # ------------------------------------------------------------------
    # completion -> validate(Report) -> retire | escalate
    # ------------------------------------------------------------------

    async def _handle_done(self, task, orch: dict) -> None:
        from krewhub.routes.schemas import Report

        report_ok = False
        if task.report is not None:
            try:
                Report.model_validate(task.report)
                report_ok = True
            except ValidationError:
                report_ok = False

        if report_ok:
            orch["accepted_at"] = datetime.now(timezone.utc).isoformat()
            await self._write_orch(task.id, orch)
            prs = (task.report or {}).get("prs") or []
            await self._emit(
                task,
                EventType.MILESTONE,
                "orch: Report accepted — worker retired"
                + (f" ({len(prs)} PR(s))" if prs else ""),
                {"orch": "accepted", "report": task.report},
            )
            logger.info("OrchController: accepted report for task %s", task.id)
            return

        if not orch.get("report_invalid"):
            orch["report_invalid"] = True
            await self._write_orch(task.id, orch)
            await self._emit(
                task,
                EventType.NEEDS_HUMAN,
                "orch: task completed without a valid Report "
                "(missing or schema-invalid) — needs human acceptance",
                {"orch": "report_invalid"},
            )
            logger.warning(
                "OrchController: task %s done without valid report", task.id,
            )

    # ------------------------------------------------------------------
    # liveness_lost -> respawn(Brief replay) | park after cap
    # ------------------------------------------------------------------

    async def _check_liveness(self, task, orch: dict) -> None:
        runtime_id = task.assigned_runtime_id
        if runtime_id is None:
            return  # Legacy claim flow — PresenceController's beat.

        cursor = await self._db.execute(
            "SELECT status, last_seen_at FROM agent_runtimes WHERE id = ?",
            (runtime_id,),
        )
        row = await cursor.fetchone()

        alive = False
        if row is not None and row["status"] != "offline":
            try:
                last_seen = datetime.fromisoformat(row["last_seen_at"])
                if last_seen.tzinfo is None:
                    last_seen = last_seen.replace(tzinfo=timezone.utc)
                cutoff = datetime.now(timezone.utc) - timedelta(
                    seconds=self._liveness_timeout,
                )
                alive = last_seen >= cutoff
            except (TypeError, ValueError):
                alive = False

        if alive:
            return

        respawns = int(orch.get("respawns", 0))
        if respawns >= self._max_respawns:
            orch["halted"] = True
            await self._write_orch(task.id, orch)
            updated = await TaskRepo(self._db).update(
                task.id,
                status=TaskStatus.BLOCKED,
                blocked_reason=(
                    f"orch: respawn limit reached ({respawns}/"
                    f"{self._max_respawns}) — needs root-cause review"
                ),
            )
            if updated is not None:
                await self._watch.record_resource(
                    "task", task.id, WatchEventType.MODIFIED, updated,
                )
            await self._emit(
                task,
                EventType.BLOCKER,
                f"orch: worker died {respawns + 1}x; respawn limit "
                f"({self._max_respawns}) reached — parked for root-cause review",
                {"orch": "respawn_limit", "respawns": respawns},
            )
            logger.warning(
                "OrchController: task %s hit respawn limit (%d)",
                task.id, respawns,
            )
            return

        # Respawn: reopen with claim cleared. brief_json stays on the row,
        # so TaskDispatchController's next push replays the Brief verbatim.
        orch["respawns"] = respawns + 1
        orch["last_respawn_at"] = datetime.now(timezone.utc).isoformat()
        await self._write_orch(task.id, orch)
        reopened = await TaskRepo(self._db).reopen_for_rerun(task.id)
        if reopened is not None:
            await self._watch.record_resource(
                "task", task.id, WatchEventType.MODIFIED, reopened,
            )
        await self._emit(
            task,
            EventType.LOG,
            f"orch: liveness lost on runtime {runtime_id}; respawned with "
            f"Brief replay (attempt {respawns + 1}/{self._max_respawns})",
            {"orch": "respawn", "runtime_id": runtime_id,
             "attempt": respawns + 1},
        )
        logger.info(
            "OrchController: respawned task %s (attempt %d/%d, runtime %s)",
            task.id, respawns + 1, self._max_respawns, runtime_id,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _write_orch(self, task_id: str, orch: dict) -> None:
        await self._db.execute(
            "UPDATE tasks SET orch_json = ?, "
            "resource_version = resource_version + 1 WHERE id = ?",
            (json.dumps(orch), task_id),
        )
        await self._db.commit()

    async def _emit(
        self, task, event_type: EventType, body: str, payload: dict,
    ) -> None:
        """Append an orch system event to the task tape (best-effort —
        the state transition is authoritative, the event is narration)."""
        from krewhub.services.task_service import TaskService

        try:
            await TaskService(self._db, self._watch).post_event(
                task_id=task.id,
                event_type=event_type,
                actor_id=_ORCH_ACTOR,
                actor_type=ActorType.SYSTEM,
                body=body,
                payload=payload,
                facts=[],
                code_refs=[],
            )
        except Exception:
            logger.exception(
                "OrchController: failed to emit %s event for task %s",
                event_type, task.id,
            )
