"""Link reconciler — the mechanical link pipeline (design §5.3), always-on.

This owns the *firing* of task_links — pure, idempotent plumbing that is
deliberately decoupled from the orch brain:

  pipe      when A completes, render A's output and inject it into B's
            prompt (the dep added at link-creation gates B until then).
  subagent  when delegate B completes, project its Report back onto the
            delegator A's tape as a human-turn event.

S2 hole #3 (Notes 1.2 §A.1): pipe firing used to live inside
OrchController, which only starts when ``KREWHUB_ORCH_ENABLED=1``. A
human's shift-drag pipe link then *persisted but never fired* with the
flag off — "manual-link == orchestration" silently broke. Firing is
mechanical, not a decision, so it lives here and runs regardless of the
orch flag. OrchController keeps the *decisions* (Report accept/escalate,
liveness respawn).

Idempotent: ``fired_at`` is the one-shot marker; a crash between steps
re-converges on the next tick. Safe to run alongside any other
controller — each link fires exactly once.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import aiosqlite

from krewhub.controllers.base import BaseController
from krewhub.models import ActorType, EventType, TaskStatus, WatchEventType
from krewhub.repositories.task_repo import TaskRepo
from krewhub.watch.service import WatchService

logger = logging.getLogger(__name__)

_ORCH_ACTOR = "orch"


class LinkReconcileController(BaseController):
    """Fires pipe + subagent links one-shot. Flag-independent: always
    registered by ControllerManager so manual pipes drive even when the
    orch reconciler is disabled (S2 B3)."""

    def __init__(
        self,
        db: aiosqlite.Connection,
        watch: WatchService,
        *,
        interval: float = 5.0,
    ) -> None:
        super().__init__(db, watch, interval=interval)

    async def reconcile(self) -> None:
        """One pass over unfired, unrevoked links whose endpoints have
        reached the firing condition."""
        cursor = await self._db.execute(
            "SELECT * FROM task_links WHERE fired_at IS NULL AND revoked_at IS NULL",
        )
        for row in await cursor.fetchall():
            try:
                if row["kind"] == "pipe":
                    await self._maybe_fire_pipe(row)
                elif row["kind"] == "subagent":
                    await self._maybe_flow_subagent_report(row)
            except Exception:
                logger.exception(
                    "LinkReconcileController: link %s reconcile failed", row["id"],
                )

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    async def _upstream_output(self, task, payload_map: dict) -> str | None:
        """Render the upstream task's output per payload_map.source.
        report (default): the structured O1 Report, compactly rendered.
        last_reply: the task's final agent_reply event body."""
        source = (payload_map or {}).get("source", "report")
        if source == "report" and task.report:
            rep = task.report
            parts = [f"status: {rep.get('status', '?')}"]
            for key in ("prs", "artifacts", "blockers", "decisions_needed"):
                vals = rep.get(key) or []
                if vals:
                    parts.append(f"{key}: " + ", ".join(str(v) for v in vals))
            return "\n".join(parts)
        # last_reply (or report-less fallback): newest agent_reply body.
        cursor = await self._db.execute(
            "SELECT body, payload FROM events WHERE task_id = ? "
            "AND type = 'agent_reply' ORDER BY sequence DESC LIMIT 1",
            (task.id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        body = row["body"] or ""
        try:
            payload = json.loads(row["payload"] or "{}")
            text = payload.get("text")
            if isinstance(text, str) and text.strip():
                body = text
        except Exception:
            pass
        return body or None

    # ------------------------------------------------------------------
    # pipe firing (send --text, API-fied)
    # ------------------------------------------------------------------

    async def _maybe_fire_pipe(self, link) -> None:
        """pipe: when A completes (orch-accepted if Brief-managed), render
        A's output and inject it into B's input — the dep added at link
        creation then lets B dispatch with the upstream context aboard."""
        repo = TaskRepo(self._db)
        upstream = await repo.get(link["from_task_id"])
        if upstream is None or upstream.status != TaskStatus.DONE:
            return
        # Brief-managed upstream: fire on orch acceptance, so the payload
        # is a validated Report, not an unvetted completion.
        if upstream.brief is not None and not (upstream.orch or {}).get("accepted_at"):
            return

        downstream = await repo.get(link["to_task_id"])
        if downstream is None or downstream.status in (
            TaskStatus.DONE, TaskStatus.CANCELLED,
        ):
            # Downstream gone/terminal — mark fired so we stop retrying.
            await self._mark_link_fired(link["id"])
            return

        payload_map = json.loads(link["payload_map"] or "{}")
        content = await self._upstream_output(upstream, payload_map)
        if content is None:
            content = f"(upstream task {upstream.id} completed with no rendered output)"

        header = f"[UPSTREAM OUTPUT · {upstream.id} via link {link['id']}]"
        block = f"{header}\n{content}"

        target = payload_map.get("target", "followup")
        if target == "brief_context" and downstream.brief is not None:
            brief = dict(downstream.brief)
            brief["context"] = ((brief.get("context") or "") + "\n\n" + block).strip()
            await self._db.execute(
                "UPDATE tasks SET brief_json = ?, "
                "resource_version = resource_version + 1 WHERE id = ?",
                (json.dumps(brief), downstream.id),
            )
        else:
            # followup (default): append to the description — krewcli's
            # prompt builder concatenates title + description, so the
            # upstream output rides into B's next prompt.
            base = (downstream.description or "").rstrip()
            await self._db.execute(
                "UPDATE tasks SET description = ?, "
                "resource_version = resource_version + 1 WHERE id = ?",
                ((base + "\n\n" + block).strip(), downstream.id),
            )
        await self._mark_link_fired(link["id"])

        fresh = await repo.get(downstream.id)
        if fresh is not None:
            await self._watch.record_resource(
                "task", downstream.id, WatchEventType.MODIFIED, fresh,
            )
            await self._emit(
                fresh,
                EventType.LOG,
                f"orch: pipe {link['id']} fired — output of {upstream.id} "
                "injected into this task's prompt",
                {"orch": "pipe_fired", "link_id": link["id"],
                 "from_task": upstream.id},
            )
        logger.info(
            "LinkReconcileController: pipe %s fired %s -> %s",
            link["id"], upstream.id, downstream.id,
        )

    # ------------------------------------------------------------------
    # subagent up-flow (Report back onto the delegator's tape)
    # ------------------------------------------------------------------

    async def _maybe_flow_subagent_report(self, link) -> None:
        """subagent: when the delegate B completes, project its Report back
        onto the delegator A's tape as a human-turn event — the same
        projection convention invocations use (`delegate_answer`), so A's
        brain threads it on its next prompt build."""
        repo = TaskRepo(self._db)
        child = await repo.get(link["to_task_id"])
        if child is None:
            await self._mark_link_fired(link["id"])
            return
        if child.status != TaskStatus.DONE:
            return
        # Brief-managed child: wait for orch acceptance (validated Report).
        if child.brief is not None and not (child.orch or {}).get("accepted_at"):
            return

        parent = await repo.get(link["from_task_id"])
        if parent is None or parent.status in (TaskStatus.CANCELLED,):
            await self._mark_link_fired(link["id"])
            return

        content = await self._upstream_output(
            child, json.loads(link["payload_map"] or "{}"),
        )
        if content is None:
            content = f"(subagent task {child.id} completed with no rendered output)"

        # Project onto A's tape with the established followup convention
        # (type=agent_reply + actor_type=human) so A's prompt-builder
        # threads it as an input turn; payload.kind discriminates for UI.
        from uuid import uuid4
        cursor = await self._db.execute(
            "SELECT b.cookbook_id FROM tasks t JOIN bundles b "
            "ON b.id = t.bundle_id WHERE t.id = ?",
            (parent.id,),
        )
        row = await cursor.fetchone()
        cookbook_id = row["cookbook_id"] if row else None
        seq_row = await self._db.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM events WHERE task_id = ?",
            (parent.id,),
        )
        seq = (await seq_row.fetchone())[0] or 1
        body = f"[SUBAGENT REPORT · {child.id} via link {link['id']}]\n{content}"
        await self._db.execute(
            "INSERT INTO events (id, cookbook_id, bundle_id, task_id, type, "
            "actor_id, actor_type, body, payload, sequence, facts, code_refs, "
            "visibility, created_at) "
            "VALUES (?, ?, ?, ?, 'agent_reply', ?, 'human', ?, ?, ?, "
            "'[]', '[]', 'user', ?)",
            (f"evt_{uuid4().hex[:12]}", cookbook_id, parent.bundle_id,
             parent.id, _ORCH_ACTOR, body,
             json.dumps({
                 "kind": "subagent_report", "link_id": link["id"],
                 "from_task": child.id, "report": child.report,
             }),
             seq, datetime.now(timezone.utc).isoformat()),
        )
        await self._mark_link_fired(link["id"])

        fresh = await repo.get(parent.id)
        if fresh is not None:
            await self._watch.record_resource(
                "task", parent.id, WatchEventType.MODIFIED, fresh,
            )
        logger.info(
            "LinkReconcileController: subagent report flowed %s -> %s (link %s)",
            child.id, parent.id, link["id"],
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _mark_link_fired(self, link_id: str) -> None:
        await self._db.execute(
            "UPDATE task_links SET fired_at = ? WHERE id = ?",
            (datetime.now(timezone.utc).isoformat(), link_id),
        )
        await self._db.commit()

    async def _emit(
        self, task, event_type: EventType, body: str, payload: dict,
    ) -> None:
        """Append a link system event to the task tape (best-effort — the
        state transition is authoritative, the event is narration)."""
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
                "LinkReconcileController: failed to emit %s event for task %s",
                event_type, task.id,
            )
