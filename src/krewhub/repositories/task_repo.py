from __future__ import annotations

import json
from datetime import datetime

import aiosqlite

from krewhub.models import Task, TaskStatus
from krewhub.repositories.bundle_repo import StaleResourceError


class TaskRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(self, task: Task) -> Task:
        from datetime import timezone
        now_iso = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO tasks
               (id, bundle_id, title, description, status, depends_on_task_ids,
                assigned_agent_id, claimed_by_agent_id, claimed_at, completed_at,
                blocked_reason, graph_node_id, resource_version, generation,
                updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (task.id, task.bundle_id, task.title, task.description, task.status,
             json.dumps(task.depends_on_task_ids),
             task.assigned_agent_id,
             task.claimed_by_agent_id,
             task.claimed_at.isoformat() if task.claimed_at else None,
             task.completed_at.isoformat() if task.completed_at else None,
             task.blocked_reason,
             task.graph_node_id,
             task.resource_version, task.generation,
             now_iso),
        )
        await self._db.commit()
        return task.model_copy(update={
            "updated_at": datetime.fromisoformat(now_iso),
        })

    async def create_many(self, tasks: list[Task]) -> list[Task]:
        for task in tasks:
            await self.create(task)
        return tasks

    async def get(self, task_id: str) -> Task | None:
        cursor = await self._db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
        row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_task(row)

    async def list_by_bundle(self, bundle_id: str) -> list[Task]:
        cursor = await self._db.execute(
            "SELECT * FROM tasks WHERE bundle_id = ? ORDER BY rowid",
            (bundle_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_task(r) for r in rows]

    async def list_open_by_cookbook(self, cookbook_id: str) -> list[Task]:
        """Return open tasks eligible for legacy direct dispatch.

        Tasks with ``graph_node_id`` set are *excluded* because they are
        owned by ``GraphRunnerController``.
        """
        cursor = await self._db.execute(
            """SELECT t.* FROM tasks t
               JOIN bundles b ON t.bundle_id = b.id
               WHERE b.cookbook_id = ?
                 AND t.status = 'open'
                 AND t.graph_node_id IS NULL
               ORDER BY t.rowid""",
            (cookbook_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_task(r) for r in rows]

    async def list_active_by_agent(
        self, cookbook_id: str | None, agent_id: str,
    ) -> list[Task]:
        """Active tasks an agent already holds in a given cookbook.

        Used by claim_task to enforce single-active-task per agent.
        cookbook_id=None matches the agent's active tasks globally.
        """
        if cookbook_id is None:
            cursor = await self._db.execute(
                """SELECT t.* FROM tasks t
                   WHERE t.claimed_by_agent_id = ?
                     AND t.status IN ('claimed', 'working')
                   ORDER BY t.rowid""",
                (agent_id,),
            )
        else:
            cursor = await self._db.execute(
                """SELECT t.* FROM tasks t
                   JOIN bundles b ON t.bundle_id = b.id
                   WHERE b.cookbook_id = ?
                     AND t.claimed_by_agent_id = ?
                     AND t.status IN ('claimed', 'working')
                   ORDER BY t.rowid""",
                (cookbook_id, agent_id),
            )
        rows = await cursor.fetchall()
        return [_row_to_task(r) for r in rows]

    async def list_assigned_unclaimed_by_agent(
        self, cookbook_id: str, agent_id: str,
    ) -> list[Task]:
        """Find tasks assigned to an agent but never claimed."""
        cursor = await self._db.execute(
            """SELECT t.* FROM tasks t
               JOIN bundles b ON t.bundle_id = b.id
               WHERE b.cookbook_id = ?
                 AND t.assigned_agent_id = ?
                 AND t.status = 'open'
                 AND t.claimed_by_agent_id IS NULL
               ORDER BY t.rowid""",
            (cookbook_id, agent_id),
        )
        rows = await cursor.fetchall()
        return [_row_to_task(r) for r in rows]

    async def update(
        self,
        task_id: str,
        *,
        title: str | None = None,
        description: str | None = None,
        status: TaskStatus | None = None,
        assigned_agent_id: str | None = None,
        claimed_by_agent_id: str | None = None,
        claimed_at: datetime | None = None,
        completed_at: datetime | None = None,
        blocked_reason: str | None = None,
        clear_blocked_reason: bool = False,
        clear_claim: bool = False,
        depends_on_task_ids: list[str] | None = None,
        expected_version: int | None = None,
    ) -> Task | None:
        parts: list[str] = ["resource_version = resource_version + 1"]
        params: list[object] = []

        # Track whether spec fields changed (for generation bump)
        spec_changed = (
            title is not None
            or description is not None
            or depends_on_task_ids is not None
            or assigned_agent_id is not None
        )

        if title is not None:
            parts.append("title = ?")
            params.append(title)
        if description is not None:
            parts.append("description = ?")
            params.append(description)
        if status is not None:
            parts.append("status = ?")
            params.append(status)
        if assigned_agent_id is not None:
            parts.append("assigned_agent_id = ?")
            params.append(assigned_agent_id)
        if clear_claim:
            # HITL retry path: reset claim columns to NULL so
            # TaskDispatchController re-dispatches on next reconcile.
            parts.append("claimed_by_agent_id = NULL")
            parts.append("claimed_at = NULL")
            parts.append("assigned_agent_id = NULL")
        if claimed_by_agent_id is not None:
            parts.append("claimed_by_agent_id = ?")
            params.append(claimed_by_agent_id)
        if claimed_at is not None:
            parts.append("claimed_at = ?")
            params.append(claimed_at.isoformat())
        if completed_at is not None:
            parts.append("completed_at = ?")
            params.append(completed_at.isoformat())
        if clear_blocked_reason:
            parts.append("blocked_reason = NULL")
        elif blocked_reason is not None:
            parts.append("blocked_reason = ?")
            params.append(blocked_reason)
        if depends_on_task_ids is not None:
            parts.append("depends_on_task_ids = ?")
            params.append(json.dumps(depends_on_task_ids))

        if spec_changed:
            parts.append("generation = generation + 1")

        if len(parts) == 1:
            # Only resource_version bump, nothing actually changed
            return await self.get(task_id)

        # Bundle lifecycle: bump updated_at on every real change so
        # MAX(tasks.updated_at) per bundle drives the frontend's
        # active/idle bucket.
        from datetime import timezone
        parts.append("updated_at = ?")
        params.append(datetime.now(timezone.utc).isoformat())

        where = "id = ?"
        params.append(task_id)

        if expected_version is not None:
            where += " AND resource_version = ?"
            params.append(expected_version)

        cursor = await self._db.execute(
            f"UPDATE tasks SET {', '.join(parts)} WHERE {where}",
            params,
        )
        await self._db.commit()

        if expected_version is not None and cursor.rowcount == 0:
            existing = await self.get(task_id)
            if existing is not None:
                raise StaleResourceError("task", task_id)
            return None

        return await self.get(task_id)

    async def delete(self, task_id: str) -> bool:
        cursor = await self._db.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        await self._db.commit()
        return cursor.rowcount > 0

    async def reopen_for_rerun(self, task_id: str) -> Task | None:
        from datetime import timezone
        await self._db.execute(
            """UPDATE tasks
               SET status = 'open',
                   assigned_agent_id = NULL,
                   claimed_by_agent_id = NULL,
                   claimed_at = NULL,
                   completed_at = NULL,
                   blocked_reason = NULL,
                   resource_version = resource_version + 1,
                   updated_at = ?
               WHERE id = ?""",
            (datetime.now(timezone.utc).isoformat(), task_id),
        )
        await self._db.commit()
        return await self.get(task_id)

    async def set_session_token(self, task_id: str, token: str) -> None:
        """Stamp session_token on a task (first-writer-wins)."""
        await self._db.execute(
            "UPDATE tasks SET session_token = ? WHERE id = ? AND session_token IS NULL",
            (token, task_id),
        )
        await self._db.commit()

    async def build_node_id_map(self, bundle_id: str) -> dict[str, str]:
        """Return {graph_node_id: task_id} for a bundle's graph-bound tasks.

        Used by GraphRunnerController to construct OrchestratorDeps.task_id_map
        before invoking dispatch_cycle. Tasks with NULL graph_node_id are
        skipped — they were created outside the graph flow and shouldn't be
        addressable from a graph step.
        """
        cursor = await self._db.execute(
            """SELECT graph_node_id, id FROM tasks
               WHERE bundle_id = ? AND graph_node_id IS NOT NULL""",
            (bundle_id,),
        )
        rows = await cursor.fetchall()
        return {row["graph_node_id"]: row["id"] for row in rows}


def _row_to_task(row: aiosqlite.Row) -> Task:
    # progress_json column may not exist on older DBs
    try:
        progress_raw = row["progress_json"]
        progress = json.loads(progress_raw) if progress_raw else None
    except (IndexError, KeyError):
        progress = None

    # Phase 4 M3 columns may not exist on older DBs
    try:
        session_id = row["session_id"]
    except (IndexError, KeyError):
        session_id = None
    try:
        work_dir = row["work_dir"]
    except (IndexError, KeyError):
        work_dir = None
    try:
        artifacts_raw = row["artifacts_json"]
        artifacts = json.loads(artifacts_raw) if artifacts_raw else {}
    except (IndexError, KeyError):
        artifacts = {}

    try:
        session_token = row["session_token"]
    except (IndexError, KeyError):
        session_token = None

    # Auth track A2: assigned_runtime_id + sandbox_id may not exist on older DBs
    try:
        assigned_runtime_id = row["assigned_runtime_id"]
    except (IndexError, KeyError):
        assigned_runtime_id = None
    try:
        sandbox_id = row["sandbox_id"]
    except (IndexError, KeyError):
        sandbox_id = None

    try:
        updated_at_raw = row["updated_at"]
        updated_at = datetime.fromisoformat(updated_at_raw) if updated_at_raw else None
    except (IndexError, KeyError):
        updated_at = None

    # Orch mode (O1): brief/report columns may not exist on older DBs.
    try:
        brief_raw = row["brief_json"]
        brief = json.loads(brief_raw) if brief_raw else None
    except (IndexError, KeyError):
        brief = None
    try:
        report_raw = row["report_json"]
        report = json.loads(report_raw) if report_raw else None
    except (IndexError, KeyError):
        report = None
    try:
        orch_raw = row["orch_json"]
        orch = json.loads(orch_raw) if orch_raw else None
    except (IndexError, KeyError):
        orch = None

    return Task(
        id=row["id"],
        bundle_id=row["bundle_id"],
        title=row["title"],
        description=row["description"],
        status=row["status"],
        depends_on_task_ids=json.loads(row["depends_on_task_ids"]),
        assigned_agent_id=row["assigned_agent_id"],
        claimed_by_agent_id=row["claimed_by_agent_id"],
        claimed_at=datetime.fromisoformat(row["claimed_at"]) if row["claimed_at"] else None,
        completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
        blocked_reason=row["blocked_reason"],
        graph_node_id=row["graph_node_id"] if "graph_node_id" in row.keys() else None,
        resource_version=row["resource_version"],
        generation=row["generation"],
        progress=progress,
        session_id=session_id,
        work_dir=work_dir,
        artifacts=artifacts,
        session_token=session_token,
        assigned_runtime_id=assigned_runtime_id,
        sandbox_id=sandbox_id,
        brief=brief,
        report=report,
        orch=orch,
        updated_at=updated_at,
    )
