from __future__ import annotations

import json
from datetime import datetime

import aiosqlite

from krewhub.models import Task, TaskStatus


class TaskRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(self, task: Task) -> Task:
        await self._db.execute(
            """INSERT INTO tasks
               (id, bundle_id, title, description, status, depends_on_task_ids,
                claimed_by_agent_id, claimed_at, completed_at, blocked_reason)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (task.id, task.bundle_id, task.title, task.description, task.status,
             json.dumps(task.depends_on_task_ids),
             task.claimed_by_agent_id,
             task.claimed_at.isoformat() if task.claimed_at else None,
             task.completed_at.isoformat() if task.completed_at else None,
             task.blocked_reason),
        )
        await self._db.commit()
        return task

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

    async def list_open_by_recipe(self, recipe_id: str) -> list[Task]:
        cursor = await self._db.execute(
            """SELECT t.* FROM tasks t
               JOIN bundles b ON t.bundle_id = b.id
               WHERE b.recipe_id = ? AND t.status = 'open'
               ORDER BY t.rowid""",
            (recipe_id,),
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
        claimed_by_agent_id: str | None = None,
        claimed_at: datetime | None = None,
        completed_at: datetime | None = None,
        blocked_reason: str | None = None,
        depends_on_task_ids: list[str] | None = None,
    ) -> Task | None:
        parts: list[str] = []
        params: list[object] = []

        if title is not None:
            parts.append("title = ?")
            params.append(title)
        if description is not None:
            parts.append("description = ?")
            params.append(description)
        if status is not None:
            parts.append("status = ?")
            params.append(status)
        if claimed_by_agent_id is not None:
            parts.append("claimed_by_agent_id = ?")
            params.append(claimed_by_agent_id)
        if claimed_at is not None:
            parts.append("claimed_at = ?")
            params.append(claimed_at.isoformat())
        if completed_at is not None:
            parts.append("completed_at = ?")
            params.append(completed_at.isoformat())
        if blocked_reason is not None:
            parts.append("blocked_reason = ?")
            params.append(blocked_reason)
        if depends_on_task_ids is not None:
            parts.append("depends_on_task_ids = ?")
            params.append(json.dumps(depends_on_task_ids))

        if not parts:
            return await self.get(task_id)

        params.append(task_id)
        await self._db.execute(
            f"UPDATE tasks SET {', '.join(parts)} WHERE id = ?",
            params,
        )
        await self._db.commit()
        return await self.get(task_id)

    async def delete(self, task_id: str) -> bool:
        cursor = await self._db.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        await self._db.commit()
        return cursor.rowcount > 0


def _row_to_task(row: aiosqlite.Row) -> Task:
    return Task(
        id=row["id"],
        bundle_id=row["bundle_id"],
        title=row["title"],
        description=row["description"],
        status=row["status"],
        depends_on_task_ids=json.loads(row["depends_on_task_ids"]),
        claimed_by_agent_id=row["claimed_by_agent_id"],
        claimed_at=datetime.fromisoformat(row["claimed_at"]) if row["claimed_at"] else None,
        completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
        blocked_reason=row["blocked_reason"],
    )
