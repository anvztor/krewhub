"""Sandbox repository — CRUD on the sandboxes table."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import aiosqlite

from krewhub.models.sandbox import Sandbox


def _row_to_sandbox(row: aiosqlite.Row) -> Sandbox:
    return Sandbox(
        id=row["id"],
        task_id=row["task_id"],
        owner_account_id=row["owner_account_id"],
        e2b_sandbox_id=row["e2b_sandbox_id"],
        template=row["template"],
        status=row["status"],
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        terminated_at=(
            datetime.fromisoformat(row["terminated_at"]) if row["terminated_at"] else None
        ),
        last_event_at=(
            datetime.fromisoformat(row["last_event_at"]) if row["last_event_at"] else None
        ),
    )


class SandboxRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(self, sb: Sandbox) -> None:
        await self._db.execute(
            "INSERT INTO sandboxes (id, task_id, owner_account_id, e2b_sandbox_id, "
            "template, status, created_at, updated_at, terminated_at, last_event_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                sb.id,
                sb.task_id,
                sb.owner_account_id,
                sb.e2b_sandbox_id,
                sb.template,
                sb.status,
                sb.created_at.isoformat(),
                sb.updated_at.isoformat(),
                sb.terminated_at.isoformat() if sb.terminated_at else None,
                sb.last_event_at.isoformat() if sb.last_event_at else None,
            ),
        )
        await self._db.commit()

    async def get(self, sandbox_id: str) -> Sandbox | None:
        cursor = await self._db.execute(
            "SELECT * FROM sandboxes WHERE id = ?", (sandbox_id,)
        )
        row = await cursor.fetchone()
        return _row_to_sandbox(row) if row else None

    async def update_status(self, sandbox_id: str, status: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        terminated = None
        if status == "terminated":
            terminated = now
        await self._db.execute(
            "UPDATE sandboxes SET status = ?, updated_at = ?, "
            "terminated_at = COALESCE(?, terminated_at) WHERE id = ?",
            (status, now, terminated, sandbox_id),
        )
        await self._db.commit()

    async def mark_event(self, sandbox_id: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "UPDATE sandboxes SET last_event_at = ?, updated_at = ? WHERE id = ?",
            (now, now, sandbox_id),
        )
        await self._db.commit()

    async def list_idle_or_expired(
        self, *, idle_seconds: int, max_age_seconds: int,
    ) -> list[Sandbox]:
        """Return sandboxes that are sweep-eligible.

        A sandbox is eligible when it is in a non-terminal state
        (provisioning/ready/running) AND either:
          - last_event_at is older than idle_seconds, OR
          - created_at is older than max_age_seconds.
        """
        now = datetime.now(timezone.utc)
        idle_cutoff = (now - timedelta(seconds=idle_seconds)).isoformat()
        max_age_cutoff = (now - timedelta(seconds=max_age_seconds)).isoformat()
        cursor = await self._db.execute(
            "SELECT * FROM sandboxes "
            "WHERE status IN ('ready','running','provisioning') "
            "AND ((last_event_at IS NOT NULL AND last_event_at < ?) "
            "OR created_at < ?)",
            (idle_cutoff, max_age_cutoff),
        )
        rows = await cursor.fetchall()
        return [_row_to_sandbox(r) for r in rows]

    async def list_by_task(self, task_id: str) -> list[Sandbox]:
        cursor = await self._db.execute(
            "SELECT * FROM sandboxes WHERE task_id = ? ORDER BY rowid",
            (task_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_sandbox(r) for r in rows]
