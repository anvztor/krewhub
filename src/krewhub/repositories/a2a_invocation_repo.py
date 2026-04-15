from __future__ import annotations

import json
from datetime import datetime, timezone

import aiosqlite


class A2AInvocationRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(
        self,
        id: str,
        owner: str,
        agent_name: str,
        method: str,
        params_json: str,
        caller_id: str | None,
        expires_at: datetime,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "INSERT INTO a2a_invocations "
            "(id, owner, agent_name, method, params, caller_id, status, created_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
            (id, owner, agent_name, method, params_json, caller_id, now, expires_at.isoformat()),
        )
        await self._db.commit()

    async def get(self, id: str) -> dict | None:
        cursor = await self._db.execute(
            "SELECT * FROM a2a_invocations WHERE id = ?", (id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return dict(row)

    async def update_status(
        self,
        id: str,
        status: str,
        *,
        result: str | None = None,
        error: str | None = None,
    ) -> int:
        """Update invocation status. Returns rowcount (0 if no matching row)."""
        parts = ["status = ?"]
        params: list[object] = [status]

        if result is not None:
            parts.append("result = ?")
            params.append(result)

        if error is not None:
            parts.append("error = ?")
            params.append(error)

        if status in ("completed", "failed"):
            parts.append("completed_at = ?")
            params.append(datetime.now(timezone.utc).isoformat())

        params.append(id)

        cursor = await self._db.execute(
            f"UPDATE a2a_invocations SET {', '.join(parts)} "
            f"WHERE id = ? AND status IN ('pending', 'processing')",
            params,
        )
        await self._db.commit()
        return cursor.rowcount

    async def mark_timeout(self, id: str) -> int:
        """Mark a pending invocation as timed out. Returns rowcount."""
        cursor = await self._db.execute(
            "UPDATE a2a_invocations SET status = 'timeout' WHERE id = ? AND status = 'pending'",
            (id,),
        )
        await self._db.commit()
        return cursor.rowcount

    async def list_pending(self, owner: str, agent_name: str) -> list[dict]:
        now = datetime.now(timezone.utc).isoformat()
        cursor = await self._db.execute(
            "SELECT id, method, params, caller_id, created_at FROM a2a_invocations "
            "WHERE owner = ? AND agent_name = ? AND status = 'pending' AND expires_at > ? "
            "ORDER BY created_at ASC",
            (owner, agent_name, now),
        )
        rows = await cursor.fetchall()
        return [
            {
                "invocation_id": r["id"],
                "method": r["method"],
                "params": json.loads(r["params"]),
                "caller_id": r["caller_id"],
                "created_at": r["created_at"],
            }
            for r in rows
        ]
