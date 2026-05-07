"""CRUD on the `invocations` table (Invocation Contract slice 1)."""
from __future__ import annotations

import json
from datetime import datetime

import aiosqlite

from krewhub.models.invocation import (
    Invocation,
    InvocationStatus,
    ResultEnvelope,
)


class InvocationRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(self, inv: Invocation) -> Invocation:
        await self._db.execute(
            """INSERT INTO invocations
               (id, target_type, target_id, input_json, schema_json, deadline_s,
                label, parent_tape_id, parent_fork_point, idempotency_key,
                tape_id, status, result_json, created_at, started_at,
                completed_at, created_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                inv.id, inv.target_type, inv.target_id,
                json.dumps(inv.input), json.dumps(inv.schema) if inv.schema else None,
                inv.deadline_s, inv.label,
                inv.parent_tape_id, inv.parent_fork_point, inv.idempotency_key,
                inv.tape_id, inv.status,
                inv.result.model_dump_json() if inv.result else None,
                inv.created_at.isoformat(),
                inv.started_at.isoformat() if inv.started_at else None,
                inv.completed_at.isoformat() if inv.completed_at else None,
                inv.created_by,
            ),
        )
        await self._db.commit()
        return inv

    async def get(self, invocation_id: str) -> Invocation | None:
        cursor = await self._db.execute(
            "SELECT * FROM invocations WHERE id = ?", (invocation_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_invocation(row)

    async def find_by_idempotency_key(
        self, parent_tape_id: str, idempotency_key: str,
    ) -> Invocation | None:
        cursor = await self._db.execute(
            """SELECT * FROM invocations
               WHERE parent_tape_id = ? AND idempotency_key = ?""",
            (parent_tape_id, idempotency_key),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_invocation(row)

    async def set_running(
        self, invocation_id: str, started_at: datetime,
    ) -> None:
        await self._db.execute(
            """UPDATE invocations SET status='running', started_at=?
               WHERE id=? AND status='pending'""",
            (started_at.isoformat(), invocation_id),
        )
        await self._db.commit()

    async def set_terminal(
        self,
        invocation_id: str,
        status: InvocationStatus,
        result: ResultEnvelope,
        completed_at: datetime,
    ) -> None:
        await self._db.execute(
            """UPDATE invocations
               SET status=?, result_json=?, completed_at=?
               WHERE id=? AND status NOT IN ('completed','cancelled','errored')""",
            (
                status, result.model_dump_json(),
                completed_at.isoformat(), invocation_id,
            ),
        )
        await self._db.commit()


def _row_to_invocation(row) -> Invocation:
    return Invocation(
        id=row["id"],
        target_type=row["target_type"],
        target_id=row["target_id"],
        input=json.loads(row["input_json"]),
        schema=json.loads(row["schema_json"]) if row["schema_json"] else None,
        deadline_s=row["deadline_s"],
        label=row["label"],
        parent_tape_id=row["parent_tape_id"],
        parent_fork_point=row["parent_fork_point"],
        idempotency_key=row["idempotency_key"],
        tape_id=row["tape_id"],
        status=row["status"],
        result=ResultEnvelope.model_validate_json(row["result_json"])
        if row["result_json"] else None,
        created_at=datetime.fromisoformat(row["created_at"]),
        started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
        completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
        created_by=row["created_by"],
    )
