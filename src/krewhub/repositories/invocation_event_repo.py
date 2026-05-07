"""Append-only writer for invocation_events (Invocation Contract slice 1).

Events are addressed by `(tape_id, id)` with `id` monotonic per tape and
allocated server-side. The repo serializes appends per tape via an
in-process lock so two concurrent writes can't conflict on `id`.
"""
from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from datetime import datetime, timezone

import aiosqlite

from krewhub.models.invocation import ActorType, Event, EventKind


# One asyncio.Lock per tape_id. The set is per-process, which is fine for
# single-server krewhub. Multi-server deployments would need a DB-side
# lock (or SELECT MAX(id) ... INSERT in one transaction with retry).
_tape_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


class InvocationEventRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def append(
        self,
        tape_id: str,
        kind: EventKind,
        *,
        body: str = "",
        payload: dict | None = None,
        actor_type: ActorType = "system",
        actor_id: str = "",
        parent_id: int | None = None,
        fork_id: str | None = None,
    ) -> Event:
        lock = _tape_locks[tape_id]
        async with lock:
            cursor = await self._db.execute(
                "SELECT COALESCE(MAX(id), -1) + 1 FROM invocation_events WHERE tape_id = ?",
                (tape_id,),
            )
            (next_id,) = await cursor.fetchone()  # type: ignore[misc]
            ts = datetime.now(timezone.utc)
            await self._db.execute(
                """INSERT INTO invocation_events
                   (tape_id, id, parent_id, fork_id, actor_type, actor_id,
                    kind, body, payload_json, ts)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    tape_id, next_id, parent_id, fork_id,
                    actor_type, actor_id, kind, body,
                    json.dumps(payload or {}), ts.isoformat(),
                ),
            )
            await self._db.commit()
        return Event(
            tape_id=tape_id,
            id=next_id,
            parent_id=parent_id,
            fork_id=fork_id,
            actor_type=actor_type,
            actor_id=actor_id,
            kind=kind,
            body=body,
            payload=payload or {},
            ts=ts,
        )

    async def list_for_tape(
        self,
        tape_id: str,
        *,
        after: int | None = None,
        limit: int | None = None,
    ) -> list[Event]:
        sql = "SELECT * FROM invocation_events WHERE tape_id = ?"
        args: list = [tape_id]
        if after is not None:
            sql += " AND id > ?"
            args.append(after)
        sql += " ORDER BY id ASC"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        cursor = await self._db.execute(sql, args)
        rows = await cursor.fetchall()
        return [_row_to_event(r) for r in rows]

    async def latest_id(self, tape_id: str) -> int:
        cursor = await self._db.execute(
            "SELECT MAX(id) FROM invocation_events WHERE tape_id = ?",
            (tape_id,),
        )
        row = await cursor.fetchone()
        if not row or row[0] is None:
            return -1
        return row[0]

    async def register_fork(
        self,
        child_tape_id: str,
        parent_tape_id: str,
        fork_point_event_id: int,
    ) -> None:
        await self._db.execute(
            """INSERT OR IGNORE INTO tape_forks
               (child_tape_id, parent_tape_id, fork_point_event_id, created_at)
               VALUES (?, ?, ?, ?)""",
            (
                child_tape_id, parent_tape_id, fork_point_event_id,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        await self._db.commit()


def _row_to_event(row) -> Event:
    return Event(
        tape_id=row["tape_id"],
        id=row["id"],
        parent_id=row["parent_id"],
        fork_id=row["fork_id"],
        actor_type=row["actor_type"],
        actor_id=row["actor_id"],
        kind=row["kind"],
        body=row["body"],
        payload=json.loads(row["payload_json"]),
        ts=datetime.fromisoformat(row["ts"]),
    )
