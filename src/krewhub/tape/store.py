"""
Tape-inspired append-only fact store.

Follows the Tape protocol pattern from tape.systems:
- Append-only entries (immutable facts)
- Anchors as reconstruction checkpoints (digests)
- Chainable queries by tape name, kind, and date range

This is a self-contained implementation backed by SQLite.
When republic becomes available on PyPI, this can be replaced
with a TapeStore adapter wrapping the republic library.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import aiosqlite


@dataclass(frozen=True)
class TapeEntry:
    id: int
    kind: str
    payload: dict[str, Any]
    meta: dict[str, Any]
    date: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "payload": self.payload,
            "meta": self.meta,
            "date": self.date,
        }


class TapeStore:
    """SQLite-backed append-only tape store."""

    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def append(
        self,
        tape_name: str,
        kind: str,
        payload: dict[str, Any],
        meta: dict[str, Any] | None = None,
    ) -> TapeEntry:
        now = datetime.now(timezone.utc).isoformat()
        cursor = await self._db.execute(
            """INSERT INTO tape_entries (tape_name, kind, payload, meta, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (tape_name, kind, json.dumps(payload), json.dumps(meta or {}), now),
        )
        await self._db.commit()
        return TapeEntry(
            id=cursor.lastrowid,
            kind=kind,
            payload=payload,
            meta=meta or {},
            date=now,
        )

    async def fetch_all(
        self,
        tape_name: str,
        kinds: list[str] | None = None,
        after_id: int | None = None,
        limit: int | None = None,
    ) -> list[TapeEntry]:
        query = "SELECT * FROM tape_entries WHERE tape_name = ?"
        params: list[Any] = [tape_name]

        if kinds:
            placeholders = ", ".join("?" for _ in kinds)
            query += f" AND kind IN ({placeholders})"
            params.extend(kinds)

        if after_id is not None:
            query += " AND id > ?"
            params.append(after_id)

        query += " ORDER BY id ASC"

        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        cursor = await self._db.execute(query, params)
        rows = await cursor.fetchall()
        return [_row_to_entry(r) for r in rows]

    async def last_anchor(self, tape_name: str) -> TapeEntry | None:
        cursor = await self._db.execute(
            """SELECT * FROM tape_entries
               WHERE tape_name = ? AND kind = 'anchor'
               ORDER BY id DESC LIMIT 1""",
            (tape_name,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_entry(row)

    async def entries_after_anchor(
        self, tape_name: str, anchor_id: int
    ) -> list[TapeEntry]:
        return await self.fetch_all(tape_name, after_id=anchor_id)

    async def list_tapes(self) -> list[str]:
        cursor = await self._db.execute(
            "SELECT DISTINCT tape_name FROM tape_entries ORDER BY tape_name"
        )
        rows = await cursor.fetchall()
        return [row["tape_name"] for row in rows]


def _row_to_entry(row: aiosqlite.Row) -> TapeEntry:
    return TapeEntry(
        id=row["id"],
        kind=row["kind"],
        payload=json.loads(row["payload"]),
        meta=json.loads(row["meta"]),
        date=row["created_at"],
    )
