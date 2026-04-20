"""
Republic-backed append-only tape store.

Uses republic's TapeEntry and InMemoryQueryMixin for query logic
(anchor traversal, kind filtering, date ranges) while persisting
to SQLite via aiosqlite.

See: https://tape.systems — context as architecture.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import aiosqlite
from republic import TapeEntry, TapeQuery
from republic.tape import InMemoryQueryMixin


def entry_to_dict(entry: TapeEntry) -> dict[str, Any]:
    """Serialize a republic TapeEntry to a JSON-safe dict."""
    return asdict(entry)


class SqliteTapeStore(InMemoryQueryMixin):
    """SQLite-backed tape store using republic's InMemoryQueryMixin.

    Design: cache-hydrate pattern.
    - ``read()`` (sync, required by mixin) returns cached entries.
    - ``ensure_loaded()`` (async) hydrates the cache from SQLite.
    - ``append()`` (async) writes to SQLite AND updates the cache.

    This gives us republic's full TapeQuery capabilities for free
    without reimplementing anchor/kind/date filtering logic.
    """

    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db
        self._cache: dict[str, list[TapeEntry]] = {}
        self._loaded: set[str] = set()

    # ── sync read (required by InMemoryQueryMixin) ────────────────

    def read(self, tape: str) -> list[TapeEntry] | None:
        return self._cache.get(tape)

    # ── async hydration ───────────────────────────────────────────

    async def load(self, tape: str) -> None:
        """Hydrate the in-memory cache from SQLite for *tape*."""
        cursor = await self._db.execute(
            "SELECT * FROM tape_entries WHERE tape_name = ? ORDER BY id ASC",
            (tape,),
        )
        rows = await cursor.fetchall()
        self._cache[tape] = [_row_to_entry(r) for r in rows]
        self._loaded.add(tape)

    async def ensure_loaded(self, tape: str) -> None:
        if tape not in self._loaded:
            await self.load(tape)

    # ── async mutations ───────────────────────────────────────────

    async def append(self, tape: str, entry: TapeEntry) -> TapeEntry:
        """Append *entry* to *tape*, persist to SQLite, update cache."""
        now = entry.date if entry.date else datetime.now(timezone.utc).isoformat()
        cursor = await self._db.execute(
            """INSERT INTO tape_entries (tape_name, kind, payload, meta, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (tape, entry.kind, json.dumps(entry.payload), json.dumps(entry.meta), now),
        )
        await self._db.commit()
        stored = TapeEntry(
            id=cursor.lastrowid,
            kind=entry.kind,
            payload=entry.payload,
            meta=entry.meta,
            date=now,
        )
        self._cache.setdefault(tape, []).append(stored)
        self._loaded.add(tape)
        return stored

    async def reset(self, tape: str) -> None:
        await self._db.execute(
            "DELETE FROM tape_entries WHERE tape_name = ?", (tape,),
        )
        await self._db.commit()
        self._cache.pop(tape, None)
        self._loaded.discard(tape)

    # ── async queries ─────────────────────────────────────────────

    async def async_fetch_all(self, query: TapeQuery) -> list[TapeEntry]:
        """Ensure tape is loaded, then delegate to InMemoryQueryMixin."""
        await self.ensure_loaded(query.tape)
        return list(self.fetch_all(query))

    async def entries_after_id(
        self, tape: str, after_id: int
    ) -> list[TapeEntry]:
        """Return entries with id > *after_id* (for sinceAnchor int API)."""
        cursor = await self._db.execute(
            "SELECT * FROM tape_entries WHERE tape_name = ? AND id > ? ORDER BY id ASC",
            (tape, after_id),
        )
        rows = await cursor.fetchall()
        return [_row_to_entry(r) for r in rows]

    async def entries_by_kind(self, tape: str, kind: str) -> list[TapeEntry]:
        """Return entries of a specific kind via direct SQL (no cache)."""
        cursor = await self._db.execute(
            "SELECT * FROM tape_entries WHERE tape_name = ? AND kind = ? ORDER BY id ASC",
            (tape, kind),
        )
        rows = await cursor.fetchall()
        return [_row_to_entry(r) for r in rows]

    async def last_anchor_sql(self, tape: str) -> TapeEntry | None:
        """Return the most recent anchor entry via direct SQL (no cache)."""
        cursor = await self._db.execute(
            "SELECT * FROM tape_entries WHERE tape_name = ? AND kind = 'anchor' "
            "ORDER BY id DESC LIMIT 1",
            (tape,),
        )
        row = await cursor.fetchone()
        return _row_to_entry(row) if row else None

    async def entries_after_id_by_tape(
        self, tape: str, after_id: int, limit: int | None = None,
    ) -> list[TapeEntry]:
        """Return entries after an ID via direct SQL (no cache)."""
        query = "SELECT * FROM tape_entries WHERE tape_name = ? AND id > ? ORDER BY id ASC"
        params: list = [tape, after_id]
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        cursor = await self._db.execute(query, params)
        rows = await cursor.fetchall()
        return [_row_to_entry(r) for r in rows]

    async def entries_by_tape_prefix(self, prefix: str) -> list[TapeEntry]:
        """Return all entries whose tape_name starts with *prefix*."""
        cursor = await self._db.execute(
            "SELECT * FROM tape_entries WHERE tape_name LIKE ? ORDER BY id ASC",
            (prefix + "%",),
        )
        rows = await cursor.fetchall()
        return [_row_to_entry(r) for r in rows]

    async def list_tapes(self) -> list[str]:
        cursor = await self._db.execute(
            "SELECT DISTINCT tape_name FROM tape_entries ORDER BY tape_name",
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
