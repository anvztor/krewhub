from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import aiosqlite

from krewhub.models import WatchEntry, WatchEventType
from krewhub.watch.types import WatchEvent


class WatchLogStore:
    """Persistent watch log backed by the watch_log SQLite table.

    Every resource mutation is appended here. Subscribers can replay
    from any sequence number, making reconnects reliable.
    """

    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def append(
        self,
        resource_type: str,
        resource_id: str,
        event_type: WatchEventType,
        resource_version: int,
        payload: dict[str, Any],
        recipe_id: str | None = None,
    ) -> WatchEntry:
        now = datetime.now(timezone.utc)
        cursor = await self._db.execute(
            """INSERT INTO watch_log
               (resource_type, resource_id, event_type, resource_version,
                payload, recipe_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                resource_type,
                resource_id,
                event_type,
                resource_version,
                json.dumps(payload),
                recipe_id,
                now.isoformat(),
            ),
        )
        await self._db.commit()

        return WatchEntry(
            seq=cursor.lastrowid or 0,
            resource_type=resource_type,
            resource_id=resource_id,
            event_type=event_type,
            resource_version=resource_version,
            payload=payload,
            recipe_id=recipe_id,
            created_at=now,
        )

    async def list_since(
        self,
        since: int = 0,
        resource_type: str | None = None,
        recipe_id: str | None = None,
        limit: int = 500,
    ) -> list[WatchEntry]:
        parts = ["seq > ?"]
        params: list[object] = [since]

        if resource_type is not None:
            parts.append("resource_type = ?")
            params.append(resource_type)
        if recipe_id is not None:
            parts.append("recipe_id = ?")
            params.append(recipe_id)

        where = " AND ".join(parts)
        params.append(limit)

        cursor = await self._db.execute(
            f"SELECT * FROM watch_log WHERE {where} ORDER BY seq ASC LIMIT ?",
            params,
        )
        rows = await cursor.fetchall()
        return [_row_to_entry(r) for r in rows]

    async def latest_seq(self) -> int:
        cursor = await self._db.execute(
            "SELECT MAX(seq) FROM watch_log"
        )
        row = await cursor.fetchone()
        if row is None or row[0] is None:
            return 0
        return row[0]

    async def trim_before(self, seq: int) -> int:
        cursor = await self._db.execute(
            "DELETE FROM watch_log WHERE seq < ?", (seq,)
        )
        await self._db.commit()
        return cursor.rowcount


def _row_to_entry(row: aiosqlite.Row) -> WatchEntry:
    return WatchEntry(
        seq=row["seq"],
        resource_type=row["resource_type"],
        resource_id=row["resource_id"],
        event_type=row["event_type"],
        resource_version=row["resource_version"],
        payload=json.loads(row["payload"]),
        recipe_id=row["recipe_id"],
        created_at=datetime.fromisoformat(row["created_at"]),
    )


def entry_to_watch_event(entry: WatchEntry) -> WatchEvent:
    from krewhub.services.watch_channels import derive_channel
    channel = derive_channel(
        resource_type=entry.resource_type,
        event_type=entry.event_type,
        obj=entry.payload if isinstance(entry.payload, dict) else {},
    )
    return WatchEvent(
        event_type=entry.event_type,
        resource_type=entry.resource_type,
        resource_id=entry.resource_id,
        resource_version=entry.resource_version,
        object=entry.payload,
        recipe_id=entry.recipe_id,
        seq=entry.seq,
        channel=channel,
    )
