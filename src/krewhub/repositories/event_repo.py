from __future__ import annotations

import json
from datetime import datetime

import aiosqlite

from krewhub.models import Event, EventType, FactRef, CodeRef
from krewhub.tape.manager import TapeManager


class EventRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(self, event: Event) -> Event:
        await self._db.execute(
            """INSERT INTO events
               (id, recipe_id, bundle_id, task_id, type, actor_id, actor_type,
                body, facts, code_refs, created_at, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (event.id, event.recipe_id, event.bundle_id, event.task_id,
             event.type, event.actor_id, event.actor_type, event.body,
             json.dumps([f.model_dump() for f in event.facts]),
             json.dumps([c.model_dump() for c in event.code_refs]),
             event.created_at.isoformat(),
             event.expires_at.isoformat() if event.expires_at else None),
        )
        await TapeManager(self._db, event.recipe_id).record_event(event)
        await self._db.commit()
        return event

    async def list_by_recipe(self, recipe_id: str) -> list[Event]:
        cursor = await self._db.execute(
            "SELECT * FROM events WHERE recipe_id = ? ORDER BY created_at",
            (recipe_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_event(r) for r in rows]

    async def list_by_bundle(self, bundle_id: str) -> list[Event]:
        cursor = await self._db.execute(
            "SELECT * FROM events WHERE bundle_id = ? ORDER BY created_at",
            (bundle_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_event(r) for r in rows]

    async def delete_expired(self, now: datetime) -> int:
        cursor = await self._db.execute(
            "DELETE FROM events WHERE expires_at IS NOT NULL AND expires_at <= ?",
            (now.isoformat(),),
        )
        await self._db.commit()
        return cursor.rowcount

    async def set_expiry_for_bundle(
        self, bundle_id: str, expires_at: datetime
    ) -> None:
        await self._db.execute(
            "UPDATE events SET expires_at = ? WHERE bundle_id = ? AND expires_at IS NULL",
            (expires_at.isoformat(), bundle_id),
        )
        await self._db.commit()


def _row_to_event(row: aiosqlite.Row) -> Event:
    return Event(
        id=row["id"],
        recipe_id=row["recipe_id"],
        bundle_id=row["bundle_id"],
        task_id=row["task_id"],
        type=row["type"],
        actor_id=row["actor_id"],
        actor_type=row["actor_type"],
        body=row["body"],
        facts=[FactRef(**f) for f in json.loads(row["facts"])],
        code_refs=[CodeRef(**c) for c in json.loads(row["code_refs"])],
        created_at=datetime.fromisoformat(row["created_at"]),
        expires_at=datetime.fromisoformat(row["expires_at"]) if row["expires_at"] else None,
    )
