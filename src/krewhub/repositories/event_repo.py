from __future__ import annotations

import json
from datetime import datetime

import aiosqlite

from krewhub.models import Event, FactRef, CodeRef
from krewhub.tape.manager import TapeManager


class EventRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def next_sequence(self, task_id: str | None) -> int:
        """Allocate the next monotonic sequence for a task (or recipe-level if None)."""
        if task_id is None:
            return 0
        cursor = await self._db.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS next FROM events WHERE task_id = ?",
            (task_id,),
        )
        row = await cursor.fetchone()
        return int(row["next"]) if row else 1

    async def create(self, event: Event) -> Event:
        await self._db.execute(
            """INSERT INTO events
               (id, cookbook_id, bundle_id, task_id, type, actor_id,
                actor_type, body, payload, sequence, facts, code_refs,
                visibility, created_at, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (event.id, event.cookbook_id, event.bundle_id,
             event.task_id, event.type, event.actor_id, event.actor_type,
             event.body,
             json.dumps(event.payload) if event.payload is not None else None,
             event.sequence,
             json.dumps([f.model_dump() for f in event.facts]),
             json.dumps([c.model_dump() for c in event.code_refs]),
             event.visibility,
             event.created_at.isoformat(),
             event.expires_at.isoformat() if event.expires_at else None),
        )
        # Phase 12 step (e): TapeManager keys on cookbook directly.
        tape_key = (
            f"cookbook:{event.cookbook_id}" if event.cookbook_id else "orphan"
        )
        await TapeManager(self._db, tape_key).record_event(event)
        await self._db.commit()
        return event

    async def list_by_cookbook(self, cookbook_id: str) -> list[Event]:
        """Phase 12: direct cookbook lookup (no recipes join).

        Returns events stamped with this cookbook_id at create time.
        Legacy events without cookbook_id are picked up by the
        migration backfill; until that runs they only appear in
        list_by_recipe.
        """
        cursor = await self._db.execute(
            "SELECT * FROM events WHERE cookbook_id = ? "
            "ORDER BY sequence, created_at",
            (cookbook_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_event(r) for r in rows]

    async def list_by_cookbook_type(
        self, cookbook_id: str, event_type: str,
    ) -> list[Event]:
        cursor = await self._db.execute(
            "SELECT * FROM events WHERE cookbook_id = ? AND type = ? "
            "ORDER BY sequence, created_at",
            (cookbook_id, event_type),
        )
        rows = await cursor.fetchall()
        return [_row_to_event(r) for r in rows]

    async def list_by_bundle(self, bundle_id: str) -> list[Event]:
        cursor = await self._db.execute(
            "SELECT * FROM events WHERE bundle_id = ? ORDER BY sequence, created_at",
            (bundle_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_event(r) for r in rows]

    async def list_by_task(self, task_id: str) -> list[Event]:
        cursor = await self._db.execute(
            "SELECT * FROM events WHERE task_id = ? ORDER BY sequence, created_at",
            (task_id,),
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
    # Old rows may not have payload/sequence/visibility columns if the
    # migration hasn't run yet; guard access with a row-key check.
    row_keys = set(row.keys())
    payload_raw = row["payload"] if "payload" in row_keys else None
    sequence = row["sequence"] if "sequence" in row_keys else 0
    visibility = row["visibility"] if "visibility" in row_keys else "system"
    cookbook_id = row["cookbook_id"] if "cookbook_id" in row_keys else None
    return Event(
        id=row["id"],
        cookbook_id=cookbook_id,
        bundle_id=row["bundle_id"],
        task_id=row["task_id"],
        type=row["type"],
        actor_id=row["actor_id"],
        actor_type=row["actor_type"],
        body=row["body"],
        payload=json.loads(payload_raw) if payload_raw else None,
        sequence=sequence or 0,
        facts=[FactRef(**f) for f in json.loads(row["facts"])],
        code_refs=[CodeRef(**c) for c in json.loads(row["code_refs"])],
        visibility=visibility or "system",
        created_at=datetime.fromisoformat(row["created_at"]),
        expires_at=datetime.fromisoformat(row["expires_at"]) if row["expires_at"] else None,
    )
