"""
Tape manager for digest lifecycle.

Uses republic's TapeEntry and TapeQuery for structured queries,
backed by SqliteTapeStore for persistence.
"""

from __future__ import annotations

from typing import Any

import aiosqlite
from republic import TapeEntry, TapeQuery

from krewhub.models import Digest, Event
from krewhub.tape.store import SqliteTapeStore


class TapeManager:
    """Manages tape operations for a recipe."""

    def __init__(self, db: aiosqlite.Connection, recipe_id: str) -> None:
        self._store = SqliteTapeStore(db)
        self._tape_name = f"recipe:{recipe_id}"

    async def record_event(self, event: Event) -> TapeEntry:
        payload: dict[str, Any] = {
            "event_id": event.id,
            "bundle_id": event.bundle_id,
            "task_id": event.task_id,
            "body": event.body,
            "facts": [f.model_dump() for f in event.facts],
            "code_refs": [c.model_dump() for c in event.code_refs],
        }
        meta = {
            "actor_id": event.actor_id,
            "actor_type": event.actor_type,
            "event_type": event.type,
        }
        entry = TapeEntry(id=0, kind=event.type, payload=payload, meta=meta)
        return await self._store.append(self._tape_name, entry)

    async def create_digest_anchor(self, digest: Digest) -> TapeEntry:
        payload: dict[str, Any] = {
            "name": f"digest:{digest.bundle_id}",
            "phase": "digested",
            "digest_id": digest.id,
            "bundle_id": digest.bundle_id,
            "summary": digest.summary,
            "task_results": [tr.model_dump() for tr in digest.task_results],
            "facts": [f.model_dump() for f in digest.facts],
            "code_refs": [c.model_dump() for c in digest.code_refs],
            "decision": digest.decision,
            "decided_by": digest.decided_by,
        }
        meta = {
            "submitted_by": digest.submitted_by,
        }
        entry = TapeEntry(id=0, kind="anchor", payload=payload, meta=meta)
        return await self._store.append(self._tape_name, entry)

    async def get_history(self) -> list[TapeEntry]:
        await self._store.ensure_loaded(self._tape_name)
        query = TapeQuery(tape=self._tape_name, store=self._store)
        return list(self._store.fetch_all(query))

    async def get_history_since_last_anchor(self) -> list[TapeEntry]:
        await self._store.ensure_loaded(self._tape_name)
        try:
            query = TapeQuery(tape=self._tape_name, store=self._store).last_anchor()
            return list(self._store.fetch_all(query))
        except Exception:
            # No anchors found — return full history
            query = TapeQuery(tape=self._tape_name, store=self._store)
            return list(self._store.fetch_all(query))

    async def get_anchors(self) -> list[TapeEntry]:
        await self._store.ensure_loaded(self._tape_name)
        query = TapeQuery(tape=self._tape_name, store=self._store).kinds("anchor")
        return list(self._store.fetch_all(query))
