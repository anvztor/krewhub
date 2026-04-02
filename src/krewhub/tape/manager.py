"""
Tape manager for digest lifecycle.

Uses the TapeStore to:
- Record events as tape entries
- Create anchors when digests are approved
- Query tape history for recipe reconstruction
"""

from __future__ import annotations

from typing import Any

import aiosqlite

from krewhub.models import Digest, Event
from krewhub.tape.store import TapeEntry, TapeStore


class TapeManager:
    """Manages tape operations for a recipe."""

    def __init__(self, db: aiosqlite.Connection, recipe_id: str) -> None:
        self._store = TapeStore(db)
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
        return await self._store.append(
            self._tape_name,
            kind=event.type,
            payload=payload,
            meta=meta,
        )

    async def create_digest_anchor(self, digest: Digest) -> TapeEntry:
        payload: dict[str, Any] = {
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
        return await self._store.append(
            self._tape_name,
            kind="anchor",
            payload=payload,
            meta=meta,
        )

    async def get_history(self) -> list[TapeEntry]:
        return await self._store.fetch_all(self._tape_name)

    async def get_history_since_last_anchor(self) -> list[TapeEntry]:
        anchor = await self._store.last_anchor(self._tape_name)
        if anchor is None:
            return await self._store.fetch_all(self._tape_name)
        return await self._store.entries_after_anchor(self._tape_name, anchor.id)

    async def get_anchors(self) -> list[TapeEntry]:
        return await self._store.fetch_all(self._tape_name, kinds=["anchor"])
