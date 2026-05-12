"""
Tape manager for event lifecycle.

Uses republic's TapeEntry and TapeQuery for structured queries,
backed by SqliteTapeStore for persistence.
"""

from __future__ import annotations

from typing import Any

import aiosqlite
from republic import TapeEntry, TapeQuery

from krewhub.models import Event
from krewhub.tape.store import SqliteTapeStore


class TapeManager:
    """Manages tape operations for a tape namespace.

    Phase 12 step (e): callers pass a fully-qualified tape name —
    e.g. ``cookbook:{cookbook_id}`` or ``recipe:{recipe_id}`` — and
    the manager stores entries under that exact key. Previously the
    constructor implicitly prefixed with ``recipe:``; that hard-coded
    coupling was retired when recipes were collapsed away.
    """

    def __init__(self, db: aiosqlite.Connection, tape_name: str) -> None:
        self._store = SqliteTapeStore(db)
        if ":" not in tape_name:
            # Back-compat: a bare id (legacy call sites) means recipe-scoped.
            tape_name = f"recipe:{tape_name}"
        self._tape_name = tape_name

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

    # ── fork tape operations ──────────────────────────────────────

    async def append_fork_entries(
        self,
        bundle_id: str,
        task_id: str,
        entries: list[dict[str, Any]],
    ) -> list[TapeEntry]:
        """Write entries to a fork tape for a specific task."""
        fork_tape = f"fork:{bundle_id}/{task_id}"
        stored: list[TapeEntry] = []
        for e in entries:
            entry = TapeEntry(
                id=0,
                kind=e["kind"],
                payload=e.get("payload", {}),
                meta=e.get("meta", {}),
            )
            stored.append(await self._store.append(fork_tape, entry))
        return stored

    async def get_fork_entries(
        self, bundle_id: str, task_id: str
    ) -> list[TapeEntry]:
        """Read all entries from a task's fork tape."""
        fork_tape = f"fork:{bundle_id}/{task_id}"
        await self._store.ensure_loaded(fork_tape)
        query = TapeQuery(tape=fork_tape, store=self._store)
        return list(self._store.fetch_all(query))

    async def get_bundle_fork_entries(self, bundle_id: str) -> list[TapeEntry]:
        """Read all fork entries across every task in a bundle."""
        return await self._store.entries_by_tape_prefix(f"fork:{bundle_id}/")

    async def merge_fork_to_parent(
        self, bundle_id: str, task_ids: list[str]
    ) -> int:
        """Merge fork entries into the parent recipe tape. Returns count."""
        count = 0
        for task_id in task_ids:
            fork_entries = await self.get_fork_entries(bundle_id, task_id)
            for fe in fork_entries:
                merged = TapeEntry(
                    id=0,
                    kind=fe.kind,
                    payload=fe.payload,
                    meta={**fe.meta, "fork_source": f"fork:{bundle_id}/{task_id}"},
                )
                await self._store.append(self._tape_name, merged)
                count += 1
        return count
