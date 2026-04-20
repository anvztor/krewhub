from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from republic import TapeEntry

import aiosqlite

from krewhub.auth import resolve_caller_or_cookie
from krewhub.db.connection import get_db
from krewhub.tape.manager import TapeManager
from krewhub.tape.store import SqliteTapeStore, entry_to_dict

router = APIRouter(tags=["tapes"], dependencies=[Depends(resolve_caller_or_cookie)])


class AppendEntryRequest(BaseModel):
    kind: str
    payload: dict = {}
    meta: dict = {}


@router.get("/tapes/{tape_name}/context")
async def get_tape_context(
    tape_name: str,
    since_anchor: int | None = Query(None, alias="sinceAnchor"),
    limit: int = Query(100),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Read context entries from a tape.

    CSI equivalent: agents "mount" a tape and read context from it
    before executing a task. This gives them awareness of prior work.

    - Without sinceAnchor: returns entries since the last anchor
    - With sinceAnchor: returns entries after that specific anchor ID
    """
    manager = TapeManager(db, _strip_prefix(tape_name))

    if since_anchor is not None:
        store = SqliteTapeStore(db)
        entries = await store.entries_after_id(
            f"recipe:{_strip_prefix(tape_name)}", since_anchor
        )
    else:
        entries = await manager.get_history_since_last_anchor()

    return {
        "tape_name": tape_name,
        "entries": [entry_to_dict(e) for e in entries[:limit]],
        "count": len(entries),
    }


@router.get("/tapes/{tape_name}/anchors")
async def list_tape_anchors(
    tape_name: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    """List all anchors (approved digest checkpoints) for a tape."""
    manager = TapeManager(db, _strip_prefix(tape_name))
    anchors = await manager.get_anchors()
    return {
        "tape_name": tape_name,
        "anchors": [entry_to_dict(a) for a in anchors],
    }


@router.get("/tapes/{tape_name}/history")
async def get_tape_history(
    tape_name: str,
    limit: int = Query(500),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Get full tape history."""
    manager = TapeManager(db, _strip_prefix(tape_name))
    entries = await manager.get_history()
    return {
        "tape_name": tape_name,
        "entries": [entry_to_dict(e) for e in entries[:limit]],
        "count": len(entries),
    }


@router.post("/tapes/{tape_name}/entries")
async def append_tape_entry(
    tape_name: str,
    req: AppendEntryRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Append an entry to a tape.

    CSI equivalent: agents write context back to the tape after
    completing work, so future agents can build on it.
    """
    store = SqliteTapeStore(db)
    entry = await store.append(
        tape=f"recipe:{_strip_prefix(tape_name)}",
        entry=TapeEntry(id=0, kind=req.kind, payload=req.payload, meta=req.meta),
    )
    return {"entry": entry_to_dict(entry)}


class ForkEntriesRequest(BaseModel):
    bundle_id: str
    task_id: str
    entries: list[AppendEntryRequest]


@router.post("/tapes/{recipe_id}/fork-entries")
async def push_fork_entries(
    recipe_id: str,
    req: ForkEntriesRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Push fork tape entries from a task execution.

    Agents accumulate entries locally during task execution,
    then push them here as a batch on task completion.
    """
    manager = TapeManager(db, _strip_prefix(recipe_id))
    stored = await manager.append_fork_entries(
        req.bundle_id,
        req.task_id,
        [e.model_dump() for e in req.entries],
    )
    return {
        "entries": [entry_to_dict(e) for e in stored],
        "count": len(stored),
    }


@router.get("/tapes/{recipe_id}/fork-entries/{bundle_id}")
async def get_fork_entries(
    recipe_id: str,
    bundle_id: str,
    task_id: str | None = Query(None),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Read fork tape entries for a bundle (optionally filtered by task)."""
    manager = TapeManager(db, _strip_prefix(recipe_id))
    if task_id is not None:
        entries = await manager.get_fork_entries(bundle_id, task_id)
    else:
        entries = await manager.get_bundle_fork_entries(bundle_id)
    return {
        "entries": [entry_to_dict(e) for e in entries],
        "count": len(entries),
    }


@router.get("/tapes/{tape_name}/entries/{entry_id}")
async def get_tape_entry(
    tape_name: str,
    entry_id: int,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Get a single tape entry by ID."""
    store = SqliteTapeStore(db)
    entry = await store.get_entry(entry_id)
    if entry is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Entry not found")
    return {"entry": entry_to_dict(entry)}


@router.get("/tapes/{tape_name}/range")
async def get_tape_range(
    tape_name: str,
    from_id: int = Query(..., alias="from"),
    to_id: int = Query(..., alias="to"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Get entries between two IDs (from < id <= to).

    Use for viewing entries between two anchors:
      GET /tapes/{recipe}/range?from=4342&to=4344
    """
    store = SqliteTapeStore(db)
    entries = await store.entries_between_ids(
        f"recipe:{_strip_prefix(tape_name)}", from_id, to_id,
    )
    return {
        "entries": [entry_to_dict(e) for e in entries],
        "count": len(entries),
        "from_id": from_id,
        "to_id": to_id,
    }


@router.get("/tapes")
async def list_tapes(
    db: aiosqlite.Connection = Depends(get_db),
):
    """List all known tape names."""
    store = SqliteTapeStore(db)
    tapes = await store.list_tapes()
    return {"tapes": tapes}


def _strip_prefix(tape_name: str) -> str:
    """Strip 'recipe:' prefix if present, since TapeManager adds it."""
    if tape_name.startswith("recipe:"):
        return tape_name[len("recipe:"):]
    return tape_name
