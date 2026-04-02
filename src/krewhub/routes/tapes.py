from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

import aiosqlite

from krewhub.auth import verify_api_key
from krewhub.db.connection import get_db
from krewhub.tape.manager import TapeManager
from krewhub.tape.store import TapeStore

router = APIRouter(tags=["tapes"], dependencies=[Depends(verify_api_key)])


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
        store = TapeStore(db)
        entries = await store.entries_after_anchor(
            f"recipe:{_strip_prefix(tape_name)}", since_anchor
        )
    else:
        entries = await manager.get_history_since_last_anchor()

    return {
        "tape_name": tape_name,
        "entries": [e.to_dict() for e in entries[:limit]],
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
        "anchors": [a.to_dict() for a in anchors],
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
        "entries": [e.to_dict() for e in entries[:limit]],
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
    store = TapeStore(db)
    entry = await store.append(
        tape_name=f"recipe:{_strip_prefix(tape_name)}",
        kind=req.kind,
        payload=req.payload,
        meta=req.meta,
    )
    return {"entry": entry.to_dict()}


@router.get("/tapes")
async def list_tapes(
    db: aiosqlite.Connection = Depends(get_db),
):
    """List all known tape names."""
    store = TapeStore(db)
    tapes = await store.list_tapes()
    return {"tapes": tapes}


def _strip_prefix(tape_name: str) -> str:
    """Strip 'recipe:' prefix if present, since TapeManager adds it."""
    if tape_name.startswith("recipe:"):
        return tape_name[len("recipe:"):]
    return tape_name
