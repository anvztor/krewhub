"""Daemon runtime health tracking.

Each krewcli process instance registers itself here. It heartbeats
every 15s. If more than 60s elapses without a heartbeat, the runtime
is marked `offline` by the sweep endpoint (called periodically by
a controller, or manually for tests).

Separate from `agent_presence` because that table tracks agent
IDENTITY (claude@alice), whereas this tracks RUNTIME INSTANCES —
the actual python process running somewhere. If krewcli crashes,
the runtime row goes stale but the agent identity persists.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, Query

from krewhub.auth import resolve_caller_or_cookie
from krewhub.db.connection import get_db
from krewhub.routes.schemas import RegisterRuntimeRequest

# Cookie-friendly auth so the cookrew-beta SPA (browser session) can
# read its own roster. Daemon callers (Bearer token) keep working —
# resolve_caller_or_cookie supports both.
router = APIRouter(
    tags=["agent-runtimes"], dependencies=[Depends(resolve_caller_or_cookie)],
)

# A runtime that hasn't heartbeated in this many seconds is stale.
STALE_THRESHOLD_SECONDS = 60


def _row_to_runtime(row: aiosqlite.Row) -> dict:
    return {
        "id": row["id"],
        "agent_id": row["agent_id"],
        "account_id": row["account_id"],
        "daemon_version": row["daemon_version"],
        "provider": row["provider"],
        "host_info": json.loads(row["host_info"]) if row["host_info"] else {},
        "status": row["status"],
        "last_seen_at": row["last_seen_at"],
        "started_at": row["started_at"],
    }


def _load_host_info(row: aiosqlite.Row) -> dict:
    try:
        return json.loads(row["host_info"]) if row["host_info"] else {}
    except json.JSONDecodeError:
        return {}


def _same_device(existing: dict, incoming: dict) -> bool:
    """Best-effort same-device check used to make daemon startup idempotent."""
    existing_device = existing.get("device_id")
    incoming_device = incoming.get("device_id")
    if existing_device and incoming_device:
        return existing_device == incoming_device

    # Migration path for rows created before krewcli sent device_id.
    existing_endpoint = existing.get("endpoint_url")
    incoming_endpoint = incoming.get("endpoint_url")
    if existing_endpoint and incoming_endpoint:
        return existing_endpoint == incoming_endpoint

    existing_host = existing.get("hostname")
    incoming_host = incoming.get("hostname")
    return bool(existing_host and incoming_host and existing_host == incoming_host)


async def _mark_stale(db: aiosqlite.Connection) -> int:
    cutoff = (
        datetime.now(timezone.utc) - timedelta(seconds=STALE_THRESHOLD_SECONDS)
    ).isoformat()
    cursor = await db.execute(
        """UPDATE agent_runtimes
           SET status = 'offline'
           WHERE last_seen_at < ? AND status != 'offline'""",
        (cutoff,),
    )
    await db.commit()
    return cursor.rowcount


@router.post("/agents/runtime/register")
async def register_runtime(
    req: RegisterRuntimeRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Register a krewcli daemon process.

    Called once at startup. Returns the runtime id — daemon stores this
    and uses it for subsequent heartbeats.
    """
    now = datetime.now(timezone.utc).isoformat()
    host_info_json = json.dumps(req.host_info)

    cursor = await db.execute(
        """SELECT * FROM agent_runtimes
           WHERE account_id = ?
             AND agent_id = ?
             AND COALESCE(provider, '') = COALESCE(?, '')
           ORDER BY last_seen_at DESC""",
        (req.account_id, req.agent_id, req.provider),
    )
    rows = await cursor.fetchall()
    matches = [r for r in rows if _same_device(_load_host_info(r), req.host_info)]

    if matches:
        rt_id = matches[0]["id"]
        await db.execute(
            """UPDATE agent_runtimes
               SET daemon_version = ?,
                   host_info = ?,
                   status = 'online',
                   last_seen_at = ?
               WHERE id = ?""",
            (req.daemon_version, host_info_json, now, rt_id),
        )
        duplicate_ids = [r["id"] for r in matches[1:]]
        if duplicate_ids:
            placeholders = ",".join("?" for _ in duplicate_ids)
            await db.execute(
                f"""UPDATE agent_runtimes
                    SET status = 'offline'
                    WHERE id IN ({placeholders})""",
                duplicate_ids,
            )
        await db.commit()

        cursor2 = await db.execute(
            "SELECT * FROM agent_runtimes WHERE id = ?", (rt_id,),
        )
        row = await cursor2.fetchone()
        assert row is not None
        return {"runtime": _row_to_runtime(row)}

    rt_id = f"rt_{uuid.uuid4().hex[:12]}"
    await db.execute(
        """INSERT INTO agent_runtimes
           (id, agent_id, account_id, daemon_version, provider, host_info,
            status, last_seen_at, started_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            rt_id, req.agent_id, req.account_id,
            req.daemon_version, req.provider,
            host_info_json,
            "online", now, now,
        ),
    )
    await db.commit()

    cursor = await db.execute(
        "SELECT * FROM agent_runtimes WHERE id = ?", (rt_id,),
    )
    row = await cursor.fetchone()
    assert row is not None
    return {"runtime": _row_to_runtime(row)}


@router.post("/agents/runtime/{runtime_id}/heartbeat")
async def heartbeat_runtime(
    runtime_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Record that this runtime is still alive.

    Called every ~15s. Updates last_seen_at and resets status to online.
    Returns 404 if the runtime was never registered (client should
    re-register and use the new id).
    """
    cursor = await db.execute(
        "SELECT id FROM agent_runtimes WHERE id = ?", (runtime_id,),
    )
    if await cursor.fetchone() is None:
        raise HTTPException(status_code=404, detail="Runtime not registered")

    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "UPDATE agent_runtimes SET last_seen_at = ?, status = 'online' WHERE id = ?",
        (now, runtime_id),
    )
    await db.commit()

    cursor2 = await db.execute(
        "SELECT * FROM agent_runtimes WHERE id = ?", (runtime_id,),
    )
    row = await cursor2.fetchone()
    assert row is not None
    return {"runtime": _row_to_runtime(row)}


@router.get("/agents/runtimes")
async def list_runtimes(
    account_id: str | None = Query(None),
    db: aiosqlite.Connection = Depends(get_db),
):
    """List all registered runtimes, optionally filtered by account_id."""
    await _mark_stale(db)
    if account_id:
        cursor = await db.execute(
            "SELECT * FROM agent_runtimes WHERE account_id = ? ORDER BY last_seen_at DESC",
            (account_id,),
        )
    else:
        cursor = await db.execute(
            "SELECT * FROM agent_runtimes ORDER BY last_seen_at DESC",
        )
    rows = await cursor.fetchall()
    return {"runtimes": [_row_to_runtime(r) for r in rows]}


@router.post("/agents/runtime/sweep")
async def sweep_stale_runtimes(
    db: aiosqlite.Connection = Depends(get_db),
):
    """Mark runtimes offline if their last_seen_at is older than the stale threshold.

    Intended to be called periodically (e.g. every 30s) by a controller
    or by cookrew's polling watcher. Tests call it directly.
    """
    marked = await _mark_stale(db)
    cutoff = (
        datetime.now(timezone.utc) - timedelta(seconds=STALE_THRESHOLD_SECONDS)
    ).isoformat()
    return {"marked_offline": marked, "cutoff": cutoff}
