"""A2A Hub Gateway — standard-compliant A2A protocol endpoints.

Implements the Agent2Agent (A2A) protocol specification:
  - Agent card discovery at /.well-known/agent.json
  - JSON-RPC 2.0 message/send
  - Task lifecycle (submitted → working → completed/failed)
  - Mailbox pattern for NAT-traversal (agents pick up via SSE)

Flow:
  External caller → POST /a2a/{owner}/{agent} (JSON-RPC message/send)
  → krewhub stores as a2a_invocation
  → watch event emitted (SSE)
  → krewcli picks up via SSE, processes locally
  → krewcli POST /a2a/respond with result
  → krewhub returns result to original caller
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from krewhub.db.connection import get_db
from krewhub.watch.globals import get_watch_service
from krewhub.models import WatchEventType

router = APIRouter(tags=["a2a-gateway"])

_INVOCATION_TTL = timedelta(seconds=300)

# In-memory signal: task_id → asyncio.Event
# When respond_invocation() is called, it sets the event,
# instantly waking the blocked _handle_message_send() coroutine.
_pending_events: dict[str, asyncio.Event] = {}


# ---------------------------------------------------------------------------
# Agent Card (A2A standard: /.well-known/agent.json)
# ---------------------------------------------------------------------------


@router.get("/a2a/{owner}/{agent}/.well-known/agent.json")
async def get_agent_card(
    owner: str, agent: str,
    db: aiosqlite.Connection = Depends(get_db),
) -> JSONResponse:
    cursor = await db.execute(
        "SELECT card_json FROM a2a_agent_cards WHERE owner = ? AND agent_name = ?",
        (owner, agent),
    )
    row = await cursor.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Agent {owner}/{agent} not found")
    return JSONResponse(content=json.loads(row["card_json"]))


# Legacy path compat
@router.get("/a2a/{owner}/{agent}/.well-known/agent-card.json")
async def get_agent_card_legacy(
    owner: str, agent: str,
    db: aiosqlite.Connection = Depends(get_db),
) -> JSONResponse:
    return await get_agent_card(owner, agent, db)


# ---------------------------------------------------------------------------
# A2A JSON-RPC endpoint (message/send, tasks/get, tasks/cancel)
# ---------------------------------------------------------------------------


@router.get("/a2a/{owner}/{agent}")
async def get_agent_card_root(
    owner: str, agent: str,
    db: aiosqlite.Connection = Depends(get_db),
) -> JSONResponse:
    """GET on the agent endpoint returns the agent card (A2A standard)."""
    return await get_agent_card(owner, agent, db)


@router.post("/a2a/{owner}/{agent}")
async def a2a_jsonrpc(
    owner: str, agent: str,
    request: Request,
    db: aiosqlite.Connection = Depends(get_db),
) -> JSONResponse:
    """Handle A2A JSON-RPC 2.0 requests."""
    body = await request.json()

    # Validate JSON-RPC structure
    jsonrpc = body.get("jsonrpc", "2.0")
    req_id = body.get("id")
    method = body.get("method", "")
    params = body.get("params", {})

    if method == "message/send":
        return await _handle_message_send(owner, agent, req_id, params, db)
    elif method == "tasks/get":
        return await _handle_tasks_get(req_id, params, db)
    elif method == "tasks/cancel":
        return await _handle_tasks_cancel(req_id, params, db)
    else:
        return _jsonrpc_error(req_id, -32601, f"Method not found: {method}")


async def _handle_message_send(
    owner: str, agent: str,
    req_id: str | None,
    params: dict,
    db: aiosqlite.Connection,
) -> JSONResponse:
    """Handle message/send — create task, store in mailbox, wait for agent."""

    # Check agent exists
    cursor = await db.execute(
        "SELECT card_json FROM a2a_agent_cards WHERE owner = ? AND agent_name = ?",
        (owner, agent),
    )
    if await cursor.fetchone() is None:
        return _jsonrpc_error(req_id, -32000, f"Agent {owner}/{agent} not found")

    # Check agent is online
    agent_id_pattern = f"{agent}@%"
    cursor = await db.execute(
        "SELECT status FROM agent_presence WHERE agent_id LIKE ? AND status != 'offline' LIMIT 1",
        (agent_id_pattern,),
    )
    if await cursor.fetchone() is None:
        return _jsonrpc_error(req_id, -32000, f"Agent {owner}/{agent} is offline")

    # Extract message text from A2A message parts
    message = params.get("message", {})
    parts = message.get("parts", [])
    text_parts = [p.get("text", "") for p in parts if p.get("kind") == "text" or "text" in p]
    message_text = "\n".join(text_parts) or json.dumps(message)

    # Create invocation (task)
    task_id = f"task_{uuid.uuid4().hex[:12]}"
    context_id = params.get("contextId", f"ctx_{uuid.uuid4().hex[:8]}")
    now = datetime.now(timezone.utc)
    expires = now + _INVOCATION_TTL

    await db.execute(
        "INSERT INTO a2a_invocations "
        "(id, owner, agent_name, method, params, caller_id, status, created_at, expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
        (task_id, owner, agent, "message/send", json.dumps(params),
         params.get("caller_id"), now.isoformat(), expires.isoformat()),
    )
    await db.commit()

    # Emit watch event for agent's SSE subscription
    watch = get_watch_service()
    await watch.record_resource(
        "a2a_invocation", task_id, WatchEventType.ADDED,
        {
            "id": task_id,
            "owner": owner,
            "agent_name": agent,
            "method": "message/send",
            "message": message_text,
            "params": params,
            "context_id": context_id,
        },
    )

    # Check if caller wants immediate return
    config = params.get("configuration", {})
    if config.get("returnImmediately", False):
        return _jsonrpc_result(req_id, {
            "id": task_id,
            "contextId": context_id,
            "status": {"state": "submitted", "timestamp": now.isoformat()},
            "artifacts": [],
        })

    # Wait for agent to respond via asyncio.Event (no polling)
    event = asyncio.Event()
    _pending_events[task_id] = event

    try:
        await asyncio.wait_for(event.wait(), timeout=_INVOCATION_TTL.total_seconds())
    except asyncio.TimeoutError:
        _pending_events.pop(task_id, None)
        await db.execute(
            "UPDATE a2a_invocations SET status = 'timeout' WHERE id = ? AND status = 'pending'",
            (task_id,),
        )
        await db.commit()
        return _jsonrpc_result(req_id, {
            "id": task_id, "contextId": context_id,
            "status": {"state": "failed", "timestamp": datetime.now(timezone.utc).isoformat(),
                       "message": {"role": "agent", "parts": [{"kind": "text", "text": "Agent did not respond within timeout"}]}},
            "artifacts": [],
        })
    finally:
        _pending_events.pop(task_id, None)

    # Event fired — read result (single DB query)
    cursor = await db.execute(
        "SELECT status, result, error FROM a2a_invocations WHERE id = ?", (task_id,),
    )
    row = await cursor.fetchone()

    if row and row["status"] == "completed":
        result_data = json.loads(row["result"]) if row["result"] else {}
        return _jsonrpc_result(req_id, {
            "id": task_id, "contextId": context_id,
            "status": {"state": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
            "artifacts": [{"parts": [{"kind": "text", "text": result_data.get("text", json.dumps(result_data))}]}],
        })

    error_msg = (row["error"] if row else None) or "Agent failed"
    return _jsonrpc_result(req_id, {
        "id": task_id, "contextId": context_id,
        "status": {"state": "failed", "timestamp": datetime.now(timezone.utc).isoformat(),
                   "message": {"role": "agent", "parts": [{"kind": "text", "text": error_msg}]}},
        "artifacts": [],
    })


async def _handle_tasks_get(req_id, params, db) -> JSONResponse:
    task_id = params.get("id")
    if not task_id:
        return _jsonrpc_error(req_id, -32602, "Missing task id")

    cursor = await db.execute(
        "SELECT * FROM a2a_invocations WHERE id = ?", (task_id,),
    )
    row = await cursor.fetchone()
    if row is None:
        return _jsonrpc_error(req_id, -32000, "Task not found")

    state_map = {"pending": "submitted", "processing": "working",
                 "completed": "completed", "failed": "failed", "timeout": "failed"}

    result = {
        "id": row["id"],
        "status": {"state": state_map.get(row["status"], "unknown")},
        "artifacts": [],
    }
    if row["result"]:
        result_data = json.loads(row["result"])
        result["artifacts"] = [{"parts": [{"kind": "text", "text": result_data.get("text", json.dumps(result_data))}]}]

    return _jsonrpc_result(req_id, result)


async def _handle_tasks_cancel(req_id, params, db) -> JSONResponse:
    task_id = params.get("id")
    if not task_id:
        return _jsonrpc_error(req_id, -32602, "Missing task id")

    await db.execute(
        "UPDATE a2a_invocations SET status = 'failed', error = 'Canceled by caller' WHERE id = ? AND status IN ('pending', 'processing')",
        (task_id,),
    )
    await db.commit()
    return _jsonrpc_result(req_id, {"id": task_id, "status": {"state": "canceled"}})


# ---------------------------------------------------------------------------
# Agent respond (krewcli posts result back after processing)
# ---------------------------------------------------------------------------


@router.post("/a2a/respond")
async def respond_invocation(
    request: Request,
    db: aiosqlite.Connection = Depends(get_db),
) -> dict:
    """Agent responds to an A2A invocation with result."""
    body = await request.json()
    invocation_id = body.get("invocation_id") or body.get("task_id")
    result = body.get("result")
    error = body.get("error")

    if not invocation_id:
        raise HTTPException(status_code=400, detail="Missing invocation_id")

    now = datetime.now(timezone.utc).isoformat()
    status = "completed" if error is None else "failed"

    cursor = await db.execute(
        "UPDATE a2a_invocations SET status = ?, result = ?, error = ?, completed_at = ? "
        "WHERE id = ? AND status IN ('pending', 'processing')",
        (status, json.dumps(result) if result else None, error, now, invocation_id),
    )
    await db.commit()

    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Invocation not found or already completed")

    # Signal the waiting coroutine in _handle_message_send (instant wake)
    event = _pending_events.get(invocation_id)
    if event:
        event.set()

    return {"detail": "Response recorded", "invocation_id": invocation_id}


# ---------------------------------------------------------------------------
# Pending invocations (agent polls or receives via SSE)
# ---------------------------------------------------------------------------


@router.get("/a2a/{owner}/{agent}/pending")
async def list_pending(
    owner: str, agent: str,
    db: aiosqlite.Connection = Depends(get_db),
) -> list[dict]:
    now = datetime.now(timezone.utc).isoformat()
    cursor = await db.execute(
        "SELECT id, method, params, caller_id, created_at FROM a2a_invocations "
        "WHERE owner = ? AND agent_name = ? AND status = 'pending' AND expires_at > ? "
        "ORDER BY created_at ASC",
        (owner, agent, now),
    )
    rows = await cursor.fetchall()
    return [{"invocation_id": r["id"], "method": r["method"],
             "params": json.loads(r["params"]), "caller_id": r["caller_id"],
             "created_at": r["created_at"]} for r in rows]


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


@router.get("/a2a/{owner}/{agent}/status")
async def agent_status(
    owner: str, agent: str,
    db: aiosqlite.Connection = Depends(get_db),
) -> dict:
    agent_id_pattern = f"{agent}@%"
    cursor = await db.execute(
        "SELECT status FROM agent_presence WHERE agent_id LIKE ? ORDER BY last_heartbeat_at DESC LIMIT 1",
        (agent_id_pattern,),
    )
    row = await cursor.fetchone()
    return {"owner": owner, "agent_name": agent,
            "status": row["status"] if row else "offline",
            "a2a_endpoint": f"/a2a/{owner}/{agent}"}


# ---------------------------------------------------------------------------
# Agent card management
# ---------------------------------------------------------------------------


async def upsert_agent_card(
    db: aiosqlite.Connection,
    owner: str, agent_name: str,
    display_name: str, capabilities: list[str],
    endpoint_base: str = "",
) -> None:
    """Create or update an agent's A2A-compliant card."""
    card = {
        "name": display_name,
        "description": f"{display_name} on Cookrew",
        "url": f"{endpoint_base}/a2a/{owner}/{agent_name}",
        "version": "1.0.0",
        "protocolVersion": "0.2.5",
        "capabilities": {"streaming": False, "pushNotifications": False},
        "skills": [{"id": cap, "name": cap} for cap in capabilities],
        "defaultInputModes": ["text"],
        "defaultOutputModes": ["text"],
        "provider": {"organization": owner},
    }
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT INTO a2a_agent_cards (owner, agent_name, card_json, updated_at) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(owner, agent_name) DO UPDATE SET card_json = excluded.card_json, updated_at = excluded.updated_at",
        (owner, agent_name, json.dumps(card), now),
    )
    await db.commit()


# ---------------------------------------------------------------------------
# JSON-RPC helpers
# ---------------------------------------------------------------------------


def _jsonrpc_result(req_id, result) -> JSONResponse:
    return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": result})


def _jsonrpc_error(req_id, code, message) -> JSONResponse:
    return JSONResponse(
        {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}},
        status_code=200,  # JSON-RPC errors are still 200 HTTP
    )
