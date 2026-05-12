"""Hooks ingest endpoint.

Receives lifecycle hook events from the krewcli-hook shim spawned by
agents we orchestrate. Each event is redacted, persisted into the
events table with actor_type='hook', and broadcast to SSE subscribers.

This is the only path that produces hook events. The shim is invoked
once per hook event by the spawned agent (PreToolUse/PostToolUse/
SessionStart/Stop). The agent's `KREWHUB_TASK_ID` env var (set by
SpawnManager) is forwarded by the shim and binds the event to a task.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from krewhub.auth import resolve_caller
from krewhub.db.connection import get_db
from krewhub.models import (
    ActorType,
    Event,
    EventType,
    WatchEventType,
)
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.routes.redact import redact_text, redact_value
from krewhub.watch.globals import get_watch_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["hooks"], dependencies=[Depends(resolve_caller)])


# Map hook event names → EventType
_HOOK_NAME_TO_TYPE: dict[str, EventType] = {
    "PreToolUse": EventType.TOOL_USE,
    "PostToolUse": EventType.TOOL_USE,
    "SessionStart": EventType.SESSION_START,
    "Stop": EventType.SESSION_END,
    "SessionEnd": EventType.SESSION_END,
    "Notification": EventType.TOOL_USE,
    "UserPromptSubmit": EventType.PROMPT,
}


class HookIngestRequest(BaseModel):
    """Payload from the krewcli-hook shim.

    The shim reads its hook event payload from stdin and the
    KREWHUB_* env vars set by SpawnManager, then forwards it here.
    """

    hook_event_name: str = Field(..., description="e.g. PreToolUse, Stop")
    task_id: str | None = None
    bundle_id: str | None = None
    recipe_id: str | None = None
    agent_id: str = "spawned-agent"
    session_id: str | None = None
    cwd: str | None = None
    payload: dict = Field(default_factory=dict)


@router.post("/hooks/ingest")
async def ingest_hook(
    req: HookIngestRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Persist a hook event and broadcast it via SSE."""
    event_type = _HOOK_NAME_TO_TYPE.get(req.hook_event_name, EventType.TOOL_USE)

    # Resolve recipe / bundle from task_id when needed.
    recipe_id = req.recipe_id
    bundle_id = req.bundle_id
    if req.task_id and (not recipe_id or not bundle_id):
        task = await TaskRepo(db).get(req.task_id)
        if task is not None:
            bundle_id = bundle_id or task.bundle_id
            if not recipe_id:
                bundle = await BundleRepo(db).get(task.bundle_id)
                if bundle is not None:
                    recipe_id = bundle.recipe_id

    if not recipe_id:
        raise HTTPException(
            status_code=400,
            detail="Cannot resolve recipe_id (provide recipe_id, bundle_id, or task_id)",
        )

    redacted_payload = redact_value(req.payload)
    body = redact_text(_summarize(req.hook_event_name, req.payload))

    # Wrap structured fields into the body since the events table has
    # no payload column. Keep body short and store JSON suffix.
    payload_blob = {
        "hook_event_name": req.hook_event_name,
        "session_id": req.session_id,
        "cwd": req.cwd,
        "payload": redacted_payload,
    }
    body_with_meta = body[:240]

    now = datetime.now(timezone.utc)
    event = Event(
        id=f"evt_{uuid.uuid4().hex[:8]}",
        bundle_id=bundle_id,
        task_id=req.task_id,
        type=event_type,
        actor_id=req.agent_id,
        actor_type=ActorType.HOOK,
        body=body_with_meta,
        created_at=now,
    )

    await EventRepo(db).create(event)

    # Broadcast: SSE consumers see new hook events live.
    watch = get_watch_service()
    payload_for_sse = event.model_dump(mode="json")
    payload_for_sse["payload"] = payload_blob
    await watch.record(
        resource_type="event",
        resource_id=event.id,
        event_type=WatchEventType.ADDED,
        resource_version=1,
        payload=payload_for_sse,
    )

    return {"event": event.model_dump(mode="json")}


def _summarize(hook_event_name: str, payload: dict) -> str:
    """Build a short, human-readable body for the event."""
    tool_name = payload.get("tool_name") or ""
    tool_input = payload.get("tool_input") or {}

    if hook_event_name in ("PreToolUse", "PostToolUse") and tool_name:
        target = ""
        if isinstance(tool_input, dict):
            target = (
                tool_input.get("file_path")
                or tool_input.get("path")
                or tool_input.get("command")
                or tool_input.get("query")
                or tool_input.get("pattern")
                or ""
            )
            if isinstance(target, str) and len(target) > 160:
                target = target[:157] + "..."
        suffix = "" if hook_event_name == "PreToolUse" else " ✓"
        return f"{tool_name}({target}){suffix}" if target else f"{tool_name}{suffix}"

    if hook_event_name == "SessionStart":
        sid = payload.get("session_id", "")
        cwd = payload.get("cwd", "")
        return f"session_start session={sid[:8]} cwd={cwd}"

    if hook_event_name in ("Stop", "SessionEnd"):
        sid = payload.get("session_id", "")
        return f"session_end session={sid[:8]}"

    if hook_event_name == "UserPromptSubmit":
        prompt = payload.get("prompt", "")
        return prompt[:200] if isinstance(prompt, str) else hook_event_name

    if hook_event_name == "Notification":
        # Codex emits assistant prose and reasoning as Notification.
        # Show the actual text instead of the literal "Notification".
        msg = payload.get("last_assistant_message")
        if isinstance(msg, str) and msg.strip():
            return msg.strip()
        summary = payload.get("summary")
        if isinstance(summary, str) and summary.strip():
            return summary.strip()

    return hook_event_name
