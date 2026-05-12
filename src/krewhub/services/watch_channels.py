"""Derive typed watch channels from resource+event_type+status.

This gives SSE subscribers a semantic channel (e.g. `task:completed`,
`digest:submitted`) instead of forcing them to filter on generic
`resource_type + event_type + object.status` client-side.
"""
from __future__ import annotations

from typing import Any

# Task status → channel suffix (for MODIFIED)
_TASK_STATUS_TO_CHANNEL = {
    "claimed": "task:claimed",
    "working": "task:working",
    "done": "task:completed",
    "blocked": "task:failed",
    "cancelled": "task:cancelled",
}

# Bundle status → channel suffix (for MODIFIED).
# Phase 12 step (d.1): bundle FSM is just OPEN/CLOSED. The MODIFIED
# event surfaces close/reopen transitions; UI subscribes to the
# typed channels for those.
_BUNDLE_STATUS_TO_CHANNEL = {
    "closed": "bundle:closed",
    "open": "bundle:reopened",  # MODIFIED→open implies reopen
}

# Event type → channel (for nested events in tasks)
_EVENT_TYPE_TO_CHANNEL = {
    "tool_use": "task:message",
    "tool_result": "task:message",
    "thinking": "task:message",
    "agent_reply": "task:message",
    "milestone": "task:message",
    "fact_added": "task:message",
    "code_pushed": "task:message",
    "session_start": "task:session_start",
    "session_end": "task:session_end",
    "bundle_closed": "bundle:closed",
    "bundle_reopened": "bundle:reopened",
    "prompt": "bundle:prompt",
    "plan": "bundle:plan",
    "task_claimed": "task:claimed",
    "task_working": "task:working",
}

# Agent status → channel
_AGENT_STATUS_TO_CHANNEL = {
    "online": "agent:online",
    "offline": "agent:offline",
    "busy": "agent:busy",
}


def derive_channel(
    resource_type: str,
    event_type: str,
    obj: dict[str, Any],
) -> str:
    """Return a typed channel like `task:completed`, `digest:submitted`.

    Falls back to `{resource_type}:{event_type.lower()}` when no specific
    mapping applies.
    """
    event_type_lower = (event_type or "").lower()

    if resource_type == "task":
        if event_type_lower == "added":
            return "task:added"
        # Explicit hint set by the progress endpoint to distinguish
        # in-flight progress updates from status transitions.
        if obj.get("_channel_hint") == "task:progress":
            return "task:progress"
        status = obj.get("status")
        if status and status in _TASK_STATUS_TO_CHANNEL:
            return _TASK_STATUS_TO_CHANNEL[status]
        return f"task:{event_type_lower}"

    if resource_type == "bundle":
        if event_type_lower == "added":
            return "bundle:added"
        status = obj.get("status")
        if status and status in _BUNDLE_STATUS_TO_CHANNEL:
            return _BUNDLE_STATUS_TO_CHANNEL[status]
        return f"bundle:{event_type_lower}"

    if resource_type == "event":
        evt_type = obj.get("type")
        if evt_type and evt_type in _EVENT_TYPE_TO_CHANNEL:
            return _EVENT_TYPE_TO_CHANNEL[evt_type]
        return f"event:{event_type_lower}"

    # Phase 12 step (d): digest resource is gone.

    if resource_type == "agent":
        if event_type_lower == "added":
            return "agent:added"
        status = obj.get("status")
        if status and status in _AGENT_STATUS_TO_CHANNEL:
            return _AGENT_STATUS_TO_CHANNEL[status]
        return f"agent:{event_type_lower}"

    # Fallback: resource:event_type
    return f"{resource_type}:{event_type_lower}"
