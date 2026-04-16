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

# Bundle status → channel suffix (for MODIFIED)
_BUNDLE_STATUS_TO_CHANNEL = {
    "cooked": "bundle:cooked",
    "blocked": "bundle:blocked",
    "cancelled": "bundle:cancelled",
    "claimed": "bundle:claimed",
    "digested": "bundle:digested",
}

# Event type → channel (for nested events in tasks)
# Grouping: tool_use/tool_result/thinking/agent_reply/milestone → task:message
# session_* → task:session_*
# digest_* → digest:*
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
    "digest_submitted": "digest:submitted",
    "digest_approved": "digest:approved",
    "digest_rejected": "digest:rejected",
    "prompt": "bundle:prompt",
    "plan": "bundle:plan",
    "task_claimed": "task:claimed",
    "task_working": "task:working",
}

# Digest decision → channel
_DIGEST_DECISION_TO_CHANNEL = {
    "pending": "digest:submitted",
    "approved": "digest:approved",
    "rejected": "digest:rejected",
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

    if resource_type == "digest":
        decision = obj.get("decision")
        if decision and decision in _DIGEST_DECISION_TO_CHANNEL:
            return _DIGEST_DECISION_TO_CHANNEL[decision]
        return f"digest:{event_type_lower}"

    if resource_type == "agent":
        if event_type_lower == "added":
            return "agent:added"
        status = obj.get("status")
        if status and status in _AGENT_STATUS_TO_CHANNEL:
            return _AGENT_STATUS_TO_CHANNEL[status]
        return f"agent:{event_type_lower}"

    # Fallback: resource:event_type
    return f"{resource_type}:{event_type_lower}"
