"""Classify event types as user-visible or system-only.

Used to split the firehose of agent execution events (tool_use,
thinking, agent_reply) from human-visible milestones (bundle close
events, fact_added, code_pushed).

This enables the frontend to show a clean "activity log" by filtering
on ?visibility=user, while keeping the full trace available for
debugging/replay via ?visibility=system or no filter.
"""
from __future__ import annotations

from typing import Literal

Visibility = Literal["user", "system"]

# Event types a human in cookrew would care about at a glance.
_USER_VISIBLE_TYPES = frozenset({
    "prompt",
    "plan",
    "task_claimed",  # "agent picked up task"
    "milestone",     # human-authored or agent-summarized milestone
    "fact_added",
    "code_pushed",
    # Phase 12 step (d): bundle lifecycle replaces the digest decisions.
    "bundle_closed",
    "bundle_reopened",
})

# Verbose execution telemetry — hidden by default in the "activity log"
# view. Still streamed live via SSE for the TaskLiveCard.
_SYSTEM_TYPES = frozenset({
    "task_working",
    "session_start",
    "session_end",
    "tool_use",
    "tool_result",
    "agent_reply",
    "thinking",
})


def classify_visibility(event_type: str) -> Visibility:
    """Return 'user' or 'system' based on the event type.

    Unknown types default to 'system' — conservative: if we don't
    recognize it, hide it from the activity log rather than spam.
    """
    if event_type in _USER_VISIBLE_TYPES:
        return "user"
    if event_type in _SYSTEM_TYPES:
        return "system"
    return "system"
