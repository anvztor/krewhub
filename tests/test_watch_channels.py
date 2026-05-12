"""Tests for typed watch channel derivation."""
from __future__ import annotations

from krewhub.services.watch_channels import derive_channel


class TestDeriveChannel:
    # Task events
    def test_task_claimed(self):
        obj = {"status": "claimed"}
        assert derive_channel("task", "MODIFIED", obj) == "task:claimed"

    def test_task_working(self):
        obj = {"status": "working"}
        assert derive_channel("task", "MODIFIED", obj) == "task:working"

    def test_task_completed(self):
        obj = {"status": "done"}
        assert derive_channel("task", "MODIFIED", obj) == "task:completed"

    def test_task_failed(self):
        obj = {"status": "blocked"}
        assert derive_channel("task", "MODIFIED", obj) == "task:failed"

    def test_task_cancelled(self):
        obj = {"status": "cancelled"}
        assert derive_channel("task", "MODIFIED", obj) == "task:cancelled"

    def test_task_progress_hinted(self):
        # When the progress endpoint emits, it sets _channel_hint
        obj = {"status": "working", "_channel_hint": "task:progress", "progress": {"step": 3}}
        assert derive_channel("task", "MODIFIED", obj) == "task:progress"

    def test_task_added(self):
        obj = {"status": "open"}
        assert derive_channel("task", "ADDED", obj) == "task:added"

    # Bundle events — step (d.1): only open/closed exist now.
    def test_bundle_closed(self):
        obj = {"status": "closed"}
        assert derive_channel("bundle", "MODIFIED", obj) == "bundle:closed"

    def test_bundle_reopened(self):
        obj = {"status": "open"}
        assert derive_channel("bundle", "MODIFIED", obj) == "bundle:reopened"

    def test_bundle_added(self):
        obj = {"status": "open"}
        assert derive_channel("bundle", "ADDED", obj) == "bundle:added"

    # Event row events (nested events posted to tasks)
    def test_event_message_for_tool_use(self):
        obj = {"type": "tool_use"}
        assert derive_channel("event", "ADDED", obj) == "task:message"

    def test_event_message_for_tool_result(self):
        assert derive_channel("event", "ADDED", {"type": "tool_result"}) == "task:message"

    def test_event_message_for_thinking(self):
        assert derive_channel("event", "ADDED", {"type": "thinking"}) == "task:message"

    def test_event_message_for_agent_reply(self):
        assert derive_channel("event", "ADDED", {"type": "agent_reply"}) == "task:message"

    def test_event_message_for_milestone(self):
        assert derive_channel("event", "ADDED", {"type": "milestone"}) == "task:message"

    def test_event_session_start(self):
        assert derive_channel("event", "ADDED", {"type": "session_start"}) == "task:session_start"

    def test_event_session_end(self):
        assert derive_channel("event", "ADDED", {"type": "session_end"}) == "task:session_end"

    # Phase 12 step (d): digest channels gone. Bundle lifecycle uses
    # bundle:closed / bundle:reopened instead.
    def test_event_bundle_closed(self):
        assert derive_channel("event", "ADDED", {"type": "bundle_closed"}) == "bundle:closed"

    def test_event_bundle_reopened(self):
        assert derive_channel("event", "ADDED", {"type": "bundle_reopened"}) == "bundle:reopened"

    # Agent events
    def test_agent_added(self):
        assert derive_channel("agent", "ADDED", {}) == "agent:added"

    def test_agent_heartbeat(self):
        assert derive_channel("agent", "MODIFIED", {"status": "online"}) == "agent:online"

    def test_agent_offline(self):
        assert derive_channel("agent", "MODIFIED", {"status": "offline"}) == "agent:offline"

    # Fallback
    def test_unknown_resource_falls_back(self):
        assert derive_channel("unknown", "ADDED", {}) == "unknown:added"

    def test_missing_status_falls_back_to_event_type(self):
        obj = {}  # no status field
        assert derive_channel("task", "MODIFIED", obj) == "task:modified"

    def test_case_insensitive_event_type(self):
        assert derive_channel("task", "added", {}) == "task:added"
