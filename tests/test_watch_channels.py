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

    # Bundle events
    def test_bundle_cooked(self):
        obj = {"status": "cooked"}
        assert derive_channel("bundle", "MODIFIED", obj) == "bundle:cooked"

    def test_bundle_blocked(self):
        obj = {"status": "blocked"}
        assert derive_channel("bundle", "MODIFIED", obj) == "bundle:blocked"

    def test_bundle_cancelled(self):
        obj = {"status": "cancelled"}
        assert derive_channel("bundle", "MODIFIED", obj) == "bundle:cancelled"

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

    def test_event_digest_submitted(self):
        assert derive_channel("event", "ADDED", {"type": "digest_submitted"}) == "digest:submitted"

    def test_event_digest_approved(self):
        assert derive_channel("event", "ADDED", {"type": "digest_approved"}) == "digest:approved"

    def test_event_digest_rejected(self):
        assert derive_channel("event", "ADDED", {"type": "digest_rejected"}) == "digest:rejected"

    # Digest events
    def test_digest_submitted_resource(self):
        assert derive_channel("digest", "ADDED", {"decision": "pending"}) == "digest:submitted"

    def test_digest_approved_resource(self):
        assert derive_channel("digest", "MODIFIED", {"decision": "approved"}) == "digest:approved"

    def test_digest_rejected_resource(self):
        assert derive_channel("digest", "MODIFIED", {"decision": "rejected"}) == "digest:rejected"

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
