"""Tests for event visibility split (system vs user-visible)."""
from __future__ import annotations

import pytest


async def _setup_task(client) -> tuple[str, str, str]:
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "vis-test", "owner_id": "acc_legacy_apikey",
    })
    cb_id = cb.json()["cookbook"]["id"]
    bun = await client.post(f"/api/v1/cookbooks/{cb_id}/bundles", json={
        "prompt": "p", "tasks": [{"title": "t"}],
    })
    bun_id = bun.json()["bundle"]["id"]
    task_id = bun.json()["tasks"][0]["id"]
    await client.post(f"/api/v1/tasks/{task_id}/claim", json={"agent_id": "a1"})
    return cb_id, bun_id, task_id


class TestDefaultVisibility:
    """Test that event types get sensible default visibility."""

    @pytest.mark.asyncio
    async def test_milestone_is_user_visible(self, client):
        _, _, task_id = await _setup_task(client)
        resp = await client.post(f"/api/v1/tasks/{task_id}/events", json={
            "type": "milestone", "actor_id": "a1", "actor_type": "agent",
            "body": "User-visible milestone",
        })
        assert resp.status_code == 200
        assert resp.json()["event"]["visibility"] == "user"

    @pytest.mark.asyncio
    async def test_tool_use_is_system_visible(self, client):
        _, _, task_id = await _setup_task(client)
        resp = await client.post(f"/api/v1/tasks/{task_id}/events", json={
            "type": "tool_use", "actor_id": "a1", "actor_type": "agent",
            "body": "bash(ls)",
        })
        assert resp.status_code == 200
        assert resp.json()["event"]["visibility"] == "system"

    @pytest.mark.asyncio
    async def test_thinking_is_system_visible(self, client):
        _, _, task_id = await _setup_task(client)
        resp = await client.post(f"/api/v1/tasks/{task_id}/events", json={
            "type": "thinking", "actor_id": "a1", "actor_type": "agent",
            "body": "pondering",
        })
        assert resp.status_code == 200
        assert resp.json()["event"]["visibility"] == "system"

    @pytest.mark.asyncio
    async def test_bundle_lifecycle_events_are_user_visible(self, client):
        _, _, task_id = await _setup_task(client)
        resp = await client.post(f"/api/v1/tasks/{task_id}/events", json={
            "type": "bundle_closed", "actor_id": "u1", "actor_type": "human",
            "body": "Closed",
        })
        assert resp.status_code == 200
        assert resp.json()["event"]["visibility"] == "user"

    @pytest.mark.asyncio
    async def test_explicit_visibility_override(self, client):
        """Clients can override the default by passing visibility in the payload."""
        _, _, task_id = await _setup_task(client)
        resp = await client.post(f"/api/v1/tasks/{task_id}/events", json={
            "type": "milestone", "actor_id": "a1", "actor_type": "agent",
            "body": "secret milestone",
            "visibility": "system",
        })
        assert resp.status_code == 200
        assert resp.json()["event"]["visibility"] == "system"


class TestVisibilityClassifier:
    """Unit tests for the classifier function."""

    def test_user_visible_types(self):
        from krewhub.services.event_visibility import classify_visibility
        for t in ("milestone", "bundle_closed", "bundle_reopened",
                  "fact_added", "code_pushed", "prompt", "plan", "task_claimed"):
            assert classify_visibility(t) == "user", f"{t} should be user-visible"

    def test_system_visible_types(self):
        from krewhub.services.event_visibility import classify_visibility
        for t in ("tool_use", "tool_result", "thinking", "agent_reply",
                  "task_working", "session_start", "session_end"):
            assert classify_visibility(t) == "system", f"{t} should be system"

    def test_unknown_type_defaults_to_system(self):
        from krewhub.services.event_visibility import classify_visibility
        assert classify_visibility("unknown_type") == "system"
