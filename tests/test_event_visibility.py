"""Tests for event visibility split (system vs user-visible)."""
from __future__ import annotations

import pytest


async def _setup_task(client) -> tuple[str, str, str]:
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "vis-test", "description": "x", "owner_id": "u1",
    })
    cb_id = cb.json()["cookbook"]["id"]
    rec = await client.post("/api/v1/recipes", json={
        "cookbook_id": cb_id, "name": "r",
        "repo_url": "https://example.com/x.git", "created_by": "u1",
    })
    rec_id = rec.json()["recipe"]["id"]
    bun = await client.post(f"/api/v1/recipes/{rec_id}/bundles", json={
        "prompt": "p", "requested_by": "u1",
    })
    bun_id = bun.json()["bundle"]["id"]
    task = await client.post(f"/api/v1/bundles/{bun_id}/tasks", json={"title": "t"})
    task_id = task.json()["task"]["id"]
    await client.post(f"/api/v1/tasks/{task_id}/claim", json={"agent_id": "a1"})
    return rec_id, bun_id, task_id


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
    async def test_digest_events_are_user_visible(self, client):
        _, _, task_id = await _setup_task(client)
        resp = await client.post(f"/api/v1/tasks/{task_id}/events", json={
            "type": "digest_approved", "actor_id": "u1", "actor_type": "human",
            "body": "Approved",
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
        for t in ("milestone", "digest_submitted", "digest_approved", "digest_rejected",
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
