"""Tests for Layer 4: session token isolation on event ingestion."""

from __future__ import annotations

import pytest


async def _setup_claimed_task(client) -> tuple[str, str, str]:
    """Create a cookbook/recipe/bundle/task and claim it, returning IDs."""
    resp = await client.post("/api/v1/cookbooks", json={
        "name": "session-iso-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = resp.json()["cookbook"]["id"]

    resp = await client.post("/api/v1/recipes", json={
        "name": "test/session-iso",
        "repo_url": "git@github.com:test/session-iso.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Session isolation test",
        "requested_by": "human_1",
        "tasks": [{"title": "Guarded task"}],
    })
    bundle_id = resp.json()["bundle"]["id"]
    task_id = resp.json()["tasks"][0]["id"]

    await client.post(f"/api/v1/tasks/{task_id}/claim", json={
        "agent_id": "agent_alpha",
    })
    return recipe_id, bundle_id, task_id


def _event_payload(session_token: str | None = None) -> dict:
    """Build a minimal event body, optionally with session_token."""
    body: dict = {
        "type": "milestone",
        "actor_id": "agent_alpha",
        "body": "progress",
    }
    if session_token is not None:
        body["session_token"] = session_token
    return body


def _batch_payload(session_token: str | None = None) -> dict:
    """Build a minimal batch body, optionally with session_token."""
    body: dict = {
        "events": [
            {
                "type": "milestone",
                "actor_id": "agent_alpha",
                "body": "batch progress",
            },
        ],
    }
    if session_token is not None:
        body["session_token"] = session_token
    return body


@pytest.mark.asyncio
async def test_first_event_stamps_token(client):
    """First event with a session_token stamps it on the task."""
    _recipe_id, _bundle_id, task_id = await _setup_claimed_task(client)

    resp = await client.post(
        f"/api/v1/tasks/{task_id}/events",
        json=_event_payload(session_token="tok-A"),
    )
    assert resp.status_code == 200

    # Verify the task now carries the token
    resp = await client.get(f"/api/v1/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["task"]["session_token"] == "tok-A"


@pytest.mark.asyncio
async def test_matching_token_accepted(client):
    """Subsequent events with the same token are accepted."""
    _recipe_id, _bundle_id, task_id = await _setup_claimed_task(client)

    resp = await client.post(
        f"/api/v1/tasks/{task_id}/events",
        json=_event_payload(session_token="tok-A"),
    )
    assert resp.status_code == 200

    resp = await client.post(
        f"/api/v1/tasks/{task_id}/events",
        json=_event_payload(session_token="tok-A"),
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_mismatched_token_rejected(client):
    """Events with a different session_token are rejected 409."""
    _recipe_id, _bundle_id, task_id = await _setup_claimed_task(client)

    resp = await client.post(
        f"/api/v1/tasks/{task_id}/events",
        json=_event_payload(session_token="tok-A"),
    )
    assert resp.status_code == 200

    resp = await client.post(
        f"/api/v1/tasks/{task_id}/events",
        json=_event_payload(session_token="tok-B"),
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_no_token_always_accepted(client):
    """Events without session_token are accepted (backward compat)."""
    _recipe_id, _bundle_id, task_id = await _setup_claimed_task(client)

    # Stamp a token first
    resp = await client.post(
        f"/api/v1/tasks/{task_id}/events",
        json=_event_payload(session_token="tok-A"),
    )
    assert resp.status_code == 200

    # Now send without token — should still be accepted
    resp = await client.post(
        f"/api/v1/tasks/{task_id}/events",
        json=_event_payload(),
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_batch_endpoint_guard(client):
    """Batch endpoint enforces session token isolation identically."""
    _recipe_id, _bundle_id, task_id = await _setup_claimed_task(client)

    # Stamp via batch
    resp = await client.post(
        f"/api/v1/tasks/{task_id}/events:batch",
        json=_batch_payload(session_token="tok-A"),
    )
    assert resp.status_code == 200

    # Same token — accepted
    resp = await client.post(
        f"/api/v1/tasks/{task_id}/events:batch",
        json=_batch_payload(session_token="tok-A"),
    )
    assert resp.status_code == 200

    # Different token — rejected
    resp = await client.post(
        f"/api/v1/tasks/{task_id}/events:batch",
        json=_batch_payload(session_token="tok-B"),
    )
    assert resp.status_code == 409

    # No token — accepted (backward compat)
    resp = await client.post(
        f"/api/v1/tasks/{task_id}/events:batch",
        json=_batch_payload(),
    )
    assert resp.status_code == 200
