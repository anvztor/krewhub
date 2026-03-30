from __future__ import annotations

import pytest

from krewhub.db.connection import get_db
from krewhub.repositories.event_repo import EventRepo


async def _create_cooked_bundle(client) -> tuple[str, str, list[str]]:
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/digests",
        "repo_url": "git@github.com:test/digests.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Digest test",
        "requested_by": "human_1",
        "tasks": [
            {"title": "Task A"},
            {"title": "Task B"},
        ],
    })
    bundle_id = resp.json()["bundle"]["id"]
    task_ids = [t["id"] for t in resp.json()["tasks"]]

    for tid in task_ids:
        await client.post(f"/api/v1/tasks/{tid}/claim", json={"agent_id": "agent_alpha"})
        await client.patch(f"/api/v1/tasks/{tid}/status", json={"status": "done"})

    return recipe_id, bundle_id, task_ids


@pytest.mark.asyncio
async def test_submit_digest(client):
    recipe_id, bundle_id, task_ids = await _create_cooked_bundle(client)

    resp = await client.post(f"/api/v1/bundles/{bundle_id}/digest", json={
        "submitted_by": "agent_alpha",
        "summary": "Both tasks completed successfully.",
        "task_results": [
            {"task_id": task_ids[0], "outcome": "Task A done"},
            {"task_id": task_ids[1], "outcome": "Task B done"},
        ],
        "facts": [{"id": "f1", "claim": "Tests pass", "captured_by": "agent_alpha"}],
        "code_refs": [{
            "repo_url": "git@github.com:test/digests.git",
            "branch": "main",
            "commit_sha": "def456",
            "paths": ["src/main.py"],
        }],
    })
    assert resp.status_code == 200
    digest = resp.json()["digest"]
    assert digest["decision"] == "pending"
    assert digest["summary"] == "Both tasks completed successfully."


@pytest.mark.asyncio
async def test_approve_digest(client):
    recipe_id, bundle_id, task_ids = await _create_cooked_bundle(client)

    await client.post(f"/api/v1/bundles/{bundle_id}/digest", json={
        "submitted_by": "agent_alpha",
        "summary": "Ready for approval.",
        "task_results": [{"task_id": tid, "outcome": "Done"} for tid in task_ids],
    })

    resp = await client.post(f"/api/v1/bundles/{bundle_id}/decision", json={
        "decision": "approved",
        "decided_by": "human_1",
    })
    assert resp.status_code == 200
    assert resp.json()["digest"]["decision"] == "approved"

    # Bundle should be digested
    bundle_resp = await client.get(f"/api/v1/bundles/{bundle_id}")
    assert bundle_resp.json()["bundle"]["status"] == "digested"

    # Should appear in approved digests
    hist_resp = await client.get(f"/api/v1/recipes/{recipe_id}/digests")
    assert len(hist_resp.json()["digests"]) >= 1


@pytest.mark.asyncio
async def test_reject_digest(client):
    recipe_id, bundle_id, task_ids = await _create_cooked_bundle(client)

    await client.post(f"/api/v1/bundles/{bundle_id}/digest", json={
        "submitted_by": "agent_alpha",
        "summary": "Will be rejected.",
        "task_results": [{"task_id": tid, "outcome": "Done"} for tid in task_ids],
    })

    resp = await client.post(f"/api/v1/bundles/{bundle_id}/decision", json={
        "decision": "rejected",
        "decided_by": "human_1",
    })
    assert resp.status_code == 200
    assert resp.json()["digest"]["decision"] == "rejected"

    # Bundle should be rejected
    bundle_resp = await client.get(f"/api/v1/bundles/{bundle_id}")
    assert bundle_resp.json()["bundle"]["status"] == "rejected"

    db = await get_db()
    events = await EventRepo(db).list_by_bundle(bundle_id)
    assert events
    assert all(event.expires_at is not None for event in events)


@pytest.mark.asyncio
async def test_cannot_submit_digest_with_open_tasks(client):
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/incomplete",
        "repo_url": "git@github.com:test/incomplete.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Incomplete",
        "requested_by": "human_1",
        "tasks": [{"title": "Not done yet"}],
    })
    bundle_id = resp.json()["bundle"]["id"]

    resp = await client.post(f"/api/v1/bundles/{bundle_id}/digest", json={
        "submitted_by": "agent_alpha",
        "summary": "Premature digest",
    })
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_cannot_submit_digest_for_cancelled_bundle(client):
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/cancelled-digest",
        "repo_url": "git@github.com:test/cancelled-digest.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Cancel then digest",
        "requested_by": "human_1",
        "tasks": [{"title": "Cancelled task"}],
    })
    bundle_id = resp.json()["bundle"]["id"]

    cancel = await client.patch(f"/api/v1/bundles/{bundle_id}")
    assert cancel.status_code == 200
    assert cancel.json()["bundle"]["status"] == "cancelled"

    digest = await client.post(f"/api/v1/bundles/{bundle_id}/digest", json={
        "submitted_by": "agent_alpha",
        "summary": "Should fail",
    })
    assert digest.status_code == 400


@pytest.mark.asyncio
async def test_can_submit_digest_for_blocked_bundle(client):
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/blocked-digest",
        "repo_url": "git@github.com:test/blocked-digest.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Blocked digest",
        "requested_by": "human_1",
        "tasks": [{"title": "Blocked task"}],
    })
    bundle_id = resp.json()["bundle"]["id"]
    task_id = resp.json()["tasks"][0]["id"]

    await client.post(f"/api/v1/tasks/{task_id}/claim", json={"agent_id": "agent_alpha"})
    await client.patch(f"/api/v1/tasks/{task_id}/status", json={
        "status": "blocked",
        "blocked_reason": "Missing dependency",
    })

    digest = await client.post(f"/api/v1/bundles/{bundle_id}/digest", json={
        "submitted_by": "agent_alpha",
        "summary": "Blocked but summarized",
        "task_results": [{"task_id": task_id, "outcome": "Blocked"}],
    })
    assert digest.status_code == 200
