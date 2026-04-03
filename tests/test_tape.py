from __future__ import annotations

import pytest

from krewhub.db.connection import get_db


@pytest.mark.asyncio
async def test_events_are_written_to_tape_and_approved_digest_creates_anchor(client):
    resp = await client.post("/api/v1/cookbooks", json={
        "name": "test-tape-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = resp.json()["cookbook"]["id"]
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/tape",
        "repo_url": "git@github.com:test/tape.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Track the full lifecycle",
        "requested_by": "human_1",
        "tasks": [{"title": "Ship tape history"}],
    })
    bundle_id = resp.json()["bundle"]["id"]
    task_id = resp.json()["tasks"][0]["id"]

    await client.post(f"/api/v1/tasks/{task_id}/claim", json={"agent_id": "agent_alpha"})
    await client.post(f"/api/v1/tasks/{task_id}/events", json={
        "type": "milestone",
        "actor_id": "agent_alpha",
        "body": "Lifecycle event captured",
    })
    await client.patch(f"/api/v1/tasks/{task_id}/status", json={"status": "done"})
    await client.post(f"/api/v1/bundles/{bundle_id}/digest", json={
        "submitted_by": "agent_alpha",
        "summary": "Ready to anchor",
        "task_results": [{"task_id": task_id, "outcome": "Done"}],
    })
    await client.post(f"/api/v1/bundles/{bundle_id}/decision", json={
        "decision": "approved",
        "decided_by": "human_1",
    })

    db = await get_db()
    cursor = await db.execute(
        "SELECT kind FROM tape_entries WHERE tape_name = ? ORDER BY id",
        (f"recipe:{recipe_id}",),
    )
    rows = await cursor.fetchall()
    kinds = [row["kind"] for row in rows]

    assert kinds == [
        "prompt",
        "plan",
        "task_claimed",
        "milestone",
        "digest_submitted",
        "anchor",
        "digest_approved",
    ]
