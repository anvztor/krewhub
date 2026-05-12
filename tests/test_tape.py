from __future__ import annotations

import pytest

from krewhub.db.connection import get_db


@pytest.mark.asyncio
async def test_events_are_written_to_tape_and_approved_digest_creates_anchor(client):
    """Step (e): recipes + digest layer are gone. Tape chain now keys
    on cookbook:{cookbook_id}; bundle creation drops prompt/plan events,
    claim emits task_claimed, post_event emits the kind verbatim."""
    resp = await client.post("/api/v1/cookbooks", json={
        "name": "test-tape-cookbook",
        "owner_id": "acc_legacy_apikey",
    })
    cookbook_id = resp.json()["cookbook"]["id"]

    resp = await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
        "prompt": "Track the full lifecycle",
        "tasks": [{"title": "Ship tape history"}],
    })
    task_id = resp.json()["tasks"][0]["id"]

    await client.post(f"/api/v1/tasks/{task_id}/claim", json={"agent_id": "agent_alpha"})
    await client.post(f"/api/v1/tasks/{task_id}/events", json={
        "type": "milestone",
        "actor_id": "agent_alpha",
        "body": "Lifecycle event captured",
    })
    await client.patch(f"/api/v1/tasks/{task_id}/status", json={"status": "done"})

    db = await get_db()
    cursor = await db.execute(
        "SELECT kind FROM tape_entries WHERE tape_name = ? ORDER BY id",
        (f"cookbook:{cookbook_id}",),
    )
    rows = await cursor.fetchall()
    kinds = [row["kind"] for row in rows]

    # bundle create emits prompt + plan, claim emits task_claimed,
    # milestone is the operator-supplied event kind.
    assert "prompt" in kinds
    assert "plan" in kinds
    assert "task_claimed" in kinds
    assert "milestone" in kinds
