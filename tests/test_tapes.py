from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_list_tapes_empty(client):
    resp = await client.get("/api/v1/tapes")
    assert resp.status_code == 200
    assert resp.json()["tapes"] == []


@pytest.mark.asyncio
async def test_append_and_read_tape_entry(client):
    # Create a recipe first (tape name = recipe id)
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/tape-api",
        "repo_url": "git@github.com:test/tape-api.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    # Append an entry
    resp = await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "milestone",
        "payload": {"body": "First milestone", "task_id": "t1"},
        "meta": {"actor_id": "agent_1"},
    })
    assert resp.status_code == 200
    entry = resp.json()["entry"]
    assert entry["kind"] == "milestone"
    assert entry["payload"]["body"] == "First milestone"

    # Read context (should include the entry)
    resp = await client.get(f"/api/v1/tapes/{recipe_id}/context")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["entries"][0]["kind"] == "milestone"


@pytest.mark.asyncio
async def test_tape_context_since_anchor(client):
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/tape-anchor",
        "repo_url": "git@github.com:test/tape-anchor.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    # Append some entries
    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "prompt", "payload": {"body": "Old prompt"},
    })
    # Create an anchor
    resp = await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "anchor",
        "payload": {"summary": "Approved digest", "phase": "digested"},
    })
    anchor_id = resp.json()["entry"]["id"]

    # Append entry after anchor
    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "milestone", "payload": {"body": "New work"},
    })

    # Context since last anchor should only include new work
    resp = await client.get(f"/api/v1/tapes/{recipe_id}/context")
    assert resp.status_code == 200
    entries = resp.json()["entries"]
    assert len(entries) == 1
    assert entries[0]["kind"] == "milestone"

    # Context since specific anchor
    resp = await client.get(
        f"/api/v1/tapes/{recipe_id}/context",
        params={"sinceAnchor": anchor_id},
    )
    assert resp.status_code == 200
    assert len(resp.json()["entries"]) == 1


@pytest.mark.asyncio
async def test_list_tape_anchors(client):
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/tape-anchors-list",
        "repo_url": "git@github.com:test/tape-anchors-list.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    # No anchors initially
    resp = await client.get(f"/api/v1/tapes/{recipe_id}/anchors")
    assert resp.status_code == 200
    assert resp.json()["anchors"] == []

    # Add an anchor
    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "anchor",
        "payload": {"summary": "First digest"},
    })

    resp = await client.get(f"/api/v1/tapes/{recipe_id}/anchors")
    assert resp.status_code == 200
    assert len(resp.json()["anchors"]) == 1
    assert resp.json()["anchors"][0]["payload"]["summary"] == "First digest"


@pytest.mark.asyncio
async def test_tape_history(client):
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/tape-history",
        "repo_url": "git@github.com:test/tape-history.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    for i in range(3):
        await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
            "kind": "milestone", "payload": {"body": f"Entry {i}"},
        })

    resp = await client.get(f"/api/v1/tapes/{recipe_id}/history")
    assert resp.status_code == 200
    assert resp.json()["count"] == 3


@pytest.mark.asyncio
async def test_tapes_listed_after_entries(client):
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/tape-list",
        "repo_url": "git@github.com:test/tape-list.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "prompt", "payload": {"body": "Hello"},
    })

    resp = await client.get("/api/v1/tapes")
    assert resp.status_code == 200
    assert f"recipe:{recipe_id}" in resp.json()["tapes"]
