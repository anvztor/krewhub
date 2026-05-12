from __future__ import annotations

import pytest


# Step (e): tape_name is just a string. We use a synthetic name
# directly (was recipe_id; now any unique label works).


async def _tape_name(client, suffix: str = "default") -> str:
    """Return a tape name. (No recipe seeding needed.)"""
    return f"test-tape-{suffix}"


@pytest.mark.asyncio
async def test_list_tapes_empty(client):
    resp = await client.get("/api/v1/tapes")
    assert resp.status_code == 200
    assert resp.json()["tapes"] == []


@pytest.mark.asyncio
async def test_append_and_read_tape_entry(client):
    name = await _tape_name(client, "api")

    resp = await client.post(f"/api/v1/tapes/{name}/entries", json={
        "kind": "milestone",
        "payload": {"body": "First milestone", "task_id": "t1"},
        "meta": {"actor_id": "agent_1"},
    })
    assert resp.status_code == 200
    entry = resp.json()["entry"]
    assert entry["kind"] == "milestone"
    assert entry["payload"]["body"] == "First milestone"

    resp = await client.get(f"/api/v1/tapes/{name}/context")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["entries"][0]["kind"] == "milestone"


@pytest.mark.asyncio
async def test_tape_context_since_anchor(client):
    name = await _tape_name(client, "anchor")

    await client.post(f"/api/v1/tapes/{name}/entries", json={
        "kind": "event", "payload": {"body": "before anchor"},
    })
    resp = await client.post(f"/api/v1/tapes/{name}/entries", json={
        "kind": "anchor", "payload": {"name": "checkpoint", "phase": "step"},
    })
    anchor_id = resp.json()["entry"]["id"]
    await client.post(f"/api/v1/tapes/{name}/entries", json={
        "kind": "event", "payload": {"body": "after anchor"},
    })

    resp = await client.get(f"/api/v1/tapes/{name}/context")
    assert resp.status_code == 200
    data = resp.json()
    # Default context returns entries since the last anchor
    assert data["count"] >= 1

    # With explicit since=anchor_id should return everything after
    resp = await client.get(
        f"/api/v1/tapes/{name}/context",
        params={"since": anchor_id},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_list_tape_anchors(client):
    name = await _tape_name(client, "anchors")

    resp = await client.get(f"/api/v1/tapes/{name}/anchors")
    assert resp.status_code == 200
    assert resp.json()["anchors"] == []

    await client.post(f"/api/v1/tapes/{name}/entries", json={
        "kind": "anchor", "payload": {"name": "checkpoint-1"},
    })

    resp = await client.get(f"/api/v1/tapes/{name}/anchors")
    assert resp.status_code == 200
    anchors = resp.json()["anchors"]
    assert len(anchors) == 1
    assert anchors[0]["kind"] == "anchor"


@pytest.mark.asyncio
async def test_tape_history(client):
    name = await _tape_name(client, "history")

    for i in range(3):
        await client.post(f"/api/v1/tapes/{name}/entries", json={
            "kind": "event", "payload": {"i": i},
        })

    resp = await client.get(f"/api/v1/tapes/{name}/history")
    assert resp.status_code == 200
    assert resp.json()["count"] == 3


@pytest.mark.asyncio
async def test_tapes_listed_after_entries(client):
    name = await _tape_name(client, "listed")

    await client.post(f"/api/v1/tapes/{name}/entries", json={
        "kind": "prompt", "payload": {"body": "Hello"},
    })

    resp = await client.get("/api/v1/tapes")
    assert resp.status_code == 200
    # tapes route prefixes with "recipe:" for legacy compat
    assert any(name in t for t in resp.json()["tapes"])


# ── fork-entries tests ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_push_and_read_fork_entries(client):
    name = await _tape_name(client, "fork-1")
    bundle_id = "bun_test1"
    task_id = "task_test1"

    resp = await client.post(f"/api/v1/tapes/{name}/fork-entries", json={
        "bundle_id": bundle_id,
        "task_id": task_id,
        "entries": [
            {"kind": "milestone", "payload": {"body": "step 1"}},
            {"kind": "tool_call", "payload": {"tool": "read_file"}},
            {"kind": "anchor", "payload": {
                "name": f"handoff:{bundle_id}/{task_id}",
                "phase": "task_complete",
                "summary": "Done",
            }},
        ],
    })
    assert resp.status_code == 200
    assert resp.json()["count"] == 3

    resp = await client.get(
        f"/api/v1/tapes/{name}/fork-entries/{bundle_id}",
        params={"task_id": task_id},
    )
    assert resp.status_code == 200
    entries = resp.json()["entries"]
    assert len(entries) == 3
    assert [e["kind"] for e in entries] == ["milestone", "tool_call", "anchor"]


@pytest.mark.asyncio
async def test_get_all_fork_entries_for_bundle(client):
    name = await _tape_name(client, "fork-multi")
    bundle_id = "bun_multi"

    for tid in ["task_a", "task_b"]:
        await client.post(f"/api/v1/tapes/{name}/fork-entries", json={
            "bundle_id": bundle_id,
            "task_id": tid,
            "entries": [
                {"kind": "milestone", "payload": {"body": f"work by {tid}"}},
            ],
        })

    resp = await client.get(f"/api/v1/tapes/{name}/fork-entries/{bundle_id}")
    assert resp.status_code == 200
    assert resp.json()["count"] == 2


# test_fork_entries_merged_on_digest_approval removed in step (d) —
# digest flow no longer exists.

# test_fork_anchors_in_workspace_data removed in step (e) —
# workspace route now keyed on cookbook_id; out of scope for this
# focused tapes test file.


@pytest.mark.asyncio
async def test_tapes_route_accepts_cookie_auth(cookie_client):
    """Browser clients (post-BFF-elimination) authenticate via cookie."""
    resp = await cookie_client.get("/api/v1/tapes")
    assert resp.status_code == 200
