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
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "test-tape-api-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = cb.json()["cookbook"]["id"]
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/tape-api",
        "repo_url": "git@github.com:test/tape-api.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
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
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "test-tape-anchor-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = cb.json()["cookbook"]["id"]
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/tape-anchor",
        "repo_url": "git@github.com:test/tape-anchor.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
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
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "test-tape-anchors-list-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = cb.json()["cookbook"]["id"]
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/tape-anchors-list",
        "repo_url": "git@github.com:test/tape-anchors-list.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
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
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "test-tape-history-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = cb.json()["cookbook"]["id"]
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/tape-history",
        "repo_url": "git@github.com:test/tape-history.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
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
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "test-tape-list-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = cb.json()["cookbook"]["id"]
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/tape-list",
        "repo_url": "git@github.com:test/tape-list.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    recipe_id = resp.json()["recipe"]["id"]

    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "prompt", "payload": {"body": "Hello"},
    })

    resp = await client.get("/api/v1/tapes")
    assert resp.status_code == 200
    assert f"recipe:{recipe_id}" in resp.json()["tapes"]


# ── fork-entries tests ────────────────────────────────────────────


async def _create_recipe(client) -> str:
    cb = await client.post("/api/v1/cookbooks", json={
        "name": f"test-fork-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = cb.json()["cookbook"]["id"]
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/fork-tape",
        "repo_url": "git@github.com:test/fork-tape.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    return resp.json()["recipe"]["id"]


@pytest.mark.asyncio
async def test_push_and_read_fork_entries(client):
    recipe_id = await _create_recipe(client)
    bundle_id = "bun_test1"
    task_id = "task_test1"

    resp = await client.post(f"/api/v1/tapes/{recipe_id}/fork-entries", json={
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
        f"/api/v1/tapes/{recipe_id}/fork-entries/{bundle_id}",
        params={"task_id": task_id},
    )
    assert resp.status_code == 200
    entries = resp.json()["entries"]
    assert len(entries) == 3
    assert [e["kind"] for e in entries] == ["milestone", "tool_call", "anchor"]


@pytest.mark.asyncio
async def test_get_all_fork_entries_for_bundle(client):
    recipe_id = await _create_recipe(client)
    bundle_id = "bun_multi"

    for tid in ["task_a", "task_b"]:
        await client.post(f"/api/v1/tapes/{recipe_id}/fork-entries", json={
            "bundle_id": bundle_id,
            "task_id": tid,
            "entries": [
                {"kind": "milestone", "payload": {"body": f"work by {tid}"}},
            ],
        })

    resp = await client.get(f"/api/v1/tapes/{recipe_id}/fork-entries/{bundle_id}")
    assert resp.status_code == 200
    assert resp.json()["count"] == 2


# test_fork_entries_merged_on_digest_approval removed in step (d) —
# digest flow no longer exists. Fork-tape merge belongs on bundle
# close now; covered separately when that lands.


@pytest.mark.asyncio
async def test_fork_anchors_in_workspace_data(client):
    """Workspace endpoint includes fork_anchors in selected_bundle."""
    recipe_id = await _create_recipe(client)
    bundle_id = "bun_ws_test"
    task_id = "task_ws_test"

    # Push fork entries with a handoff anchor
    await client.post(f"/api/v1/tapes/{recipe_id}/fork-entries", json={
        "bundle_id": bundle_id,
        "task_id": task_id,
        "entries": [
            {"kind": "anchor", "payload": {
                "name": f"handoff:{bundle_id}/{task_id}",
                "phase": "task_complete",
                "summary": "Built the thing",
                "facts": [{"claim": "it works"}],
                "code_ref": {"branch": "main", "commit_sha": "abc123", "paths": ["src/x.py"]},
            }},
        ],
    })

    # Create a real bundle so workspace can select it
    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "workspace fork anchor test",
        "requested_by": "human_1",
        "tasks": [{"title": "task ws"}],
    })
    real_bundle_id = resp.json()["bundle"]["id"]
    real_task_id = resp.json()["tasks"][0]["id"]

    # Push fork entries under the real bundle_id
    await client.post(f"/api/v1/tapes/{recipe_id}/fork-entries", json={
        "bundle_id": real_bundle_id,
        "task_id": real_task_id,
        "entries": [
            {"kind": "anchor", "payload": {
                "name": f"handoff:{real_bundle_id}/{real_task_id}",
                "phase": "task_complete",
                "summary": "Real task completed",
                "facts": [{"claim": "workspace shows anchors"}],
            }},
        ],
    })

    # Workspace endpoint should include fork_anchors
    resp = await client.get(
        f"/api/v1/recipes/{recipe_id}/workspace",
        params={"bundle_id": real_bundle_id},
    )
    assert resp.status_code == 200
    sb = resp.json().get("selected_bundle", {})
    fork_anchors = sb.get("fork_anchors", [])
    assert len(fork_anchors) >= 1, f"expected fork_anchors, got {fork_anchors}"
    assert fork_anchors[0]["payload"]["summary"] == "Real task completed"


@pytest.mark.asyncio
async def test_tapes_route_accepts_cookie_auth(cookie_client):
    """Tapes route accepts krew_session cookie (not just Bearer/API key)."""
    resp = await cookie_client.get("/api/v1/tapes")
    assert resp.status_code == 200
    assert "tapes" in resp.json()
