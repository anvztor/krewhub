from __future__ import annotations

import pytest


async def _setup_bundle_with_task(client) -> tuple[str, str, str]:
    resp = await client.post("/api/v1/cookbooks", json={
        "name": "test-tasks-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = resp.json()["cookbook"]["id"]
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/tasks",
        "repo_url": "git@github.com:test/tasks.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Task test",
        "requested_by": "human_1",
        "tasks": [{"title": "Claimable task"}],
    })
    bundle_id = resp.json()["bundle"]["id"]
    task_id = resp.json()["tasks"][0]["id"]
    return recipe_id, bundle_id, task_id


@pytest.mark.asyncio
async def test_claim_task(client):
    recipe_id, bundle_id, task_id = await _setup_bundle_with_task(client)

    resp = await client.post(f"/api/v1/tasks/{task_id}/claim", json={
        "agent_id": "agent_alpha",
    })
    assert resp.status_code == 200
    task = resp.json()["task"]
    assert task["status"] == "claimed"
    assert task["claimed_by_agent_id"] == "agent_alpha"


@pytest.mark.asyncio
async def test_post_milestone_event(client):
    recipe_id, bundle_id, task_id = await _setup_bundle_with_task(client)

    await client.post(f"/api/v1/tasks/{task_id}/claim", json={
        "agent_id": "agent_alpha",
    })

    resp = await client.post(f"/api/v1/tasks/{task_id}/events", json={
        "type": "milestone",
        "actor_id": "agent_alpha",
        "body": "Heartbeat endpoint ready.",
        "facts": [{"id": "f1", "claim": "Heartbeat < 30s = online", "captured_by": "agent_alpha"}],
        "code_refs": [{
            "repo_url": "git@github.com:test/tasks.git",
            "branch": "feat/heartbeat",
            "commit_sha": "abc123",
            "paths": ["server/heartbeat.py"],
        }],
    })
    assert resp.status_code == 200
    event = resp.json()["event"]
    assert event["type"] == "milestone"
    assert len(event["facts"]) == 1
    assert len(event["code_refs"]) == 1


@pytest.mark.asyncio
async def test_mark_task_done(client):
    _, _, task_id = await _setup_bundle_with_task(client)

    await client.post(f"/api/v1/tasks/{task_id}/claim", json={
        "agent_id": "agent_alpha",
    })

    resp = await client.patch(f"/api/v1/tasks/{task_id}/status", json={
        "status": "done",
    })
    assert resp.status_code == 200
    assert resp.json()["task"]["status"] == "done"


@pytest.mark.asyncio
async def test_mark_task_blocked(client):
    _, _, task_id = await _setup_bundle_with_task(client)

    await client.post(f"/api/v1/tasks/{task_id}/claim", json={
        "agent_id": "agent_alpha",
    })

    resp = await client.patch(f"/api/v1/tasks/{task_id}/status", json={
        "status": "blocked",
        "blocked_reason": "Missing dependency",
    })
    assert resp.status_code == 200
    assert resp.json()["task"]["status"] == "blocked"


# test_rerun_reopens_blocked_tasks removed in step (d) — the
# /bundles/{id}/rerun route is gone. Bundle BLOCKED state no longer
# exists; per-task rerun belongs on the task layer (reopen_for_rerun).


@pytest.mark.asyncio
async def test_edit_task(client):
    _, _, task_id = await _setup_bundle_with_task(client)

    resp = await client.patch(f"/api/v1/tasks/{task_id}", json={
        "title": "Updated title",
        "description": "New description",
    })
    assert resp.status_code == 200
    assert resp.json()["task"]["title"] == "Updated title"


@pytest.mark.asyncio
async def test_remove_task(client):
    _, _, task_id = await _setup_bundle_with_task(client)

    resp = await client.delete(f"/api/v1/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["removed"] is True


@pytest.mark.asyncio
async def test_dependency_blocks_claim(client):
    resp = await client.post("/api/v1/cookbooks", json={
        "name": "test-deps-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = resp.json()["cookbook"]["id"]
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/deps",
        "repo_url": "git@github.com:test/deps.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Dependency test",
        "requested_by": "human_1",
        "tasks": [
            {"title": "First task"},
            {"title": "Second task", "depends_on_task_ids": ["placeholder"]},
        ],
    })
    tasks = resp.json()["tasks"]
    task_1_id = tasks[0]["id"]
    task_2_id = tasks[1]["id"]

    # Update task 2 to depend on task 1
    await client.patch(f"/api/v1/tasks/{task_2_id}", json={
        "depends_on_task_ids": [task_1_id],
    })

    # Should fail: task 1 not done yet
    resp = await client.post(f"/api/v1/tasks/{task_2_id}/claim", json={
        "agent_id": "agent_alpha",
    })
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_agent_can_only_hold_one_active_task(client):
    resp = await client.post("/api/v1/cookbooks", json={
        "name": "test-one-active-task-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = resp.json()["cookbook"]["id"]
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/one-active-task",
        "repo_url": "git@github.com:test/one-active-task.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Two parallel tasks",
        "requested_by": "human_1",
        "tasks": [
            {"title": "Task one"},
            {"title": "Task two"},
        ],
    })
    task_1_id = resp.json()["tasks"][0]["id"]
    task_2_id = resp.json()["tasks"][1]["id"]

    first_claim = await client.post(f"/api/v1/tasks/{task_1_id}/claim", json={
        "agent_id": "agent_alpha",
    })
    second_claim = await client.post(f"/api/v1/tasks/{task_2_id}/claim", json={
        "agent_id": "agent_alpha",
    })

    assert first_claim.status_code == 200
    assert second_claim.status_code == 400


@pytest.mark.asyncio
async def test_cancel_task_via_cookie_auth(client, cookie_client):
    """Regression: browser cancel-task POST must accept the krew_session
    cookie. Same BFF-elimination gap that affected /bundles routes —
    tasks router was on resolve_caller until Phase 3.
    """
    # Seed via API key (still works), then cancel via cookie.
    resp = await client.post("/api/v1/cookbooks", json={
        "name": "test-task-cookie-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = resp.json()["cookbook"]["id"]
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/task-cookie",
        "repo_url": "git@github.com:test/task-cookie.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    recipe_id = resp.json()["recipe"]["id"]
    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Cancel-via-cookie smoke",
        "requested_by": "human_1",
        "tasks": [{"title": "Will be cancelled"}],
    })
    task_id = resp.json()["tasks"][0]["id"]

    resp = await cookie_client.post(f"/api/v1/tasks/{task_id}/cancel")
    assert resp.status_code == 200, (
        f"expected 200, got {resp.status_code}: {resp.text}"
    )
    assert resp.json()["task"]["status"] == "cancelled"
