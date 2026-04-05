from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, Mock, patch

from krewhub.controllers.task_dispatch import TaskDispatchController
from krewhub.db.connection import get_db
from krewhub.models import TaskStatus
from krewhub.repositories.task_repo import TaskRepo
from krewhub.watch.globals import get_watch_service


def _mock_gateway_response(*, state="working", task_id="task-123"):
    resp = Mock()
    resp.status_code = 200
    resp.json.return_value = {
        "result": {"id": task_id, "status": {"state": state}},
    }
    return resp


async def _setup_cookbook_recipe_task(client, *, cookbook_suffix="dispatch"):
    resp = await client.post("/api/v1/cookbooks", json={
        "name": f"test-{cookbook_suffix}-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = resp.json()["cookbook"]["id"]

    resp = await client.post("/api/v1/recipes", json={
        "name": f"test/{cookbook_suffix}",
        "repo_url": f"git@github.com:test/{cookbook_suffix}.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Test dispatch",
        "requested_by": "human_1",
        "tasks": [{"title": "Dispatchable task"}],
    })
    task_id = resp.json()["tasks"][0]["id"]

    return cookbook_id, recipe_id, task_id


# --- TaskDispatchController tests ---


@pytest.mark.asyncio
async def test_dispatch_sends_to_gateway_and_marks_claimed(client):
    db = await get_db()
    watch = get_watch_service()

    cookbook_id, recipe_id, task_id = await _setup_cookbook_recipe_task(
        client, cookbook_suffix="dispatch-claim",
    )

    await client.post("/api/v1/agents/register", json={
        "agent_id": "gw_agent_1",
        "cookbook_id": cookbook_id,
        "display_name": "Gateway Agent",
        "capabilities": ["claim"],
        "endpoint_url": "http://localhost:9000/agents/claude",
        "max_concurrent_tasks": 1,
    })

    controller = TaskDispatchController(db, watch)
    mock_resp = _mock_gateway_response(task_id=task_id)

    with patch.object(controller._http, "post", new_callable=AsyncMock, return_value=mock_resp):
        await controller.reconcile()

    task_repo = TaskRepo(db)
    task = await task_repo.get(task_id)
    assert task.status == TaskStatus.CLAIMED
    assert task.assigned_agent_id == "gw_agent_1"
    assert task.claimed_by_agent_id == "gw_agent_1"
    assert task.claimed_at is not None

    await controller.stop()


@pytest.mark.asyncio
async def test_dispatch_skips_agents_without_endpoint_url(client):
    db = await get_db()
    watch = get_watch_service()

    cookbook_id, recipe_id, task_id = await _setup_cookbook_recipe_task(
        client, cookbook_suffix="dispatch-no-url",
    )

    await client.post("/api/v1/agents/register", json={
        "agent_id": "no_url_agent",
        "cookbook_id": cookbook_id,
        "display_name": "No URL Agent",
        "capabilities": ["claim"],
        "max_concurrent_tasks": 1,
    })

    controller = TaskDispatchController(db, watch)
    await controller.reconcile()

    task_repo = TaskRepo(db)
    task = await task_repo.get(task_id)
    assert task.status == TaskStatus.OPEN
    assert task.assigned_agent_id is None

    await controller.stop()


@pytest.mark.asyncio
async def test_dispatch_respects_dependencies(client):
    db = await get_db()
    watch = get_watch_service()

    resp = await client.post("/api/v1/cookbooks", json={
        "name": "test-dispatch-deps-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = resp.json()["cookbook"]["id"]

    resp = await client.post("/api/v1/recipes", json={
        "name": "test/dispatch-deps",
        "repo_url": "git@github.com:test/dispatch-deps.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Test deps dispatch",
        "requested_by": "human_1",
        "tasks": [
            {"title": "Task A"},
            {"title": "Task B"},
        ],
    })
    task_a_id = resp.json()["tasks"][0]["id"]
    task_b_id = resp.json()["tasks"][1]["id"]

    task_repo = TaskRepo(db)
    await task_repo.update(task_b_id, depends_on_task_ids=[task_a_id])

    await client.post("/api/v1/agents/register", json={
        "agent_id": "gw_deps_agent",
        "cookbook_id": cookbook_id,
        "display_name": "Deps Gateway",
        "capabilities": ["claim"],
        "endpoint_url": "http://localhost:9000/agents/deps",
        "max_concurrent_tasks": 5,
    })

    controller = TaskDispatchController(db, watch)
    mock_resp = _mock_gateway_response()

    with patch.object(controller._http, "post", new_callable=AsyncMock, return_value=mock_resp):
        await controller.reconcile()

    task_a = await task_repo.get(task_a_id)
    task_b = await task_repo.get(task_b_id)
    assert task_a.status == TaskStatus.CLAIMED
    assert task_b.status == TaskStatus.OPEN
    assert task_b.assigned_agent_id is None

    await controller.stop()


@pytest.mark.asyncio
async def test_dispatch_respects_capacity(client):
    db = await get_db()
    watch = get_watch_service()

    resp = await client.post("/api/v1/cookbooks", json={
        "name": "test-dispatch-cap-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = resp.json()["cookbook"]["id"]

    resp = await client.post("/api/v1/recipes", json={
        "name": "test/dispatch-cap",
        "repo_url": "git@github.com:test/dispatch-cap.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    recipe_id = resp.json()["recipe"]["id"]

    await client.post("/api/v1/agents/register", json={
        "agent_id": "gw_cap_agent",
        "cookbook_id": cookbook_id,
        "display_name": "Capacity Gateway",
        "capabilities": ["claim"],
        "endpoint_url": "http://localhost:9000/agents/cap",
        "max_concurrent_tasks": 1,
    })

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Test capacity dispatch",
        "requested_by": "human_1",
        "tasks": [{"title": "Task 1"}, {"title": "Task 2"}],
    })
    task_1_id = resp.json()["tasks"][0]["id"]
    task_2_id = resp.json()["tasks"][1]["id"]

    await client.post(f"/api/v1/tasks/{task_1_id}/claim", json={
        "agent_id": "gw_cap_agent",
    })

    controller = TaskDispatchController(db, watch)
    mock_resp = _mock_gateway_response()

    with patch.object(controller._http, "post", new_callable=AsyncMock, return_value=mock_resp):
        await controller.reconcile()

    task_repo = TaskRepo(db)
    task_2 = await task_repo.get(task_2_id)
    assert task_2.status == TaskStatus.OPEN
    assert task_2.assigned_agent_id is None

    await controller.stop()


# --- A2A Callback route tests ---


async def _create_and_claim_task(client):
    cookbook_id, recipe_id, task_id = await _setup_cookbook_recipe_task(
        client, cookbook_suffix="callback",
    )

    await client.post("/api/v1/agents/register", json={
        "agent_id": "cb_agent",
        "cookbook_id": cookbook_id,
        "display_name": "Callback Agent",
        "capabilities": ["claim"],
    })

    await client.post(f"/api/v1/tasks/{task_id}/claim", json={
        "agent_id": "cb_agent",
    })

    return task_id


@pytest.mark.asyncio
async def test_callback_marks_task_done(client):
    task_id = await _create_and_claim_task(client)

    resp = await client.post("/api/v1/a2a/callback", json={
        "task_id": task_id,
        "agent_id": "cb_agent",
        "success": True,
        "summary": "All tests pass",
    })
    assert resp.status_code == 200

    task = resp.json()["task"]
    assert task["status"] == "done"
    assert task["completed_at"] is not None

    event = resp.json()["event"]
    assert event["task_id"] == task_id
    assert event["type"] == "milestone"


@pytest.mark.asyncio
async def test_callback_marks_task_blocked(client):
    task_id = await _create_and_claim_task(client)

    resp = await client.post("/api/v1/a2a/callback", json={
        "task_id": task_id,
        "agent_id": "cb_agent",
        "success": False,
        "blocked_reason": "dependency unavailable",
    })
    assert resp.status_code == 200

    task = resp.json()["task"]
    assert task["status"] == "blocked"
    assert task["blocked_reason"] == "dependency unavailable"


@pytest.mark.asyncio
async def test_callback_rejects_unknown_task(client):
    resp = await client.post("/api/v1/a2a/callback", json={
        "task_id": "task_nonexistent",
        "agent_id": "cb_agent",
        "success": True,
    })
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_callback_rejects_non_in_progress_task(client):
    cookbook_id, recipe_id, task_id = await _setup_cookbook_recipe_task(
        client, cookbook_suffix="callback-reject",
    )

    resp = await client.post("/api/v1/a2a/callback", json={
        "task_id": task_id,
        "agent_id": "some_agent",
        "success": True,
    })
    assert resp.status_code == 400


# --- Recipe metadata in dispatch tests ---


@pytest.mark.asyncio
async def test_dispatch_includes_recipe_metadata_in_payload(client):
    """Verify A2A dispatch payload includes recipe_name, repo_url, and branch."""
    db = await get_db()
    watch = get_watch_service()

    cookbook_id, recipe_id, task_id = await _setup_cookbook_recipe_task(
        client, cookbook_suffix="dispatch-meta",
    )

    await client.post("/api/v1/agents/register", json={
        "agent_id": "gw_meta_agent",
        "cookbook_id": cookbook_id,
        "display_name": "Meta Gateway",
        "capabilities": ["claim"],
        "endpoint_url": "http://localhost:9000/agents/meta",
        "max_concurrent_tasks": 1,
    })

    controller = TaskDispatchController(db, watch)
    mock_resp = _mock_gateway_response(task_id=task_id)

    captured_payload = None

    async def capture_post(url, json=None, **kwargs):
        nonlocal captured_payload
        captured_payload = json
        return mock_resp

    with patch.object(controller._http, "post", side_effect=capture_post):
        await controller.reconcile()

    assert captured_payload is not None
    metadata = captured_payload["params"]["message"]["metadata"]
    assert metadata["task_id"] == task_id
    assert "recipe_name" in metadata
    assert "repo_url" in metadata
    assert "branch" in metadata
    assert metadata["recipe_name"] == "test/dispatch-meta"
    assert "dispatch-meta.git" in metadata["repo_url"]

    await controller.stop()
