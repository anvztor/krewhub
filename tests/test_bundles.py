from __future__ import annotations

import pytest


async def _create_recipe(client) -> str:
    resp = await client.post("/api/v1/cookbooks", json={
        "name": "test-bundles-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = resp.json()["cookbook"]["id"]
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/bundles",
        "repo_url": "git@github.com:test/bundles.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    return resp.json()["recipe"]["id"]


@pytest.mark.asyncio
async def test_create_bundle_with_tasks(client):
    recipe_id = await _create_recipe(client)

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Add heartbeat endpoint",
        "requested_by": "human_1",
        "tasks": [
            {"title": "Design API", "depends_on_task_ids": []},
            {"title": "Implement endpoint"},
        ],
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["bundle"]["status"] == "open"
    assert len(data["tasks"]) == 2


@pytest.mark.asyncio
async def test_list_bundles(client):
    recipe_id = await _create_recipe(client)

    await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Bundle A",
        "requested_by": "human_1",
        "tasks": [{"title": "Task A"}],
    })
    await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Bundle B",
        "requested_by": "human_1",
        "tasks": [{"title": "Task B"}],
    })

    resp = await client.get(f"/api/v1/recipes/{recipe_id}/bundles")
    assert resp.status_code == 200
    assert len(resp.json()["bundles"]) == 2


@pytest.mark.asyncio
async def test_get_bundle_detail(client):
    recipe_id = await _create_recipe(client)

    create = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Detail test",
        "requested_by": "human_1",
        "tasks": [{"title": "One task"}],
    })
    bundle_id = create.json()["bundle"]["id"]

    resp = await client.get(f"/api/v1/bundles/{bundle_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["bundle"]["id"] == bundle_id
    assert len(data["tasks"]) == 1
    assert len(data["events"]) >= 2  # prompt + plan events


@pytest.mark.asyncio
async def test_cancel_bundle(client):
    recipe_id = await _create_recipe(client)

    create = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Cancel me",
        "requested_by": "human_1",
        "tasks": [{"title": "Will be cancelled"}],
    })
    bundle_id = create.json()["bundle"]["id"]

    resp = await client.patch(f"/api/v1/bundles/{bundle_id}")
    assert resp.status_code == 200
    assert resp.json()["bundle"]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_add_task_to_bundle(client):
    recipe_id = await _create_recipe(client)

    create = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Expandable",
        "requested_by": "human_1",
        "tasks": [{"title": "Initial task"}],
    })
    bundle_id = create.json()["bundle"]["id"]

    resp = await client.post(f"/api/v1/bundles/{bundle_id}/tasks", json={
        "title": "Added later",
        "description": "A manually added task",
    })
    assert resp.status_code == 200
    assert resp.json()["task"]["title"] == "Added later"


@pytest.mark.asyncio
async def test_create_bundle_requires_existing_recipe(client):
    resp = await client.post("/api/v1/recipes/rec_missing/bundles", json={
        "prompt": "Should fail",
        "requested_by": "human_1",
        "tasks": [{"title": "No parent recipe"}],
    })

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Recipe not found"


@pytest.mark.asyncio
async def test_add_task_requires_existing_bundle(client):
    resp = await client.post("/api/v1/bundles/bun_missing/tasks", json={
        "title": "Should fail",
    })

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Bundle not found"


@pytest.mark.asyncio
async def test_create_bundle_via_cookie_auth(client, cookie_client):
    """Regression: browser clients (post-BFF-elimination) authenticate via the
    krew_session cookie only. The bundles router must accept cookie auth,
    not just Bearer JWT / X-API-Key.
    """
    # Seed the recipe with API-key client (existing path still works).
    recipe_id = await _create_recipe(client)

    # Submit a bundle as the browser would — cookie-only.
    resp = await cookie_client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Cookie-auth submit from Prompt Composer",
        "requested_by": "cookie_tester",
        "tasks": [{"title": "Smoke task"}],
    })
    assert resp.status_code == 200, (
        f"expected 200, got {resp.status_code}: {resp.text}"
    )
    data = resp.json()
    assert data["bundle"]["status"] == "open"
    # Server uses caller identity from cookie JWT, not client-supplied value.
    assert data["bundle"]["created_by"] == "cookie_tester"
