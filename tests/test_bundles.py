from __future__ import annotations

import pytest


async def _create_cookbook(client) -> str:
    resp = await client.post("/api/v1/cookbooks", json={
        "name": "test-bundles-cookbook",
        "owner_id": "acc_legacy_apikey",
    })
    return resp.json()["cookbook"]["id"]


@pytest.mark.asyncio
async def test_create_bundle_with_tasks(client):
    cookbook_id = await _create_cookbook(client)

    resp = await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
        "prompt": "Add heartbeat endpoint",
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
    cookbook_id = await _create_cookbook(client)

    await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
        "prompt": "Bundle A",
        "tasks": [{"title": "Task A"}],
    })
    await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
        "prompt": "Bundle B",
        "tasks": [{"title": "Task B"}],
    })

    resp = await client.get(f"/api/v1/cookbooks/{cookbook_id}/bundles")
    assert resp.status_code == 200
    assert len(resp.json()["bundles"]) == 2


@pytest.mark.asyncio
async def test_get_bundle_detail(client):
    cookbook_id = await _create_cookbook(client)

    create = await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
        "prompt": "Detail test",
        "tasks": [{"title": "One task"}],
    })
    bundle_id = create.json()["bundle"]["id"]

    resp = await client.get(f"/api/v1/bundles/{bundle_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["bundle"]["id"] == bundle_id
    assert len(data["tasks"]) == 1
    assert len(data["events"]) >= 2  # prompt + plan events


# test_cancel_bundle removed in step (d) — cancel route deleted in
# favor of PATCH /cookbooks/{cb}/bundles/{id}.


@pytest.mark.asyncio
async def test_add_task_to_bundle(client):
    cookbook_id = await _create_cookbook(client)

    create = await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
        "prompt": "Expandable",
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
async def test_add_task_requires_existing_bundle(client):
    resp = await client.post("/api/v1/bundles/bun_missing/tasks", json={
        "title": "Should fail",
    })

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Bundle not found"


@pytest.mark.asyncio
async def test_create_bundle_via_cookie_auth(client, cookie_client):
    """Browser clients authenticate via the krew_session cookie."""
    # Cookbook owner_id must match the cookie's account_id ("acc_test_cookie")
    # for the cookie caller to have OWNER role.
    resp = await client.post("/api/v1/cookbooks", json={
        "name": "cookie-auth-cookbook",
        "owner_id": "acc_test_cookie",
    })
    cookbook_id = resp.json()["cookbook"]["id"]

    resp = await cookie_client.post(
        f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
            "prompt": "Cookie-auth submit",
            "tasks": [{"title": "Smoke task"}],
        },
    )
    assert resp.status_code == 200, (
        f"expected 200, got {resp.status_code}: {resp.text}"
    )
    data = resp.json()
    assert data["bundle"]["status"] == "open"
