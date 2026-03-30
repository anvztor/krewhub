from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_create_recipe(client):
    resp = await client.post("/api/v1/recipes", json={
        "name": "platform/core",
        "repo_url": "git@github.com:org/core.git",
        "default_branch": "main",
        "created_by": "human_1",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["recipe"]["name"] == "platform/core"
    assert data["recipe"]["id"].startswith("rec_")


@pytest.mark.asyncio
async def test_list_recipes(client):
    await client.post("/api/v1/recipes", json={
        "name": "platform/web",
        "repo_url": "git@github.com:org/web.git",
        "created_by": "human_1",
    })
    resp = await client.get("/api/v1/recipes")
    assert resp.status_code == 200
    recipes = resp.json()["recipes"]
    assert len(recipes) >= 1


@pytest.mark.asyncio
async def test_get_recipe_detail(client):
    create = await client.post("/api/v1/recipes", json={
        "name": "platform/detail",
        "repo_url": "git@github.com:org/detail.git",
        "created_by": "human_1",
    })
    recipe_id = create.json()["recipe"]["id"]

    resp = await client.get(f"/api/v1/recipes/{recipe_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["recipe"]["id"] == recipe_id
    assert len(data["members"]) == 1  # owner auto-added


@pytest.mark.asyncio
async def test_invite_member(client):
    create = await client.post("/api/v1/recipes", json={
        "name": "platform/invite",
        "repo_url": "git@github.com:org/invite.git",
        "created_by": "human_1",
    })
    recipe_id = create.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/members", json={
        "actor_id": "human_2",
        "actor_type": "human",
        "role": "member",
    })
    assert resp.status_code == 200
    assert resp.json()["member"]["actor_id"] == "human_2"


@pytest.mark.asyncio
async def test_recipe_not_found(client):
    resp = await client.get("/api/v1/recipes/nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_auth_required(client):
    transport = client._transport
    from httpx import AsyncClient as AC
    async with AC(transport=transport, base_url="http://test") as no_auth:
        resp = await no_auth.get("/api/v1/recipes")
        assert resp.status_code == 401
