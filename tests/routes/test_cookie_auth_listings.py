"""Regression coverage for cookie auth on /cookbooks and /recipes.

Bug history: both routers were declared with `Depends(resolve_caller)`
(Bearer-only). cookrew-beta SPA hits them with a krewauth_session
cookie and got 401 → workspace stuck on "recipe discovery failed:
cookbooks_401". Same regression hit /agents/runtimes earlier
(commit 0844d8c). These tests pin the cookie path so any future
revert is caught at CI.
"""
from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_cookbooks_list_via_cookie(cookie_client: AsyncClient):
    r = await cookie_client.get(
        "/api/v1/cookbooks",
        params={"owner_id": "acc_test_cookie"},
    )
    assert r.status_code == 200, r.text
    assert "cookbooks" in r.json()


@pytest.mark.asyncio
async def test_cookbooks_list_anon_rejected(anon_client: AsyncClient):
    r = await anon_client.get("/api/v1/cookbooks")
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_cookbook_detail_via_cookie_after_init(cookie_client: AsyncClient):
    """Init creates the cookbook; the SPA's getCookbookDetail call
    (used by resolveActiveRecipeId) must succeed via cookie."""
    init = await cookie_client.post("/api/v1/me/init-workspace")
    cb_id = init.json()["cookbook"]["id"]

    r = await cookie_client.get(f"/api/v1/cookbooks/{cb_id}")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["cookbook"]["id"] == cb_id
    assert any(rec["name"] == "my-recipe" for rec in body["recipes"])


@pytest.mark.asyncio
async def test_recipe_get_via_cookie(cookie_client: AsyncClient):
    init = await cookie_client.post("/api/v1/me/init-workspace")
    rec_id = init.json()["recipe"]["id"]

    r = await cookie_client.get(f"/api/v1/recipes/{rec_id}")
    assert r.status_code == 200, r.text
    assert r.json()["recipe"]["id"] == rec_id


@pytest.mark.asyncio
async def test_recipe_bundles_list_via_cookie(cookie_client: AsyncClient):
    init = await cookie_client.post("/api/v1/me/init-workspace")
    rec_id = init.json()["recipe"]["id"]

    r = await cookie_client.get(f"/api/v1/recipes/{rec_id}/bundles")
    assert r.status_code == 200, r.text
    assert "bundles" in r.json()
