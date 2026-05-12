"""Regression coverage for cookie auth on /cookbooks.

Step (e): /recipes routes are gone; the cookbook-scoped equivalents
must accept cookie auth from the cookrew-beta SPA.
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
    """getCookbookDetail succeeds via cookie."""
    init = await cookie_client.post("/api/v1/me/init-workspace")
    cb_id = init.json()["cookbook"]["id"]

    r = await cookie_client.get(f"/api/v1/cookbooks/{cb_id}")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["cookbook"]["id"] == cb_id


@pytest.mark.asyncio
async def test_cookbook_bundles_list_via_cookie(cookie_client: AsyncClient):
    """List bundles under a cookbook via cookie auth."""
    init = await cookie_client.post("/api/v1/me/init-workspace")
    cb_id = init.json()["cookbook"]["id"]

    r = await cookie_client.get(f"/api/v1/cookbooks/{cb_id}/bundles")
    assert r.status_code == 200, r.text
    assert "bundles" in r.json()
