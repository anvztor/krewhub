"""Tests for POST /api/v1/me/init-workspace.

First-time web users (those landing on beta.cookrew.dev BEFORE
running `krewcli login`) used to get stuck on "recipe still loading"
because no cookbook existed for their account. The endpoint is the
server-side equivalent of krewcli's _ensure_cookbook + _ensure_recipe
auto-bootstrap, callable from the SPA on auth load.
"""
from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_init_creates_cookbook_for_new_user(cookie_client: AsyncClient):
    """Step (e): init-workspace creates cookbook only — recipes are gone."""
    r = await cookie_client.post("/api/v1/me/init-workspace")
    assert r.status_code == 200, r.text
    body = r.json()

    assert body["cookbook"]["name"] == "my-cookbook"
    assert body["cookbook"]["owner_id"] == "acc_test_cookie"
    assert "recipe" not in body


@pytest.mark.asyncio
async def test_init_is_idempotent(cookie_client: AsyncClient):
    """Calling /init-workspace twice returns the same cookbook."""
    first = await cookie_client.post("/api/v1/me/init-workspace")
    assert first.status_code == 200, first.text
    second = await cookie_client.post("/api/v1/me/init-workspace")
    assert second.status_code == 200, second.text

    assert first.json()["cookbook"]["id"] == second.json()["cookbook"]["id"]


@pytest.mark.asyncio
async def test_init_requires_auth(anon_client: AsyncClient):
    r = await anon_client.post("/api/v1/me/init-workspace")
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_init_reuses_existing_cookbook_named_my_cookbook(
    cookie_client: AsyncClient,
):
    """If the user already created a cookbook named 'my-cookbook' via
    krewcli, /init-workspace must reuse it (not create a duplicate)."""
    # First call creates everything.
    first = await cookie_client.post("/api/v1/me/init-workspace")
    cb_id = first.json()["cookbook"]["id"]

    # The endpoint resolves by (name, owner_id) — second call hits the
    # find-existing branch and returns the same id.
    second = await cookie_client.post("/api/v1/me/init-workspace")
    assert second.json()["cookbook"]["id"] == cb_id
