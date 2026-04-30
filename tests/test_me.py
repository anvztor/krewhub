"""Tests for GET /me (Track A1 contract)."""
from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_me_requires_auth(anon_client: AsyncClient):
    r = await anon_client.get("/me")
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_me_returns_caller_for_api_key(client: AsyncClient):
    r = await client.get("/me")
    assert r.status_code == 200
    body = r.json()
    assert "account_id" in body
    assert body["auth_method"] == "api_key"


@pytest.mark.asyncio
async def test_me_returns_caller_for_cookie(cookie_client: AsyncClient):
    r = await cookie_client.get("/me")
    assert r.status_code == 200
    body = r.json()
    assert body["account_id"] == "acc_test_cookie"
