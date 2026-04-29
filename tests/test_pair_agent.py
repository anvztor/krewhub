"""Tests for POST /bundles/{id}/pair-agent."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import jwt
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient, Request, Response

from krewhub.app import create_app
from krewhub.config import get_settings
from krewhub.db.connection import get_db


async def _seed_bundle(bundle_id: str, owner_account_id: str) -> None:
    db = await get_db()
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT OR IGNORE INTO cookbooks (id, name, owner_id, created_at) "
        "VALUES (?,?,?,?)",
        ("cb-pair", "cb", "owner-x", now),
    )
    await db.execute(
        "INSERT OR IGNORE INTO recipes (id, name, repo_url, default_branch, "
        "created_by, created_at, cookbook_id) VALUES (?,?,?,?,?,?,?)",
        ("r-pair", "r", "x", "main", "owner-x", now, "cb-pair"),
    )
    await db.execute(
        "INSERT INTO bundles "
        "(id, recipe_id, prompt, status, created_by, created_at, "
        "owner_account_id) VALUES (?,?,?,?,?,?,?)",
        (bundle_id, "r-pair", "p", "open", "owner-x", now, owner_account_id),
    )
    await db.commit()


def _cookie_for(account_id: str) -> str:
    settings = get_settings()
    return jwt.encode(
        {"sub": account_id, "method": "passkey"},
        settings.jwt_secret,
        algorithm="HS256",
    )


@pytest_asyncio.fixture
async def alice_client(_setup_db, monkeypatch):
    monkeypatch.setenv("KREWHUB_KREWAUTH_SERVICE_TOKEN", "secret123")
    get_settings.cache_clear()
    token = _cookie_for("alice")
    app = create_app()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={"krewauth_session": token},
    ) as ac:
        yield ac
    get_settings.cache_clear()


class _FakeAsyncClient:
    """Minimal stand-in for httpx.AsyncClient in tests."""

    def __init__(self, response: Response):
        self._response = response
        self.last_request: Request | None = None

    async def __aenter__(self) -> "_FakeAsyncClient":
        return self

    async def __aexit__(self, *exc) -> None:
        return None

    async def post(self, url: str, *, json=None, headers=None, cookies=None) -> Response:
        self.last_request = Request("POST", url, json=json, headers=headers)
        return self._response


@pytest.mark.asyncio
async def test_pair_agent_requires_bundle_owner(alice_client, monkeypatch):
    await _seed_bundle("b-bob", "bob")  # alice tries to pair bob's bundle
    r = await alice_client.post(
        "/bundles/b-bob/pair-agent", json={"user_code": "ABCD-1234"},
    )
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_pair_agent_calls_krewauth_and_sets_default_runtime(
    alice_client, monkeypatch,
):
    await _seed_bundle("b-alice", "alice")

    fake_response = Response(
        200, content=json.dumps({"detail": "approved", "account_id": "alice"}).encode(),
        headers={"content-type": "application/json"},
    )
    fake_client = _FakeAsyncClient(fake_response)

    def _factory(*args, **kwargs):
        return fake_client

    import krewhub.routes.auth_web as mod
    monkeypatch.setattr(mod.httpx, "AsyncClient", _factory)

    r = await alice_client.post(
        "/bundles/b-alice/pair-agent",
        json={"user_code": "ABCD-1234"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert "runtime_id" in body

    db = await get_db()
    cursor = await db.execute(
        "SELECT default_agent_runtime_id FROM bundles WHERE id = ?",
        ("b-alice",),
    )
    row = await cursor.fetchone()
    assert row["default_agent_runtime_id"] == body["runtime_id"]
    cursor = await db.execute(
        "SELECT account_id, agent_id FROM agent_runtimes WHERE id = ?",
        (body["runtime_id"],),
    )
    runtime_row = await cursor.fetchone()
    assert runtime_row["account_id"] == "alice"


@pytest.mark.asyncio
async def test_pair_agent_propagates_krewauth_404(alice_client, monkeypatch):
    await _seed_bundle("b-alice2", "alice")
    fake_response = Response(
        404, content=json.dumps({"detail": "invalid_or_expired_code"}).encode(),
        headers={"content-type": "application/json"},
    )
    fake_client = _FakeAsyncClient(fake_response)

    def _factory(*args, **kwargs):
        return fake_client

    import krewhub.routes.auth_web as mod
    monkeypatch.setattr(mod.httpx, "AsyncClient", _factory)

    r = await alice_client.post(
        "/bundles/b-alice2/pair-agent",
        json={"user_code": "ZZZZ-9999"},
    )
    assert r.status_code == 404
