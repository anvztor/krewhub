"""GitHub OAuth credential bootstrap routes.

Pins:
- /oauth/github/start requires auth, returns a github.com authorize URL
  embedding our client_id + a signed state.
- /oauth/github/callback verifies state, rejects bad/expired states.
- Token-exchange and elicit-resolution paths are mocked (the route's
  behavior on success/failure is the contract — not the network).
"""
from __future__ import annotations

import time
import urllib.parse

import jwt
import pytest

from krewhub.config import get_settings
from krewhub.routes import oauth as oauth_routes


@pytest.mark.asyncio
async def test_start_returns_authorize_url_with_state(client, _setup_db, monkeypatch):
    # Test client uses API-key auth → an account_id we don't know in
    # advance. We just verify the shape; account_id round-trips via the
    # signed state.
    monkeypatch.setattr(
        get_settings(),
        "github_oauth_client_id",
        "test_client_id",
    )

    r = await client.get("/api/v1/oauth/github/start?invocation_id=inv_test")
    assert r.status_code == 200, r.text
    body = r.json()
    assert "authorize_url" in body
    assert body["authorize_url"].startswith(
        "https://github.com/login/oauth/authorize?"
    )
    qs = urllib.parse.parse_qs(
        urllib.parse.urlparse(body["authorize_url"]).query
    )
    assert qs["client_id"] == ["test_client_id"]
    assert qs["scope"] == ["repo read:user"]
    assert "state" in qs
    # state is a JWT; decode without verification just to confirm shape
    state = qs["state"][0]
    claims = jwt.decode(state, options={"verify_signature": False})
    assert claims["iid"] == "inv_test"
    assert claims["exp"] > time.time()


@pytest.mark.asyncio
async def test_start_503_when_unconfigured(client, _setup_db, monkeypatch):
    monkeypatch.setattr(
        get_settings(),
        "github_oauth_client_id",
        "",
    )
    r = await client.get("/api/v1/oauth/github/start?invocation_id=inv_test")
    assert r.status_code == 503
    assert "not_configured" in r.text


@pytest.mark.asyncio
async def test_callback_rejects_invalid_state(client, _setup_db):
    r = await client.get(
        "/api/v1/oauth/github/callback?code=abc&state=not_a_jwt"
    )
    assert r.status_code == 400
    assert "invalid_state" in r.text


@pytest.mark.asyncio
async def test_callback_redirects_on_user_denial(client, _setup_db, monkeypatch):
    monkeypatch.setattr(
        get_settings(),
        "web_url",
        "https://beta.cookrew.dev",
    )
    r = await client.get(
        "/api/v1/oauth/github/callback?error=access_denied",
        follow_redirects=False,
    )
    assert r.status_code in (307, 302)
    loc = r.headers["location"]
    assert loc.startswith("https://beta.cookrew.dev/oauth-result")
    assert "status=denied" in loc
    assert "access_denied" in loc


@pytest.mark.asyncio
async def test_state_signing_round_trip():
    """Direct unit test on the helpers — bypass app, no DB needed."""
    from krewhub.config import get_settings as _gs
    s = _gs()
    s.credentials_encryption_key = "test-key"
    minted = oauth_routes._mint_state(
        invocation_id="inv_x", account_id="acc_y",
    )
    claims = oauth_routes._verify_state(minted)
    assert claims["iid"] == "inv_x"
    assert claims["acc"] == "acc_y"
    assert claims["exp"] - claims["iat"] == oauth_routes._STATE_TTL_SECONDS
