"""Tests for POST /api/v1/tasks/{task_id}/credential-relay.

Covers:
- Successful relay (204)
- Ownership checks (403)
- Non-existent task (404)
- Invocation not on task (404)
- No pending elicit (404)
- Provider mismatch (409)
- Host mismatch (409)
- asyncio.timeout surfaces 504
- Atomic consume — second relay attempt gets 404
- access_token does NOT appear in caplog
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any
from unittest.mock import AsyncMock, patch

import jwt
import pytest
from httpx import ASGITransport, AsyncClient

from krewhub.app import create_app
from krewhub.config import get_settings
from krewhub.db.connection import get_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cookie_headers(account_id: str = "acc_test_relay") -> dict:
    """Return cookie headers for a browser-session caller."""
    settings = get_settings()
    token = jwt.encode(
        {"sub": account_id, "username": "relay_tester", "method": "passkey"},
        settings.jwt_secret,
        algorithm="HS256",
    )
    return {"Cookie": f"krew_session={token}"}


async def _setup_owned_bundle_and_task(
    account_id: str = "acc_test_relay",
) -> tuple[str, str]:
    """Create a cookbook + bundle + task owned by account_id. Returns (bundle_id, task_id)."""
    from uuid import uuid4
    db = await get_db()
    cb_id = f"cb_{uuid4().hex[:8]}"
    b_id = f"b_{uuid4().hex[:8]}"
    t_id = f"t_{uuid4().hex[:8]}"
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
        (cb_id, "relay-test", account_id, "2026-01-01T00:00:00+00:00"),
    )
    await db.execute(
        "INSERT INTO bundles (id, cookbook_id, prompt, status, created_by, created_at, "
        "resource_version, generation, owner_account_id) VALUES (?, ?, '', 'open', ?, ?, 1, 1, ?)",
        (b_id, cb_id, account_id, "2026-01-01T00:00:00+00:00", account_id),
    )
    await db.execute(
        "INSERT INTO tasks (id, bundle_id, title, status, depends_on_task_ids, "
        "resource_version, generation) VALUES (?, ?, 'test', 'open', '[]', 1, 1)",
        (t_id, b_id),
    )
    await db.commit()
    return b_id, t_id


async def _seed_invocation_with_elicit(
    task_id: str,
    account_id: str = "acc_test_relay",
    provider: str = "github",
    host: str = "api.github.com",
) -> tuple[str, str]:
    """Seed an invocation (with task_id) and a pending auth_required elicit row.
    Returns (invocation_id, elicit_id).
    """
    from uuid import uuid4
    db = await get_db()
    inv_id = f"inv_{uuid4().hex[:12]}"
    tape_id = f"tape_{uuid4().hex[:12]}"
    el_id = f"el_{uuid4().hex[:12]}"
    await db.execute(
        "INSERT INTO invocations (id, target_type, target_id, input_json, deadline_s, "
        "tape_id, task_id, status, created_at, created_by) "
        "VALUES (?, 'human', NULL, '{}', 60, ?, ?, 'running', datetime('now'), ?)",
        (inv_id, tape_id, task_id, account_id),
    )
    payload = json.dumps({"provider": provider, "host": host})
    await db.execute(
        "INSERT INTO elicits (id, invocation_id, op, payload_json, status) "
        "VALUES (?, ?, 'auth_required', ?, 'pending')",
        (el_id, inv_id, payload),
    )
    await db.commit()
    return inv_id, el_id


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def app():
    return create_app()


@pytest.fixture
def relay_client(app):
    """Returns an async context manager that yields an httpx AsyncClient."""
    # We yield the app so tests can build their own client with specific headers.
    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_relay_succeeds(app):
    """Happy path: 204 with mocked inject."""
    _bundle_id, task_id = await _setup_owned_bundle_and_task()
    inv_id, el_id = await _seed_invocation_with_elicit(task_id)

    with patch(
        "krewhub.workers.sandbox_hand.SandboxHand.inject_env_one_shot",
        new_callable=AsyncMock,
        return_value=None,
    ):
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        async with AsyncClient(
            transport=transport, base_url="http://test",
            headers=_cookie_headers(),
        ) as client:
            r = await client.post(
                f"/api/v1/tasks/{task_id}/credential-relay",
                json={
                    "invocation_id": inv_id,
                    "elicit_id": el_id,
                    "provider": "github",
                    "host": "api.github.com",
                    "access_token": "ghs_test_token_1234567890",
                    "ttl_s": 300,
                },
            )
    assert r.status_code == 204, r.text


@pytest.mark.asyncio
async def test_relay_404_on_missing_task(app):
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport, base_url="http://test",
        headers=_cookie_headers(),
    ) as client:
        r = await client.post(
            "/api/v1/tasks/task_does_not_exist/credential-relay",
            json={
                "invocation_id": "inv_x",
                "elicit_id": "el_x",
                "provider": "github",
                "host": "api.github.com",
                "access_token": "ghs_test_token_1234567890",
            },
        )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_relay_403_cross_account(app):
    """Task owned by acc_owner; acc_other tries to relay → 403."""
    _b, task_id = await _setup_owned_bundle_and_task(account_id="acc_owner")
    inv_id, el_id = await _seed_invocation_with_elicit(task_id, account_id="acc_owner")

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport, base_url="http://test",
        headers=_cookie_headers(account_id="acc_other"),
    ) as client:
        r = await client.post(
            f"/api/v1/tasks/{task_id}/credential-relay",
            json={
                "invocation_id": inv_id,
                "elicit_id": el_id,
                "provider": "github",
                "host": "api.github.com",
                "access_token": "ghs_test_token_1234567890",
            },
        )
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_relay_401_no_auth(app):
    _b, task_id = await _setup_owned_bundle_and_task()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport, base_url="http://test",
    ) as client:
        r = await client.post(
            f"/api/v1/tasks/{task_id}/credential-relay",
            json={
                "invocation_id": "inv_x",
                "elicit_id": "el_x",
                "provider": "github",
                "host": "api.github.com",
                "access_token": "ghs_test_token_1234567890",
            },
        )
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_relay_403_bundle_no_owner(app):
    """Bundle has no owner_account_id → 403."""
    from uuid import uuid4
    db = await get_db()
    cb_id = f"cb_{uuid4().hex[:8]}"
    b_id = f"b_{uuid4().hex[:8]}"
    t_id = f"t_{uuid4().hex[:8]}"
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
        (cb_id, "no-owner", "acc_test_relay", "2026-01-01T00:00:00+00:00"),
    )
    # owner_account_id intentionally NULL
    await db.execute(
        "INSERT INTO bundles (id, cookbook_id, prompt, status, created_by, created_at, "
        "resource_version, generation) VALUES (?, ?, '', 'open', 'acc_test_relay', ?, 1, 1)",
        (b_id, cb_id, "2026-01-01T00:00:00+00:00"),
    )
    await db.execute(
        "INSERT INTO tasks (id, bundle_id, title, status, depends_on_task_ids, "
        "resource_version, generation) VALUES (?, ?, 'test', 'open', '[]', 1, 1)",
        (t_id, b_id),
    )
    await db.commit()

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport, base_url="http://test",
        headers=_cookie_headers(),
    ) as client:
        r = await client.post(
            f"/api/v1/tasks/{t_id}/credential-relay",
            json={
                "invocation_id": "inv_x",
                "elicit_id": "el_x",
                "provider": "github",
                "host": "api.github.com",
                "access_token": "ghs_test_token_1234567890",
            },
        )
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_relay_404_invocation_not_on_task(app):
    _b, task_id = await _setup_owned_bundle_and_task()
    # Seed invocation with a DIFFERENT task_id
    from uuid import uuid4
    db = await get_db()
    inv_id = f"inv_{uuid4().hex[:12]}"
    tape_id = f"tape_{uuid4().hex[:12]}"
    await db.execute(
        "INSERT INTO invocations (id, target_type, input_json, deadline_s, "
        "tape_id, task_id, status, created_at, created_by) "
        "VALUES (?, 'human', '{}', 60, ?, 'task_other', 'running', datetime('now'), 'acc_test_relay')",
        (inv_id, tape_id),
    )
    await db.commit()

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport, base_url="http://test",
        headers=_cookie_headers(),
    ) as client:
        r = await client.post(
            f"/api/v1/tasks/{task_id}/credential-relay",
            json={
                "invocation_id": inv_id,
                "elicit_id": "el_x",
                "provider": "github",
                "host": "api.github.com",
                "access_token": "ghs_test_token_1234567890",
            },
        )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_relay_404_no_pending_elicit(app):
    _b, task_id = await _setup_owned_bundle_and_task()
    inv_id, el_id = await _seed_invocation_with_elicit(task_id)

    # Resolve the elicit so it's no longer pending.
    db = await get_db()
    await db.execute(
        "UPDATE elicits SET status='resolved' WHERE id=?", (el_id,),
    )
    await db.commit()

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport, base_url="http://test",
        headers=_cookie_headers(),
    ) as client:
        r = await client.post(
            f"/api/v1/tasks/{task_id}/credential-relay",
            json={
                "invocation_id": inv_id,
                "elicit_id": el_id,
                "provider": "github",
                "host": "api.github.com",
                "access_token": "ghs_test_token_1234567890",
            },
        )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_relay_409_provider_mismatch(app):
    _b, task_id = await _setup_owned_bundle_and_task()
    inv_id, el_id = await _seed_invocation_with_elicit(task_id, provider="github")

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport, base_url="http://test",
        headers=_cookie_headers(),
    ) as client:
        r = await client.post(
            f"/api/v1/tasks/{task_id}/credential-relay",
            json={
                "invocation_id": inv_id,
                "elicit_id": el_id,
                "provider": "gitlab",   # wrong provider
                "host": "api.github.com",
                "access_token": "ghs_test_token_1234567890",
            },
        )
    assert r.status_code == 409
    assert "provider" in r.text.lower()


@pytest.mark.asyncio
async def test_relay_409_host_mismatch(app):
    _b, task_id = await _setup_owned_bundle_and_task()
    inv_id, el_id = await _seed_invocation_with_elicit(
        task_id, provider="github", host="api.github.com",
    )

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport, base_url="http://test",
        headers=_cookie_headers(),
    ) as client:
        r = await client.post(
            f"/api/v1/tasks/{task_id}/credential-relay",
            json={
                "invocation_id": inv_id,
                "elicit_id": el_id,
                "provider": "github",
                "host": "gitlab.com",   # wrong host
                "access_token": "ghs_test_token_1234567890",
            },
        )
    assert r.status_code == 409
    assert "host" in r.text.lower()


@pytest.mark.asyncio
async def test_relay_504_on_inject_timeout(app):
    """asyncio.TimeoutError during inject → 504."""
    _b, task_id = await _setup_owned_bundle_and_task()
    inv_id, el_id = await _seed_invocation_with_elicit(task_id)

    async def _slow_inject(**kwargs):
        await asyncio.sleep(999)

    with patch(
        "krewhub.workers.sandbox_hand.SandboxHand.inject_env_one_shot",
        side_effect=_slow_inject,
    ), patch(
        "krewhub.config.get_settings",
        return_value=get_settings().model_copy(update={"sandbox_inject_timeout_s": 0}),
    ):
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        async with AsyncClient(
            transport=transport, base_url="http://test",
            headers=_cookie_headers(),
        ) as client:
            r = await client.post(
                f"/api/v1/tasks/{task_id}/credential-relay",
                json={
                    "invocation_id": inv_id,
                    "elicit_id": el_id,
                    "provider": "github",
                    "host": "api.github.com",
                    "access_token": "ghs_test_token_1234567890",
                },
            )
    assert r.status_code == 504


@pytest.mark.asyncio
async def test_relay_atomic_consume_second_attempt_404(app):
    """After a successful relay, the elicit is resolved — second attempt is 404."""
    _b, task_id = await _setup_owned_bundle_and_task()
    inv_id, el_id = await _seed_invocation_with_elicit(task_id)

    with patch(
        "krewhub.workers.sandbox_hand.SandboxHand.inject_env_one_shot",
        new_callable=AsyncMock,
        return_value=None,
    ):
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        async with AsyncClient(
            transport=transport, base_url="http://test",
            headers=_cookie_headers(),
        ) as client:
            payload = {
                "invocation_id": inv_id,
                "elicit_id": el_id,
                "provider": "github",
                "host": "api.github.com",
                "access_token": "ghs_test_token_1234567890",
            }
            r1 = await client.post(
                f"/api/v1/tasks/{task_id}/credential-relay", json=payload,
            )
            assert r1.status_code == 204

            r2 = await client.post(
                f"/api/v1/tasks/{task_id}/credential-relay", json=payload,
            )
    # Second attempt: elicit is no longer pending → 404
    assert r2.status_code == 404


@pytest.mark.asyncio
async def test_relay_access_token_not_in_caplog(app, caplog):
    """Security regression guard: access_token must NOT appear in any log."""
    _b, task_id = await _setup_owned_bundle_and_task()
    inv_id, el_id = await _seed_invocation_with_elicit(task_id)
    secret_token = "super_secret_ghs_token_1234567890"

    with patch(
        "krewhub.workers.sandbox_hand.SandboxHand.inject_env_one_shot",
        new_callable=AsyncMock,
        return_value=None,
    ), caplog.at_level(logging.DEBUG):
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        async with AsyncClient(
            transport=transport, base_url="http://test",
            headers=_cookie_headers(),
        ) as client:
            await client.post(
                f"/api/v1/tasks/{task_id}/credential-relay",
                json={
                    "invocation_id": inv_id,
                    "elicit_id": el_id,
                    "provider": "github",
                    "host": "api.github.com",
                    "access_token": secret_token,
                },
            )

    # The secret token must not appear in any captured log record.
    for record in caplog.records:
        assert secret_token not in record.getMessage(), (
            f"access_token leaked in log: {record.getMessage()!r}"
        )
