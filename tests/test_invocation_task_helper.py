"""Tests for GET /api/v1/invocations/{id}/task.

Covers:
- Returns task_id + elicit_id when invocation is on a task with a pending elicit
- Returns null elicit_id when no pending auth_required elicit exists
- 401 for anonymous callers
- 403 for cross-account callers
- 404 for missing invocation
"""
from __future__ import annotations

import json
from uuid import uuid4

import jwt
import pytest
from httpx import ASGITransport, AsyncClient

from krewhub.app import create_app
from krewhub.config import get_settings
from krewhub.db.connection import get_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cookie_headers(account_id: str = "acc_inv_task_test") -> dict:
    settings = get_settings()
    token = jwt.encode(
        {"sub": account_id, "username": "inv_task_tester", "method": "passkey"},
        settings.jwt_secret,
        algorithm="HS256",
    )
    return {"Cookie": f"krew_session={token}"}


async def _seed_task_invocation_elicit(
    account_id: str = "acc_inv_task_test",
    with_elicit: bool = True,
) -> tuple[str, str, str, str | None]:
    """Seed cookbook+bundle+task+invocation (and optionally an elicit).
    Returns (task_id, invocation_id, tape_id, elicit_id | None).
    """
    db = await get_db()
    cb_id = f"cb_{uuid4().hex[:8]}"
    b_id = f"b_{uuid4().hex[:8]}"
    t_id = f"t_{uuid4().hex[:8]}"
    inv_id = f"inv_{uuid4().hex[:12]}"
    tape_id = f"tape_{uuid4().hex[:12]}"

    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
        (cb_id, "inv-task-test", account_id, "2026-01-01T00:00:00+00:00"),
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
    await db.execute(
        "INSERT INTO invocations (id, target_type, input_json, deadline_s, "
        "tape_id, task_id, status, created_at, created_by) "
        "VALUES (?, 'human', '{}', 60, ?, ?, 'running', datetime('now'), ?)",
        (inv_id, tape_id, t_id, account_id),
    )

    el_id = None
    if with_elicit:
        el_id = f"el_{uuid4().hex[:12]}"
        payload = json.dumps({"provider": "github", "host": "api.github.com"})
        await db.execute(
            "INSERT INTO elicits (id, invocation_id, op, payload_json, status) "
            "VALUES (?, ?, 'auth_required', ?, 'pending')",
            (el_id, inv_id, payload),
        )

    await db.commit()
    return t_id, inv_id, tape_id, el_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invocation_task_returns_task_and_elicit():
    app = create_app()
    task_id, inv_id, _, el_id = await _seed_task_invocation_elicit()

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport, base_url="http://test",
        headers=_cookie_headers(),
    ) as client:
        r = await client.get(f"/api/v1/invocations/{inv_id}/task")

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["task_id"] == task_id
    assert body["elicit_id"] == el_id


@pytest.mark.asyncio
async def test_invocation_task_null_elicit_when_none_pending():
    app = create_app()
    task_id, inv_id, _, _el = await _seed_task_invocation_elicit(with_elicit=False)

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport, base_url="http://test",
        headers=_cookie_headers(),
    ) as client:
        r = await client.get(f"/api/v1/invocations/{inv_id}/task")

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["task_id"] == task_id
    assert body["elicit_id"] is None


@pytest.mark.asyncio
async def test_invocation_task_null_elicit_after_resolved():
    app = create_app()
    _, inv_id, _, el_id = await _seed_task_invocation_elicit(with_elicit=True)

    # Resolve the elicit
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
        r = await client.get(f"/api/v1/invocations/{inv_id}/task")

    assert r.status_code == 200
    assert r.json()["elicit_id"] is None


@pytest.mark.asyncio
async def test_invocation_task_401_anonymous():
    app = create_app()
    _, inv_id, _, _ = await _seed_task_invocation_elicit()

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport, base_url="http://test",
    ) as client:
        r = await client.get(f"/api/v1/invocations/{inv_id}/task")

    assert r.status_code == 401


@pytest.mark.asyncio
async def test_invocation_task_403_cross_account():
    app = create_app()
    _, inv_id, _, _ = await _seed_task_invocation_elicit(account_id="acc_owner")

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport, base_url="http://test",
        headers=_cookie_headers(account_id="acc_stranger"),
    ) as client:
        r = await client.get(f"/api/v1/invocations/{inv_id}/task")

    assert r.status_code == 403


@pytest.mark.asyncio
async def test_invocation_task_404_missing_invocation():
    app = create_app()

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport, base_url="http://test",
        headers=_cookie_headers(),
    ) as client:
        r = await client.get("/api/v1/invocations/inv_does_not_exist/task")

    assert r.status_code == 404
