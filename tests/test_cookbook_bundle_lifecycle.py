"""Tests for Phase 12 step (d): bundle OPEN ↔ CLOSED lifecycle.

These cover the new PATCH /cookbooks/{cb}/bundles/{id} endpoint and
the BundleService.close_bundle / reopen_bundle methods that replace
the legacy cancel/rerun/decision triad.
"""

from __future__ import annotations

from datetime import datetime, timezone

import jwt
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from krewhub.app import create_app
from krewhub.config import get_settings
from krewhub.db.connection import get_db

OWNER_ACCOUNT = "acc_legacy_apikey"
GUEST_ACCOUNT = "acc_guest_d"
STRANGER_ACCOUNT = "acc_stranger_d"


def _jwt_for(account_id: str) -> str:
    settings = get_settings()
    return jwt.encode(
        {"sub": account_id, "username": account_id, "method": "passkey"},
        settings.jwt_secret,
        algorithm="HS256",
    )


@pytest_asyncio.fixture
async def guest_client(_setup_db):
    app = create_app()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={"krew_session": _jwt_for(GUEST_ACCOUNT)},
    ) as ac:
        yield ac


@pytest_asyncio.fixture
async def stranger_client(_setup_db):
    app = create_app()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={"krew_session": _jwt_for(STRANGER_ACCOUNT)},
    ) as ac:
        yield ac


async def _seed_cookbook_with_bundle(client) -> tuple[str, str]:
    """Seed cookbook + cookbook-scoped bundle. Returns (cookbook_id, bundle_id)."""
    db = await get_db()
    now = datetime.now(timezone.utc).isoformat()
    for acc in (OWNER_ACCOUNT, GUEST_ACCOUNT, STRANGER_ACCOUNT):
        await db.execute(
            "INSERT OR IGNORE INTO accounts (id, display_name, created_at, updated_at) "
            "VALUES (?, ?, ?, ?)",
            (acc, acc, now, now),
        )
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
        ("cb_d_1", "Step D Cookbook", OWNER_ACCOUNT, now),
    )
    await db.commit()
    cookbook_id = "cb_d_1"

    create = await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/bundles",
        json={"prompt": "lifecycle test", "tasks": [{"title": "t1"}]},
    )
    assert create.status_code == 200
    return cookbook_id, create.json()["bundle"]["id"]


# --------------------------------------------------------------------------
# close_bundle happy path
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_bundle_via_route(client):
    cookbook_id, bundle_id = await _seed_cookbook_with_bundle(client)

    resp = await client.patch(
        f"/api/v1/cookbooks/{cookbook_id}/bundles/{bundle_id}",
        json={"status": "closed", "reason": "no longer needed"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["bundle"]["status"] == "closed"

    # Audit event written
    db = await get_db()
    cursor = await db.execute(
        "SELECT type, body FROM events WHERE bundle_id = ? AND type = 'bundle_closed'",
        (bundle_id,),
    )
    row = await cursor.fetchone()
    assert row is not None
    assert "no longer needed" in row["body"]


@pytest.mark.asyncio
async def test_close_is_idempotent(client):
    """Re-closing a closed bundle is a no-op, not an error."""
    cookbook_id, bundle_id = await _seed_cookbook_with_bundle(client)

    first = await client.patch(
        f"/api/v1/cookbooks/{cookbook_id}/bundles/{bundle_id}",
        json={"status": "closed"},
    )
    assert first.status_code == 200
    second = await client.patch(
        f"/api/v1/cookbooks/{cookbook_id}/bundles/{bundle_id}",
        json={"status": "closed"},
    )
    assert second.status_code == 200
    assert second.json()["bundle"]["status"] == "closed"

    # Exactly one bundle_closed event despite two close calls (the
    # second is a no-op that doesn't emit a duplicate audit row).
    db = await get_db()
    cursor = await db.execute(
        "SELECT COUNT(*) AS n FROM events WHERE bundle_id = ? AND type = 'bundle_closed'",
        (bundle_id,),
    )
    row = await cursor.fetchone()
    assert row["n"] == 1


@pytest.mark.asyncio
async def test_close_does_not_cascade_to_tasks(client):
    """Closing the bundle MUST leave task statuses untouched.

    Tasks live their own lifecycle; bundle is just a label.
    """
    cookbook_id, bundle_id = await _seed_cookbook_with_bundle(client)

    detail_before = await client.get(f"/api/v1/bundles/{bundle_id}")
    tasks_before = detail_before.json()["tasks"]
    statuses_before = [t["status"] for t in tasks_before]

    await client.patch(
        f"/api/v1/cookbooks/{cookbook_id}/bundles/{bundle_id}",
        json={"status": "closed"},
    )

    detail_after = await client.get(f"/api/v1/bundles/{bundle_id}")
    tasks_after = detail_after.json()["tasks"]
    statuses_after = [t["status"] for t in tasks_after]
    assert statuses_before == statuses_after


@pytest.mark.asyncio
async def test_reopen_bundle(client):
    cookbook_id, bundle_id = await _seed_cookbook_with_bundle(client)

    await client.patch(
        f"/api/v1/cookbooks/{cookbook_id}/bundles/{bundle_id}",
        json={"status": "closed"},
    )
    resp = await client.patch(
        f"/api/v1/cookbooks/{cookbook_id}/bundles/{bundle_id}",
        json={"status": "open", "reason": "more work to do"},
    )
    assert resp.status_code == 200
    assert resp.json()["bundle"]["status"] == "open"

    db = await get_db()
    cursor = await db.execute(
        "SELECT body FROM events WHERE bundle_id = ? AND type = 'bundle_reopened'",
        (bundle_id,),
    )
    row = await cursor.fetchone()
    assert row is not None and "more work to do" in row["body"]


# --------------------------------------------------------------------------
# RBAC
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_member_can_close(client, guest_client):
    cookbook_id, bundle_id = await _seed_cookbook_with_bundle(client)
    await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "member"},
    )
    resp = await guest_client.patch(
        f"/api/v1/cookbooks/{cookbook_id}/bundles/{bundle_id}",
        json={"status": "closed"},
    )
    assert resp.status_code == 200, resp.text


@pytest.mark.asyncio
async def test_viewer_cannot_close(client, guest_client):
    cookbook_id, bundle_id = await _seed_cookbook_with_bundle(client)
    await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "viewer"},
    )
    resp = await guest_client.patch(
        f"/api/v1/cookbooks/{cookbook_id}/bundles/{bundle_id}",
        json={"status": "closed"},
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_non_member_cannot_close(stranger_client, client):
    cookbook_id, bundle_id = await _seed_cookbook_with_bundle(client)
    resp = await stranger_client.patch(
        f"/api/v1/cookbooks/{cookbook_id}/bundles/{bundle_id}",
        json={"status": "closed"},
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_404_for_bundle_not_in_cookbook(client):
    """Bundle from a different cookbook -> 404 (don't leak across)."""
    cookbook_a, bundle_a = await _seed_cookbook_with_bundle(client)

    # Create cookbook B owned by the same account
    db = await get_db()
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
        ("cb_d_other", "Other", OWNER_ACCOUNT, now),
    )
    await db.commit()

    resp = await client.patch(
        f"/api/v1/cookbooks/cb_d_other/bundles/{bundle_a}",
        json={"status": "closed"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_invalid_status_400(client):
    cookbook_id, bundle_id = await _seed_cookbook_with_bundle(client)
    resp = await client.patch(
        f"/api/v1/cookbooks/{cookbook_id}/bundles/{bundle_id}",
        json={"status": "cancelled"},  # legacy term — no longer valid
    )
    assert resp.status_code == 400
