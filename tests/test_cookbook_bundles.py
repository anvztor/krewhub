"""Tests for Phase 12 step (c): cookbook-scoped bundle routes + dual-write.

These exercise the new POST/GET /cookbooks/{id}/bundles endpoints,
verify dual-write of cookbook_id at create time, and confirm that
list_by_cookbook returns the right rows independent of the legacy
recipe path.
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
GUEST_ACCOUNT = "acc_guest_c"
STRANGER_ACCOUNT = "acc_stranger_c"


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


async def _seed_cookbook_owned_by_legacy_apikey() -> str:
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
        ("cb_step_c_1", "Step C Cookbook", OWNER_ACCOUNT, now),
    )
    await db.commit()
    return "cb_step_c_1"


# --------------------------------------------------------------------------
# POST /cookbooks/{id}/bundles
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_owner_creates_cookbook_scoped_bundle(client):
    """Happy path: owner creates a cookbook-scoped bundle. recipe_id
    is NULL on the resulting row; cookbook_id is set."""
    cookbook_id = await _seed_cookbook_owned_by_legacy_apikey()

    resp = await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/bundles",
        json={
            "prompt": "Fix the login flow",
            "tasks": [{"title": "Investigate", "description": "look at auth.ts"}],
            "autoplan": False,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    bundle = body["bundle"]
    assert bundle["cookbook_id"] == cookbook_id
    assert bundle["recipe_id"] is None
    assert bundle["status"] == "open"
    assert len(body["tasks"]) == 1

    # Verify the row in the DB carries the expected nullability.
    db = await get_db()
    cursor = await db.execute(
        "SELECT recipe_id, cookbook_id FROM bundles WHERE id = ?",
        (bundle["id"],),
    )
    row = await cursor.fetchone()
    assert row["recipe_id"] is None
    assert row["cookbook_id"] == cookbook_id


@pytest.mark.asyncio
async def test_member_can_create_bundle(client, guest_client):
    """A MEMBER share is enough to create bundles."""
    cookbook_id = await _seed_cookbook_owned_by_legacy_apikey()
    # Owner adds guest as member
    await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "member"},
    )

    resp = await guest_client.post(
        f"/api/v1/cookbooks/{cookbook_id}/bundles",
        json={"prompt": "Member's mission", "tasks": []},
    )
    assert resp.status_code == 200, resp.text


@pytest.mark.asyncio
async def test_viewer_cannot_create_bundle(client, guest_client):
    """VIEWER is read-only; cannot trigger bundles."""
    cookbook_id = await _seed_cookbook_owned_by_legacy_apikey()
    await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "viewer"},
    )

    resp = await guest_client.post(
        f"/api/v1/cookbooks/{cookbook_id}/bundles",
        json={"prompt": "Should be denied", "tasks": []},
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_non_member_cannot_create_bundle(stranger_client):
    cookbook_id = await _seed_cookbook_owned_by_legacy_apikey()
    resp = await stranger_client.post(
        f"/api/v1/cookbooks/{cookbook_id}/bundles",
        json={"prompt": "No access", "tasks": []},
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_404_for_unknown_cookbook(client):
    resp = await client.post(
        "/api/v1/cookbooks/cb_does_not_exist/bundles",
        json={"prompt": "ghost", "tasks": []},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_cookbook_bundle_with_repo_spec(client):
    """repo_spec persists on the bundle row for the JIT clone path."""
    cookbook_id = await _seed_cookbook_owned_by_legacy_apikey()
    repo_spec = {
        "provider": "github",
        "owner": "octo",
        "repo": "demo",
        "ref": "main",
    }
    resp = await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/bundles",
        json={"prompt": "build", "tasks": [], "repo_spec": repo_spec},
    )
    assert resp.status_code == 200, resp.text
    bundle = resp.json()["bundle"]
    assert bundle["repo_spec"] == repo_spec


# --------------------------------------------------------------------------
# GET /cookbooks/{id}/bundles
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_returns_only_cookbook_bundles(client):
    """list_by_cookbook should NOT bleed bundles from other cookbooks."""
    cookbook_a = await _seed_cookbook_owned_by_legacy_apikey()
    # Create cookbook B
    db = await get_db()
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
        ("cb_step_c_2", "Other Cookbook", OWNER_ACCOUNT, now),
    )
    await db.commit()
    cookbook_b = "cb_step_c_2"

    await client.post(
        f"/api/v1/cookbooks/{cookbook_a}/bundles",
        json={"prompt": "in A", "tasks": []},
    )
    await client.post(
        f"/api/v1/cookbooks/{cookbook_b}/bundles",
        json={"prompt": "in B", "tasks": []},
    )

    resp_a = await client.get(f"/api/v1/cookbooks/{cookbook_a}/bundles")
    assert resp_a.status_code == 200
    bundles_a = resp_a.json()["bundles"]
    assert len(bundles_a) == 1
    assert bundles_a[0]["prompt"] == "in A"

    resp_b = await client.get(f"/api/v1/cookbooks/{cookbook_b}/bundles")
    bundles_b = resp_b.json()["bundles"]
    assert len(bundles_b) == 1
    assert bundles_b[0]["prompt"] == "in B"


@pytest.mark.asyncio
async def test_viewer_can_list_bundles(client, guest_client):
    """VIEWER can read the list but not create — matches the RBAC matrix."""
    cookbook_id = await _seed_cookbook_owned_by_legacy_apikey()
    await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "viewer"},
    )
    await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/bundles",
        json={"prompt": "for the viewer", "tasks": []},
    )

    resp = await guest_client.get(f"/api/v1/cookbooks/{cookbook_id}/bundles")
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["bundles"]) == 1


# --------------------------------------------------------------------------
# Dual-write verification — legacy /recipes/{id}/bundles populates cookbook_id
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_legacy_recipe_route_still_stamps_cookbook_id(client):
    """A bundle created via the legacy /recipes/{id}/bundles route
    should have its cookbook_id populated from recipe.cookbook_id —
    that's the dual-write commitment."""
    cookbook_id = await _seed_cookbook_owned_by_legacy_apikey()

    # Seed a recipe inside the cookbook so the legacy route works.
    db = await get_db()
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT INTO recipes (id, name, repo_url, default_branch, "
        "created_by, created_at, cookbook_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            "rec_step_c_1", "Legacy Recipe", "https://example.com/repo.git",
            "main", OWNER_ACCOUNT, now, cookbook_id,
        ),
    )
    await db.commit()

    resp = await client.post(
        "/api/v1/recipes/rec_step_c_1/bundles",
        json={
            "prompt": "via legacy route",
            "requested_by": OWNER_ACCOUNT,
            "tasks": [],
        },
    )
    assert resp.status_code == 200, resp.text
    bundle = resp.json()["bundle"]
    assert bundle["recipe_id"] == "rec_step_c_1"
    assert bundle["cookbook_id"] == cookbook_id
