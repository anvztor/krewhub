"""POST /bundles/{id}/tasks — auth track A2 sandbox provisioning."""
from __future__ import annotations

import json

import jwt
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from krewhub.config import get_settings


async def _seed_minimal_recipe_and_bundle(
    db,
    *,
    owner: str,
    bundle_id: str = "BUN_OWN",
    runtime_id: str | None = None,
) -> str:
    await db.execute(
        "INSERT OR IGNORE INTO cookbooks (id, name, owner_id, created_at) "
        "VALUES (?,?,?,?)",
        ("cb_a2", "test", owner, "2026-01-01"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO recipes (id, name, repo_url, default_branch, "
        "created_by, created_at, cookbook_id) VALUES (?,?,?,?,?,?,?)",
        ("r_a2", "test", "https://example", "main", owner, "2026-01-01", "cb_a2"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO bundles (id, recipe_id, prompt, status, "
        "created_by, created_at, owner_account_id, default_agent_runtime_id) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            bundle_id,
            "r_a2",
            "prompt",
            "open",
            owner,
            "2026-01-01",
            owner,
            runtime_id,
        ),
    )
    if runtime_id is not None:
        await db.execute(
            "INSERT OR IGNORE INTO agent_runtimes (id, agent_id, account_id, "
            "host_info, status, last_seen_at, started_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                runtime_id,
                "agent_a2",
                owner,
                "{}",
                "online",
                "2026-01-01",
                "2026-01-01",
            ),
        )
    await db.commit()
    return bundle_id


@pytest_asyncio.fixture
async def cookie_client_alice(_setup_db):
    """A cookie-authed client whose JWT sub is 'alice'."""
    from krewhub.app import create_app

    settings = get_settings()
    token = jwt.encode(
        {"sub": "alice", "username": "alice", "method": "passkey"},
        settings.jwt_secret,
        algorithm="HS256",
    )
    app = create_app()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={"krew_session": token},
    ) as ac:
        yield ac


@pytest_asyncio.fixture
async def cookie_client_bob(_setup_db):
    from krewhub.app import create_app

    settings = get_settings()
    token = jwt.encode(
        {"sub": "bob", "username": "bob", "method": "passkey"},
        settings.jwt_secret,
        algorithm="HS256",
    )
    app = create_app()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={"krew_session": token},
    ) as ac:
        yield ac


@pytest.mark.asyncio
async def test_create_task_provisions_sandbox(
    cookie_client_alice, httpx_mock,
):
    from krewhub.db.connection import get_db
    db = await get_db()
    bundle_id = await _seed_minimal_recipe_and_bundle(
        db, owner="alice", runtime_id="rt_alice",
    )

    httpx_mock.add_response(
        method="POST",
        url="http://10.20.100.214:3000/sandboxes",
        json={"sandboxID": "e2b_x"},
        status_code=201,
    )

    r = await cookie_client_alice.post(
        f"/api/v1/bundles/{bundle_id}/tasks",
        json={"title": "echo hi", "description": ""},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["sandbox"]["e2b_sandbox_id"] == "e2b_x"
    assert body["sandbox"]["status"] == "ready"
    assert body["task"]["sandbox_id"] == body["sandbox"]["id"]
    assert body["task"]["assigned_runtime_id"] == "rt_alice"


@pytest.mark.asyncio
async def test_create_task_rejects_no_paired_agent(cookie_client_alice):
    from krewhub.db.connection import get_db
    db = await get_db()
    bundle_id = await _seed_minimal_recipe_and_bundle(
        db, owner="alice", runtime_id=None, bundle_id="BUN_NO_AGENT",
    )
    r = await cookie_client_alice.post(
        f"/api/v1/bundles/{bundle_id}/tasks",
        json={"title": "x"},
    )
    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert detail["code"] == "no_paired_agent"


@pytest.mark.asyncio
async def test_create_task_denied_for_non_owner(
    cookie_client_alice, cookie_client_bob, httpx_mock,
):
    from krewhub.db.connection import get_db
    db = await get_db()
    # Bundle owned by bob; alice tries to create.
    bundle_id = await _seed_minimal_recipe_and_bundle(
        db, owner="bob", runtime_id="rt_bob", bundle_id="BUN_BOB",
    )
    r = await cookie_client_alice.post(
        f"/api/v1/bundles/{bundle_id}/tasks",
        json={"title": "x"},
    )
    assert r.status_code == 403, r.text
