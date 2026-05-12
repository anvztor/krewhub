"""ABAC predicate is_assigned_runtime + tasks.py route enforcement."""
from __future__ import annotations

from types import SimpleNamespace

import jwt
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from krewhub.auth import CallerContext, is_assigned_runtime
from krewhub.config import get_settings


@pytest.mark.asyncio
async def test_is_assigned_runtime_true_when_caller_owns_runtime(test_db):
    await test_db.execute(
        "INSERT INTO agent_runtimes (id, agent_id, account_id, host_info, "
        "status, last_seen_at, started_at) VALUES (?,?,?,?,?,?,?)",
        ("rt_a", "agent_a", "alice", "{}", "online", "2026-01-01", "2026-01-01"),
    )
    await test_db.commit()
    caller = CallerContext(account_id="alice", auth_method="passkey")
    task = SimpleNamespace(assigned_runtime_id="rt_a")
    assert await is_assigned_runtime(caller, task, test_db) is True


@pytest.mark.asyncio
async def test_is_assigned_runtime_false_for_other_account(test_db):
    await test_db.execute(
        "INSERT INTO agent_runtimes (id, agent_id, account_id, host_info, "
        "status, last_seen_at, started_at) VALUES (?,?,?,?,?,?,?)",
        ("rt_a", "agent_a", "alice", "{}", "online", "2026-01-01", "2026-01-01"),
    )
    await test_db.commit()
    caller = CallerContext(account_id="bob", auth_method="passkey")
    task = SimpleNamespace(assigned_runtime_id="rt_a")
    assert await is_assigned_runtime(caller, task, test_db) is False


@pytest.mark.asyncio
async def test_is_assigned_runtime_false_when_no_runtime(test_db):
    caller = CallerContext(account_id="alice", auth_method="passkey")
    task = SimpleNamespace(assigned_runtime_id=None)
    assert await is_assigned_runtime(caller, task, test_db) is False


@pytest_asyncio.fixture
async def cookie_client_for_account(_setup_db):
    from krewhub.app import create_app

    settings = get_settings()

    def _client_for(account_id: str):
        token = jwt.encode(
            {"sub": account_id, "method": "passkey"},
            settings.jwt_secret,
            algorithm="HS256",
        )
        app = create_app()
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        return AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"krew_session": token},
        )

    yield _client_for


@pytest.mark.asyncio
async def test_post_event_denied_for_unassigned_caller(cookie_client_for_account):
    from krewhub.db.connection import get_db
    db = await get_db()
    # Seed bundle owned by alice with task assigned to rt_alice
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?,?,?,?)",
        ("cb_x", "x", "alice", "2026-01-01"),
    )
    await db.execute(
        "INSERT INTO bundles (id, cookbook_id, prompt, status, created_by, "
        "created_at, owner_account_id, default_agent_runtime_id) "
        "VALUES (?,?,?,?,?,?,?,?)",
        ("BUN_X", "cb_x", "p", "open", "alice", "2026-01-01", "alice", "rt_alice"),
    )
    await db.execute(
        "INSERT INTO agent_runtimes (id, agent_id, account_id, host_info, "
        "status, last_seen_at, started_at) VALUES (?,?,?,?,?,?,?)",
        ("rt_alice", "agent_a", "alice", "{}", "online", "2026-01-01", "2026-01-01"),
    )
    await db.execute(
        "INSERT INTO tasks (id, bundle_id, title, description, status, "
        "depends_on_task_ids, resource_version, generation, "
        "assigned_runtime_id) VALUES (?,?,?,?,?,?,?,?,?)",
        ("task_x", "BUN_X", "x", "", "open", "[]", 1, 1, "rt_alice"),
    )
    await db.commit()

    async with cookie_client_for_account("bob") as bob:
        r = await bob.post(
            "/api/v1/tasks/task_x/events",
            json={
                "type": "agent_reply",
                "actor_id": "agent_a",
                "actor_type": "agent",
                "body": "hello",
            },
        )
    assert r.status_code == 403, r.text


@pytest.mark.asyncio
async def test_post_event_allowed_for_assigned_runtime_owner(
    cookie_client_for_account,
):
    from krewhub.db.connection import get_db
    db = await get_db()
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?,?,?,?)",
        ("cb_y", "y", "alice", "2026-01-01"),
    )
    await db.execute(
        "INSERT INTO bundles (id, cookbook_id, prompt, status, created_by, "
        "created_at, owner_account_id, default_agent_runtime_id) "
        "VALUES (?,?,?,?,?,?,?,?)",
        ("BUN_Y", "cb_y", "p", "open", "alice", "2026-01-01", "alice", "rt_alice2"),
    )
    await db.execute(
        "INSERT INTO agent_runtimes (id, agent_id, account_id, host_info, "
        "status, last_seen_at, started_at) VALUES (?,?,?,?,?,?,?)",
        (
            "rt_alice2", "agent_a", "alice", "{}", "online",
            "2026-01-01", "2026-01-01",
        ),
    )
    await db.execute(
        "INSERT INTO tasks (id, bundle_id, title, description, status, "
        "depends_on_task_ids, resource_version, generation, "
        "assigned_runtime_id) VALUES (?,?,?,?,?,?,?,?,?)",
        ("task_y", "BUN_Y", "x", "", "open", "[]", 1, 1, "rt_alice2"),
    )
    await db.commit()

    async with cookie_client_for_account("alice") as alice:
        r = await alice.post(
            "/api/v1/tasks/task_y/events",
            json={
                "type": "agent_reply",
                "actor_id": "agent_a",
                "actor_type": "agent",
                "body": "hello",
            },
        )
    assert r.status_code == 200, r.text
