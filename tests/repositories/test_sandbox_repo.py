"""SandboxRepo tests."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from krewhub.models.sandbox import Sandbox
from krewhub.repositories.sandbox_repo import SandboxRepo


async def _seed_task(db, task_id: str = "t1", bundle_id: str = "b1") -> None:
    """Seed cookbook + bundle + task. Step (e): no recipes."""
    await db.execute(
        "INSERT OR IGNORE INTO cookbooks (id, name, owner_id, created_at) "
        "VALUES (?,?,?,?)",
        ("cb1", "test", "alice", "2026-01-01"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO bundles (id, cookbook_id, prompt, status, "
        "created_by, created_at) VALUES (?,?,?,?,?,?)",
        (bundle_id, "cb1", "p", "open", "alice", "2026-01-01"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO tasks (id, bundle_id, title, description, "
        "status, depends_on_task_ids, resource_version, generation) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (task_id, bundle_id, "x", "", "open", "[]", 1, 1),
    )
    await db.commit()


@pytest.mark.asyncio
async def test_create_and_get(test_db):
    await _seed_task(test_db, "t1")
    repo = SandboxRepo(test_db)
    now = datetime.now(timezone.utc)
    sb = Sandbox(
        id=str(uuid.uuid4()),
        task_id="t1",
        owner_account_id="alice",
        e2b_sandbox_id="e2b_x",
        template="base",
        status="provisioning",
        created_at=now,
        updated_at=now,
    )
    await repo.create(sb)
    got = await repo.get(sb.id)
    assert got is not None
    assert got.e2b_sandbox_id == "e2b_x"
    assert got.status == "provisioning"
    assert got.template == "base"


@pytest.mark.asyncio
async def test_update_status_and_mark_event(test_db):
    await _seed_task(test_db, "t2")
    repo = SandboxRepo(test_db)
    now = datetime.now(timezone.utc)
    sb = Sandbox(
        id="sb_a",
        task_id="t2",
        owner_account_id="alice",
        e2b_sandbox_id="e2b_a",
        template="base",
        status="ready",
        created_at=now,
        updated_at=now,
    )
    await repo.create(sb)
    await repo.update_status(sb.id, "running")
    got = await repo.get(sb.id)
    assert got is not None and got.status == "running"

    await repo.mark_event(sb.id)
    got2 = await repo.get(sb.id)
    assert got2 is not None and got2.last_event_at is not None


@pytest.mark.asyncio
async def test_list_idle_or_expired(test_db):
    await _seed_task(test_db, "t3")
    repo = SandboxRepo(test_db)
    now = datetime.now(timezone.utc)
    old = now - timedelta(hours=2)

    fresh = Sandbox(
        id="sb_fresh", task_id="t3", owner_account_id="alice",
        e2b_sandbox_id="e2b_fresh", template="base", status="ready",
        created_at=now, updated_at=now, last_event_at=now,
    )
    stale_idle = Sandbox(
        id="sb_idle", task_id="t3", owner_account_id="alice",
        e2b_sandbox_id="e2b_idle", template="base", status="running",
        created_at=now, updated_at=now,
        last_event_at=now - timedelta(minutes=30),
    )
    aged = Sandbox(
        id="sb_aged", task_id="t3", owner_account_id="alice",
        e2b_sandbox_id="e2b_aged", template="base", status="ready",
        created_at=old, updated_at=old, last_event_at=now,
    )
    terminated = Sandbox(
        id="sb_done", task_id="t3", owner_account_id="alice",
        e2b_sandbox_id="e2b_done", template="base", status="terminated",
        created_at=old, updated_at=old, last_event_at=now,
    )

    for sb in (fresh, stale_idle, aged, terminated):
        await repo.create(sb)

    rows = await repo.list_idle_or_expired(idle_seconds=600, max_age_seconds=3600)
    ids = {sb.id for sb in rows}
    assert "sb_idle" in ids, "stale-idle sandbox must be flagged"
    assert "sb_aged" in ids, "aged sandbox must be flagged"
    assert "sb_fresh" not in ids
    assert "sb_done" not in ids, "terminated sandbox must be skipped"
