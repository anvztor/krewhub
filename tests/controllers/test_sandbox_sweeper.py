"""SandboxSweeper unit tests — exercise tick() directly."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from krewhub.controllers.sandbox_sweeper import SandboxSweeper
from krewhub.models.sandbox import Sandbox
from krewhub.repositories.sandbox_repo import SandboxRepo


async def _seed_minimal(db) -> None:
    await db.execute(
        "INSERT OR IGNORE INTO cookbooks (id, name, owner_id, created_at) "
        "VALUES (?,?,?,?)",
        ("cb_s", "test", "alice", "2026-01-01"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO bundles (id, cookbook_id, prompt, status, "
        "created_by, created_at) VALUES (?,?,?,?,?,?)",
        ("b_s", "cb_s", "p", "open", "alice", "2026-01-01"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO tasks (id, bundle_id, title, description, "
        "status, depends_on_task_ids, resource_version, generation) "
        "VALUES (?,?,?,?,?,?,?,?)",
        ("t_s", "b_s", "x", "", "open", "[]", 1, 1),
    )
    await db.commit()


@pytest.mark.asyncio
async def test_sweeper_terminates_idle_sandbox(test_db):
    await _seed_minimal(test_db)
    repo = SandboxRepo(test_db)
    now = datetime.now(timezone.utc)
    idle = Sandbox(
        id="sb_idle", task_id="t_s", owner_account_id="alice",
        e2b_sandbox_id="e2b_idle", template="base", status="running",
        created_at=now, updated_at=now,
        last_event_at=now - timedelta(minutes=30),
    )
    await repo.create(idle)

    e2b = AsyncMock()

    async def _get_db_local():
        return test_db

    sweeper = SandboxSweeper(
        get_db=_get_db_local, e2b=e2b,
        idle_seconds=600, max_age_seconds=3600,
    )
    terminated = await sweeper.tick()
    assert terminated == 1
    e2b.terminate.assert_awaited_once_with("e2b_idle")
    after = await repo.get("sb_idle")
    assert after is not None and after.status == "terminated"


@pytest.mark.asyncio
async def test_sweeper_skips_fresh_sandbox(test_db):
    await _seed_minimal(test_db)
    repo = SandboxRepo(test_db)
    now = datetime.now(timezone.utc)
    fresh = Sandbox(
        id="sb_fresh", task_id="t_s", owner_account_id="alice",
        e2b_sandbox_id="e2b_fresh", template="base", status="running",
        created_at=now, updated_at=now, last_event_at=now,
    )
    await repo.create(fresh)

    e2b = AsyncMock()

    async def _get_db_local():
        return test_db

    sweeper = SandboxSweeper(
        get_db=_get_db_local, e2b=e2b,
        idle_seconds=600, max_age_seconds=3600,
    )
    terminated = await sweeper.tick()
    assert terminated == 0
    e2b.terminate.assert_not_awaited()


@pytest.mark.asyncio
async def test_sweeper_terminates_aged_sandbox(test_db):
    await _seed_minimal(test_db)
    repo = SandboxRepo(test_db)
    now = datetime.now(timezone.utc)
    aged = Sandbox(
        id="sb_aged", task_id="t_s", owner_account_id="alice",
        e2b_sandbox_id="e2b_aged", template="base", status="ready",
        created_at=now - timedelta(hours=2),
        updated_at=now - timedelta(hours=2),
        last_event_at=now,  # not idle, but aged
    )
    await repo.create(aged)

    e2b = AsyncMock()

    async def _get_db_local():
        return test_db

    sweeper = SandboxSweeper(
        get_db=_get_db_local, e2b=e2b,
        idle_seconds=600, max_age_seconds=3600,
    )
    terminated = await sweeper.tick()
    assert terminated == 1
    e2b.terminate.assert_awaited_once_with("e2b_aged")
