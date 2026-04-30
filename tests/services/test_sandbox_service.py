"""SandboxService tests."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from krewhub.services.sandbox_service import SandboxService


async def _seed_task(db, task_id: str = "t1") -> None:
    await db.execute(
        "INSERT OR IGNORE INTO cookbooks (id, name, owner_id, created_at) "
        "VALUES (?,?,?,?)",
        ("cb1", "test", "alice", "2026-01-01"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO recipes (id, name, repo_url, default_branch, "
        "created_by, created_at, cookbook_id) VALUES (?,?,?,?,?,?,?)",
        ("r1", "test", "https://example", "main", "alice", "2026-01-01", "cb1"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO bundles (id, recipe_id, prompt, status, "
        "created_by, created_at) VALUES (?,?,?,?,?,?)",
        ("b1", "r1", "p", "open", "alice", "2026-01-01"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO tasks (id, bundle_id, title, description, "
        "status, depends_on_task_ids, resource_version, generation) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (task_id, "b1", "x", "", "open", "[]", 1, 1),
    )
    await db.commit()


@pytest.mark.asyncio
async def test_create_for_task_persists_and_returns(test_db):
    await _seed_task(test_db, "t1")
    e2b = AsyncMock()
    e2b.create_sandbox.return_value = "e2b_xyz"
    svc = SandboxService(test_db, e2b)
    sb = await svc.create_for_task(
        task_id="t1", owner_account_id="alice", template="base",
    )
    assert sb.e2b_sandbox_id == "e2b_xyz"
    assert sb.status == "ready"
    assert sb.task_id == "t1"
    assert sb.owner_account_id == "alice"
    e2b.create_sandbox.assert_awaited_once_with(template="base")

    # And it persisted to the DB.
    cursor = await test_db.execute(
        "SELECT * FROM sandboxes WHERE id = ?", (sb.id,),
    )
    row = await cursor.fetchone()
    assert row is not None
    assert row["e2b_sandbox_id"] == "e2b_xyz"


@pytest.mark.asyncio
async def test_create_for_task_marks_error_on_e2b_failure(test_db):
    await _seed_task(test_db, "t1")
    e2b = AsyncMock()
    e2b.create_sandbox.side_effect = RuntimeError("boom")
    svc = SandboxService(test_db, e2b)
    with pytest.raises(RuntimeError, match="boom"):
        await svc.create_for_task(
            task_id="t1", owner_account_id="alice", template="base",
        )
    # No sandbox row should be left behind on failure.
    cursor = await test_db.execute(
        "SELECT COUNT(*) AS n FROM sandboxes WHERE task_id = 't1'"
    )
    row = await cursor.fetchone()
    assert row["n"] == 0


@pytest.mark.asyncio
async def test_terminate_calls_e2b_and_marks_terminated(test_db):
    await _seed_task(test_db, "t1")
    e2b = AsyncMock()
    e2b.create_sandbox.return_value = "e2b_x"
    svc = SandboxService(test_db, e2b)
    sb = await svc.create_for_task(
        task_id="t1", owner_account_id="alice", template="base",
    )
    await svc.terminate(sb.id)
    e2b.terminate.assert_awaited_once_with("e2b_x")
    cursor = await test_db.execute(
        "SELECT status, terminated_at FROM sandboxes WHERE id = ?", (sb.id,),
    )
    row = await cursor.fetchone()
    assert row["status"] == "terminated"
    assert row["terminated_at"] is not None


@pytest.mark.asyncio
async def test_terminate_idempotent_on_terminated_sandbox(test_db):
    await _seed_task(test_db, "t1")
    e2b = AsyncMock()
    e2b.create_sandbox.return_value = "e2b_x"
    svc = SandboxService(test_db, e2b)
    sb = await svc.create_for_task(
        task_id="t1", owner_account_id="alice", template="base",
    )
    await svc.terminate(sb.id)
    e2b.terminate.reset_mock()
    await svc.terminate(sb.id)  # second call is a no-op
    e2b.terminate.assert_not_called()
