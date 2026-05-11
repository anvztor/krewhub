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


# ---------------------------------------------------------------------------
# reprovision_for_bundle — Anthropic Managed Agents `provision({resources})`
# Triggered by SandboxHand when a Hand op detects a dead sandbox.
# ---------------------------------------------------------------------------


async def _seed_bundle_with_sandbox(
    db,
    bundle_id: str = "b1",
    sandbox_id: str = "sbx_dead_xyz",
    template: str = "base",
) -> None:
    await _seed_task(db)  # creates b1 + task
    # Set bundle.sandbox_id, bundle.owner_account_id
    await db.execute(
        "UPDATE bundles SET sandbox_id = ?, owner_account_id = ? WHERE id = ?",
        (sandbox_id, "alice", bundle_id),
    )
    await db.execute(
        "INSERT OR IGNORE INTO sandboxes (id, task_id, bundle_id, "
        "owner_account_id, e2b_sandbox_id, template, status, created_at, "
        "updated_at, terminated_at, last_event_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            sandbox_id, bundle_id, bundle_id, "alice",
            "e2b_dead_id", template, "ready",
            "2026-05-09T00:00:00+00:00", "2026-05-09T00:00:00+00:00",
            None, "2026-05-09T00:00:00+00:00",
        ),
    )
    await db.commit()


@pytest.mark.asyncio
async def test_reprovision_terminates_old_and_creates_new(test_db):
    """The dead sandbox row gets status='terminated'; a fresh row is
    inserted with the same template + bundle_id; bundle.sandbox_id is
    atomically swapped to point at the fresh one."""
    await _seed_bundle_with_sandbox(test_db)
    e2b = AsyncMock()
    e2b.create_sandbox.return_value = "e2b_fresh_id"
    e2b.terminate.return_value = None
    svc = SandboxService(test_db, e2b)

    fresh = await svc.reprovision_for_bundle("b1")

    # Fresh sandbox returned + has same template + bundle_id
    assert fresh.template == "base"
    assert fresh.bundle_id == "b1"
    assert fresh.e2b_sandbox_id == "e2b_fresh_id"
    assert fresh.id != "sbx_dead_xyz"

    # Old sandbox row is terminated
    cursor = await test_db.execute(
        "SELECT status FROM sandboxes WHERE id = ?", ("sbx_dead_xyz",),
    )
    old_row = await cursor.fetchone()
    assert old_row["status"] == "terminated"

    # Bundle now points at fresh sandbox
    cursor = await test_db.execute(
        "SELECT sandbox_id FROM bundles WHERE id = ?", ("b1",),
    )
    bundle_row = await cursor.fetchone()
    assert bundle_row["sandbox_id"] == fresh.id


@pytest.mark.asyncio
async def test_reprovision_carries_template_forward(test_db):
    """If the prior sandbox was provisioned with a non-default template,
    the fresh one inherits it. Otherwise reprovision would silently
    downgrade a customized environment to plain `base`."""
    await _seed_bundle_with_sandbox(test_db, template="python-ml")
    e2b = AsyncMock()
    e2b.create_sandbox.return_value = "e2b_fresh_id"
    svc = SandboxService(test_db, e2b)

    fresh = await svc.reprovision_for_bundle("b1")
    assert fresh.template == "python-ml"
    e2b.create_sandbox.assert_awaited_once_with(template="python-ml")


@pytest.mark.asyncio
async def test_reprovision_continues_when_terminate_fails(test_db):
    """The dead sandbox can't be terminated (envd already gone, 502, etc.).
    `reprovision` must continue and create a fresh one anyway — the dead
    sandbox staying alive in DB is preferable to leaving the bundle
    completely sandbox-less."""
    await _seed_bundle_with_sandbox(test_db)
    e2b = AsyncMock()
    e2b.terminate.side_effect = RuntimeError("orchestrator 502")
    e2b.create_sandbox.return_value = "e2b_fresh_id"
    svc = SandboxService(test_db, e2b)

    fresh = await svc.reprovision_for_bundle("b1")
    assert fresh.e2b_sandbox_id == "e2b_fresh_id"


@pytest.mark.asyncio
async def test_reprovision_idempotent_when_bundle_already_replaced(test_db):
    """Two SandboxHand ops detect a dead sandbox simultaneously and both
    call reprovision. The first replaces bundle.sandbox_id; the second
    sees the bundle now points at a fresh, ready sandbox and returns
    THAT instead of provisioning yet a third one. Prevents thundering-herd
    re-provisions when many concurrent ops hit a death."""
    await _seed_bundle_with_sandbox(test_db)

    # Simulate the "first" reprovision having already happened: the
    # original sandbox is terminated, and bundle.sandbox_id points at a
    # different, ready sandbox.
    await test_db.execute(
        "UPDATE sandboxes SET status = 'terminated' WHERE id = ?",
        ("sbx_dead_xyz",),
    )
    await test_db.execute(
        "INSERT INTO sandboxes (id, task_id, bundle_id, owner_account_id, "
        "e2b_sandbox_id, template, status, created_at, updated_at, "
        "terminated_at, last_event_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            "sbx_first_replacement", "b1", "b1", "alice",
            "e2b_first_replacement_id", "base", "ready",
            "2026-05-09T00:01:00+00:00", "2026-05-09T00:01:00+00:00",
            None, "2026-05-09T00:01:00+00:00",
        ),
    )
    await test_db.execute(
        "UPDATE bundles SET sandbox_id = ? WHERE id = ?",
        ("sbx_first_replacement", "b1"),
    )
    await test_db.commit()

    e2b = AsyncMock()
    svc = SandboxService(test_db, e2b)

    # The "second" caller passes the original dead id as its idempotency
    # token. The service sees bundle.sandbox_id has already moved, so
    # returns the fresh replacement instead of provisioning a third one.
    result = await svc.reprovision_for_bundle(
        "b1", dead_sandbox_id="sbx_dead_xyz",
    )
    assert result.id == "sbx_first_replacement"
    e2b.create_sandbox.assert_not_called()


@pytest.mark.asyncio
async def test_reprovision_raises_for_missing_bundle(test_db):
    e2b = AsyncMock()
    svc = SandboxService(test_db, e2b)
    with pytest.raises(ValueError, match="not found"):
        await svc.reprovision_for_bundle("does_not_exist")


# ---------------------------------------------------------------------------
# ensure_sandbox_for_bundle — Slice A: bridge calls this on bare
# `target: "sandbox"`. Idempotent platform-side guarantee that the
# bundle has a ready sandbox; brain never sees substrate state.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_returns_existing_when_ready(test_db):
    """If the bundle's current sandbox is ready in db, just return it.
    No e2b API calls."""
    await _seed_bundle_with_sandbox(test_db)
    e2b = AsyncMock()
    svc = SandboxService(test_db, e2b)

    result = await svc.ensure_sandbox_for_bundle("b1")
    assert result.id == "sbx_dead_xyz"  # the seeded ready sandbox
    e2b.create_sandbox.assert_not_called()
    e2b.terminate.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_provisions_when_bundle_has_no_sandbox(test_db):
    """Brand new bundle without a sandbox — ensure provisions one
    transparently."""
    await _seed_task(test_db)  # bundle b1 with sandbox_id=NULL by default
    await test_db.execute(
        "UPDATE bundles SET owner_account_id = ? WHERE id = ?",
        ("alice", "b1"),
    )
    await test_db.commit()
    e2b = AsyncMock()
    e2b.create_sandbox.return_value = "e2b_brand_new"
    svc = SandboxService(test_db, e2b)

    result = await svc.ensure_sandbox_for_bundle("b1")
    assert result.bundle_id == "b1"
    assert result.e2b_sandbox_id == "e2b_brand_new"
    e2b.create_sandbox.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_reprovisions_when_current_is_terminated(test_db):
    """Current sandbox row is terminated (e2b VM gone, sweeper marked
    it). Ensure provides a fresh one without operator involvement."""
    await _seed_bundle_with_sandbox(test_db)
    # Mark the seeded sandbox terminated (simulates sweeper).
    await test_db.execute(
        "UPDATE sandboxes SET status = 'terminated' WHERE id = ?",
        ("sbx_dead_xyz",),
    )
    await test_db.commit()
    e2b = AsyncMock()
    e2b.create_sandbox.return_value = "e2b_fresh_after_term"
    svc = SandboxService(test_db, e2b)

    result = await svc.ensure_sandbox_for_bundle("b1")
    assert result.e2b_sandbox_id == "e2b_fresh_after_term"
    assert result.status == "ready"


@pytest.mark.asyncio
async def test_ensure_raises_for_missing_bundle(test_db):
    e2b = AsyncMock()
    svc = SandboxService(test_db, e2b)
    with pytest.raises(ValueError, match="not found"):
        await svc.ensure_sandbox_for_bundle("does_not_exist")
