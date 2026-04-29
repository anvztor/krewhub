"""Schema tests: sandboxes table + tasks.sandbox_id/assigned_runtime_id columns."""
from __future__ import annotations

import aiosqlite
import pytest

from krewhub.db.schema import SCHEMA_SQL


@pytest.mark.asyncio
async def test_sandboxes_table_exists():
    async with aiosqlite.connect(":memory:") as db:
        db.row_factory = aiosqlite.Row
        await db.executescript(SCHEMA_SQL)
        cursor = await db.execute("PRAGMA table_info(sandboxes)")
        cols = {row["name"] for row in await cursor.fetchall()}
    expected = {
        "id",
        "task_id",
        "owner_account_id",
        "e2b_sandbox_id",
        "template",
        "status",
        "created_at",
        "updated_at",
        "terminated_at",
        "last_event_at",
    }
    assert expected.issubset(cols), f"Missing columns: {expected - cols}"


@pytest.mark.asyncio
async def test_tasks_has_sandbox_columns():
    async with aiosqlite.connect(":memory:") as db:
        db.row_factory = aiosqlite.Row
        await db.executescript(SCHEMA_SQL)
        cursor = await db.execute("PRAGMA table_info(tasks)")
        cols = {row["name"] for row in await cursor.fetchall()}
    assert "assigned_runtime_id" in cols
    assert "sandbox_id" in cols


@pytest.mark.asyncio
async def test_bundles_has_owner_account_id_and_default_runtime():
    """Defensive coordination: A2 ensures these columns exist even if A1 hasn't merged."""
    async with aiosqlite.connect(":memory:") as db:
        db.row_factory = aiosqlite.Row
        await db.executescript(SCHEMA_SQL)
        # Explicitly run the migrations module so idempotent ALTERs apply
        from krewhub.db.migrations import run_migrations
        await run_migrations(db)
        cursor = await db.execute("PRAGMA table_info(bundles)")
        cols = {row["name"] for row in await cursor.fetchall()}
    assert "owner_account_id" in cols
    assert "default_agent_runtime_id" in cols
