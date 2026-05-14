"""Verifies the additive migration adds tasks.updated_at and backfills it."""
from __future__ import annotations

import aiosqlite
import pytest

from krewhub.db.migrations import run_migrations


@pytest.mark.asyncio
async def test_updated_at_added_and_backfilled(tmp_path):
    db_path = tmp_path / "test.db"

    # Pre-create a DB shaped like the OLD schema (no updated_at on tasks).
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("""
            CREATE TABLE bundles (
                id TEXT PRIMARY KEY,
                cookbook_id TEXT,
                prompt TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'open',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                claimed_at TEXT, cooked_at TEXT, digested_at TEXT,
                blocked_reason TEXT,
                resource_version INTEGER NOT NULL DEFAULT 1,
                generation INTEGER NOT NULL DEFAULT 1
            )
        """)
        await db.execute("""
            CREATE TABLE tasks (
                id TEXT PRIMARY KEY,
                bundle_id TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                depends_on_task_ids TEXT NOT NULL DEFAULT '[]',
                claimed_at TEXT,
                completed_at TEXT,
                resource_version INTEGER NOT NULL DEFAULT 1,
                generation INTEGER NOT NULL DEFAULT 1
            )
        """)
        await db.execute(
            "INSERT INTO bundles (id, created_by, created_at) VALUES "
            "('b1', 'u1', '2026-05-01T00:00:00+00:00')",
        )
        await db.execute(
            "INSERT INTO tasks (id, bundle_id, title, claimed_at, completed_at) "
            "VALUES ('t-claimed', 'b1', 'a', '2026-05-02T00:00:00+00:00', NULL),"
            "       ('t-done',    'b1', 'b', NULL, '2026-05-03T00:00:00+00:00'),"
            "       ('t-fresh',   'b1', 'c', NULL, NULL)",
        )
        await db.commit()

    # Run the full migration suite.
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await run_migrations(db)

        # Column exists.
        cur = await db.execute("PRAGMA table_info(tasks)")
        cols = {r["name"] for r in await cur.fetchall()}
        assert "updated_at" in cols

        # Backfill: completed > claimed > bundle.created_at fallback.
        cur = await db.execute("SELECT id, updated_at FROM tasks ORDER BY id")
        rows = {r["id"]: r["updated_at"] for r in await cur.fetchall()}
        assert rows["t-claimed"] == "2026-05-02T00:00:00+00:00"
        assert rows["t-done"] == "2026-05-03T00:00:00+00:00"
        assert rows["t-fresh"] == "2026-05-01T00:00:00+00:00"  # bundle.created_at
