"""Regression: graph_terminal_at migration MUST backfill from digested_at.

The graph runner's terminal marker moved from the borrowed `digested_at`
column to a graph-native `graph_terminal_at` column. The cutover is only
safe if the migration backfills graph_terminal_at = digested_at for every
already-terminal bundle BEFORE list_runnable() starts reading the new
column. Without the backfill, every completed bundle would read
graph_terminal_at IS NULL -> re-enter the runnable set -> the alert#34
runaway recurs (the exact failure this whole line of work fixes).

These tests pin: the backfill carries the old marker forward, an
already-terminal bundle stays OUT of list_runnable after migration, a
genuinely-runnable bundle stays IN, and the migration is idempotent /
one-shot.
"""
from __future__ import annotations

import aiosqlite
import pytest

from krewhub.db.migrations import _migrate_bundles_graph_terminal_at
from krewhub.repositories.bundle_repo import BundleRepo

# Pre-migration bundles table: has digested_at, does NOT yet have
# graph_terminal_at. Mirrors the column set _row_to_bundle / list_runnable
# read so we can exercise the real repo after migrating.
_PRE_DDL = """
    CREATE TABLE bundles (
        id TEXT PRIMARY KEY,
        cookbook_id TEXT,
        repo_spec TEXT,
        prompt TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'open',
        created_by TEXT NOT NULL,
        created_at TEXT NOT NULL,
        claimed_at TEXT,
        cooked_at TEXT,
        digested_at TEXT,
        blocked_reason TEXT,
        graph_code TEXT,
        graph_mermaid TEXT,
        resource_version INTEGER NOT NULL DEFAULT 1,
        generation INTEGER NOT NULL DEFAULT 1,
        owner_account_id TEXT,
        default_agent_runtime_id TEXT,
        sandbox_id TEXT
    )
"""


async def _insert(db, _id, *, digested_at=None, graph_code="g=1"):
    await db.execute(
        """INSERT INTO bundles
           (id, prompt, status, created_by, created_at, digested_at, graph_code)
           VALUES (?, 'p', 'open', 'acc', '2026-06-15T00:00:00+00:00', ?, ?)""",
        (_id, digested_at, graph_code),
    )


@pytest.mark.asyncio
async def test_backfill_carries_terminal_marker_forward(tmp_path):
    db_path = tmp_path / "pre.db"
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await db.executescript(_PRE_DDL)
        # A completed-pre-cutover bundle (digested_at stamped) and a
        # genuinely-runnable one (no terminal marker).
        await _insert(db, "bnd_done", digested_at="2026-06-15T00:00:00+00:00")
        await _insert(db, "bnd_runnable", digested_at=None)
        await db.commit()

        await _migrate_bundles_graph_terminal_at(db)

        # Backfill carried the old marker forward...
        cur = await db.execute(
            "SELECT graph_terminal_at, digested_at FROM bundles WHERE id = 'bnd_done'")
        row = await cur.fetchone()
        assert row["graph_terminal_at"] == row["digested_at"] != None  # noqa: E711
        # ...and did NOT invent one for the runnable bundle.
        cur = await db.execute(
            "SELECT graph_terminal_at FROM bundles WHERE id = 'bnd_runnable'")
        assert (await cur.fetchone())["graph_terminal_at"] is None

        # THE anti-runaway guarantee: the already-terminal bundle is excluded
        # from list_runnable; the runnable one is included.
        runnable = [b.id for b in await BundleRepo(db).list_runnable()]
        assert "bnd_done" not in runnable, "terminal bundle re-entered runnable -> runaway"
        assert "bnd_runnable" in runnable


@pytest.mark.asyncio
async def test_migration_is_one_shot_and_idempotent(tmp_path):
    db_path = tmp_path / "idem.db"
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await db.executescript(_PRE_DDL)
        await _insert(db, "bnd_done", digested_at="2026-06-15T00:00:00+00:00")
        await db.commit()

        await _migrate_bundles_graph_terminal_at(db)
        # Simulate an intentional re-queue AFTER migration: clear the new
        # marker but leave the digested_at tombstone. A second migration run
        # must NOT resurrect the marker (one-shot, guarded by column
        # existence) — otherwise a restart would un-runnable a reopened bundle.
        await db.execute(
            "UPDATE bundles SET graph_terminal_at = NULL WHERE id = 'bnd_done'")
        await db.commit()

        await _migrate_bundles_graph_terminal_at(db)  # no-op: column exists
        cur = await db.execute(
            "SELECT graph_terminal_at FROM bundles WHERE id = 'bnd_done'")
        assert (await cur.fetchone())["graph_terminal_at"] is None, (
            "second migration re-stamped the marker -> reopened bundle resurrected"
        )


@pytest.mark.asyncio
async def test_migration_skips_when_column_present(tmp_path):
    """Fresh DBs already have graph_terminal_at (schema.py) — migration is a
    clean no-op with nothing to backfill."""
    db_path = tmp_path / "fresh.db"
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await db.executescript(
            _PRE_DDL.replace(
                "blocked_reason TEXT,", "blocked_reason TEXT,\n        graph_terminal_at TEXT,"
            )
        )
        await _insert(db, "bnd", digested_at="2026-06-15T00:00:00+00:00")
        await db.commit()
        # Should not raise and should not touch graph_terminal_at.
        await _migrate_bundles_graph_terminal_at(db)
        cur = await db.execute("SELECT graph_terminal_at FROM bundles WHERE id = 'bnd'")
        assert (await cur.fetchone())["graph_terminal_at"] is None
