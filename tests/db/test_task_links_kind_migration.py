"""Regression: task_links.kind CHECK drop (v3 'drives' convergence).

A DB created before v3 has `kind TEXT NOT NULL CHECK (kind IN
('pipe','subagent'))`. Inserting the v3 'drives' value would raise
`sqlite3.IntegrityError: CHECK constraint failed` (the same shape that
crash-looped events.type, PR #11). `_migrate_task_links_drop_kind_check`
rebuilds the table without the kind CHECK; correctness is enforced in app
code (_VALID_KINDS). These tests pin: legacy rows are preserved, 'drives'
inserts after migration, and the migration is idempotent.
"""
from __future__ import annotations

import aiosqlite
import pytest

from krewhub.db.migrations import _migrate_task_links_drop_kind_check

_COLS = (
    "id, bundle_id, from_task_id, to_task_id, kind, payload_map, "
    "created_by_account, created_by_task, created_at, fired_at, revoked_at"
)


def _task_links_table(with_kind_check: bool) -> str:
    kind_col = "kind TEXT NOT NULL"
    if with_kind_check:
        kind_col += " CHECK (kind IN ('pipe', 'subagent'))"
    return f"""
        CREATE TABLE task_links (
            id                 TEXT PRIMARY KEY,
            bundle_id          TEXT NOT NULL,
            from_task_id       TEXT NOT NULL,
            to_task_id         TEXT NOT NULL,
            {kind_col},
            payload_map        TEXT NOT NULL DEFAULT '{{}}',
            created_by_account TEXT NOT NULL,
            created_by_task    TEXT,
            created_at         TEXT NOT NULL,
            fired_at           TEXT,
            revoked_at         TEXT
        )
    """


async def _insert(db, _id, kind):
    await db.execute(
        f"INSERT INTO task_links ({_COLS}) VALUES "
        "(?, 'bnd', 'a', 'b', ?, '{}', 'acc', NULL, "
        "'2026-06-15T00:00:00+00:00', NULL, NULL)",
        (_id, kind),
    )


@pytest.mark.asyncio
async def test_rebuild_drops_check_and_admits_drives(tmp_path):
    db_path = tmp_path / "legacy.db"
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await db.executescript(_task_links_table(with_kind_check=True))
        await _insert(db, "lnk_pipe", "pipe")
        await _insert(db, "lnk_sub", "subagent")
        await db.commit()

        # Before migration the CHECK rejects 'drives'.
        with pytest.raises(aiosqlite.IntegrityError):
            await _insert(db, "lnk_pre", "drives")
        await db.rollback()

        await _migrate_task_links_drop_kind_check(db)  # must not raise

        # Legacy rows preserved.
        cur = await db.execute("SELECT kind FROM task_links ORDER BY id")
        assert {r[0] for r in await cur.fetchall()} == {"pipe", "subagent"}

        # 'drives' now inserts cleanly.
        await _insert(db, "lnk_drives", "drives")
        await db.commit()
        cur = await db.execute(
            "SELECT kind FROM task_links WHERE id = 'lnk_drives'")
        assert (await cur.fetchone())[0] == "drives"

        # CHECK is gone from the live DDL.
        cur = await db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='task_links'")
        assert "CHECK (kind" not in (await cur.fetchone())[0]


@pytest.mark.asyncio
async def test_migration_idempotent_and_skips_fresh_db(tmp_path):
    db_path = tmp_path / "fresh.db"
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        # Fresh DB: no kind CHECK already (created from v3 schema).
        await db.executescript(_task_links_table(with_kind_check=False))
        await _insert(db, "lnk_drives", "drives")
        await db.commit()

        # Skips cleanly (no CHECK in DDL) and is a no-op on re-run.
        await _migrate_task_links_drop_kind_check(db)
        await _migrate_task_links_drop_kind_check(db)
        cur = await db.execute("SELECT COUNT(*) FROM task_links")
        assert (await cur.fetchone())[0] == 1
