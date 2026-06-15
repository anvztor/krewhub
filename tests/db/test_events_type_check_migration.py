"""Regression: events.type CHECK migration must admit the O1 semantic
event types (progress/blocker/needs_review/needs_human/log).

Incident (testnet3, 2026-06-15): `_migrate_events_add_bundle_lifecycle_types`
rebuilt `events` with a CHECK that OMITTED the five O1 types that schema.py
already allows and OrchController already writes. On any DB that had
accumulated such rows, the row-copy raised
`sqlite3.IntegrityError: CHECK constraint failed` at startup →
CrashLoopBackOff. The guard also never tripped (it keyed on digest_*, which
the rebuild keeps), so the migration re-ran every startup.

These tests pin the fix: the rebuilt CHECK is a superset of schema.py and
the migration is idempotent + re-runnable, additive only (no row deletion).
"""
from __future__ import annotations

import aiosqlite
import pytest

from krewhub.db.migrations import _migrate_events_add_bundle_lifecycle_types

# events_new in the migration copies whatever columns the live table has;
# keep this a subset of the rebuilt column set.
_COLS = (
    "id, bundle_id, task_id, type, actor_id, actor_type, body, payload, "
    "sequence, facts, code_refs, visibility, created_at"
)


def _events_table(check_types_sql: str | None) -> str:
    type_col = "type TEXT NOT NULL"
    if check_types_sql is not None:
        type_col += f" CHECK(type IN ({check_types_sql}))"
    # The rebuilt events table re-declares FK references to these; in prod
    # they always exist. Stub them so FK enforcement (re-enabled at the end
    # of the rebuild) resolves cleanly.
    return f"""
        CREATE TABLE IF NOT EXISTS recipes (id TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS cookbooks (id TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS bundles (id TEXT PRIMARY KEY);
        CREATE TABLE events (
            id TEXT PRIMARY KEY,
            bundle_id TEXT,
            task_id TEXT,
            {type_col},
            actor_id TEXT NOT NULL,
            actor_type TEXT NOT NULL,
            body TEXT NOT NULL DEFAULT '',
            payload TEXT NOT NULL DEFAULT '{{}}',
            sequence INTEGER NOT NULL DEFAULT 0,
            facts TEXT NOT NULL DEFAULT '[]',
            code_refs TEXT NOT NULL DEFAULT '[]',
            visibility TEXT NOT NULL DEFAULT 'system',
            created_at TEXT NOT NULL
        )
    """


async def _insert(db, _id, etype):
    # bundle_id NULL: the rebuilt table re-adds `bundle_id REFERENCES
    # bundles(id)` and turns FK enforcement back on; the minimal fixture has
    # no bundles table, so NULL keeps the FK out of the way (NULL is never
    # FK-checked). The test targets the type CHECK, not FK behaviour.
    await db.execute(
        f"INSERT INTO events ({_COLS}) VALUES "
        "(?, NULL, 't1', ?, 'orch', 'system', '', '{}', 0, '[]', '[]', "
        "'system', '2026-06-15T00:00:00+00:00')",
        (_id, etype),
    )


async def _types(db) -> set[str]:
    cur = await db.execute("SELECT type FROM events ORDER BY id")
    return {r[0] for r in await cur.fetchall()}


@pytest.mark.asyncio
async def test_skips_when_table_already_permits_o1_types(tmp_path):
    """testnet3-shaped DB: the live events CHECK already allows the O1 types
    (so O1 rows exist). The migration must NOT rebuild — skip cleanly,
    preserve every row, and be idempotent (un-brick path)."""
    db_path = tmp_path / "permissive.db"
    permissive = (
        "'prompt','plan','milestone','agent_reply','bundle_closed',"
        "'progress','blocker','needs_review','needs_human','log',"
        "'digest_submitted'"
    )
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await db.executescript(_events_table(permissive))
        await _insert(db, "e_log", "log")
        await _insert(db, "e_blocker", "blocker")
        await _insert(db, "e_needs", "needs_human")
        await db.commit()

        # Must not raise; must preserve rows; idempotent.
        await _migrate_events_add_bundle_lifecycle_types(db)
        await _migrate_events_add_bundle_lifecycle_types(db)
        assert await _types(db) == {"log", "blocker", "needs_human"}


@pytest.mark.asyncio
async def test_rebuild_copies_o1_rows_and_is_idempotent(tmp_path):
    """A DB that DOES trigger a rebuild (no needs_human in the CHECK) while
    holding an O1-typed row. The OLD code raised IntegrityError here; the
    widened CHECK must copy the row cleanly, then be a no-op on re-run."""
    db_path = tmp_path / "rebuild.db"
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        # No type CHECK at all → guard sees no 'needs_human' → rebuild runs,
        # and the table can legally hold an O1 'log' row to be copied.
        await db.executescript(_events_table(None))
        await _insert(db, "e_log", "log")
        await _insert(db, "e_milestone", "milestone")
        await db.commit()

        await _migrate_events_add_bundle_lifecycle_types(db)  # must not raise
        assert await _types(db) == {"log", "milestone"}  # additive: nothing dropped

        # Rebuilt CHECK admits the full O1 vocabulary.
        for et in ("progress", "blocker", "needs_review", "needs_human", "log"):
            await _insert(db, f"post_{et}", et)
        await db.commit()
        cur = await db.execute("SELECT COUNT(*) FROM events")
        assert (await cur.fetchone())[0] == 7

        # Idempotent: re-run is a no-op (guard now sees needs_human).
        await _migrate_events_add_bundle_lifecycle_types(db)
        cur = await db.execute("SELECT COUNT(*) FROM events")
        assert (await cur.fetchone())[0] == 7


@pytest.mark.asyncio
async def test_rebuilt_check_matches_schema_o1_types(tmp_path):
    """The rebuilt CHECK must list every O1 semantic type (lockstep with
    schema.py:226)."""
    db_path = tmp_path / "shape.db"
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await db.executescript(_events_table(None))
        await db.commit()
        await _migrate_events_add_bundle_lifecycle_types(db)
        cur = await db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='events'"
        )
        sql = (await cur.fetchone())[0]
    for et in ("progress", "blocker", "needs_review", "needs_human", "log"):
        assert f"'{et}'" in sql, f"rebuilt CHECK missing {et}"
