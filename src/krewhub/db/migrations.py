"""
Idempotent schema migrations for existing databases.

Each migration checks whether the change is needed before applying it.
This runs after CREATE TABLE IF NOT EXISTS, so it only matters for
databases created before these columns/tables were added.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def run_migrations(db: aiosqlite.Connection) -> None:
    await _add_column_if_missing(db, "bundles", "resource_version", "INTEGER NOT NULL DEFAULT 1")
    await _add_column_if_missing(db, "bundles", "generation", "INTEGER NOT NULL DEFAULT 1")
    await _add_column_if_missing(db, "tasks", "resource_version", "INTEGER NOT NULL DEFAULT 1")
    await _add_column_if_missing(db, "tasks", "generation", "INTEGER NOT NULL DEFAULT 1")
    await _add_column_if_missing(db, "tasks", "assigned_agent_id", "TEXT")
    await _add_column_if_missing(db, "agent_presence", "resource_version", "INTEGER NOT NULL DEFAULT 1")
    await _add_column_if_missing(db, "agent_presence", "max_concurrent_tasks", "INTEGER NOT NULL DEFAULT 1")
    await _add_column_if_missing(db, "digests", "resource_version", "INTEGER NOT NULL DEFAULT 1")
    await _add_column_if_missing(db, "digests", "generation", "INTEGER NOT NULL DEFAULT 1")

    await _create_table_if_missing(db, "watch_log", """
        CREATE TABLE IF NOT EXISTS watch_log (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            resource_type TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            event_type TEXT NOT NULL CHECK(event_type IN ('ADDED', 'MODIFIED', 'DELETED')),
            resource_version INTEGER NOT NULL,
            payload TEXT NOT NULL DEFAULT '{}',
            recipe_id TEXT,
            created_at TEXT NOT NULL
        )
    """)
    await _create_index_if_missing(db, "idx_watch_log_type_seq", "watch_log", "(resource_type, seq)")
    await _create_index_if_missing(db, "idx_watch_log_recipe_seq", "watch_log", "(recipe_id, seq)")
    await _create_index_if_missing(db, "idx_tasks_assigned", "tasks", "(assigned_agent_id)")

    await db.commit()


async def _add_column_if_missing(
    db: aiosqlite.Connection,
    table: str,
    column: str,
    column_def: str,
) -> None:
    # Skip if table doesn't exist yet (fresh DB — schema will create it)
    if not await _table_exists(db, table):
        return

    cursor = await db.execute(f"PRAGMA table_info({table})")
    columns = await cursor.fetchall()
    existing = {row["name"] for row in columns}

    if column not in existing:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_def}")
        logger.info("Migration: added %s.%s", table, column)


async def _table_exists(db: aiosqlite.Connection, table: str) -> bool:
    cursor = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return await cursor.fetchone() is not None


async def _create_table_if_missing(
    db: aiosqlite.Connection,
    table: str,
    ddl: str,
) -> None:
    cursor = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    if await cursor.fetchone() is None:
        await db.executescript(ddl)
        logger.info("Migration: created table %s", table)


async def _create_index_if_missing(
    db: aiosqlite.Connection,
    index_name: str,
    table: str,
    columns: str,
) -> None:
    if not await _table_exists(db, table):
        return

    cursor = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
        (index_name,),
    )
    if await cursor.fetchone() is None:
        await db.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table}{columns}")
        logger.info("Migration: created index %s", index_name)
