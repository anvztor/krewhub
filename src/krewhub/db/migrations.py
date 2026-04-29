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
    await _add_column_if_missing(db, "agent_presence", "endpoint_url", "TEXT")
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

    # Phase 2b: cookbook_id columns (added when cookbooks were introduced)
    await _add_column_if_missing(db, "recipes", "cookbook_id", "TEXT NOT NULL DEFAULT ''")
    await _add_column_if_missing(db, "agent_presence", "cookbook_id", "TEXT NOT NULL DEFAULT ''")

    # Phase 3: git-based storage columns
    await _add_column_if_missing(db, "cookbooks", "repo_path", "TEXT")
    await _add_column_if_missing(db, "recipes", "commit_sha", "TEXT")

    # Phase 5: extend events.actor_type CHECK to include 'hook'
    await _migrate_events_actor_type_hook(db)

    # Phase 5b: structured payload column for hook events
    await _add_column_if_missing(db, "events", "payload", "TEXT NOT NULL DEFAULT '{}'")

    # Phase 6: graph runtime — store validated graph code + mermaid on the
    # bundle, and tag tasks with the graph node they correspond to.
    await _add_column_if_missing(db, "bundles", "graph_code", "TEXT")
    await _add_column_if_missing(db, "bundles", "graph_mermaid", "TEXT")
    await _add_column_if_missing(db, "tasks", "graph_node_id", "TEXT")
    await _create_index_if_missing(
        db, "idx_tasks_node", "tasks", "(bundle_id, graph_node_id)",
    )

    # Phase 6a: agent owner tracking
    await _add_column_if_missing(db, "agent_presence", "owner_username", "TEXT")

    # Phase 6b: event streaming — sequence + new event types
    await _add_column_if_missing(db, "events", "sequence", "INTEGER NOT NULL DEFAULT 0")
    await _create_index_if_missing(db, "idx_events_task_sequence", "events", "(task_id, sequence)")
    await _migrate_events_add_types(db)

    # Phase 7: wallet-based identity + SIWE auth
    await _create_table_if_missing(db, "identities", """
        CREATE TABLE IF NOT EXISTS identities (
            wallet_address TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            oauth_provider TEXT,
            oauth_sub TEXT,
            mpc_provider TEXT,
            mpc_key_id TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(oauth_provider, oauth_sub)
        )
    """)
    await _create_table_if_missing(db, "sessions", """
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            wallet_address TEXT NOT NULL REFERENCES identities(wallet_address),
            auth_method TEXT NOT NULL CHECK(auth_method IN ('siwe', 'oauth_mpc', 'api_key')),
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
    """)
    await _create_index_if_missing(db, "idx_sessions_wallet", "sessions", "(wallet_address)")
    await _create_index_if_missing(db, "idx_sessions_expires", "sessions", "(expires_at)")
    await _create_table_if_missing(db, "nonces", """
        CREATE TABLE IF NOT EXISTS nonces (
            nonce TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            used INTEGER NOT NULL DEFAULT 0
        )
    """)
    await _create_index_if_missing(db, "idx_nonces_expires", "nonces", "(expires_at)")

    # Phase 7b: device authorization flow for CLI login
    await _create_table_if_missing(db, "device_codes", """
        CREATE TABLE IF NOT EXISTS device_codes (
            device_code TEXT PRIMARY KEY,
            user_code TEXT NOT NULL UNIQUE,
            wallet_address TEXT,
            approved INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
    """)
    await _create_index_if_missing(db, "idx_device_codes_user", "device_codes", "(user_code)")

    # Phase 8: identity graph — stable account root + passkeys
    await _create_table_if_missing(db, "accounts", """
        CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    await _create_table_if_missing(db, "wallet_links", """
        CREATE TABLE IF NOT EXISTS wallet_links (
            wallet_address TEXT PRIMARY KEY,
            account_id TEXT NOT NULL REFERENCES accounts(id),
            chain_id INTEGER NOT NULL DEFAULT 48816,
            is_primary INTEGER NOT NULL DEFAULT 0,
            linked_at TEXT NOT NULL
        )
    """)
    await _create_index_if_missing(db, "idx_wallet_links_account", "wallet_links", "(account_id)")

    await _create_table_if_missing(db, "passkeys", """
        CREATE TABLE IF NOT EXISTS passkeys (
            credential_id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL REFERENCES accounts(id),
            public_key TEXT NOT NULL,
            sign_count INTEGER NOT NULL DEFAULT 0,
            transports TEXT NOT NULL DEFAULT '[]',
            device_name TEXT,
            created_at TEXT NOT NULL,
            last_used_at TEXT NOT NULL
        )
    """)
    await _create_index_if_missing(db, "idx_passkeys_account", "passkeys", "(account_id)")

    await _create_table_if_missing(db, "passkey_challenges", """
        CREATE TABLE IF NOT EXISTS passkey_challenges (
            challenge TEXT PRIMARY KEY,
            account_id TEXT,
            purpose TEXT NOT NULL CHECK(purpose IN ('register', 'authenticate')),
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            used INTEGER NOT NULL DEFAULT 0
        )
    """)

    # Add account_id columns BEFORE backfill (backfill writes to sessions.account_id)
    await _add_column_if_missing(db, "sessions", "account_id", "TEXT")
    await _add_column_if_missing(db, "device_codes", "account_id", "TEXT")

    # Backfill: migrate existing identities → accounts + wallet_links
    await _backfill_accounts_from_identities(db)

    # Phase 9: A2A hub gateway
    await _create_table_if_missing(db, "a2a_agent_cards", """
        CREATE TABLE IF NOT EXISTS a2a_agent_cards (
            owner TEXT NOT NULL,
            agent_name TEXT NOT NULL,
            card_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (owner, agent_name)
        )
    """)
    await _create_table_if_missing(db, "a2a_invocations", """
        CREATE TABLE IF NOT EXISTS a2a_invocations (
            id TEXT PRIMARY KEY,
            owner TEXT NOT NULL,
            agent_name TEXT NOT NULL,
            method TEXT NOT NULL,
            params TEXT NOT NULL DEFAULT '{}',
            caller_id TEXT,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK(status IN ('pending', 'processing', 'completed', 'failed', 'timeout')),
            result TEXT,
            error TEXT,
            created_at TEXT NOT NULL,
            completed_at TEXT,
            expires_at TEXT NOT NULL
        )
    """)
    await _create_index_if_missing(db, "idx_a2a_invocations_agent", "a2a_invocations", "(owner, agent_name, status)")
    await _create_index_if_missing(db, "idx_a2a_invocations_expires", "a2a_invocations", "(expires_at)")

    # Phase 10: on-chain agent metadata
    await _add_column_if_missing(db, "agent_presence", "mint_tx_hash", "TEXT")
    await _add_column_if_missing(db, "agent_presence", "mint_token_id", "INTEGER")
    await _add_column_if_missing(db, "agent_presence", "aa_wallet_address", "TEXT")

    # Phase 11: task progress (latest-only, JSON blob)
    await _add_column_if_missing(db, "tasks", "progress_json", "TEXT")

    # Phase 4 M3: completion metadata for resumability
    await _add_column_if_missing(db, "tasks", "session_id", "TEXT")
    await _add_column_if_missing(db, "tasks", "work_dir", "TEXT")
    await _add_column_if_missing(db, "tasks", "artifacts_json", "TEXT")

    # Phase 4 M5: event visibility split
    await _add_column_if_missing(db, "events", "visibility", "TEXT NOT NULL DEFAULT 'system'")

    # Layer 4: session token isolation for event ingestion
    await _add_column_if_missing(db, "tasks", "session_token", "TEXT")

    # Auth track A2: sandbox provisioning + runtime assignment.
    # The sandboxes table itself is created by SCHEMA_SQL (CREATE IF NOT EXISTS);
    # these are the column additions for existing DBs.
    await _add_column_if_missing(db, "tasks", "assigned_runtime_id", "TEXT")
    await _add_column_if_missing(db, "tasks", "sandbox_id", "TEXT")

    # Auth track A1 columns added defensively here so A2 can develop in
    # parallel. If A1 migration logic lands first this is a no-op.
    # REMOVE coordination comment once A1 merges.
    await _add_column_if_missing(db, "bundles", "owner_account_id", "TEXT")
    await _add_column_if_missing(db, "bundles", "default_agent_runtime_id", "TEXT")

    await db.commit()


async def _migrate_events_actor_type_hook(db: aiosqlite.Connection) -> None:
    """Recreate events table if its actor_type CHECK doesn't allow 'hook'."""
    if not await _table_exists(db, "events"):
        return

    cursor = await db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='events'"
    )
    row = await cursor.fetchone()
    if row is None:
        return
    ddl = row["sql"] or ""
    if "'hook'" in ddl:
        return

    logger.info("Migration: rebuilding events table to allow actor_type='hook'")
    await db.executescript(
        """
        ALTER TABLE events RENAME TO events_old_hook_migration;
        CREATE TABLE events (
            id TEXT PRIMARY KEY,
            recipe_id TEXT NOT NULL REFERENCES recipes(id),
            bundle_id TEXT REFERENCES bundles(id),
            task_id TEXT,
            type TEXT NOT NULL
                CHECK(type IN (
                    'prompt', 'plan', 'task_claimed', 'milestone',
                    'fact_added', 'code_pushed', 'digest_submitted',
                    'digest_approved', 'digest_rejected',
                    'session_start', 'session_end', 'tool_use', 'agent_reply'
                )),
            actor_id TEXT NOT NULL,
            actor_type TEXT NOT NULL CHECK(actor_type IN ('human', 'agent', 'system', 'hook')),
            body TEXT NOT NULL DEFAULT '',
            facts TEXT NOT NULL DEFAULT '[]',
            code_refs TEXT NOT NULL DEFAULT '[]',
            payload TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            expires_at TEXT
        );
        INSERT INTO events
            (id, recipe_id, bundle_id, task_id, type, actor_id, actor_type,
             body, facts, code_refs, payload, created_at, expires_at)
        SELECT id, recipe_id, bundle_id, task_id, type, actor_id, actor_type,
               body, facts, code_refs, '{}', created_at, expires_at
        FROM events_old_hook_migration;
        DROP TABLE events_old_hook_migration;
        CREATE INDEX IF NOT EXISTS idx_events_recipe ON events(recipe_id);
        CREATE INDEX IF NOT EXISTS idx_events_bundle ON events(bundle_id);
        CREATE INDEX IF NOT EXISTS idx_events_expires ON events(expires_at);
        """
    )


async def _migrate_events_add_types(db: aiosqlite.Connection) -> None:
    """Rebuild events table if CHECK doesn't include tool_result/thinking/task_working."""
    if not await _table_exists(db, "events"):
        return
    cursor = await db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='events'")
    row = await cursor.fetchone()
    if row is None:
        return
    ddl = row["sql"] or ""
    # If thinking is already there AND task_working is there, nothing to do
    if "'thinking'" in ddl and "'task_working'" in ddl:
        return
    logger.info("Migration: rebuilding events table for new event types")
    await db.executescript("""
        ALTER TABLE events RENAME TO events_old_types;
        CREATE TABLE events (
            id TEXT PRIMARY KEY,
            recipe_id TEXT NOT NULL REFERENCES recipes(id),
            bundle_id TEXT REFERENCES bundles(id),
            task_id TEXT,
            type TEXT NOT NULL CHECK(type IN (
                'prompt', 'plan', 'task_claimed', 'task_working', 'milestone',
                'fact_added', 'code_pushed', 'digest_submitted',
                'digest_approved', 'digest_rejected',
                'session_start', 'session_end', 'tool_use', 'tool_result',
                'agent_reply', 'thinking'
            )),
            actor_id TEXT NOT NULL,
            actor_type TEXT NOT NULL CHECK(actor_type IN ('human', 'agent', 'system', 'hook')),
            body TEXT NOT NULL DEFAULT '',
            payload TEXT NOT NULL DEFAULT '{}',
            sequence INTEGER NOT NULL DEFAULT 0,
            facts TEXT NOT NULL DEFAULT '[]',
            code_refs TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            expires_at TEXT
        );
        INSERT INTO events
            (id, recipe_id, bundle_id, task_id, type, actor_id, actor_type,
             body, payload, sequence, facts, code_refs, created_at, expires_at)
        SELECT id, recipe_id, bundle_id, task_id, type, actor_id, actor_type,
               body, COALESCE(payload, '{}'), 0, facts, code_refs, created_at, expires_at
        FROM events_old_types;
        DROP TABLE events_old_types;
        CREATE INDEX IF NOT EXISTS idx_events_recipe ON events(recipe_id);
        CREATE INDEX IF NOT EXISTS idx_events_bundle ON events(bundle_id);
        CREATE INDEX IF NOT EXISTS idx_events_expires ON events(expires_at);
        CREATE INDEX IF NOT EXISTS idx_events_task_sequence ON events(task_id, sequence);
    """)


async def _backfill_accounts_from_identities(db: aiosqlite.Connection) -> None:
    """Migrate existing identities to accounts + wallet_links."""
    if not await _table_exists(db, "identities"):
        return
    if not await _table_exists(db, "accounts"):
        return

    cursor = await db.execute(
        "SELECT wallet_address, display_name, created_at FROM identities "
        "WHERE wallet_address NOT IN (SELECT wallet_address FROM wallet_links)"
    )
    rows = await cursor.fetchall()
    if not rows:
        return

    import uuid
    for row in rows:
        account_id = f"acc_{uuid.uuid4().hex[:12]}"
        wallet = row["wallet_address"]
        name = row["display_name"]
        created = row["created_at"]

        await db.execute(
            "INSERT OR IGNORE INTO accounts (id, display_name, created_at, updated_at) "
            "VALUES (?, ?, ?, ?)",
            (account_id, name, created, created),
        )
        await db.execute(
            "INSERT OR IGNORE INTO wallet_links (wallet_address, account_id, is_primary, linked_at) "
            "VALUES (?, ?, 1, ?)",
            (wallet, account_id, created),
        )
        # Backfill account_id on sessions
        await db.execute(
            "UPDATE sessions SET account_id = ? WHERE wallet_address = ? AND account_id IS NULL",
            (account_id, wallet),
        )
    logger.info("Migration: backfilled %d identities to accounts", len(rows))


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
