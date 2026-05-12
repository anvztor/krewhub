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
            created_at TEXT NOT NULL
        )
    """)
    await _create_index_if_missing(db, "idx_watch_log_type_seq", "watch_log", "(resource_type, seq)")
    # Step (e): idx_watch_log_recipe_seq removed with recipe_id column.
    await _create_index_if_missing(db, "idx_tasks_assigned", "tasks", "(assigned_agent_id)")

    # Step (e): recipes table is dropped; the recipes.cookbook_id /
    # commit_sha column adds are no-ops. Step (e) drop runs later in
    # this file. agent_presence.cookbook_id is still relevant.
    await _add_column_if_missing(db, "agent_presence", "cookbook_id", "TEXT NOT NULL DEFAULT ''")

    # Phase 3: git-based storage columns
    await _add_column_if_missing(db, "cookbooks", "repo_path", "TEXT")

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

    # Track A1: bundle ownership + default agent runtime
    await _add_column_if_missing(db, "bundles", "owner_account_id", "TEXT")
    await _add_column_if_missing(db, "bundles", "default_agent_runtime_id", "TEXT")
    await _backfill_bundle_owner_from_created_by(db)

    # Track A2: sandbox provisioning + runtime assignment columns on tasks.
    # The sandboxes table itself is created by SCHEMA_SQL (CREATE IF NOT EXISTS).
    await _add_column_if_missing(db, "tasks", "assigned_runtime_id", "TEXT")
    await _add_column_if_missing(db, "tasks", "sandbox_id", "TEXT")
    # Bundle-level sandbox: when cookrew-beta opens a bundle tab, krewhub
    # provisions ONE e2b sandbox and every task in the bundle reuses it.
    # bundles.sandbox_id is the bundle's primary sandbox; sandboxes.bundle_id
    # is the reverse lookup (and lets a sandbox row be bundle-scoped instead
    # of task-scoped).
    await _add_column_if_missing(db, "bundles", "sandbox_id", "TEXT")
    await _add_column_if_missing(db, "sandboxes", "bundle_id", "TEXT")
    # The original sandboxes schema declared task_id as NOT NULL with a
    # FOREIGN KEY into tasks(id). Bundle-scoped sandboxes don't have an
    # owning task — rebuild the table to make task_id nullable and drop
    # the FK. Idempotent via the column probe at the top.
    await _relax_sandboxes_task_id(db)

    # Opt-in autoplan: every bundle previously got auto-dispatched to a
    # planner agent on the next reconcile. The cookrew-beta "+ NEW"
    # button and any other empty-bundle-from-UI flow want a blank
    # board, not a generated graph. Default to OFF; orchestrator-mode
    # flows that genuinely want a planner flip this on at create time.
    await _add_column_if_missing(
        db, "bundles", "autoplan_enabled",
        "INTEGER NOT NULL DEFAULT 0",
    )

    # Invocation Contract slice 1 — three new tables (idempotent for
    # databases predating the contract).
    await _create_table_if_missing(db, "invocations", """
        CREATE TABLE IF NOT EXISTS invocations (
            id TEXT PRIMARY KEY,
            target_type TEXT NOT NULL,
            target_id TEXT,
            input_json TEXT NOT NULL,
            schema_json TEXT,
            deadline_s INTEGER NOT NULL,
            label TEXT,
            parent_tape_id TEXT,
            parent_fork_point INTEGER,
            idempotency_key TEXT,
            tape_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL CHECK (status IN ('pending','running','completed','cancelled','errored')) DEFAULT 'pending',
            result_json TEXT,
            created_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            created_by TEXT NOT NULL
        )
    """)
    await db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_invocations_idempotency "
        "ON invocations(parent_tape_id, idempotency_key) "
        "WHERE idempotency_key IS NOT NULL"
    )
    await _create_index_if_missing(db, "idx_invocations_status", "invocations", "(status)")
    await _create_index_if_missing(db, "idx_invocations_target", "invocations", "(target_type, target_id)")

    await _create_table_if_missing(db, "invocation_events", """
        CREATE TABLE IF NOT EXISTS invocation_events (
            tape_id TEXT NOT NULL,
            id INTEGER NOT NULL,
            parent_id INTEGER,
            fork_id TEXT,
            actor_type TEXT NOT NULL CHECK (actor_type IN ('brain','sandbox','human','system')),
            actor_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            body TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            ts TEXT NOT NULL,
            PRIMARY KEY (tape_id, id)
        )
    """)
    await _create_index_if_missing(db, "idx_invocation_events_kind", "invocation_events", "(kind)")

    await _create_table_if_missing(db, "tape_forks", """
        CREATE TABLE IF NOT EXISTS tape_forks (
            child_tape_id TEXT PRIMARY KEY,
            parent_tape_id TEXT NOT NULL,
            fork_point_event_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    await _create_index_if_missing(db, "idx_tape_forks_parent", "tape_forks", "(parent_tape_id)")

    # Phase 12: recipe removal — bundles point directly at cookbooks;
    # repo binding becomes a per-bundle hint resolved at run time;
    # sharing moves to cookbook-level. Additive only; recipe_id stays
    # for dual-write until callers migrate.
    await _add_column_if_missing(db, "bundles", "cookbook_id", "TEXT")
    await _add_column_if_missing(db, "bundles", "repo_spec", "TEXT")
    await _add_column_if_missing(db, "events", "cookbook_id", "TEXT")
    await _add_column_if_missing(db, "watch_log", "cookbook_id", "TEXT")
    await _create_index_if_missing(db, "idx_bundles_cookbook", "bundles", "(cookbook_id)")
    await _create_index_if_missing(db, "idx_events_cookbook", "events", "(cookbook_id)")
    await _create_index_if_missing(
        db, "idx_watch_log_cookbook_seq", "watch_log", "(cookbook_id, seq)",
    )

    await _create_table_if_missing(db, "cookbook_shares", """
        CREATE TABLE IF NOT EXISTS cookbook_shares (
            id TEXT PRIMARY KEY,
            cookbook_id TEXT NOT NULL REFERENCES cookbooks(id),
            shared_with_account_id TEXT NOT NULL REFERENCES accounts(id),
            role TEXT NOT NULL CHECK(role IN ('owner', 'member', 'viewer')),
            shared_by_account_id TEXT NOT NULL REFERENCES accounts(id),
            shared_at TEXT NOT NULL,
            revoked_at TEXT
        )
    """)
    # Idempotent rebuild for any DB created with the old table-level
    # UNIQUE(cookbook_id, shared_with_account_id) — that constraint
    # blocked reshare-after-revoke. The current shape uses a partial
    # unique index instead (below).
    await _drop_table_level_unique_on_cookbook_shares(db)
    await _create_index_if_missing(
        db, "idx_cookbook_shares_cookbook", "cookbook_shares", "(cookbook_id)",
    )
    await _create_index_if_missing(
        db, "idx_cookbook_shares_account", "cookbook_shares",
        "(shared_with_account_id)",
    )
    # Partial unique: only one ACTIVE share per (cookbook, account).
    # Revoked rows don't block re-sharing — they keep audit history.
    await db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_cookbook_shares_unique_active "
        "ON cookbook_shares(cookbook_id, shared_with_account_id) "
        "WHERE revoked_at IS NULL"
    )

    await _create_table_if_missing(db, "repo_grants", """
        CREATE TABLE IF NOT EXISTS repo_grants (
            id TEXT PRIMARY KEY,
            cookbook_id TEXT NOT NULL REFERENCES cookbooks(id),
            provider TEXT NOT NULL CHECK(provider IN ('github', 'gitlab', 'bitbucket')),
            scope TEXT NOT NULL,
            token_ref TEXT NOT NULL,
            granted_by_account_id TEXT NOT NULL REFERENCES accounts(id),
            granted_at TEXT NOT NULL,
            revoked_at TEXT
        )
    """)
    await _create_index_if_missing(
        db, "idx_repo_grants_cookbook", "repo_grants", "(cookbook_id)",
    )
    await _create_index_if_missing(
        db, "idx_repo_grants_active", "repo_grants",
        "(cookbook_id, provider) WHERE revoked_at IS NULL",
    )

    # Backfill cookbook_id on bundles + events from the legacy recipe link.
    # Idempotent: skips rows that already have a non-null cookbook_id.
    await _backfill_cookbook_id_from_recipe(db)

    # Phase 12 step (c): cookbook-scoped bundles have no recipe.
    # Relax bundles.recipe_id and events.recipe_id from NOT NULL.
    # Idempotent: no-op if the constraint is already gone.
    await _relax_bundles_recipe_id_nullable(db)
    await _relax_events_recipe_id_nullable(db)

    # Phase 12 step (d): digest layer is gone. Drop the table and
    # add the new bundle-lifecycle event types to the events CHECK
    # constraint. Idempotent.
    await _drop_digests_table(db)
    await _migrate_events_add_bundle_lifecycle_types(db)
    await _migrate_bundles_add_closed_status(db)

    # Phase 12 step (d.1): collapse bundles.status to OPEN | CLOSED.
    # Folds legacy middle states (claimed/cooked/blocked → open;
    # cancelled/digested/rejected → closed) and tightens the CHECK
    # constraint to the two-value set. Idempotent.
    await _collapse_bundles_status_to_open_closed(db)

    # Phase 12 step (e): recipes are gone. Drop recipe_id columns and
    # the recipes / recipe_members tables. Idempotent.
    await _drop_recipe_id_columns_and_tables(db)

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


async def _backfill_cookbook_id_from_recipe(db: aiosqlite.Connection) -> None:
    """Phase 12: copy bundles.recipe_id -> bundles.cookbook_id via recipes.

    Same for events.cookbook_id (and watch_log.cookbook_id if recipe_id
    is present on that row). Idempotent — only touches rows where the
    new column is NULL. Skip silently if the legacy recipes table is
    already gone.
    """
    if not await _table_exists(db, "recipes"):
        return

    for table in ("bundles", "events"):
        if not await _table_exists(db, table):
            continue
        cursor = await db.execute(f"PRAGMA table_info({table})")
        cols = {row["name"] for row in await cursor.fetchall()}
        if "cookbook_id" not in cols or "recipe_id" not in cols:
            continue
        try:
            cursor = await db.execute(
                f"""UPDATE {table}
                    SET cookbook_id = (
                        SELECT r.cookbook_id FROM recipes r
                        WHERE r.id = {table}.recipe_id
                    )
                    WHERE cookbook_id IS NULL
                      AND recipe_id IS NOT NULL""",
            )
            if cursor.rowcount:
                logger.info(
                    "Migration: backfilled %d %s rows with cookbook_id",
                    cursor.rowcount, table,
                )
        except aiosqlite.OperationalError as exc:
            logger.warning(
                "Migration: cookbook_id backfill on %s skipped: %s",
                table, exc,
            )

    # watch_log carries recipe_id as a denormalized filter — backfill
    # cookbook_id the same way so SSE channels can flip cleanly.
    if await _table_exists(db, "watch_log"):
        cursor = await db.execute("PRAGMA table_info(watch_log)")
        cols = {row["name"] for row in await cursor.fetchall()}
        if "cookbook_id" in cols and "recipe_id" in cols:
            try:
                cursor = await db.execute(
                    """UPDATE watch_log
                       SET cookbook_id = (
                           SELECT r.cookbook_id FROM recipes r
                           WHERE r.id = watch_log.recipe_id
                       )
                       WHERE cookbook_id IS NULL
                         AND recipe_id IS NOT NULL""",
                )
                if cursor.rowcount:
                    logger.info(
                        "Migration: backfilled %d watch_log rows with cookbook_id",
                        cursor.rowcount,
                    )
            except aiosqlite.OperationalError as exc:
                logger.warning(
                    "Migration: cookbook_id backfill on watch_log skipped: %s",
                    exc,
                )


async def _drop_recipe_id_columns_and_tables(
    db: aiosqlite.Connection,
) -> None:
    """Step (e): drop recipe_id columns from bundles/events/watch_log,
    then drop the recipes + recipe_members tables.

    Idempotent: detects via PRAGMA + sqlite_master before each step.
    """
    if not await _table_exists(db, "bundles"):
        return

    # Rebuild bundles without recipe_id column (if present).
    cursor = await db.execute("PRAGMA table_info(bundles)")
    bundle_cols = [r["name"] for r in await cursor.fetchall()]
    if "recipe_id" in bundle_cols:
        logger.info("Migration: dropping bundles.recipe_id column")
        keep = [c for c in bundle_cols if c != "recipe_id"]
        keep_list = ", ".join(keep)
        await db.executescript(
            f"""
            CREATE TABLE bundles_new (
                id TEXT PRIMARY KEY,
                cookbook_id TEXT REFERENCES cookbooks(id),
                repo_spec TEXT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open'
                    CHECK(status IN ('open', 'closed')),
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
                sandbox_id TEXT,
                autoplan_enabled INTEGER NOT NULL DEFAULT 0
            );
            INSERT INTO bundles_new ({keep_list})
                SELECT {keep_list} FROM bundles;
            DROP TABLE bundles;
            ALTER TABLE bundles_new RENAME TO bundles;
            CREATE INDEX IF NOT EXISTS idx_bundles_cookbook ON bundles(cookbook_id);
            CREATE INDEX IF NOT EXISTS idx_bundles_runnable
                ON bundles(status) WHERE graph_code IS NOT NULL;
            """
        )

    # Rebuild events without recipe_id column (if present).
    if await _table_exists(db, "events"):
        cursor = await db.execute("PRAGMA table_info(events)")
        event_cols = [r["name"] for r in await cursor.fetchall()]
        if "recipe_id" in event_cols:
            logger.info("Migration: dropping events.recipe_id column")
            keep = [c for c in event_cols if c != "recipe_id"]
            keep_list = ", ".join(keep)
            await db.executescript(
                f"""
                CREATE TABLE events_new (
                    id TEXT PRIMARY KEY,
                    cookbook_id TEXT REFERENCES cookbooks(id),
                    bundle_id TEXT REFERENCES bundles(id),
                    task_id TEXT,
                    type TEXT NOT NULL
                        CHECK(type IN (
                            'prompt', 'plan', 'task_claimed', 'task_working',
                            'milestone', 'fact_added', 'code_pushed',
                            'bundle_closed', 'bundle_reopened',
                            'session_start', 'session_end', 'tool_use', 'tool_result',
                            'agent_reply', 'thinking'
                        )),
                    actor_id TEXT NOT NULL,
                    actor_type TEXT NOT NULL
                        CHECK(actor_type IN ('human', 'agent', 'system', 'hook')),
                    body TEXT NOT NULL DEFAULT '',
                    payload TEXT NOT NULL DEFAULT '{{}}',
                    sequence INTEGER NOT NULL DEFAULT 0,
                    facts TEXT NOT NULL DEFAULT '[]',
                    code_refs TEXT NOT NULL DEFAULT '[]',
                    visibility TEXT NOT NULL DEFAULT 'system',
                    created_at TEXT NOT NULL,
                    expires_at TEXT
                );
                INSERT INTO events_new ({keep_list})
                    SELECT {keep_list} FROM events;
                DROP TABLE events;
                ALTER TABLE events_new RENAME TO events;
                CREATE INDEX IF NOT EXISTS idx_events_cookbook ON events(cookbook_id);
                CREATE INDEX IF NOT EXISTS idx_events_bundle ON events(bundle_id);
                CREATE INDEX IF NOT EXISTS idx_events_task_sequence
                    ON events(task_id, sequence);
                """
            )

    # watch_log: just drop the recipe_id column (data is already
    # backfilled to cookbook_id; no FK or CHECK to worry about).
    if await _table_exists(db, "watch_log"):
        cursor = await db.execute("PRAGMA table_info(watch_log)")
        wl_cols = [r["name"] for r in await cursor.fetchall()]
        if "recipe_id" in wl_cols:
            logger.info("Migration: dropping watch_log.recipe_id column")
            # SQLite 3.35+ supports DROP COLUMN; fallback to rebuild if old.
            try:
                await db.execute("ALTER TABLE watch_log DROP COLUMN recipe_id")
            except aiosqlite.OperationalError:
                keep = [c for c in wl_cols if c != "recipe_id"]
                keep_list = ", ".join(keep)
                await db.executescript(
                    f"""
                    CREATE TABLE watch_log_new (
                        seq INTEGER PRIMARY KEY AUTOINCREMENT,
                        resource_type TEXT NOT NULL,
                        resource_id TEXT NOT NULL,
                        event_type TEXT NOT NULL CHECK(event_type IN ('ADDED', 'MODIFIED', 'DELETED')),
                        resource_version INTEGER NOT NULL,
                        payload TEXT NOT NULL DEFAULT '{{}}',
                        cookbook_id TEXT,
                        created_at TEXT NOT NULL
                    );
                    INSERT INTO watch_log_new ({keep_list})
                        SELECT {keep_list} FROM watch_log;
                    DROP TABLE watch_log;
                    ALTER TABLE watch_log_new RENAME TO watch_log;
                    CREATE INDEX IF NOT EXISTS idx_watch_log_type_seq
                        ON watch_log(resource_type, seq);
                    CREATE INDEX IF NOT EXISTS idx_watch_log_cookbook_seq
                        ON watch_log(cookbook_id, seq);
                    """
                )

    # Drop the recipes and recipe_members tables themselves.
    if await _table_exists(db, "recipe_members"):
        logger.info("Migration: dropping recipe_members table")
        await db.execute("DROP TABLE IF EXISTS recipe_members")
    if await _table_exists(db, "recipes"):
        logger.info("Migration: dropping recipes table")
        await db.execute("DROP TABLE IF EXISTS recipes")


async def _collapse_bundles_status_to_open_closed(
    db: aiosqlite.Connection,
) -> None:
    """Step (d.1): collapse the BundleStatus FSM to OPEN | CLOSED.

    Two-step idempotent migration:
      1. UPDATE existing rows to fold middle states (skipped rows that
         are already canonical):
           CLAIMED / COOKED / BLOCKED         → OPEN
           CANCELLED / DIGESTED / REJECTED    → CLOSED
      2. Rebuild the table to tighten the CHECK constraint to just
         ('open', 'closed'). Skipped if already tight.
    """
    if not await _table_exists(db, "bundles"):
        return

    cursor = await db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='bundles'"
    )
    row = await cursor.fetchone()
    if row is None:
        return
    sql = (row["sql"] or "")
    # If CHECK already tight, nothing to do.
    if "'claimed'" not in sql and "'cooked'" not in sql:
        return

    logger.info("Migration: collapsing bundles.status middle states to open/closed")
    await db.execute(
        "UPDATE bundles SET status = 'open' "
        "WHERE status IN ('claimed', 'cooked', 'blocked')"
    )
    await db.execute(
        "UPDATE bundles SET status = 'closed' "
        "WHERE status IN ('cancelled', 'digested', 'rejected')"
    )

    cursor = await db.execute("PRAGMA table_info(bundles)")
    cols = [r["name"] for r in await cursor.fetchall()]
    col_list = ", ".join(cols)

    await db.executescript(
        f"""
        CREATE TABLE bundles_new (
            id TEXT PRIMARY KEY,
            recipe_id TEXT REFERENCES recipes(id),
            cookbook_id TEXT REFERENCES cookbooks(id),
            repo_spec TEXT,
            prompt TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open'
                CHECK(status IN ('open', 'closed')),
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
            sandbox_id TEXT,
            autoplan_enabled INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO bundles_new ({col_list})
            SELECT {col_list} FROM bundles;
        DROP TABLE bundles;
        ALTER TABLE bundles_new RENAME TO bundles;
        CREATE INDEX IF NOT EXISTS idx_bundles_recipe ON bundles(recipe_id);
        CREATE INDEX IF NOT EXISTS idx_bundles_cookbook ON bundles(cookbook_id);
        CREATE INDEX IF NOT EXISTS idx_bundles_runnable
            ON bundles(status) WHERE graph_code IS NOT NULL;
        """
    )


async def _migrate_bundles_add_closed_status(
    db: aiosqlite.Connection,
) -> None:
    """Step (d): widen bundles.status CHECK to include 'closed'.

    Idempotent: detects via the current CREATE TABLE statement.
    """
    if not await _table_exists(db, "bundles"):
        return

    cursor = await db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='bundles'"
    )
    row = await cursor.fetchone()
    if row is None:
        return
    sql = (row["sql"] or "")
    if "'closed'" in sql:
        return  # Already widened.

    logger.info("Migration: rebuilding bundles.status CHECK to include 'closed'")
    cursor = await db.execute("PRAGMA table_info(bundles)")
    cols = [r["name"] for r in await cursor.fetchall()]
    col_list = ", ".join(cols)

    await db.executescript(
        f"""
        CREATE TABLE bundles_new (
            id TEXT PRIMARY KEY,
            recipe_id TEXT REFERENCES recipes(id),
            cookbook_id TEXT REFERENCES cookbooks(id),
            repo_spec TEXT,
            prompt TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open'
                CHECK(status IN (
                    'open', 'closed',
                    'claimed', 'cooked', 'blocked', 'cancelled',
                    'digested', 'rejected'
                )),
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
            sandbox_id TEXT,
            autoplan_enabled INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO bundles_new ({col_list})
            SELECT {col_list} FROM bundles;
        DROP TABLE bundles;
        ALTER TABLE bundles_new RENAME TO bundles;
        CREATE INDEX IF NOT EXISTS idx_bundles_recipe ON bundles(recipe_id);
        CREATE INDEX IF NOT EXISTS idx_bundles_cookbook ON bundles(cookbook_id);
        CREATE INDEX IF NOT EXISTS idx_bundles_runnable
            ON bundles(status) WHERE graph_code IS NOT NULL;
        """
    )


async def _drop_digests_table(db: aiosqlite.Connection) -> None:
    """Step (d): drop the digests table.

    Idempotent — no-op if table is already gone. Issues a DROP TABLE
    rather than a column rebuild because nothing depends on the rows.
    """
    if not await _table_exists(db, "digests"):
        return
    logger.info("Migration: dropping digests table")
    await db.execute("DROP TABLE IF EXISTS digests")


async def _migrate_events_add_bundle_lifecycle_types(
    db: aiosqlite.Connection,
) -> None:
    """Step (d): replace digest_* event types with bundle_closed/reopened
    in events.type CHECK constraint.

    Idempotent: detects via the current CREATE TABLE statement.
    """
    if not await _table_exists(db, "events"):
        return

    cursor = await db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='events'"
    )
    row = await cursor.fetchone()
    if row is None:
        return
    sql = (row["sql"] or "")
    if "bundle_closed" in sql and "digest_submitted" not in sql:
        # Already on the new shape.
        return

    logger.info("Migration: rebuilding events.type CHECK for bundle lifecycle")
    # Capture column list from the live table so we keep every column
    # (including any added later by Phase 12).
    cursor = await db.execute("PRAGMA table_info(events)")
    cols = [r["name"] for r in await cursor.fetchall()]
    col_list = ", ".join(cols)

    await db.executescript(
        f"""
        CREATE TABLE events_new (
            id TEXT PRIMARY KEY,
            recipe_id TEXT REFERENCES recipes(id),
            cookbook_id TEXT REFERENCES cookbooks(id),
            bundle_id TEXT REFERENCES bundles(id),
            task_id TEXT,
            type TEXT NOT NULL
                CHECK(type IN (
                    'prompt', 'plan', 'task_claimed', 'task_working',
                    'milestone', 'fact_added', 'code_pushed',
                    'bundle_closed', 'bundle_reopened',
                    'session_start', 'session_end', 'tool_use', 'tool_result',
                    'agent_reply', 'thinking',
                    -- Tolerate legacy rows still using digest_* until they
                    -- expire/are deleted; new writes use the new vocabulary.
                    'digest_submitted', 'digest_approved', 'digest_rejected'
                )),
            actor_id TEXT NOT NULL,
            actor_type TEXT NOT NULL
                CHECK(actor_type IN ('human', 'agent', 'system', 'hook')),
            body TEXT NOT NULL DEFAULT '',
            payload TEXT NOT NULL DEFAULT '{{}}',
            sequence INTEGER NOT NULL DEFAULT 0,
            facts TEXT NOT NULL DEFAULT '[]',
            code_refs TEXT NOT NULL DEFAULT '[]',
            visibility TEXT NOT NULL DEFAULT 'system',
            created_at TEXT NOT NULL,
            expires_at TEXT
        );
        INSERT INTO events_new ({col_list})
            SELECT {col_list} FROM events;
        DROP TABLE events;
        ALTER TABLE events_new RENAME TO events;
        CREATE INDEX IF NOT EXISTS idx_events_recipe ON events(recipe_id);
        CREATE INDEX IF NOT EXISTS idx_events_cookbook ON events(cookbook_id);
        CREATE INDEX IF NOT EXISTS idx_events_bundle ON events(bundle_id);
        CREATE INDEX IF NOT EXISTS idx_events_task_sequence
            ON events(task_id, sequence);
        """
    )


async def _column_is_nullable(
    db: aiosqlite.Connection, table: str, column: str,
) -> bool:
    """True iff `table.column` exists with notnull=0."""
    cursor = await db.execute(f"PRAGMA table_info({table})")
    for row in await cursor.fetchall():
        if row["name"] == column:
            return int(row["notnull"]) == 0
    return False


async def _relax_bundles_recipe_id_nullable(
    db: aiosqlite.Connection,
) -> None:
    """Drop NOT NULL on bundles.recipe_id.

    Idempotent. No-op if the column is already nullable OR has been
    dropped entirely (step e). Step e's _drop_recipe_id_columns_and_tables
    runs after this, so on a post-step-e DB the column is already
    gone and this function correctly bails.
    """
    if not await _table_exists(db, "bundles"):
        return
    # Bail if the column was already dropped (step e).
    cursor = await db.execute("PRAGMA table_info(bundles)")
    if "recipe_id" not in {r["name"] for r in await cursor.fetchall()}:
        return
    if await _column_is_nullable(db, "bundles", "recipe_id"):
        return

    logger.info("Migration: rebuilding bundles to make recipe_id nullable")
    # Collect existing column list to preserve every other column.
    cursor = await db.execute("PRAGMA table_info(bundles)")
    cols = [row["name"] for row in await cursor.fetchall()]
    col_list = ", ".join(cols)

    await db.executescript(
        f"""
        CREATE TABLE bundles_new (
            id TEXT PRIMARY KEY,
            recipe_id TEXT REFERENCES recipes(id),
            cookbook_id TEXT REFERENCES cookbooks(id),
            repo_spec TEXT,
            prompt TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open'
                CHECK(status IN ('open', 'claimed', 'cooked', 'blocked',
                                 'cancelled', 'digested', 'rejected')),
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
            sandbox_id TEXT,
            autoplan_enabled INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO bundles_new ({col_list})
            SELECT {col_list} FROM bundles;
        DROP TABLE bundles;
        ALTER TABLE bundles_new RENAME TO bundles;
        CREATE INDEX IF NOT EXISTS idx_bundles_recipe ON bundles(recipe_id);
        CREATE INDEX IF NOT EXISTS idx_bundles_cookbook ON bundles(cookbook_id);
        CREATE INDEX IF NOT EXISTS idx_bundles_runnable
            ON bundles(status) WHERE graph_code IS NOT NULL;
        """
    )


async def _relax_events_recipe_id_nullable(
    db: aiosqlite.Connection,
) -> None:
    """Drop NOT NULL on events.recipe_id.

    Idempotent. No-op if the column is already nullable OR has been
    dropped (step e).
    """
    if not await _table_exists(db, "events"):
        return
    cursor = await db.execute("PRAGMA table_info(events)")
    if "recipe_id" not in {r["name"] for r in await cursor.fetchall()}:
        return
    if await _column_is_nullable(db, "events", "recipe_id"):
        return

    logger.info("Migration: rebuilding events to make recipe_id nullable")
    cursor = await db.execute("PRAGMA table_info(events)")
    cols = [row["name"] for row in await cursor.fetchall()]
    col_list = ", ".join(cols)

    await db.executescript(
        f"""
        CREATE TABLE events_new (
            id TEXT PRIMARY KEY,
            recipe_id TEXT REFERENCES recipes(id),
            cookbook_id TEXT REFERENCES cookbooks(id),
            bundle_id TEXT REFERENCES bundles(id),
            task_id TEXT,
            type TEXT NOT NULL
                CHECK(type IN (
                    'prompt', 'plan', 'task_claimed', 'task_working',
                    'milestone', 'fact_added', 'code_pushed',
                    'digest_submitted', 'digest_approved', 'digest_rejected',
                    'session_start', 'session_end', 'tool_use', 'tool_result',
                    'agent_reply', 'thinking'
                )),
            actor_id TEXT NOT NULL,
            actor_type TEXT NOT NULL
                CHECK(actor_type IN ('human', 'agent', 'system', 'hook')),
            body TEXT NOT NULL DEFAULT '',
            payload TEXT NOT NULL DEFAULT '{{}}',
            sequence INTEGER NOT NULL DEFAULT 0,
            facts TEXT NOT NULL DEFAULT '[]',
            code_refs TEXT NOT NULL DEFAULT '[]',
            visibility TEXT NOT NULL DEFAULT 'system',
            created_at TEXT NOT NULL,
            expires_at TEXT
        );
        INSERT INTO events_new ({col_list})
            SELECT {col_list} FROM events;
        DROP TABLE events;
        ALTER TABLE events_new RENAME TO events;
        CREATE INDEX IF NOT EXISTS idx_events_recipe ON events(recipe_id);
        CREATE INDEX IF NOT EXISTS idx_events_cookbook ON events(cookbook_id);
        CREATE INDEX IF NOT EXISTS idx_events_bundle ON events(bundle_id);
        CREATE INDEX IF NOT EXISTS idx_events_task_sequence
            ON events(task_id, sequence);
        """
    )


async def _drop_table_level_unique_on_cookbook_shares(
    db: aiosqlite.Connection,
) -> None:
    """Rebuild cookbook_shares without the table-level UNIQUE constraint.

    The original Phase 12 DDL had
        UNIQUE(cookbook_id, shared_with_account_id)
    baked into the table. That blocked reshare-after-revoke: a
    re-INSERT failed even when the previous row was soft-deleted via
    revoked_at. The current shape uses a partial unique index
    (revoked_at IS NULL) instead. This function detects the old
    constraint and rebuilds the table in place.

    Idempotent: no-op if (a) table doesn't exist, (b) constraint
    already removed.
    """
    if not await _table_exists(db, "cookbook_shares"):
        return

    cursor = await db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='cookbook_shares'",
    )
    row = await cursor.fetchone()
    if row is None:
        return
    existing_sql = (row["sql"] or "").lower()
    # Only rebuild if the table-level UNIQUE is present. The partial
    # index lives in sqlite_master separately as type='index'.
    if "unique(cookbook_id" not in existing_sql:
        return

    logger.info("Migration: rebuilding cookbook_shares to drop table-level UNIQUE")
    await db.executescript(
        """
        CREATE TABLE cookbook_shares_new (
            id TEXT PRIMARY KEY,
            cookbook_id TEXT NOT NULL REFERENCES cookbooks(id),
            shared_with_account_id TEXT NOT NULL REFERENCES accounts(id),
            role TEXT NOT NULL CHECK(role IN ('owner', 'member', 'viewer')),
            shared_by_account_id TEXT NOT NULL REFERENCES accounts(id),
            shared_at TEXT NOT NULL,
            revoked_at TEXT
        );
        INSERT INTO cookbook_shares_new
            (id, cookbook_id, shared_with_account_id, role,
             shared_by_account_id, shared_at, revoked_at)
            SELECT id, cookbook_id, shared_with_account_id, role,
                   shared_by_account_id, shared_at, revoked_at
            FROM cookbook_shares;
        DROP TABLE cookbook_shares;
        ALTER TABLE cookbook_shares_new RENAME TO cookbook_shares;
        """
    )


async def _backfill_bundle_owner_from_created_by(db: aiosqlite.Connection) -> None:
    """Best-effort backfill: copy bundles.created_by to owner_account_id."""
    if not await _table_exists(db, "bundles"):
        return
    cursor = await db.execute("PRAGMA table_info(bundles)")
    cols = {row["name"] for row in await cursor.fetchall()}
    if "created_by" not in cols or "owner_account_id" not in cols:
        return
    try:
        await db.execute(
            "UPDATE bundles SET owner_account_id = created_by "
            "WHERE owner_account_id IS NULL AND created_by IS NOT NULL "
            "AND created_by != ''",
        )
    except aiosqlite.OperationalError:
        # Schema variant without expected columns — silently skip
        return


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


async def _relax_sandboxes_task_id(db: aiosqlite.Connection) -> None:
    """Rebuild `sandboxes` so task_id is nullable and the FK is dropped.

    The original schema declared
        task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE
    which blocks bundle-scoped sandboxes (the row has no owning task).
    Idempotent: detects whether the constraint is still in place by
    inspecting `sqlite_master.sql` and skips if already relaxed.
    """
    if not await _table_exists(db, "sandboxes"):
        return

    cursor = await db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='sandboxes'",
    )
    row = await cursor.fetchone()
    if row is None:
        return
    ddl = (row["sql"] or "").upper()
    if "TASK_ID" not in ddl or ("NOT NULL" not in ddl and "REFERENCES" not in ddl):
        # Already relaxed.
        return

    logger.info("Migration: rebuilding sandboxes table to relax task_id")
    await db.executescript(
        """
        BEGIN;
        CREATE TABLE sandboxes_new (
            id              TEXT PRIMARY KEY,
            task_id         TEXT,
            bundle_id       TEXT,
            owner_account_id TEXT NOT NULL,
            e2b_sandbox_id  TEXT NOT NULL,
            template        TEXT NOT NULL,
            status          TEXT NOT NULL,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            terminated_at   TEXT,
            last_event_at   TEXT
        );
        INSERT INTO sandboxes_new (
            id, task_id, bundle_id, owner_account_id, e2b_sandbox_id,
            template, status, created_at, updated_at, terminated_at, last_event_at
        )
        SELECT
            id, task_id,
            CASE WHEN bundle_id IS NULL THEN NULL ELSE bundle_id END,
            owner_account_id, e2b_sandbox_id, template, status,
            created_at, updated_at, terminated_at, last_event_at
        FROM sandboxes;
        DROP TABLE sandboxes;
        ALTER TABLE sandboxes_new RENAME TO sandboxes;
        CREATE INDEX IF NOT EXISTS idx_sandboxes_status_last_event
            ON sandboxes(status, last_event_at);
        COMMIT;
        """
    )
