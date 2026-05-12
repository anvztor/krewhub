SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cookbooks (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    repo_path TEXT
);

CREATE TABLE IF NOT EXISTS recipes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    repo_url TEXT NOT NULL,
    default_branch TEXT NOT NULL DEFAULT 'main',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    cookbook_id TEXT NOT NULL REFERENCES cookbooks(id),
    commit_sha TEXT
);

CREATE INDEX IF NOT EXISTS idx_recipes_cookbook ON recipes(cookbook_id);

CREATE TABLE IF NOT EXISTS recipe_members (
    id TEXT PRIMARY KEY,
    recipe_id TEXT NOT NULL REFERENCES recipes(id),
    actor_id TEXT NOT NULL,
    actor_type TEXT NOT NULL CHECK(actor_type IN ('human', 'agent')),
    role TEXT NOT NULL CHECK(role IN ('owner', 'member', 'agent')),
    joined_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_recipe_members_recipe ON recipe_members(recipe_id);

CREATE TABLE IF NOT EXISTS agent_presence (
    agent_id TEXT NOT NULL,
    cookbook_id TEXT NOT NULL REFERENCES cookbooks(id),
    display_name TEXT NOT NULL,
    capabilities TEXT NOT NULL DEFAULT '[]',
    max_concurrent_tasks INTEGER NOT NULL DEFAULT 1,
    endpoint_url TEXT,
    status TEXT NOT NULL DEFAULT 'offline' CHECK(status IN ('online', 'offline', 'busy')),
    last_heartbeat_at TEXT NOT NULL,
    current_task_id TEXT,
    resource_version INTEGER NOT NULL DEFAULT 1,
    owner_username TEXT,
    PRIMARY KEY (agent_id, cookbook_id)
);

CREATE INDEX IF NOT EXISTS idx_agent_presence_cookbook ON agent_presence(cookbook_id);

CREATE TABLE IF NOT EXISTS bundles (
    id TEXT PRIMARY KEY,
    -- DEPRECATED — recipe_id stays during dual-write; will drop with the
    -- recipes table once callers migrate to cookbook_id. Now nullable
    -- so cookbook-scoped bundles (no recipe) can be created.
    recipe_id TEXT REFERENCES recipes(id),
    -- New direct parent: bundles point at their cookbook without going
    -- through a recipe. Nullable until backfill completes.
    cookbook_id TEXT REFERENCES cookbooks(id),
    -- Optional JIT repo hint: { provider, owner, repo, ref } resolved
    -- at task-run time against the cookbook's repo_grants. NULL means
    -- this bundle does no file work.
    repo_spec TEXT,
    prompt TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open'
        -- Phase 12 step (d.1): collapsed to OPEN/CLOSED. Middle
        -- states were folded by migration (_collapse_bundles_status).
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
    -- Track A1: bundle ownership + paired agent runtime. Nullable for
    -- legacy bundles created before the auth journey; future migration
    -- will backfill + add NOT NULL once all bundles have an owner.
    owner_account_id TEXT,
    default_agent_runtime_id TEXT,
    -- Bundle-level sandbox: cookrew-beta provisions one e2b sandbox per
    -- bundle tab; every task in the bundle reuses it so the agent's
    -- working tree (cloned repo, edits, generated files) survives
    -- across tasks.
    sandbox_id TEXT,
    -- Opt-in flag for the PlannerDispatchController. Default 0 so a
    -- plain "+ NEW" tab on cookrew-beta renders an empty board and
    -- waits for the operator. Orchestrator-mode flows that want LLM
    -- planning set this to 1 on create.
    autoplan_enabled INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_bundles_recipe ON bundles(recipe_id);
CREATE INDEX IF NOT EXISTS idx_bundles_cookbook ON bundles(cookbook_id);
CREATE INDEX IF NOT EXISTS idx_bundles_runnable
    ON bundles(status) WHERE graph_code IS NOT NULL;

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    bundle_id TEXT NOT NULL REFERENCES bundles(id),
    title TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'open'
        CHECK(status IN ('open', 'claimed', 'working', 'done', 'blocked', 'cancelled')),
    depends_on_task_ids TEXT NOT NULL DEFAULT '[]',
    assigned_agent_id TEXT,
    claimed_by_agent_id TEXT,
    claimed_at TEXT,
    completed_at TEXT,
    blocked_reason TEXT,
    graph_node_id TEXT,
    resource_version INTEGER NOT NULL DEFAULT 1,
    generation INTEGER NOT NULL DEFAULT 1,
    progress_json TEXT,
    -- Phase 4 M3: completion metadata for resumability
    session_id TEXT,
    work_dir TEXT,
    artifacts_json TEXT,
    -- Layer 4: session token isolation for event ingestion
    session_token TEXT,
    -- Auth track A2: runtime assignment + sandbox attachment
    assigned_runtime_id TEXT,
    sandbox_id TEXT
);

-- Auth track A2: e2b sandboxes — provisioned per bundle (preferred) or
-- per task (legacy). Lifecycle: provisioning -> ready -> running ->
-- terminated|error. Sweeper terminates idle (last_event_at older than
-- threshold) or aged (created_at older than max-age) sandboxes.
--
-- task_id is nullable (and has no FK) so bundle-scoped rows are valid;
-- bundle_id is the reverse pointer set when a sandbox is bundle-scoped.
-- The original schema had `task_id NOT NULL REFERENCES tasks(id)` and a
-- migration (_relax_sandboxes_task_id) rebuilds legacy DBs to match.
CREATE TABLE IF NOT EXISTS sandboxes (
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

CREATE INDEX IF NOT EXISTS idx_sandboxes_status_last_event
    ON sandboxes(status, last_event_at);

CREATE INDEX IF NOT EXISTS idx_tasks_bundle ON tasks(bundle_id);
CREATE INDEX IF NOT EXISTS idx_tasks_assigned ON tasks(assigned_agent_id);
CREATE INDEX IF NOT EXISTS idx_tasks_node ON tasks(bundle_id, graph_node_id);

-- Phase 3 M1: daemon runtime instances (krewcli processes)
CREATE TABLE IF NOT EXISTS agent_runtimes (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    daemon_version TEXT,
    provider TEXT,
    host_info TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'online'
        CHECK(status IN ('online', 'offline', 'degraded')),
    last_seen_at TEXT NOT NULL,
    started_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_runtimes_account ON agent_runtimes(account_id);
CREATE INDEX IF NOT EXISTS idx_agent_runtimes_last_seen ON agent_runtimes(last_seen_at);

-- Phase 4 M2: per-task LLM token usage (one row per reported run;
-- multiple rows accumulate for multi-turn tasks).
CREATE TABLE IF NOT EXISTS task_usage (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    model TEXT,
    cost_usd REAL,
    duration_ms INTEGER,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_usage_task ON task_usage(task_id);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    -- DEPRECATED — kept during dual-write; will drop with recipes.
    -- Nullable so cookbook-scoped events (no recipe) can be created.
    recipe_id TEXT REFERENCES recipes(id),
    -- New direct scope. Nullable until backfill completes.
    cookbook_id TEXT REFERENCES cookbooks(id),
    bundle_id TEXT REFERENCES bundles(id),
    task_id TEXT,
    type TEXT NOT NULL
        CHECK(type IN (
            'prompt', 'plan', 'task_claimed', 'task_working', 'milestone',
            'fact_added', 'code_pushed',
            'bundle_closed', 'bundle_reopened',
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
    visibility TEXT NOT NULL DEFAULT 'system',
    created_at TEXT NOT NULL,
    expires_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_recipe ON events(recipe_id);
CREATE INDEX IF NOT EXISTS idx_events_cookbook ON events(cookbook_id);
CREATE INDEX IF NOT EXISTS idx_events_bundle ON events(bundle_id);
CREATE INDEX IF NOT EXISTS idx_events_expires ON events(expires_at);
CREATE INDEX IF NOT EXISTS idx_events_task_sequence ON events(task_id, sequence);

CREATE TABLE IF NOT EXISTS tape_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tape_name TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    meta TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tape_entries_tape ON tape_entries(tape_name);

CREATE TABLE IF NOT EXISTS watch_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK(event_type IN ('ADDED', 'MODIFIED', 'DELETED')),
    resource_version INTEGER NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    recipe_id TEXT,
    cookbook_id TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_watch_log_type_seq ON watch_log(resource_type, seq);
CREATE INDEX IF NOT EXISTS idx_watch_log_recipe_seq ON watch_log(recipe_id, seq);
CREATE INDEX IF NOT EXISTS idx_watch_log_cookbook_seq ON watch_log(cookbook_id, seq);

CREATE TABLE IF NOT EXISTS identities (
    wallet_address TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    oauth_provider TEXT,
    oauth_sub TEXT,
    mpc_provider TEXT,
    mpc_key_id TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(oauth_provider, oauth_sub)
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    wallet_address TEXT NOT NULL REFERENCES identities(wallet_address),
    auth_method TEXT NOT NULL CHECK(auth_method IN ('siwe', 'oauth_mpc', 'api_key')),
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_wallet ON sessions(wallet_address);
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);

CREATE TABLE IF NOT EXISTS nonces (
    nonce TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    used INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_nonces_expires ON nonces(expires_at);

CREATE TABLE IF NOT EXISTS device_codes (
    device_code TEXT PRIMARY KEY,
    user_code TEXT NOT NULL UNIQUE,
    wallet_address TEXT,
    approved INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_device_codes_user ON device_codes(user_code);

-- Identity graph: stable account root
CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wallet_links (
    wallet_address TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    chain_id INTEGER NOT NULL DEFAULT 48816,
    is_primary INTEGER NOT NULL DEFAULT 0,
    linked_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wallet_links_account ON wallet_links(account_id);

-- WebAuthn passkeys
CREATE TABLE IF NOT EXISTS passkeys (
    credential_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    public_key TEXT NOT NULL,
    sign_count INTEGER NOT NULL DEFAULT 0,
    transports TEXT NOT NULL DEFAULT '[]',
    device_name TEXT,
    created_at TEXT NOT NULL,
    last_used_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_passkeys_account ON passkeys(account_id);

CREATE TABLE IF NOT EXISTS passkey_challenges (
    challenge TEXT PRIMARY KEY,
    account_id TEXT,
    purpose TEXT NOT NULL CHECK(purpose IN ('register', 'authenticate')),
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    used INTEGER NOT NULL DEFAULT 0
);

-- A2A hub gateway: agent cards + invocation mailbox
CREATE TABLE IF NOT EXISTS a2a_agent_cards (
    owner TEXT NOT NULL,
    agent_name TEXT NOT NULL,
    card_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (owner, agent_name)
);

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
);

CREATE INDEX IF NOT EXISTS idx_a2a_invocations_agent ON a2a_invocations(owner, agent_name, status);
CREATE INDEX IF NOT EXISTS idx_a2a_invocations_expires ON a2a_invocations(expires_at);

-- Invocation Contract — slice 1 (docs/INVOCATION-CONTRACT.md §6, §9)
-- One row per addressable Hand invocation; one tape per invocation.
CREATE TABLE IF NOT EXISTS invocations (
    id TEXT PRIMARY KEY,
    -- target_type validated at the route layer (parse_target() enforces
    -- the contract's closed set: sandbox/agent/human). Free-form here so
    -- the service can host test doubles + future Hand types without a
    -- schema bump.
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
);

-- Idempotency: same (parent_tape_id, idempotency_key) → same invocation.
-- NULL keys never collide because SQLite treats NULL as distinct in UNIQUE.
CREATE UNIQUE INDEX IF NOT EXISTS idx_invocations_idempotency
    ON invocations(parent_tape_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_invocations_status ON invocations(status);
CREATE INDEX IF NOT EXISTS idx_invocations_target ON invocations(target_type, target_id);

-- Append-only event log for invocation tapes. (tape_id, id) is monotonic
-- per tape; ids start at 0 and increment by 1.
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
);
CREATE INDEX IF NOT EXISTS idx_invocation_events_kind ON invocation_events(kind);

-- Fork registry: a child tape names its parent tape + the event id at
-- which it branched off. Handoff events on the parent tape close the loop.
CREATE TABLE IF NOT EXISTS tape_forks (
    child_tape_id TEXT PRIMARY KEY,
    parent_tape_id TEXT NOT NULL,
    fork_point_event_id INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tape_forks_parent ON tape_forks(parent_tape_id);

-- Operator credentials, scoped per cookrew account. Plaintext is held
-- only in the operator's browser (paste form) and on the wire to the
-- agent sandbox (as env var on op:exec). At rest we hold AES-GCM
-- ciphertext + 12-byte nonce. AES key is derived from
-- settings.credentials_encryption_key via sha256.
CREATE TABLE IF NOT EXISTS credentials (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    host TEXT NOT NULL,
    env_var_name TEXT NOT NULL,
    ciphertext BLOB NOT NULL,
    nonce BLOB NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    archived_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_credentials_account_host
    ON credentials(account_id, host) WHERE archived_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_credentials_account
    ON credentials(account_id) WHERE archived_at IS NULL;

-- Phase 12: recipe removal (additive — recipes table still exists for
-- the dual-write window). bundles + events get a direct cookbook_id;
-- bundles get an optional repo_spec (JSON) used for JIT repo
-- materialization gated by repo_grants.
CREATE TABLE IF NOT EXISTS cookbook_shares (
    id TEXT PRIMARY KEY,
    cookbook_id TEXT NOT NULL REFERENCES cookbooks(id),
    shared_with_account_id TEXT NOT NULL REFERENCES accounts(id),
    role TEXT NOT NULL CHECK(role IN ('owner', 'member', 'viewer')),
    shared_by_account_id TEXT NOT NULL REFERENCES accounts(id),
    shared_at TEXT NOT NULL,
    revoked_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_cookbook_shares_cookbook
    ON cookbook_shares(cookbook_id);
CREATE INDEX IF NOT EXISTS idx_cookbook_shares_account
    ON cookbook_shares(shared_with_account_id);
-- Partial unique: only one ACTIVE share per (cookbook, account).
-- Revoked rows kept for audit; reshare goes via new INSERT.
CREATE UNIQUE INDEX IF NOT EXISTS idx_cookbook_shares_unique_active
    ON cookbook_shares(cookbook_id, shared_with_account_id)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS repo_grants (
    id TEXT PRIMARY KEY,
    cookbook_id TEXT NOT NULL REFERENCES cookbooks(id),
    provider TEXT NOT NULL CHECK(provider IN ('github', 'gitlab', 'bitbucket')),
    scope TEXT NOT NULL,
    token_ref TEXT NOT NULL,
    granted_by_account_id TEXT NOT NULL REFERENCES accounts(id),
    granted_at TEXT NOT NULL,
    revoked_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_repo_grants_cookbook
    ON repo_grants(cookbook_id);
CREATE INDEX IF NOT EXISTS idx_repo_grants_active
    ON repo_grants(cookbook_id, provider) WHERE revoked_at IS NULL;
"""
