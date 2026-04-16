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
    recipe_id TEXT NOT NULL REFERENCES recipes(id),
    prompt TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open'
        CHECK(status IN ('open', 'claimed', 'cooked', 'blocked', 'cancelled', 'digested', 'rejected')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    claimed_at TEXT,
    cooked_at TEXT,
    digested_at TEXT,
    blocked_reason TEXT,
    graph_code TEXT,
    graph_mermaid TEXT,
    resource_version INTEGER NOT NULL DEFAULT 1,
    generation INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_bundles_recipe ON bundles(recipe_id);
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
    artifacts_json TEXT
);

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
    recipe_id TEXT NOT NULL REFERENCES recipes(id),
    bundle_id TEXT REFERENCES bundles(id),
    task_id TEXT,
    type TEXT NOT NULL
        CHECK(type IN (
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
    visibility TEXT NOT NULL DEFAULT 'system',
    created_at TEXT NOT NULL,
    expires_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_recipe ON events(recipe_id);
CREATE INDEX IF NOT EXISTS idx_events_bundle ON events(bundle_id);
CREATE INDEX IF NOT EXISTS idx_events_expires ON events(expires_at);
CREATE INDEX IF NOT EXISTS idx_events_task_sequence ON events(task_id, sequence);

CREATE TABLE IF NOT EXISTS digests (
    id TEXT PRIMARY KEY,
    recipe_id TEXT NOT NULL REFERENCES recipes(id),
    bundle_id TEXT NOT NULL UNIQUE REFERENCES bundles(id),
    summary TEXT NOT NULL,
    task_results TEXT NOT NULL DEFAULT '[]',
    facts TEXT NOT NULL DEFAULT '[]',
    code_refs TEXT NOT NULL DEFAULT '[]',
    submitted_by TEXT NOT NULL,
    submitted_at TEXT NOT NULL,
    decision TEXT NOT NULL DEFAULT 'pending' CHECK(decision IN ('pending', 'approved', 'rejected')),
    decided_by TEXT,
    decided_at TEXT,
    resource_version INTEGER NOT NULL DEFAULT 1,
    generation INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_digests_recipe ON digests(recipe_id);
CREATE INDEX IF NOT EXISTS idx_digests_bundle ON digests(bundle_id);

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
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_watch_log_type_seq ON watch_log(resource_type, seq);
CREATE INDEX IF NOT EXISTS idx_watch_log_recipe_seq ON watch_log(recipe_id, seq);

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
"""
