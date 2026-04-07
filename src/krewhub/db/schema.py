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
    resource_version INTEGER NOT NULL DEFAULT 1,
    generation INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_bundles_recipe ON bundles(recipe_id);

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
    resource_version INTEGER NOT NULL DEFAULT 1,
    generation INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_tasks_bundle ON tasks(bundle_id);
CREATE INDEX IF NOT EXISTS idx_tasks_assigned ON tasks(assigned_agent_id);

CREATE TABLE IF NOT EXISTS events (
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

CREATE INDEX IF NOT EXISTS idx_events_recipe ON events(recipe_id);
CREATE INDEX IF NOT EXISTS idx_events_bundle ON events(bundle_id);
CREATE INDEX IF NOT EXISTS idx_events_expires ON events(expires_at);

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
"""
