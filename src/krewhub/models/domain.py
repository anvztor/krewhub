from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field


# DEPRECATED: kept as stubs so legacy import chains compile.
# Recipes are gone in step (e); these classes never represent real data.
class Recipe(BaseModel):
    """DEPRECATED stub — recipes table no longer exists."""
    id: str
    name: str = ""
    repo_url: str = ""
    default_branch: str = "main"
    created_by: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now())
    cookbook_id: str | None = None


class Role(StrEnum):
    """DEPRECATED stub — recipe_members table no longer exists."""
    OWNER = "owner"
    MEMBER = "member"
    AGENT = "agent"


class RecipeMember(BaseModel):
    """DEPRECATED stub — recipe_members table no longer exists."""
    id: str
    recipe_id: str
    actor_id: str
    actor_type: str = "human"
    role: Role = Role.MEMBER
    joined_at: datetime = Field(default_factory=lambda: datetime.now())


class BundleStatus(StrEnum):
    """Phase 12 step (d.1): collapsed FSM.

    Bundle is a dumb container. Task-level FSM is authoritative; the
    bundle does not derive from task aggregate, does not approve or
    reject task work, and does not have BLOCKED / CANCELLED terminal
    states. Lifecycle is just open ↔ closed (idempotent, reversible).

    Migration backfilled legacy rows:
        CLAIMED / COOKED / BLOCKED → OPEN
        CANCELLED / DIGESTED / REJECTED → CLOSED
    """
    OPEN = "open"
    CLOSED = "closed"


class TaskStatus(StrEnum):
    OPEN = "open"
    CLAIMED = "claimed"
    WORKING = "working"
    DONE = "done"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"
    # Orch mode (O1): worker emitted needs_review and is parked at the
    # review gate until an owner approves/rejects via POST /tasks/{id}/review.
    BLOCKED_ON_REVIEW = "blocked_on_review"


class EventType(StrEnum):
    PROMPT = "prompt"
    PLAN = "plan"
    TASK_CLAIMED = "task_claimed"
    TASK_WORKING = "task_working"
    MILESTONE = "milestone"
    FACT_ADDED = "fact_added"
    CODE_PUSHED = "code_pushed"
    # Bundle lifecycle (Phase 12 step d).
    BUNDLE_CLOSED = "bundle_closed"
    BUNDLE_REOPENED = "bundle_reopened"
    # Agent-level events (streamed from local CLI agents)
    SESSION_START = "session_start"
    SESSION_END = "session_end"
    TOOL_USE = "tool_use"
    TOOL_RESULT = "tool_result"
    AGENT_REPLY = "agent_reply"
    THINKING = "thinking"
    # Orch mode (O1): semantic observation events a worker self-reports so
    # the orchestrator drives its control loop on meaning, not screen-scraping.
    # (milestone above is reused as-is.)
    PROGRESS = "progress"
    BLOCKER = "blocker"
    NEEDS_REVIEW = "needs_review"
    NEEDS_HUMAN = "needs_human"
    LOG = "log"


class AgentStatus(StrEnum):
    ONLINE = "online"
    OFFLINE = "offline"
    BUSY = "busy"


class ActorType(StrEnum):
    HUMAN = "human"
    AGENT = "agent"
    SYSTEM = "system"
    HOOK = "hook"


class WatchEventType(StrEnum):
    ADDED = "ADDED"
    MODIFIED = "MODIFIED"
    DELETED = "DELETED"


# --- Domain Models ---


class Cookbook(BaseModel, frozen=True):
    id: str
    name: str
    owner_id: str
    created_at: datetime


class ShareRole(StrEnum):
    OWNER = "owner"
    MEMBER = "member"
    VIEWER = "viewer"


class CookbookShare(BaseModel, frozen=True):
    id: str
    cookbook_id: str
    shared_with_account_id: str
    role: ShareRole
    shared_by_account_id: str
    shared_at: datetime
    revoked_at: datetime | None = None


class RepoProvider(StrEnum):
    GITHUB = "github"
    GITLAB = "gitlab"
    BITBUCKET = "bitbucket"


class RepoGrant(BaseModel, frozen=True):
    id: str
    cookbook_id: str
    provider: RepoProvider
    # Scope syntax:
    #   "owner/repo"   one specific repo
    #   "owner/*"      all repos under an owner/org
    #   "owner"        same as "owner/*"
    scope: str
    # Reference into the secret store (vault key, KMS arn, etc).
    # Never the raw token — krewhub does not see plaintext credentials.
    token_ref: str
    granted_by_account_id: str
    granted_at: datetime
    revoked_at: datetime | None = None


class AgentPresence(BaseModel, frozen=True):
    agent_id: str
    cookbook_id: str
    display_name: str
    capabilities: list[str]
    max_concurrent_tasks: int = 1
    endpoint_url: str | None = None
    status: AgentStatus
    last_heartbeat_at: datetime
    current_task_id: str | None = None
    resource_version: int = 1
    owner_username: str | None = None
    mint_tx_hash: str | None = None
    mint_token_id: int | None = None
    aa_wallet_address: str | None = None


class Bundle(BaseModel, frozen=True):
    id: str
    # Phase 12 step (e): bundles are cookbook-scoped only. recipes
    # gone, recipe_id column dropped, repo binding moved to repo_spec.
    cookbook_id: str | None = None
    # Phase 12: optional JIT repo hint (JSON serialized in the DB).
    # Shape: {"provider": "github", "owner": "...", "repo": "...", "ref": "main"}
    # When set, working-tree provisioning resolves this against
    # repo_grants on the cookbook. NULL means this bundle does no
    # file work.
    repo_spec: dict | None = None
    prompt: str
    status: BundleStatus
    created_by: str
    created_at: datetime
    claimed_at: datetime | None = None
    cooked_at: datetime | None = None
    digested_at: datetime | None = None
    blocked_reason: str | None = None
    # Graph runtime: validated pydantic-graph source + rendered mermaid.
    # Set by the orchestrator/bundle service after the LLM-generated code
    # passes the sandbox; consumed by GraphRunnerController.
    graph_code: str | None = None
    graph_mermaid: str | None = None
    resource_version: int = 1
    generation: int = 1
    # Bundle ownership for ABAC + paired-agent assignment.
    # owner_account_id set by A1 pair-agent flow (or KREW_DEV_FAKE_AUTH seed).
    # NULL on legacy bundles created before the auth journey.
    owner_account_id: str | None = None
    default_agent_runtime_id: str | None = None
    # Per-bundle e2b sandbox. Provisioned on bundle create; every task
    # in this bundle reuses it so the agent's working tree (cloned repo,
    # generated files, edits) survives across tasks.
    sandbox_id: str | None = None
    # Bundle lifecycle: MAX(tasks.updated_at) for this bundle, falling
    # back to bundle.created_at when no tasks exist. Drives cookrew-beta's
    # active/idle bucket. Computed at query time by BundleRepo.list_by_cookbook;
    # None on Bundle objects constructed outside that path (e.g. fresh
    # BundleService.create() return values). Callers that need a value
    # should fall back to created_at.
    latest_task_activity_at: datetime | None = None
    # Bundle lifecycle: COUNT(tasks) for this bundle, computed at query
    # time by BundleRepo.list_by_cookbook so the SPA can render tab task
    # counts without N+1 getBundle calls. None on Bundle objects
    # constructed outside that path.
    task_count: int | None = None


class Task(BaseModel, frozen=True):
    id: str
    bundle_id: str
    title: str
    description: str | None = None
    status: TaskStatus
    depends_on_task_ids: list[str] = Field(default_factory=list)
    assigned_agent_id: str | None = None
    claimed_by_agent_id: str | None = None
    claimed_at: datetime | None = None
    completed_at: datetime | None = None
    blocked_reason: str | None = None
    # Graph runtime: identifies which step of a generated pydantic-graph
    # this task corresponds to. Used by dispatch_cycle to look up task_id
    # by node name. None for tasks created outside the graph flow.
    graph_node_id: str | None = None
    resource_version: int = 1
    generation: int = 1
    # Latest progress reported by the agent (ephemeral).
    # Shape: {summary, step, total, percent, updated_at}
    progress: dict | None = None
    # Phase 4 M3: completion metadata (for resumability / audit)
    session_id: str | None = None
    work_dir: str | None = None
    artifacts: dict = Field(default_factory=dict)
    # Layer 4: session token isolation — first event stamps, mismatches rejected
    session_token: str | None = None
    # Auth track A2: runtime + sandbox assignment populated when a task is
    # dispatched to a paired agent runtime via an e2b sandbox.
    assigned_runtime_id: str | None = None
    sandbox_id: str | None = None
    # Orch mode (O1): structured Brief (hand-off) + Report (hand-back).
    # None on legacy / non-orch tasks. Stored as JSON, surfaced as dicts so
    # the orchestrator can replay a Brief (respawn) or validate a Report.
    brief: dict | None = None
    report: dict | None = None
    # Orch mode (O2): controller bookkeeping for Brief-managed tasks —
    # {respawns, last_respawn_at, accepted_at, report_invalid, halted}.
    orch: dict | None = None
    # Orch mode (O3b): provenance — the orch task that created this one
    # as its downstream (new-cell). None for human/board-created tasks.
    created_by_task: str | None = None
    # Bundle lifecycle: drives frontend active/idle bucket. Bumped by
    # TaskRepo on create + every update. Nullable on legacy rows that
    # the migration backfills.
    updated_at: datetime | None = None


class FactRef(BaseModel, frozen=True):
    id: str
    claim: str
    source_url: str | None = None
    source_title: str | None = None
    captured_by: str
    confidence: float | None = None


class CodeRef(BaseModel, frozen=True):
    repo_url: str
    branch: str
    commit_sha: str
    paths: list[str]


class Event(BaseModel, frozen=True):
    id: str
    # Phase 12 step (e): cookbook-only scope.
    cookbook_id: str | None = None
    bundle_id: str | None = None
    task_id: str | None = None
    type: EventType
    actor_id: str
    actor_type: ActorType
    body: str
    payload: dict | None = None
    sequence: int = 0
    facts: list[FactRef] = Field(default_factory=list)
    code_refs: list[CodeRef] = Field(default_factory=list)
    payload: dict = Field(default_factory=dict)
    # Phase 4 M5: 'user' for human-visible milestones, 'system' for
    # agent telemetry (tool_use, thinking, etc.)
    visibility: str = "system"
    created_at: datetime
    expires_at: datetime | None = None


class WatchEntry(BaseModel, frozen=True):
    seq: int
    resource_type: str
    resource_id: str
    event_type: WatchEventType
    resource_version: int
    payload: dict
    # Phase 12: cookbook scope for SSE channel routing.
    cookbook_id: str | None = None
    created_at: datetime
