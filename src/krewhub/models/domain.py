from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field


class Role(StrEnum):
    OWNER = "owner"
    MEMBER = "member"
    AGENT = "agent"


class BundleStatus(StrEnum):
    OPEN = "open"
    CLAIMED = "claimed"
    COOKED = "cooked"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"
    DIGESTED = "digested"
    REJECTED = "rejected"


class TaskStatus(StrEnum):
    OPEN = "open"
    CLAIMED = "claimed"
    WORKING = "working"
    DONE = "done"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


class EventType(StrEnum):
    PROMPT = "prompt"
    PLAN = "plan"
    TASK_CLAIMED = "task_claimed"
    TASK_WORKING = "task_working"
    MILESTONE = "milestone"
    FACT_ADDED = "fact_added"
    CODE_PUSHED = "code_pushed"
    DIGEST_SUBMITTED = "digest_submitted"
    DIGEST_APPROVED = "digest_approved"
    DIGEST_REJECTED = "digest_rejected"
    # Agent-level events (streamed from local CLI agents)
    SESSION_START = "session_start"
    SESSION_END = "session_end"
    TOOL_USE = "tool_use"
    TOOL_RESULT = "tool_result"
    AGENT_REPLY = "agent_reply"
    THINKING = "thinking"


class AgentStatus(StrEnum):
    ONLINE = "online"
    OFFLINE = "offline"
    BUSY = "busy"


class ActorType(StrEnum):
    HUMAN = "human"
    AGENT = "agent"
    SYSTEM = "system"
    HOOK = "hook"


class DigestDecision(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


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


class Recipe(BaseModel, frozen=True):
    id: str
    name: str
    repo_url: str
    default_branch: str
    created_by: str
    created_at: datetime
    cookbook_id: str | None = None


class RecipeMember(BaseModel, frozen=True):
    id: str
    recipe_id: str
    actor_id: str
    actor_type: Literal["human", "agent"]
    role: Role
    joined_at: datetime


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
    recipe_id: str
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
    recipe_id: str
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


class DigestTaskResult(BaseModel, frozen=True):
    task_id: str
    outcome: str


class Digest(BaseModel, frozen=True):
    id: str
    recipe_id: str
    bundle_id: str
    summary: str
    task_results: list[DigestTaskResult] = Field(default_factory=list)
    facts: list[FactRef] = Field(default_factory=list)
    code_refs: list[CodeRef] = Field(default_factory=list)
    submitted_by: str
    submitted_at: datetime
    decision: DigestDecision = DigestDecision.PENDING
    decided_by: str | None = None
    decided_at: datetime | None = None
    resource_version: int = 1
    generation: int = 1


class WatchEntry(BaseModel, frozen=True):
    seq: int
    resource_type: str
    resource_id: str
    event_type: WatchEventType
    resource_version: int
    payload: dict
    recipe_id: str | None = None
    created_at: datetime
