from __future__ import annotations

from pydantic import BaseModel, Field


class CreateRecipeRequest(BaseModel):
    name: str
    repo_url: str
    default_branch: str = "main"
    created_by: str
    cookbook_id: str


class InviteMemberRequest(BaseModel):
    actor_id: str
    actor_type: str = "human"
    role: str = "member"


# --- Cookbook sharing / repo grants (Phase 12) ---


class CreateCookbookShareRequest(BaseModel):
    shared_with_account_id: str
    role: str = "member"


class UpdateCookbookShareRequest(BaseModel):
    role: str


class CreateRepoGrantRequest(BaseModel):
    provider: str
    scope: str
    token_ref: str


class CreateTaskInput(BaseModel):
    model_config = {"populate_by_name": True}
    task_id: str | None = Field(None, alias="id")
    title: str
    description: str | None = None
    depends_on_task_ids: list[str] = []


class CreateBundleRequest(BaseModel):
    prompt: str
    requested_by: str
    tasks: list[CreateTaskInput] = []
    template: str | None = None  # built-in graph template name
    # Opt-in flag for the PlannerDispatchController. Default false so a
    # plain "new mission" tab on cookrew-beta renders an empty board and
    # waits for the operator to add tasks (double-click). Orchestrator-
    # mode flows that genuinely want LLM planning set this to true on
    # create, or POST /bundles/{id}/dispatch-planner later.
    autoplan: bool = False


class ClaimTaskRequest(BaseModel):
    agent_id: str


class PostEventRequest(BaseModel):
    type: str
    actor_id: str
    actor_type: str = "agent"
    body: str = ""
    payload: dict = Field(default_factory=dict)
    facts: list[dict] = []
    code_refs: list[dict] = []
    # Explicit visibility override ('user' or 'system'); when None,
    # classifier uses the type to decide.
    visibility: str | None = None
    # Layer 4: session token isolation — first event stamps, mismatches rejected
    session_token: str | None = None


class BatchEventItem(BaseModel):
    type: str
    actor_id: str
    actor_type: str = "agent"
    body: str = ""
    payload: dict | None = None
    facts: list[dict] = []
    code_refs: list[dict] = []
    visibility: str | None = None


class PostEventsBatchRequest(BaseModel):
    events: list[BatchEventItem] = Field(default_factory=list)
    # Layer 4: session token isolation — first event stamps, mismatches rejected
    session_token: str | None = None


class UpdateTaskStatusRequest(BaseModel):
    status: str
    blocked_reason: str | None = None


class PostTaskProgressRequest(BaseModel):
    """Progress update for a running task.

    Either step/total or percent can be provided. The summary is a
    short human-readable label (e.g. "Running tests", "Compiling").
    """
    summary: str = Field("", max_length=200)
    step: int | None = Field(None, ge=0)
    total: int | None = Field(None, ge=1)
    percent: float | None = Field(None, ge=0.0, le=1.0)


class RegisterRuntimeRequest(BaseModel):
    """Daemon registration — identifies a krewcli process instance."""
    agent_id: str
    account_id: str
    daemon_version: str | None = None
    provider: str | None = None
    host_info: dict = Field(default_factory=dict)


class PostTaskUsageRequest(BaseModel):
    """LLM token usage report for a task run (or sub-run).

    Multiple posts are allowed per task — they accumulate. Typically
    called at session_end with the totals from Claude's `usage` field.
    """
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    model: str | None = None
    cost_usd: float | None = Field(None, ge=0.0)
    duration_ms: int | None = Field(None, ge=0)


class PostTaskCompletionRequest(BaseModel):
    """Completion metadata for a task — session_id, work_dir, artifacts.

    All fields optional. session_id enables Claude resume; work_dir
    identifies where the task ran; artifacts captures files written,
    commits, PR URL, etc. for later inspection.
    """
    session_id: str | None = None
    work_dir: str | None = None
    artifacts: dict = Field(default_factory=dict)


class AddTaskRequest(BaseModel):
    title: str
    description: str | None = None
    depends_on_task_ids: list[str] = []


class AttachGraphRequest(BaseModel):
    """Body of POST /bundles/{id}/graph — orchestrator-emitted graph code."""
    code: str
    created_by: str = "orchestrator"


class EditTaskRequest(BaseModel):
    title: str | None = None
    description: str | None = None
    depends_on_task_ids: list[str] | None = None


class SubmitDigestRequest(BaseModel):
    submitted_by: str
    summary: str
    task_results: list[dict] = []
    facts: list[dict] = []
    code_refs: list[dict] = []


class DecisionRequest(BaseModel):
    decision: str
    decided_by: str
    note: str | None = None


class HeartbeatRequest(BaseModel):
    agent_id: str
    cookbook_id: str
    display_name: str
    capabilities: list[str] = []
    max_concurrent_tasks: int = 1
    endpoint_url: str | None = None
    current_task_id: str | None = None


class RegisterAgentRequest(BaseModel):
    agent_id: str
    cookbook_id: str
    display_name: str
    capabilities: list[str] = []
    max_concurrent_tasks: int = 1
    endpoint_url: str | None = None


class CreateCookbookRequest(BaseModel):
    name: str
    owner_id: str


class PostRecipeEventRequest(BaseModel):
    type: str
    actor_id: str
    actor_type: str = "agent"
    body: str = ""
    facts: list[dict] = []
    code_refs: list[dict] = []


class MintAgentRequest(BaseModel):
    cookbook_id: str
    tx_hash: str
    token_id: int | None = None


class PlanRequest(BaseModel):
    prompt: str
    recipe_id: str
