from __future__ import annotations

from pydantic import BaseModel, Field


# --- Orch mode (O1): Brief / Report contract ----------------------------
#
# A Brief is the structured task hand-off the orchestrator gives a worker
# (replaces the brittle free-text "send --text" of the shell era). A Report
# is the structured terminal hand-back the worker returns on completion.
# Both are OPTIONAL everywhere they appear: a task created without a brief,
# or completed without a report, behaves exactly as before (backward compat).
# When present, Pydantic validates the shape (e.g. a brief missing `goal`
# is a 422), giving the orchestrator a contract to rely on.


class Brief(BaseModel):
    """Structured task hand-off (orchestrator → worker)."""
    goal: str
    context: str = ""
    constraints: list[str] = Field(default_factory=list)
    deliverable: str
    report_points: list[str] = Field(default_factory=list)
    # Optional pre-authorized action chain (the orchestrator's "预授权链"):
    # actions the worker may take without re-escalating. Free-form strings.
    pre_auth: list[str] | None = None


class Report(BaseModel):
    """Structured terminal hand-back (worker → orchestrator) at completion."""
    status: str  # e.g. done | blocked | needs_review
    artifacts: list[str] = Field(default_factory=list)
    prs: list[str] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    decisions_needed: list[str] = Field(default_factory=list)


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


class BundleLifecycleRequest(BaseModel):
    """Phase 12 step (d): one verb for OPEN ↔ CLOSED.

    Idempotent and symmetric. Replaces the old cancel + rerun +
    decision endpoints.
    """
    status: str  # "open" or "closed"
    reason: str | None = None


class CreateTaskInput(BaseModel):
    model_config = {"populate_by_name": True}
    task_id: str | None = Field(None, alias="id")
    title: str
    description: str | None = None
    depends_on_task_ids: list[str] = []
    # Orch mode (O1): optional structured hand-off. Absent ⇒ legacy behavior.
    brief: Brief | None = None


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


class CreateCookbookBundleRequest(BaseModel):
    """Phase 12: cookbook-scoped bundle creation.

    Same shape as CreateBundleRequest but without the recipe coupling
    and with an optional repo_spec for JIT clone gating.
    """
    prompt: str
    tasks: list[CreateTaskInput] = []
    autoplan: bool = False
    repo_spec: dict | None = None


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
    # Orch mode (O1): optional structured Report. Absent ⇒ legacy behavior.
    report: Report | None = None


class AddTaskRequest(BaseModel):
    title: str
    description: str | None = None
    depends_on_task_ids: list[str] = []
    # Orch mode (O1): optional structured hand-off. Absent ⇒ legacy behavior.
    brief: Brief | None = None


class TaskReviewRequest(BaseModel):
    """Body of POST /tasks/{id}/review — the orch/owner review-gate verdict.

    A worker that emits a `needs_review` event parks the task at
    `blocked_on_review`; the owner resolves it here. diff_summary is an
    optional human/orch-supplied note of what was reviewed (recorded in
    the audit row).
    """
    action: str  # "approve" | "reject"
    reason: str | None = None
    diff_summary: str | None = None


class NewLinkedTaskInput(BaseModel):
    """Inline downstream-task spec for POST /tasks/{id}/links (orch's
    new-cell: A creates its own downstream with provenance)."""
    title: str
    description: str | None = None
    brief: Brief | None = None


class CreateLinkRequest(BaseModel):
    """Body of POST /tasks/{from_task_id}/links — create a data-flow edge
    (design §5). Exactly one of to_task_id (link an existing task) or
    new_task (create the downstream task inline, provenance-stamped with
    created_by_task = from-task) must be given.

    kind: pipe (A's output -> B's prompt; also appends a dep so B waits
    for A) | subagent (A delegates a Brief to B; B's Report flows back
    onto A's tape; no dep — A is waiting on B, not blocked by it).

    payload_map v1 keys: source = report|last_reply (default report),
    target = followup|brief_context (default followup).
    """
    to_task_id: str | None = None
    new_task: NewLinkedTaskInput | None = None
    kind: str = "pipe"  # "pipe" | "subagent"
    payload_map: dict = Field(default_factory=dict)


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
