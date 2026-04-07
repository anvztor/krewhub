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


class ClaimTaskRequest(BaseModel):
    agent_id: str


class PostEventRequest(BaseModel):
    type: str
    actor_id: str
    actor_type: str = "agent"
    body: str = ""
    payload: dict | None = None
    facts: list[dict] = []
    code_refs: list[dict] = []
    payload: dict = Field(default_factory=dict)


class BatchEventItem(BaseModel):
    type: str
    actor_id: str
    actor_type: str = "agent"
    body: str = ""
    payload: dict | None = None
    facts: list[dict] = []
    code_refs: list[dict] = []


class PostEventsBatchRequest(BaseModel):
    events: list[BatchEventItem] = Field(default_factory=list)


class UpdateTaskStatusRequest(BaseModel):
    status: str
    blocked_reason: str | None = None


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


class PlanRequest(BaseModel):
    prompt: str
    recipe_id: str
