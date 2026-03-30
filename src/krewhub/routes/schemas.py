from __future__ import annotations

from pydantic import BaseModel


class CreateRecipeRequest(BaseModel):
    name: str
    repo_url: str
    default_branch: str = "main"
    created_by: str


class InviteMemberRequest(BaseModel):
    actor_id: str
    actor_type: str = "human"
    role: str = "member"


class CreateTaskInput(BaseModel):
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
    facts: list[dict] = []
    code_refs: list[dict] = []


class UpdateTaskStatusRequest(BaseModel):
    status: str
    blocked_reason: str | None = None


class AddTaskRequest(BaseModel):
    title: str
    description: str | None = None
    depends_on_task_ids: list[str] = []


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
    recipe_id: str
    display_name: str
    capabilities: list[str] = []
    current_task_id: str | None = None
