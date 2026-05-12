"""A2A callback route — receives task results from gateways.

When a gateway finishes executing a task (via CLI subprocess), it
POSTs the result here. This updates the task status, records events,
and lets the BundleController reconcile the bundle phase.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

import aiosqlite

from krewhub.auth import resolve_caller
from krewhub.db.connection import get_db
from krewhub.models import (
    ActorType,
    CodeRef,
    Event,
    EventType,
    FactRef,
    TaskStatus,
    WatchEventType,
)
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.watch.globals import get_watch_service

router = APIRouter(tags=["a2a"], dependencies=[Depends(resolve_caller)])


class TaskCallbackCodeRef(BaseModel):
    repo_url: str
    branch: str
    commit_sha: str
    paths: list[str] = []


class TaskCallbackFact(BaseModel):
    claim: str
    source_url: str | None = None
    captured_by: str = "agent"
    confidence: float | None = None


class TaskCallbackRequest(BaseModel):
    task_id: str
    agent_id: str
    success: bool
    summary: str = ""
    full_output: str = ""
    blocked_reason: str | None = None
    files_modified: list[str] = []
    facts: list[TaskCallbackFact] = []
    code_refs: list[TaskCallbackCodeRef] = []


@router.post("/a2a/callback")
async def task_callback(
    req: TaskCallbackRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Receive task completion from a gateway."""
    task_repo = TaskRepo(db)
    event_repo = EventRepo(db)
    watch = get_watch_service()

    task = await task_repo.get(req.task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    # Only accept callbacks for tasks that are in progress
    if task.status not in (TaskStatus.CLAIMED, TaskStatus.WORKING):
        raise HTTPException(
            status_code=400,
            detail=f"Task {req.task_id} is not in progress (status={task.status})",
        )

    now = datetime.now(timezone.utc)
    new_status = TaskStatus.DONE if req.success else TaskStatus.BLOCKED

    updated = await task_repo.update(
        req.task_id,
        status=new_status,
        completed_at=now if req.success else None,
        blocked_reason=req.blocked_reason if not req.success else None,
    )

    # Record milestone event
    facts = [
        FactRef(
            id=f"f_{uuid.uuid4().hex[:8]}",
            claim=f.claim,
            source_url=f.source_url,
            captured_by=f.captured_by,
            confidence=f.confidence,
        )
        for f in req.facts
    ]
    code_refs = [
        CodeRef(
            repo_url=cr.repo_url,
            branch=cr.branch,
            commit_sha=cr.commit_sha,
            paths=cr.paths,
        )
        for cr in req.code_refs
    ]

    # Use full_output for the event body when available (e.g. codegen tasks),
    # fall back to summary for display-oriented events.
    event_body = req.full_output or req.summary or (
        "Task completed successfully" if req.success
        else f"Task blocked: {req.blocked_reason or 'unknown'}"
    )

    # Resolve cookbook_id from the bundle (step e — no recipes).
    from krewhub.repositories.bundle_repo import BundleRepo
    bundle = await BundleRepo(db).get(task.bundle_id)
    cookbook_id = bundle.cookbook_id if bundle else None

    event = Event(
        id=f"evt_{uuid.uuid4().hex[:8]}",
        cookbook_id=cookbook_id,
        bundle_id=task.bundle_id,
        task_id=req.task_id,
        type=EventType.MILESTONE,
        actor_id=req.agent_id,
        actor_type=ActorType.AGENT,
        body=event_body,
        facts=facts,
        code_refs=code_refs,
        created_at=now,
    )

    await event_repo.create(event)

    if updated is not None:
        await watch.record_resource(
            "task", req.task_id, WatchEventType.MODIFIED, updated,
            cookbook_id=cookbook_id,
        )

    # Step (d.1): bundle status is OPEN/CLOSED only; no recompute.

    return {
        "task": updated.model_dump(mode="json") if updated else None,
        "event": event.model_dump(mode="json"),
    }
