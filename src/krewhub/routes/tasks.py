from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

import aiosqlite

from krewhub.watch.globals import get_watch_service
from krewhub.auth import resolve_caller
from krewhub.db.connection import get_db
from krewhub.models import ActorType, CodeRef, EventType, FactRef, TaskStatus
from krewhub.repositories.task_repo import TaskRepo
from krewhub.services.bundle_service import BundleService
from krewhub.services.task_service import TaskService
from krewhub.routes.schemas import (
    ClaimTaskRequest,
    EditTaskRequest,
    PostEventRequest,
    PostEventsBatchRequest,
    UpdateTaskStatusRequest,
)

router = APIRouter(tags=["tasks"], dependencies=[Depends(resolve_caller)])


@router.get("/tasks/{task_id}")
async def get_task(
    task_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Fetch a single task by ID."""
    task = await TaskRepo(db).get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"task": task.model_dump(mode="json")}


@router.post("/tasks/{task_id}/claim")
async def claim_task(
    task_id: str,
    req: ClaimTaskRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    task = await TaskRepo(db).get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    from krewhub.repositories.bundle_repo import BundleRepo
    bundle = await BundleRepo(db).get(task.bundle_id)
    if bundle is None:
        raise HTTPException(status_code=404, detail="Bundle not found")

    watch = get_watch_service()
    svc = TaskService(db, watch)
    updated = await svc.claim_task(task_id, req.agent_id, bundle.recipe_id)
    if updated is None:
        raise HTTPException(
            status_code=400,
            detail="Cannot claim task. Check status and dependencies.",
        )

    await BundleService(db, watch).recompute_bundle_status(task.bundle_id)

    return {"task": updated.model_dump(mode="json")}


@router.post("/tasks/{task_id}/events")
async def post_task_event(
    task_id: str,
    req: PostEventRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    task = await TaskRepo(db).get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    from krewhub.repositories.bundle_repo import BundleRepo
    bundle = await BundleRepo(db).get(task.bundle_id)
    if bundle is None:
        raise HTTPException(status_code=404, detail="Bundle not found")

    facts = [FactRef(**f) for f in req.facts] if req.facts else []
    code_refs = [CodeRef(**c) for c in req.code_refs] if req.code_refs else []

    svc = TaskService(db, get_watch_service())
    event = await svc.post_event(
        task_id=task_id,
        recipe_id=bundle.recipe_id,
        event_type=EventType(req.type),
        actor_id=req.actor_id,
        actor_type=ActorType(req.actor_type),
        body=req.body,
        payload=req.payload,
        facts=facts,
        code_refs=code_refs,
        payload=req.payload,
    )
    return {"event": event.model_dump(mode="json")}


@router.post("/tasks/{task_id}/events:batch")
async def post_task_events_batch(
    task_id: str,
    req: PostEventsBatchRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Append multiple events for a single task in one call.

    Used by krewcli's KrewhubEventSink to stream telemetry from local
    CLI agents (tool calls, thinking blocks, assistant replies) with
    minimal HTTP overhead. Sequence numbers are assigned server-side.
    """
    task = await TaskRepo(db).get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    from krewhub.repositories.bundle_repo import BundleRepo
    bundle = await BundleRepo(db).get(task.bundle_id)
    if bundle is None:
        raise HTTPException(status_code=404, detail="Bundle not found")

    svc = TaskService(db, get_watch_service())
    created = await svc.post_events_batch(
        task_id=task_id,
        recipe_id=bundle.recipe_id,
        events=[item.model_dump() for item in req.events],
    )
    return {"events": [e.model_dump(mode="json") for e in created]}


@router.patch("/tasks/{task_id}/status")
async def update_task_status(
    task_id: str,
    req: UpdateTaskStatusRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    watch = get_watch_service()
    svc = TaskService(db, watch)
    status = TaskStatus(req.status)

    if status == TaskStatus.DONE:
        updated = await svc.mark_done(task_id)
    elif status == TaskStatus.BLOCKED:
        updated = await svc.mark_blocked(task_id, req.blocked_reason or "Blocked")
    else:
        raise HTTPException(status_code=400, detail=f"Cannot set status to {status}")

    if updated is None:
        raise HTTPException(status_code=404, detail="Task not found")

    task = await TaskRepo(db).get(task_id)
    if task:
        await BundleService(db, watch).recompute_bundle_status(task.bundle_id)

    return {"task": updated.model_dump(mode="json")}


@router.patch("/tasks/{task_id}")
async def edit_task(
    task_id: str,
    req: EditTaskRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    svc = TaskService(db, get_watch_service())
    updated = await svc.edit_task(
        task_id=task_id,
        title=req.title,
        description=req.description,
        depends_on_task_ids=req.depends_on_task_ids,
    )
    if updated is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"task": updated.model_dump(mode="json")}


@router.delete("/tasks/{task_id}")
async def remove_task(
    task_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    svc = TaskService(db, get_watch_service())
    removed = await svc.remove_task(task_id)
    if not removed:
        raise HTTPException(status_code=400, detail="Cannot remove task (not found or not open)")
    return {"removed": True}
