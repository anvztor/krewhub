from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

import aiosqlite

from krewhub.watch.globals import get_watch_service
from krewhub.auth import resolve_caller_or_cookie
from krewhub.db.connection import get_db
from krewhub.models import ActorType, CodeRef, EventType, FactRef, TaskStatus
from krewhub.repositories.task_repo import TaskRepo
from krewhub.services.bundle_service import BundleService
from krewhub.services.task_service import SessionTokenMismatch, TaskService
from krewhub.routes.schemas import (
    ClaimTaskRequest,
    EditTaskRequest,
    PostEventRequest,
    PostEventsBatchRequest,
    PostTaskCompletionRequest,
    PostTaskProgressRequest,
    PostTaskUsageRequest,
    UpdateTaskStatusRequest,
)

router = APIRouter(tags=["tasks"], dependencies=[Depends(resolve_caller_or_cookie)])


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

    from krewhub.services.redact import redact_json, redact_text

    facts = [FactRef(**redact_json(f)) for f in req.facts] if req.facts else []
    code_refs = [CodeRef(**redact_json(c)) for c in req.code_refs] if req.code_refs else []

    svc = TaskService(db, get_watch_service())
    try:
        event = await svc.post_event(
            task_id=task_id,
            recipe_id=bundle.recipe_id,
            event_type=EventType(req.type),
            actor_id=req.actor_id,
            actor_type=ActorType(req.actor_type),
            body=redact_text(req.body or ""),
            payload=redact_json(req.payload or {}),
            facts=facts,
            code_refs=code_refs,
            visibility=req.visibility,
            session_token=req.session_token,
        )
    except SessionTokenMismatch:
        raise HTTPException(status_code=409, detail="Session token mismatch")
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

    from krewhub.services.redact import redact_json, redact_text

    redacted_events = []
    for item in req.events:
        d = item.model_dump()
        d["body"] = redact_text(d.get("body") or "")
        d["payload"] = redact_json(d.get("payload") or {})
        d["facts"] = [redact_json(f) for f in (d.get("facts") or [])]
        d["code_refs"] = [redact_json(c) for c in (d.get("code_refs") or [])]
        redacted_events.append(d)

    svc = TaskService(db, get_watch_service())
    try:
        created = await svc.post_events_batch(
            task_id=task_id,
            recipe_id=bundle.recipe_id,
            events=redacted_events,
            session_token=req.session_token,
        )
    except SessionTokenMismatch:
        raise HTTPException(status_code=409, detail="Session token mismatch")
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


@router.post("/tasks/{task_id}/completion")
async def post_task_completion(
    task_id: str,
    req: PostTaskCompletionRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Record completion metadata — session_id, work_dir, artifacts.

    Called by the agent when the task finishes. Overwrites previous
    values (unlike usage, this is latest-wins).
    """
    import json as _json

    task = await TaskRepo(db).get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    artifacts_json = _json.dumps(req.artifacts)

    await db.execute(
        """UPDATE tasks
           SET session_id = ?, work_dir = ?, artifacts_json = ?
           WHERE id = ?""",
        (req.session_id, req.work_dir, artifacts_json, task_id),
    )
    await db.commit()

    updated = await TaskRepo(db).get(task_id)
    if updated is None:
        raise HTTPException(status_code=500, detail="Task disappeared after update")
    return {"task": updated.model_dump(mode="json")}


@router.post("/tasks/{task_id}/usage")
async def post_task_usage(
    task_id: str,
    req: PostTaskUsageRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Record LLM token usage for a task run.

    Multiple posts accumulate — one row per session. Typically called
    by the agent at session_end with Claude's usage totals.
    """
    import uuid as _uuid
    from datetime import datetime, timezone

    task = await TaskRepo(db).get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    usage_id = f"usg_{_uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc).isoformat()

    await db.execute(
        """INSERT INTO task_usage
           (id, task_id, input_tokens, output_tokens, model, cost_usd, duration_ms, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            usage_id, task_id,
            req.input_tokens, req.output_tokens,
            req.model, req.cost_usd, req.duration_ms, now,
        ),
    )
    await db.commit()

    return {"usage": {
        "id": usage_id,
        "task_id": task_id,
        "input_tokens": req.input_tokens,
        "output_tokens": req.output_tokens,
        "model": req.model,
        "cost_usd": req.cost_usd,
        "duration_ms": req.duration_ms,
        "created_at": now,
    }}


@router.get("/tasks/{task_id}/usage")
async def get_task_usage(
    task_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    """List all usage rows for a task, with totals."""
    task = await TaskRepo(db).get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    cursor = await db.execute(
        """SELECT id, task_id, input_tokens, output_tokens, model, cost_usd,
                  duration_ms, created_at
           FROM task_usage WHERE task_id = ? ORDER BY created_at ASC""",
        (task_id,),
    )
    rows = await cursor.fetchall()
    usage_list = [dict(r) for r in rows]

    totals = {
        "input_tokens": sum(r["input_tokens"] or 0 for r in usage_list),
        "output_tokens": sum(r["output_tokens"] or 0 for r in usage_list),
        "cost_usd": sum(r["cost_usd"] or 0 for r in usage_list) if any(r.get("cost_usd") for r in usage_list) else None,
        "duration_ms": sum(r["duration_ms"] or 0 for r in usage_list) if any(r.get("duration_ms") for r in usage_list) else None,
    }
    return {"usage": usage_list, "totals": totals}


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(
    task_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Cancel an in-flight task.

    Only tasks in open/claimed/working state can be cancelled.
    Emits a `task:cancelled` watch event so daemons can kill any
    running subprocess associated with this task.
    """
    svc = TaskService(db, get_watch_service())
    updated = await svc.cancel_task(task_id)
    if updated is None:
        # Differentiate 404 (no task) vs 400 (bad state)
        existing = await TaskRepo(db).get(task_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="Task not found")
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel task in status '{existing.status}'",
        )
    return {"task": updated.model_dump(mode="json")}


@router.get("/tasks/{task_id}/cancel-status")
async def get_task_cancel_status(
    task_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Lightweight endpoint for daemons to poll for cancellation.

    Designed to be hit every few seconds while a task is running.
    Returns `{cancelled: bool, task_id: str}`. Daemons should kill
    the subprocess when cancelled=true.
    """
    task = await TaskRepo(db).get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return {
        "task_id": task_id,
        "cancelled": task.status == TaskStatus.CANCELLED,
        "status": task.status,
    }


@router.post("/tasks/{task_id}/progress")
async def post_task_progress(
    task_id: str,
    req: PostTaskProgressRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Report in-flight progress for a running task.

    Stored as a JSON blob on the task row (latest-only — progress is
    ephemeral). Emits a `task:progress` watch event so the UI can
    update a live progress bar without waiting for completion.
    """
    import json as _json
    from datetime import datetime, timezone
    from krewhub.models import WatchEventType

    task = await TaskRepo(db).get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    # Derive percent if step+total given and percent missing
    percent = req.percent
    if percent is None and req.step is not None and req.total:
        percent = min(1.0, max(0.0, req.step / req.total))

    progress = {
        "summary": req.summary,
        "step": req.step,
        "total": req.total,
        "percent": percent,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    await db.execute(
        "UPDATE tasks SET progress_json = ? WHERE id = ?",
        (_json.dumps(progress), task_id),
    )
    await db.commit()

    # Emit watch event so SSE subscribers see the update.
    # Hint the channel derivation so this is classified as
    # task:progress (not task:working, even if status is working).
    watch = get_watch_service()
    updated = await TaskRepo(db).get(task_id)
    if updated is not None:
        task_dump = updated.model_dump(mode="json")
        task_dump["progress"] = progress
        task_dump["_channel_hint"] = "task:progress"
        await watch.record(
            resource_type="task",
            resource_id=task_id,
            event_type=WatchEventType.MODIFIED,
            resource_version=updated.resource_version,
            payload=task_dump,
            recipe_id=None,
        )

    return {"task_id": task_id, "progress": progress}


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
