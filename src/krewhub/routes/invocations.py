"""HTTP + SSE for /api/v1/invocations (Invocation Contract slice 1).

Endpoints (contract §9):
- POST   /api/v1/invocations              create + dispatch
- GET    /api/v1/invocations/:id          fetch state
- GET    /api/v1/invocations/:id/events   page through events
- GET    /api/v1/invocations/:id/stream   SSE stream
- POST   /api/v1/invocations/:id/result   submit terminal envelope (HumanHand)
- POST   /api/v1/invocations/:id/cancel   operator-initiated cancel
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import AsyncIterator

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from krewhub.auth import resolve_caller_or_cookie, CallerContext
from krewhub.db.connection import get_db
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.elicit_repo import ElicitRepo
from krewhub.repositories.invocation_repo import InvocationRepo as _InvocationRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.models.invocation import (
    InvocationRequest,
    ResultEnvelope,
    parse_target,
    validate_request_schema,
)
from krewhub.services.invocation_service import (
    InvocationService,
    _ConflictError,  # internal but exposed for HTTPException translation
)
from krewhub.services.sandbox_service import SandboxService
from krewhub.watch.globals import get_watch_service
from krewhub.watch.types import WatchOptions


logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# DI: resolve the InvocationService singleton from app.state
# ---------------------------------------------------------------------------


def get_service(request: Request) -> InvocationService:
    svc: InvocationService | None = getattr(
        request.app.state, "invocations", None,
    )
    if svc is None:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "invocations_not_configured",
                "message": "InvocationService not initialised on app state",
            },
        )
    return svc


# ---------------------------------------------------------------------------
# POST /api/v1/invocations
# ---------------------------------------------------------------------------


@router.post("/invocations")
async def post_invocation(
    request: Request,
    req: InvocationRequest,
    svc: InvocationService = Depends(get_service),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
    db: aiosqlite.Connection = Depends(get_db),
):
    # Bare `target: "sandbox"` (no id) triggers platform-side
    # resolution: SandboxService.ensure_sandbox_for_bundle returns the
    # bundle's current ready sandbox, provisioning if missing or
    # terminated. With this in place, the bridge no longer caches
    # KREWHUB_SANDBOX_ID at task spawn — every delegate(sandbox) call
    # gets the bundle's current sandbox transparently. Brain never
    # sees `no_sandbox_attached` and never asks the human.
    if req.target == "sandbox":
        if not req.bundle_id:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "bad_target",
                    "message": (
                        "bare `target: \"sandbox\"` requires `bundle_id` "
                        "in the request body so the platform can resolve "
                        "to (or provision) the bundle's sandbox"
                    ),
                },
            )
        e2b = getattr(request.app.state, "e2b", None)
        if e2b is None:
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "platform_unavailable",
                    "message": "e2b client not initialised on app state",
                },
            )
        try:
            resolved = await SandboxService(db, e2b).ensure_sandbox_for_bundle(
                req.bundle_id,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "platform_unavailable",
                    "message": f"could not ensure sandbox for bundle: {exc}",
                },
            )
        req = req.model_copy(update={"target": f"sandbox:{resolved.id}"})

    # Service.create() validates target shape (against its registry) and
    # schema dialect; both raise ValueError → 400. Out-of-bounds
    # `deadline_s` is already a 422 from pydantic.
    try:
        inv = await svc.create(req, caller_account_id=caller.account_id)
    except ValueError as exc:
        msg = str(exc)
        # Route the error code so the client (and tests) can grep it:
        # target-shape errors carry "target"; schema errors carry "schema".
        code = (
            "bad_target" if (
                "target" in msg or "human accepts" in msg
                or "requires an id" in msg or "unknown target type" in msg
            )
            else "bad_schema" if "schema" in msg or "nested" in msg
            else "bad_request"
        )
        raise HTTPException(
            status_code=400,
            detail={"code": code, "message": msg},
        )
    return {
        "invocation_id": inv.id,
        "tape_id": inv.tape_id,
        "status": inv.status,
    }


# ---------------------------------------------------------------------------
# GET /api/v1/invocations/:id
# ---------------------------------------------------------------------------


@router.get("/invocations/{invocation_id}")
async def get_invocation(
    invocation_id: str,
    svc: InvocationService = Depends(get_service),
    _caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    inv = await svc.get(invocation_id)
    if inv is None:
        raise HTTPException(status_code=404, detail="invocation not found")
    return {
        "invocation": inv.model_dump(mode="json"),
        "status": inv.status,
        "latest_event_id": await svc._events.latest_id(inv.tape_id),
    }


# ---------------------------------------------------------------------------
# GET /api/v1/invocations/:id/events
# ---------------------------------------------------------------------------


@router.get("/invocations/{invocation_id}/events")
async def list_events(
    invocation_id: str,
    after: int | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=1000),
    svc: InvocationService = Depends(get_service),
    _caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    inv = await svc.get(invocation_id)
    if inv is None:
        raise HTTPException(status_code=404, detail="invocation not found")
    events = await svc.list_events(inv.tape_id, after=after, limit=limit)
    return {
        "events": [e.model_dump(mode="json") for e in events],
        "next_after": events[-1].id if events else after,
    }


# ---------------------------------------------------------------------------
# POST /api/v1/invocations/:id/result   (HumanHand bridge)
# ---------------------------------------------------------------------------


@router.post("/invocations/{invocation_id}/result")
async def post_result(
    invocation_id: str,
    envelope: ResultEnvelope,
    svc: InvocationService = Depends(get_service),
    _caller: CallerContext = Depends(resolve_caller_or_cookie),
    db: aiosqlite.Connection = Depends(get_db),
):
    inv = await svc.get(invocation_id)
    try:
        ev = await svc.submit_result(invocation_id, envelope)
    except KeyError:
        raise HTTPException(status_code=404, detail="invocation not found")
    except _ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    # Project the terminal envelope onto the task tape (best-effort).
    # When the brain invoked delegate(human) while running a task, the
    # operator's answer needs to appear in the next prompt as a HUMAN
    # turn so `_build_prompt_with_context` can thread it. We piggy-back
    # on the existing human_followup workaround (type='agent_reply' +
    # actor_type='human') and tag payload.kind='delegate_answer' so the
    # UI can render it distinctly from a plain operator follow-up.
    if inv is not None and inv.task_id and envelope.action in (
        "accept", "decline", "cancel", "error",
    ):
        try:
            await _project_invocation_to_task_tape(
                db,
                task_id=inv.task_id,
                invocation_id=invocation_id,
                envelope=envelope,
                actor_id=inv.created_by or "",
            )
        except Exception:
            # Don't fail the operator's submit on a projection error —
            # the invocation result is already authoritative; the tape
            # projection is a convenience for prompt continuity.
            logger.warning(
                "post_result: tape projection failed for inv=%s task=%s",
                invocation_id, inv.task_id, exc_info=True,
            )

    return {"ok": True, "event_id": ev.id}


async def _project_invocation_to_task_tape(
    db: aiosqlite.Connection,
    *,
    task_id: str,
    invocation_id: str,
    envelope: ResultEnvelope,
    actor_id: str,
) -> None:
    """Write a synthetic `agent_reply` event (actor_type=human) to the
    task's events tape so the brain's next prompt-build threads the
    operator's answer as a HUMAN turn.

    Idempotent on (task_id, payload.invocation_id): re-submits (or
    duplicate result submissions from the bridge / SSE retries) skip
    silently rather than appending dupes.
    """
    # 1. Idempotency probe — has this invocation already been projected?
    probe = await db.execute(
        "SELECT id FROM events WHERE task_id = ? AND type = 'agent_reply' "
        "AND actor_type = 'human' AND payload LIKE ? LIMIT 1",
        (task_id, f'%"invocation_id": "{invocation_id}"%'),
    )
    if await probe.fetchone() is not None:
        return

    # 2. Find the cookbook + bundle for this task — events rows need
    # both columns populated (step (e) replaced recipe_id with
    # cookbook_id; followup uses the same shape).
    trow = await db.execute(
        "SELECT t.bundle_id, b.cookbook_id FROM tasks t "
        "JOIN bundles b ON b.id = t.bundle_id WHERE t.id = ?",
        (task_id,),
    )
    tr = await trow.fetchone()
    if tr is None:
        return
    bundle_id, cookbook_id = tr[0], tr[1]

    # 3. Render the envelope as the projected body. `accept` → content
    # text; failure actions → a short reason summary so the brain can
    # see *what happened* on re-entry instead of silently retrying.
    body = _envelope_body(envelope)
    if not body.strip():
        return

    # 4. Allocate the next sequence and write the event.
    seq_row = await db.execute(
        "SELECT COALESCE(MAX(sequence), 0) + 1 FROM events WHERE task_id = ?",
        (task_id,),
    )
    seq = (await seq_row.fetchone())[0] or 1

    from uuid import uuid4
    event_id = f"evt_{uuid4().hex[:12]}"
    now = datetime.now(timezone.utc).isoformat()
    payload = json.dumps({
        "text": body,
        "kind": "delegate_answer",
        "invocation_id": invocation_id,
        "action": envelope.action,
    })
    await db.execute(
        "INSERT INTO events (id, cookbook_id, bundle_id, task_id, type, "
        "actor_id, actor_type, body, payload, sequence, facts, code_refs, "
        "visibility, created_at) "
        "VALUES (?, ?, ?, ?, 'agent_reply', ?, 'human', ?, ?, ?, "
        "'[]', '[]', 'user', ?)",
        (event_id, cookbook_id, bundle_id, task_id, actor_id or "system",
         body, payload, seq, now),
    )
    await db.commit()


def _envelope_body(envelope: ResultEnvelope) -> str:
    """Render a ResultEnvelope as the text body the brain will see."""
    if envelope.action == "accept":
        content = envelope.content
        if isinstance(content, str):
            return content
        if isinstance(content, dict):
            # Prefer a `text` / `message` field if present, else fall
            # back to compact JSON so the brain can still parse it.
            for key in ("text", "message", "answer", "response"):
                v = content.get(key)
                if isinstance(v, str) and v.strip():
                    return v
            try:
                return json.dumps(content, sort_keys=True)
            except Exception:
                return str(content)
        return ""
    # Decline / cancel / error — surface the reason so the brain knows
    # the operator declined or the invocation failed.
    reason = envelope.reason or envelope.action
    return f"[{envelope.action}] {reason}"


# ---------------------------------------------------------------------------
# POST /api/v1/invocations/:id/cancel
# ---------------------------------------------------------------------------


@router.post("/invocations/{invocation_id}/cancel")
async def post_cancel(
    invocation_id: str,
    svc: InvocationService = Depends(get_service),
    _caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    inv = await svc.get(invocation_id)
    if inv is None:
        raise HTTPException(status_code=404, detail="invocation not found")
    await svc.cancel(invocation_id)
    return {"ok": True}


# ---------------------------------------------------------------------------
# GET /api/v1/invocations/:id/stream    (SSE)
# ---------------------------------------------------------------------------


@router.get("/invocations/{invocation_id}/stream")
async def stream_events(
    invocation_id: str,
    after: int | None = Query(default=None),
    svc: InvocationService = Depends(get_service),
    _caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    inv = await svc.get(invocation_id)
    if inv is None:
        raise HTTPException(status_code=404, detail="invocation not found")

    tape_id = inv.tape_id

    async def event_source() -> AsyncIterator[bytes]:
        watch = get_watch_service()
        # Subscribe BEFORE emitting backlog so we don't miss events that
        # land between our backlog read and the live stream.
        queue = watch.subscribe(WatchOptions(resource_type="invocation"))
        try:
            # Backlog: replay anything already on this tape.
            backlog = await svc.list_events(tape_id, after=after)
            last_id = -1
            for ev in backlog:
                yield _sse_chunk(ev.model_dump(mode="json"))
                last_id = ev.id
                if ev.kind == "done":
                    return

            # Live: forward subscriber events for this tape.
            while True:
                wev = await asyncio.wait_for(queue.get(), timeout=30.0)
                if wev.resource_id != tape_id:
                    continue
                payload = wev.object  # WatchEvent uses `object`, not `payload`
                if not isinstance(payload, dict):
                    continue
                ev_id = payload.get("id")
                if ev_id is None or ev_id <= last_id:
                    continue
                last_id = ev_id
                yield _sse_chunk(payload)
                if payload.get("kind") == "done":
                    return
        except asyncio.TimeoutError:
            return
        finally:
            watch.unsubscribe(queue)

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


def _sse_chunk(data: dict) -> bytes:
    return f"data: {json.dumps(data)}\n\n".encode()


# ---------------------------------------------------------------------------
# GET /api/v1/invocations/:id/task   (Auth Phase 0 — SPA helper)
# ---------------------------------------------------------------------------


from pydantic import BaseModel as _BaseModel


class InvocationTaskResponse(_BaseModel):
    task_id: str
    elicit_id: str | None  # most recent pending op:auth_required, if any


async def _require_cookie_session(
    caller: CallerContext = Depends(resolve_caller_or_cookie),
) -> CallerContext:
    """Cookie-session-only dep (mirrors credential_relay.require_cookie_session)."""
    method = getattr(caller, "auth_method", None)
    if method == "api_key":
        raise HTTPException(status_code=401, detail="cookie-session required")
    return caller


@router.get("/invocations/{invocation_id}/task", response_model=InvocationTaskResponse)
async def invocation_task(
    invocation_id: str,
    caller: CallerContext = Depends(_require_cookie_session),
    db: aiosqlite.Connection = Depends(get_db),
) -> InvocationTaskResponse:
    """Return the task_id + latest pending auth_required elicit_id for an invocation."""
    inv = await _InvocationRepo(db).get(invocation_id)
    if not inv or not inv.task_id:
        raise HTTPException(404, "invocation not found")
    task = await TaskRepo(db).get(inv.task_id)
    if not task:
        raise HTTPException(404, "task not found")
    bundle = await BundleRepo(db).get(task.bundle_id) if task.bundle_id else None
    if not bundle or bundle.owner_account_id is None:
        raise HTTPException(403, "bundle has no owner")
    if bundle.owner_account_id != caller.account_id:
        raise HTTPException(403, "not your invocation")
    pending = await ElicitRepo(db).latest_pending_auth_required(invocation_id=invocation_id)
    return InvocationTaskResponse(
        task_id=inv.task_id,
        elicit_id=pending.id if pending else None,
    )
