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
from typing import AsyncIterator

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from krewhub.auth import resolve_caller_or_cookie, CallerContext
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
    req: InvocationRequest,
    svc: InvocationService = Depends(get_service),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
):
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
):
    try:
        ev = await svc.submit_result(invocation_id, envelope)
    except KeyError:
        raise HTTPException(status_code=404, detail="invocation not found")
    except _ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {"ok": True, "event_id": ev.id}


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
