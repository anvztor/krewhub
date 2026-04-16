from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Depends, Query, Request
from sse_starlette.sse import EventSourceResponse

from krewhub.auth import resolve_caller_or_cookie
from krewhub.watch.globals import get_watch_service
from krewhub.watch.types import WatchEvent, WatchOptions

# Watch streams accept Bearer JWT, X-API-Key, OR krew_session cookie
# so cookrew (browser via cookie) and krewcli (Bearer) both work.
router = APIRouter(tags=["stream"], dependencies=[Depends(resolve_caller_or_cookie)])


@router.get("/recipes/{recipe_id}/stream")
async def recipe_stream(recipe_id: str, request: Request):
    """Legacy SSE stream for a recipe. Wraps the watch service
    to deliver events in the same format the old SSE clients expect."""
    watch = get_watch_service()
    options = WatchOptions(recipe_id=recipe_id)
    queue = watch.subscribe(options)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    watch_event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {
                        "event": _to_legacy_event_name(watch_event),
                        "data": json.dumps(watch_event.object),
                    }
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": ""}
        finally:
            watch.unsubscribe(queue)

    return EventSourceResponse(event_generator())


@router.get("/watch")
async def watch_stream(
    request: Request,
    resource_type: str | None = Query(None),
    recipe_id: str | None = Query(None),
    since: int = Query(0),
    channel: str | None = Query(None, description="Comma-separated channel filters. Supports trailing *, e.g. 'task:*,digest:submitted'"),
):
    """Watch API endpoint. Returns an SSE stream of WatchEvents.

    Supports replay: pass ?since=<seq> to receive events after that
    sequence number. On reconnect, the client passes the last seen
    seq to guarantee no missed events.

    Channel filtering: pass ?channel=task:message,digest:* to only receive
    events matching those typed channels.
    """
    watch = get_watch_service()
    channel_prefixes = [c.strip() for c in (channel or "").split(",") if c.strip()]
    options = WatchOptions(
        resource_type=resource_type,
        recipe_id=recipe_id,
        since=since,
        channel_prefixes=channel_prefixes,
    )

    # First replay any missed events since the given seq
    replay_events = await watch.replay(options)

    # Then subscribe for new events going forward
    queue = watch.subscribe(options)

    async def event_generator():
        try:
            # Phase 1: replay
            for event in replay_events:
                yield _format_watch_event(event)

            # Phase 2: live stream
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield _format_watch_event(event)
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": ""}
        finally:
            watch.unsubscribe(queue)

    return EventSourceResponse(event_generator())


def _format_watch_event(event: WatchEvent) -> dict:
    return {
        "event": event.channel or event.event_type,
        "data": json.dumps({
            "type": event.event_type,
            "resource_type": event.resource_type,
            "resource_id": event.resource_id,
            "resource_version": event.resource_version,
            "object": event.object,
            "seq": event.seq,
            "channel": event.channel,
        }),
    }


# Map watch resource events back to legacy SSE event names
_LEGACY_EVENT_MAP = {
    ("bundle", "ADDED"): "bundle.created",
    ("bundle", "MODIFIED"): "bundle.decision",
    ("task", "MODIFIED"): "task.updated",
    ("task", "ADDED"): "task.updated",
    ("digest", "ADDED"): "bundle.digest_submitted",
    ("digest", "MODIFIED"): "bundle.decision",
    ("agent", "MODIFIED"): "agent.presence",
}


def _to_legacy_event_name(event: WatchEvent) -> str:
    """Best-effort mapping from watch events to legacy SSE event names."""
    # Check for legacy_event key first (from publish_legacy)
    legacy = event.object.get("legacy_event")
    if legacy:
        return legacy

    key = (event.resource_type, event.event_type)
    return _LEGACY_EVENT_MAP.get(key, f"{event.resource_type}.{event.event_type.lower()}")
