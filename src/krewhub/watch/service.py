from __future__ import annotations

import asyncio
import json
from typing import Any

import aiosqlite

from krewhub.models import WatchEventType
from krewhub.watch.store import WatchLogStore, entry_to_watch_event
from krewhub.watch.types import WatchEvent, WatchOptions


class WatchService:
    """Persistent watch service combining the watch_log table with
    in-memory subscriber notification.

    Replaces the old SSEService by:
    1. Persisting every event to the watch_log table (durable replay)
    2. Notifying in-memory subscribers immediately (low latency)

    Subscribers that reconnect can replay from a sequence number,
    guaranteeing no missed events.
    """

    def __init__(self, db: aiosqlite.Connection) -> None:
        self._store = WatchLogStore(db)
        self._subscribers: list[_Subscriber] = []

    # -- Recording mutations --

    async def record(
        self,
        resource_type: str,
        resource_id: str,
        event_type: WatchEventType,
        resource_version: int,
        payload: dict[str, Any],
        recipe_id: str | None = None,
    ) -> WatchEvent:
        """Record a resource mutation and notify subscribers.

        This should be called by repository methods after every
        create/update/delete operation.
        """
        entry = await self._store.append(
            resource_type=resource_type,
            resource_id=resource_id,
            event_type=event_type,
            resource_version=resource_version,
            payload=payload,
            recipe_id=recipe_id,
        )
        event = entry_to_watch_event(entry)
        await self._notify(event)
        return event

    # -- Convenience methods matching old SSE event patterns --

    async def record_resource(
        self,
        resource_type: str,
        resource_id: str,
        event_type: WatchEventType,
        resource: Any,
        recipe_id: str | None = None,
    ) -> WatchEvent:
        """Record a mutation using a Pydantic model as the payload."""
        payload = resource.model_dump(mode="json") if hasattr(resource, "model_dump") else resource
        rv = payload.get("resource_version", 1) if isinstance(payload, dict) else 1
        return await self.record(
            resource_type=resource_type,
            resource_id=resource_id,
            event_type=event_type,
            resource_version=rv,
            payload=payload,
            recipe_id=recipe_id,
        )

    async def publish_legacy(
        self,
        recipe_id: str,
        event_name: str,
        data: dict[str, Any],
    ) -> None:
        """Backward-compatible publish that maps old SSE event names
        to watch_log entries.

        This allows gradual migration: services can keep calling
        publish_legacy while we move them to record() one by one.
        """
        resource_type, event_type, resource_id = _parse_legacy_event(event_name, data)
        await self.record(
            resource_type=resource_type,
            resource_id=resource_id,
            event_type=event_type,
            resource_version=data.get("resource_version", 0),
            payload={"legacy_event": event_name, **data},
            recipe_id=recipe_id,
        )

    # -- Subscribing --

    def subscribe(self, options: WatchOptions | None = None) -> asyncio.Queue[WatchEvent]:
        """Subscribe to watch events. Returns a queue that receives events."""
        opts = options or WatchOptions()
        queue: asyncio.Queue[WatchEvent] = asyncio.Queue(maxsize=256)
        sub = _Subscriber(queue=queue, options=opts)
        self._subscribers.append(sub)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[WatchEvent]) -> None:
        self._subscribers = [s for s in self._subscribers if s.queue is not queue]

    async def replay(self, options: WatchOptions | None = None) -> list[WatchEvent]:
        """Replay events from a sequence number, with optional filters."""
        opts = options or WatchOptions()
        entries = await self._store.list_since(
            since=opts.since,
            resource_type=opts.resource_type,
            recipe_id=opts.recipe_id,
        )
        return [entry_to_watch_event(e) for e in entries]

    async def latest_seq(self) -> int:
        return await self._store.latest_seq()

    # -- Internal --

    async def _notify(self, event: WatchEvent) -> None:
        dead: list[_Subscriber] = []
        for sub in self._subscribers:
            if not _matches(sub.options, event):
                continue
            try:
                sub.queue.put_nowait(event)
            except asyncio.QueueFull:
                dead.append(sub)

        for sub in dead:
            self._subscribers.remove(sub)


class _Subscriber:
    __slots__ = ("queue", "options")

    def __init__(self, queue: asyncio.Queue[WatchEvent], options: WatchOptions) -> None:
        self.queue = queue
        self.options = options


def _matches(opts: WatchOptions, event: WatchEvent) -> bool:
    if opts.resource_type and event.resource_type != opts.resource_type:
        return False
    if opts.resource_types and event.resource_type not in opts.resource_types:
        return False
    if opts.recipe_id and event.recipe_id != opts.recipe_id:
        return False
    if opts.channel_prefixes:
        if not any(_channel_matches(p, event.channel) for p in opts.channel_prefixes):
            return False
    return True


def _channel_matches(pattern: str, channel: str) -> bool:
    """Match a channel against a pattern. Supports trailing * wildcard."""
    if not channel:
        return False
    if pattern.endswith("*"):
        return channel.startswith(pattern[:-1])
    return channel == pattern


def _parse_legacy_event(
    event_name: str, data: dict[str, Any]
) -> tuple[str, WatchEventType, str]:
    """Map old SSE event names to (resource_type, watch_event_type, resource_id)."""
    mapping: dict[str, tuple[str, WatchEventType]] = {
        "bundle.created": ("bundle", WatchEventType.ADDED),
        "bundle.digest_submitted": ("digest", WatchEventType.ADDED),
        "bundle.decision": ("bundle", WatchEventType.MODIFIED),
        "task.claimed": ("task", WatchEventType.MODIFIED),
        "task.updated": ("task", WatchEventType.MODIFIED),
        "agent.presence": ("agent", WatchEventType.MODIFIED),
    }

    resource_type, event_type = mapping.get(
        event_name, ("unknown", WatchEventType.MODIFIED)
    )

    resource_id = (
        data.get("bundle_id")
        or data.get("task_id")
        or data.get("digest_id")
        or data.get("agent_id")
        or "unknown"
    )

    return resource_type, event_type, resource_id
