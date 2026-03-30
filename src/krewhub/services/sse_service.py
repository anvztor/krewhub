from __future__ import annotations

import asyncio
import json
from typing import Any


class SSEService:
    """In-memory broadcast hub for Server-Sent Events per recipe."""

    def __init__(self) -> None:
        self._channels: dict[str, list[asyncio.Queue[dict[str, Any]]]] = {}

    def subscribe(self, recipe_id: str) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._channels.setdefault(recipe_id, []).append(queue)
        return queue

    def unsubscribe(self, recipe_id: str, queue: asyncio.Queue[dict[str, Any]]) -> None:
        listeners = self._channels.get(recipe_id, [])
        if queue in listeners:
            listeners.remove(queue)

    async def publish(self, recipe_id: str, event_type: str, data: dict[str, Any]) -> None:
        listeners = self._channels.get(recipe_id, [])
        message = {"event": event_type, "data": data}
        for queue in listeners:
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                pass


sse_service = SSEService()
