from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Depends, Request
from sse_starlette.sse import EventSourceResponse

from krewhub.auth import verify_api_key
from krewhub.services.sse_service import sse_service

router = APIRouter(tags=["stream"], dependencies=[Depends(verify_api_key)])


@router.get("/recipes/{recipe_id}/stream")
async def recipe_stream(recipe_id: str, request: Request):
    queue = sse_service.subscribe(recipe_id)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {
                        "event": message["event"],
                        "data": json.dumps(message["data"]),
                    }
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": ""}
        finally:
            sse_service.unsubscribe(recipe_id, queue)

    return EventSourceResponse(event_generator())
