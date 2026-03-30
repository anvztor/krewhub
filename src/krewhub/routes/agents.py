from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

import aiosqlite

from krewhub.auth import verify_api_key
from krewhub.db.connection import get_db
from krewhub.models import AgentPresence, AgentStatus
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.routes.schemas import HeartbeatRequest
from krewhub.services.sse_service import sse_service

router = APIRouter(tags=["agents"], dependencies=[Depends(verify_api_key)])


@router.post("/agents/heartbeat")
async def heartbeat(
    req: HeartbeatRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    recipe = await RecipeRepo(db).get(req.recipe_id)
    if recipe is None:
        raise HTTPException(status_code=404, detail="Recipe not found")

    now = datetime.now(timezone.utc)
    status = AgentStatus.BUSY if req.current_task_id else AgentStatus.ONLINE

    presence = AgentPresence(
        agent_id=req.agent_id,
        recipe_id=req.recipe_id,
        display_name=req.display_name,
        capabilities=req.capabilities,
        status=status,
        last_heartbeat_at=now,
        current_task_id=req.current_task_id,
    )

    repo = AgentRepo(db)
    await repo.upsert_presence(presence)

    await sse_service.publish(req.recipe_id, "agent.presence", {
        "agent_id": req.agent_id,
        "status": status,
        "display_name": req.display_name,
    })

    return {"presence": presence.model_dump(mode="json")}
