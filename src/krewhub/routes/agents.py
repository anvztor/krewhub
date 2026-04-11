from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

import aiosqlite

from krewhub.auth import resolve_caller
from krewhub.db.connection import get_db
from krewhub.models import AgentPresence, AgentStatus, WatchEventType
from krewhub.repositories.agent_repo import AgentRepo, _row_to_presence
from krewhub.repositories.cookbook_repo import CookbookRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.routes.schemas import HeartbeatRequest, RegisterAgentRequest
from krewhub.watch.globals import get_watch_service

router = APIRouter(tags=["agents"], dependencies=[Depends(resolve_caller)])


@router.get("/agents")
async def list_agents(
    cookbook_id: str | None = None,
    db: aiosqlite.Connection = Depends(get_db),
):
    """List online agents, optionally filtered by cookbook."""
    repo = AgentRepo(db)
    if cookbook_id:
        agents = await repo.list_by_cookbook(cookbook_id)
    else:
        cursor = await db.execute(
            "SELECT * FROM agent_presence WHERE status != 'offline'"
        )
        rows = await cursor.fetchall()
        # Use the repo helper to decode JSON columns (capabilities, etc.);
        # constructing AgentPresence(**dict(row)) skips that step and crashes.
        agents = [_row_to_presence(r) for r in rows]
    return {"agents": [a.model_dump(mode="json") for a in agents]}


@router.post("/agents/register")
async def register_agent(
    req: RegisterAgentRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Register an agent node with krewhub.

    The agent declares its capabilities and capacity at the cookbook level,
    making it available for all recipes in that cookbook.
    """
    cookbook = await CookbookRepo(db).get(req.cookbook_id)
    if cookbook is None:
        raise HTTPException(status_code=404, detail="Cookbook not found")

    now = datetime.now(timezone.utc)
    presence = AgentPresence(
        agent_id=req.agent_id,
        cookbook_id=req.cookbook_id,
        display_name=req.display_name,
        capabilities=req.capabilities,
        max_concurrent_tasks=req.max_concurrent_tasks,
        endpoint_url=req.endpoint_url,
        status=AgentStatus.ONLINE,
        last_heartbeat_at=now,
    )

    repo = AgentRepo(db)
    updated = await repo.upsert_presence(presence)

    watch = get_watch_service()
    recipes = await RecipeRepo(db).list_by_cookbook(req.cookbook_id)
    for recipe in recipes:
        await watch.record_resource(
            "agent", req.agent_id, WatchEventType.ADDED, updated,
            recipe_id=recipe.id,
        )
    if not recipes:
        await watch.record_resource(
            "agent", req.agent_id, WatchEventType.ADDED, updated,
        )

    # Upsert A2A agent card for hub gateway
    from krewhub.routes.a2a_gateway import upsert_agent_card
    agent_short_name = req.agent_id.split("@")[0] if "@" in req.agent_id else req.agent_id
    owner = cookbook.owner_id
    await upsert_agent_card(
        db, owner=owner, agent_name=agent_short_name,
        display_name=req.display_name, capabilities=req.capabilities,
    )

    return {"presence": updated.model_dump(mode="json")}


@router.post("/agents/heartbeat")
async def heartbeat(
    req: HeartbeatRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    cookbook = await CookbookRepo(db).get(req.cookbook_id)
    if cookbook is None:
        raise HTTPException(status_code=404, detail="Cookbook not found")

    now = datetime.now(timezone.utc)
    status = AgentStatus.BUSY if req.current_task_id else AgentStatus.ONLINE

    presence = AgentPresence(
        agent_id=req.agent_id,
        cookbook_id=req.cookbook_id,
        display_name=req.display_name,
        capabilities=req.capabilities,
        max_concurrent_tasks=req.max_concurrent_tasks,
        endpoint_url=req.endpoint_url,
        status=status,
        last_heartbeat_at=now,
        current_task_id=req.current_task_id,
    )

    repo = AgentRepo(db)
    updated = await repo.upsert_presence(presence)

    watch = get_watch_service()
    recipes = await RecipeRepo(db).list_by_cookbook(req.cookbook_id)
    for recipe in recipes:
        await watch.record_resource(
            "agent", req.agent_id, WatchEventType.MODIFIED, updated,
            recipe_id=recipe.id,
        )
    if not recipes:
        await watch.record_resource(
            "agent", req.agent_id, WatchEventType.MODIFIED, updated,
        )

    return {"presence": updated.model_dump(mode="json")}
