from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

import aiosqlite
import httpx

from krewhub.auth import verify_api_key
from krewhub.db.connection import get_db
from krewhub.models import AgentPresence, AgentStatus, WatchEventType
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.routes.schemas import HeartbeatRequest, PlanRequest, RegisterAgentRequest
from krewhub.watch.globals import get_watch_service

router = APIRouter(tags=["agents"], dependencies=[Depends(verify_api_key)])


@router.post("/agents/register")
async def register_agent(
    req: RegisterAgentRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Register an agent node with krewhub.

    This is the kubelet registration step. The agent declares its
    capabilities and capacity before starting to receive task assignments.
    """
    recipe = await RecipeRepo(db).get(req.recipe_id)
    if recipe is None:
        raise HTTPException(status_code=404, detail="Recipe not found")

    now = datetime.now(timezone.utc)
    presence = AgentPresence(
        agent_id=req.agent_id,
        recipe_id=req.recipe_id,
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
    await watch.record_resource(
        "agent", req.agent_id, WatchEventType.ADDED, updated,
        recipe_id=req.recipe_id,
    )

    return {"presence": updated.model_dump(mode="json")}


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
        max_concurrent_tasks=req.max_concurrent_tasks,
        endpoint_url=req.endpoint_url,
        status=status,
        last_heartbeat_at=now,
        current_task_id=req.current_task_id,
    )

    repo = AgentRepo(db)
    updated = await repo.upsert_presence(presence)

    watch = get_watch_service()
    await watch.record_resource(
        "agent", req.agent_id, WatchEventType.MODIFIED, updated,
        recipe_id=req.recipe_id,
    )

    return {"presence": updated.model_dump(mode="json")}


@router.post("/plan")
async def plan_tasks(
    req: PlanRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Decompose a prompt into tasks with dependencies via an online agent.

    Finds an agent with planning capability and forwards the request
    to its /plan REST endpoint. The agent makes the actual LLM call.
    """
    repo = AgentRepo(db)
    agents = await repo.list_by_recipe(req.recipe_id)

    planning_caps = {"orchestrate", "plan", "predict", "summarize", "classify", "review"}
    planner = None
    for agent in agents:
        if agent.status == "offline":
            continue
        if not agent.endpoint_url:
            continue
        if planning_caps & set(agent.capabilities):
            planner = agent
            break

    if planner is None:
        raise HTTPException(
            status_code=503,
            detail="No online agent with planning capability found. "
            "Start an agent with: krewcli join --recipe <id> --provider anthropic",
        )

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{planner.endpoint_url}/plan",
                json={"prompt": req.prompt},
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"Agent planning failed: {exc.response.text[:200]}")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not reach agent at {planner.endpoint_url}: {exc}")
