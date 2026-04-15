from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request

import aiosqlite

from krewhub.auth import CallerContext, resolve_caller
from krewhub.config import Settings, get_settings
from krewhub.db.connection import get_db
from krewhub.models import AgentPresence, AgentStatus, WatchEventType
from krewhub.repositories.agent_repo import AgentRepo, _row_to_presence
from krewhub.repositories.cookbook_repo import CookbookRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.routes.schemas import HeartbeatRequest, MintAgentRequest, RegisterAgentRequest
from krewhub.watch.globals import get_watch_service

logger = logging.getLogger(__name__)

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
    request: Request,
    db: aiosqlite.Connection = Depends(get_db),
    caller: CallerContext = Depends(resolve_caller),
    settings: Settings = Depends(get_settings),
):
    """Register an agent node with krewhub.

    The agent declares its capabilities and capacity at the cookbook level,
    making it available for all recipes in that cookbook.
    Owner is set from the caller's JWT username.
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

    # Store owner_username from JWT
    owner_username = caller.username or caller.account_id
    await db.execute(
        "UPDATE agent_presence SET owner_username = ? WHERE agent_id = ? AND cookbook_id = ?",
        (owner_username, req.agent_id, req.cookbook_id),
    )
    await db.commit()

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

    # Upsert A2A agent card — uses agent owner's username, not cookbook owner
    from krewhub.routes.a2a_gateway import upsert_agent_card
    agent_short_name = req.agent_id.split("@")[0] if "@" in req.agent_id else req.agent_id
    await upsert_agent_card(
        db, owner=owner_username, agent_name=agent_short_name,
        display_name=req.display_name, capabilities=req.capabilities,
    )

    # Provision AA wallet (best-effort — registration succeeds regardless)
    if caller.wallet_address and settings.krewhub_session_pubkey:
        auth_header = request.headers.get("authorization", "")
        bearer_token = (
            auth_header.removeprefix("Bearer ")
            if auth_header.startswith("Bearer ")
            else ""
        )
        if bearer_token:
            try:
                from krewhub.clients.krewauth_client import KrewauthClient
                from krewhub.services.agent_wallet_service import provision_agent_wallet

                client = KrewauthClient(settings.krewauth_base_url)
                try:
                    result = await provision_agent_wallet(
                        client=client,
                        settings=settings,
                        caller_token=bearer_token,
                        agent_id=req.agent_id,
                        cookbook_id=req.cookbook_id,
                    )
                    await repo.set_wallet_address(
                        req.agent_id, req.cookbook_id, result.aa_wallet_address,
                    )
                    updated = await repo.get(req.agent_id, req.cookbook_id) or updated
                finally:
                    await client.close()
            except Exception as exc:
                logger.warning(
                    "AA wallet provisioning failed for agent %s: %s",
                    req.agent_id, exc,
                )

    return {"presence": updated.model_dump(mode="json")}


@router.patch("/agents/{agent_id}/mint")
async def mint_agent(
    agent_id: str,
    req: MintAgentRequest,
    db: aiosqlite.Connection = Depends(get_db),
    caller: CallerContext = Depends(resolve_caller),
):
    """Record on-chain ERC-8004 mint for an agent presence row."""
    repo = AgentRepo(db)
    presence = await repo.get(agent_id, req.cookbook_id)
    if presence is None:
        raise HTTPException(status_code=404, detail="Agent presence not found")

    await db.execute(
        """UPDATE agent_presence
           SET mint_tx_hash = ?, mint_token_id = ?,
               resource_version = resource_version + 1
           WHERE agent_id = ? AND cookbook_id = ?""",
        (req.tx_hash, req.token_id, agent_id, req.cookbook_id),
    )
    await db.commit()

    updated = await repo.get(agent_id, req.cookbook_id)
    if updated is None:
        raise HTTPException(status_code=404, detail="Agent not found after update")
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
