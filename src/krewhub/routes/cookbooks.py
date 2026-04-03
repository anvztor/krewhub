from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

import aiosqlite

from krewhub.auth import verify_api_key
from krewhub.db.connection import get_db
from krewhub.models import Cookbook, WatchEventType
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.cookbook_repo import CookbookRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.routes.schemas import CreateCookbookRequest
from krewhub.watch.globals import get_watch_service

router = APIRouter(tags=["cookbooks"], dependencies=[Depends(verify_api_key)])


@router.post("/cookbooks")
async def create_cookbook(
    req: CreateCookbookRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    now = datetime.now(timezone.utc)
    cookbook = Cookbook(
        id=f"cb_{uuid.uuid4().hex[:8]}",
        name=req.name,
        owner_id=req.owner_id,
        created_at=now,
    )
    repo = CookbookRepo(db)
    created = await repo.create(cookbook)

    watch = get_watch_service()
    await watch.record_resource(
        "cookbook", created.id, WatchEventType.ADDED, created,
    )

    return {"cookbook": created.model_dump(mode="json")}


@router.get("/cookbooks")
async def list_cookbooks(
    owner_id: str | None = None,
    db: aiosqlite.Connection = Depends(get_db),
):
    repo = CookbookRepo(db)
    if owner_id:
        cookbooks = await repo.list_by_owner(owner_id)
    else:
        cookbooks = await repo.list_all()
    return {"cookbooks": [c.model_dump(mode="json") for c in cookbooks]}


@router.get("/cookbooks/{cookbook_id}")
async def get_cookbook(
    cookbook_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    cookbook = await CookbookRepo(db).get(cookbook_id)
    if cookbook is None:
        raise HTTPException(status_code=404, detail="Cookbook not found")

    recipes = await RecipeRepo(db).list_by_cookbook(cookbook_id)
    agents = await AgentRepo(db).list_by_cookbook(cookbook_id)

    return {
        "cookbook": cookbook.model_dump(mode="json"),
        "recipes": [r.model_dump(mode="json") for r in recipes],
        "agents": [a.model_dump(mode="json") for a in agents],
    }
