from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

import aiosqlite

from krewhub.auth import resolve_caller_or_cookie
from krewhub.config import get_settings
from krewhub.db.connection import get_db
from krewhub.git.transport import ensure_bare_repo, resolve_repo_path
from krewhub.models import Cookbook, WatchEventType
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.cookbook_repo import CookbookRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.routes.schemas import CreateCookbookRequest
from krewhub.watch.globals import get_watch_service

# Cookie-friendly auth so the cookrew-beta SPA (browser session) can
# discover its owned cookbooks via /api/v1/cookbooks?owner_id=<me>.
# Daemon / api-key callers keep working — resolve_caller_or_cookie
# accepts Bearer JWT, X-API-Key, AND krewauth_session / krew_session
# cookies.
router = APIRouter(
    tags=["cookbooks"], dependencies=[Depends(resolve_caller_or_cookie)],
)


@router.post("/cookbooks")
async def create_cookbook(
    req: CreateCookbookRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    repo = CookbookRepo(db)

    # Return existing cookbook if one already exists for this name + owner
    existing = await repo.find_by_name_and_owner(req.name, req.owner_id)
    if existing is not None:
        repo_path = resolve_repo_path(req.owner_id, req.name)
        await ensure_bare_repo(repo_path)
        settings = get_settings()
        clone_url = f"http://{settings.host}:{settings.port}/{req.owner_id}/{req.name}.git"
        return {
            "cookbook": existing.model_dump(mode="json"),
            "existed": True,
            "clone_url": clone_url,
        }

    # Init bare repo on disk
    repo_path = resolve_repo_path(req.owner_id, req.name)
    await ensure_bare_repo(repo_path)

    now = datetime.now(timezone.utc)
    cookbook = Cookbook(
        id=f"cb_{uuid.uuid4().hex[:8]}",
        name=req.name,
        owner_id=req.owner_id,
        created_at=now,
    )
    created = await repo.create(cookbook, repo_path=str(repo_path))

    watch = get_watch_service()
    await watch.record_resource(
        "cookbook", created.id, WatchEventType.ADDED, created,
    )

    settings = get_settings()
    clone_url = f"http://{settings.host}:{settings.port}/{req.owner_id}/{req.name}.git"
    return {
        "cookbook": created.model_dump(mode="json"),
        "existed": False,
        "clone_url": clone_url,
    }


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
