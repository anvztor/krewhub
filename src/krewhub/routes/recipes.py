from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

import aiosqlite

from krewhub.auth import verify_api_key
from krewhub.db.connection import get_db
from krewhub.models import ActorType, CodeRef, EventType, FactRef, Recipe, RecipeMember, Role, WatchEventType
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.digest_repo import DigestRepo
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.routes.schemas import CreateRecipeRequest, InviteMemberRequest, PostRecipeEventRequest
from krewhub.watch.globals import get_watch_service

router = APIRouter(tags=["recipes"], dependencies=[Depends(verify_api_key)])


@router.post("/recipes")
async def create_recipe(
    req: CreateRecipeRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    repo = RecipeRepo(db)
    now = datetime.now(timezone.utc)
    recipe = Recipe(
        id=f"rec_{uuid.uuid4().hex[:8]}",
        name=req.name,
        repo_url=req.repo_url,
        default_branch=req.default_branch,
        created_by=req.created_by,
        created_at=now,
        cookbook_id=req.cookbook_id,
    )
    recipe = await repo.create(recipe)

    owner = RecipeMember(
        id=f"mem_{uuid.uuid4().hex[:8]}",
        recipe_id=recipe.id,
        actor_id=req.created_by,
        actor_type="human",
        role=Role.OWNER,
        joined_at=now,
    )
    await repo.add_member(owner)

    return {"recipe": recipe.model_dump(mode="json")}


@router.get("/recipes")
async def list_recipes(db: aiosqlite.Connection = Depends(get_db)):
    repo = RecipeRepo(db)
    recipes = await repo.list_all()
    return {"recipes": [r.model_dump(mode="json") for r in recipes]}


@router.get("/recipes/{recipe_id}")
async def get_recipe(recipe_id: str, db: aiosqlite.Connection = Depends(get_db)):
    recipe_repo = RecipeRepo(db)
    recipe = await recipe_repo.get(recipe_id)
    if recipe is None:
        raise HTTPException(status_code=404, detail="Recipe not found")

    members = await recipe_repo.list_members(recipe_id)
    agents = await AgentRepo(db).list_by_recipe(recipe_id)  # via cookbook join
    bundles = await BundleRepo(db).list_by_recipe(recipe_id)
    digests = await DigestRepo(db).list_approved_by_recipe(recipe_id)

    return {
        "recipe": recipe.model_dump(mode="json"),
        "members": [m.model_dump(mode="json") for m in members],
        "agents": [a.model_dump(mode="json") for a in agents],
        "bundles": [b.model_dump(mode="json") for b in bundles],
        "digests": [d.model_dump(mode="json") for d in digests],
    }


@router.post("/recipes/{recipe_id}/members")
async def invite_member(
    recipe_id: str,
    req: InviteMemberRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    repo = RecipeRepo(db)
    recipe = await repo.get(recipe_id)
    if recipe is None:
        raise HTTPException(status_code=404, detail="Recipe not found")

    now = datetime.now(timezone.utc)
    member = RecipeMember(
        id=f"mem_{uuid.uuid4().hex[:8]}",
        recipe_id=recipe_id,
        actor_id=req.actor_id,
        actor_type=req.actor_type,
        role=req.role,
        joined_at=now,
    )
    member = await repo.add_member(member)
    return {"member": member.model_dump(mode="json")}


@router.post("/recipes/{recipe_id}/events")
async def post_recipe_event(
    recipe_id: str,
    req: PostRecipeEventRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Post an agent-level event to a recipe (no bundle/task required).

    Used by hook listeners for session_start, session_end, tool_use, agent_reply.
    """
    recipe = await RecipeRepo(db).get(recipe_id)
    if recipe is None:
        raise HTTPException(status_code=404, detail="Recipe not found")

    facts = [FactRef(**f) for f in req.facts] if req.facts else []
    code_refs = [CodeRef(**c) for c in req.code_refs] if req.code_refs else []

    from krewhub.models import Event

    now = datetime.now(timezone.utc)
    event = Event(
        id=f"evt_{uuid.uuid4().hex[:8]}",
        recipe_id=recipe_id,
        bundle_id=None,
        task_id=None,
        type=EventType(req.type),
        actor_id=req.actor_id,
        actor_type=ActorType(req.actor_type),
        body=req.body,
        facts=facts,
        code_refs=code_refs,
        created_at=now,
    )

    await EventRepo(db).create(event)

    watch = get_watch_service()
    await watch.record_resource(
        "event", event.id, WatchEventType.ADDED, event,
        recipe_id=recipe_id,
    )

    return {"event": event.model_dump(mode="json")}
