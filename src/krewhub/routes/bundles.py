from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

import aiosqlite

from krewhub.auth import verify_api_key
from krewhub.db.connection import get_db
from krewhub.models import BundleStatus, DigestDecision
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.services.bundle_service import BundleService
from krewhub.services.digest_service import DigestService
from krewhub.routes.schemas import (
    AddTaskRequest,
    CreateBundleRequest,
    DecisionRequest,
    SubmitDigestRequest,
)

router = APIRouter(tags=["bundles"], dependencies=[Depends(verify_api_key)])


@router.post("/recipes/{recipe_id}/bundles")
async def create_bundle(
    recipe_id: str,
    req: CreateBundleRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    recipe = await RecipeRepo(db).get(recipe_id)
    if recipe is None:
        raise HTTPException(status_code=404, detail="Recipe not found")

    svc = BundleService(db)
    bundle, tasks = await svc.create_bundle(
        recipe_id=recipe_id,
        prompt=req.prompt,
        created_by=req.requested_by,
        tasks=[t.model_dump() for t in req.tasks],
    )
    return {
        "bundle": bundle.model_dump(mode="json"),
        "tasks": [t.model_dump(mode="json") for t in tasks],
    }


@router.get("/recipes/{recipe_id}/bundles")
async def list_bundles(
    recipe_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    repo = BundleRepo(db)
    bundles = await repo.list_by_recipe(recipe_id)
    return {"bundles": [b.model_dump(mode="json") for b in bundles]}


@router.get("/bundles/{bundle_id}")
async def get_bundle(
    bundle_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    bundle = await BundleRepo(db).get(bundle_id)
    if bundle is None:
        raise HTTPException(status_code=404, detail="Bundle not found")

    tasks = await TaskRepo(db).list_by_bundle(bundle_id)
    events = await EventRepo(db).list_by_bundle(bundle_id)

    return {
        "bundle": bundle.model_dump(mode="json"),
        "tasks": [t.model_dump(mode="json") for t in tasks],
        "events": [e.model_dump(mode="json") for e in events],
    }


@router.get("/bundles/{bundle_id}/digest")
async def get_bundle_digest(
    bundle_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    from krewhub.repositories.digest_repo import DigestRepo

    digest = await DigestRepo(db).get_by_bundle(bundle_id)
    if digest is None:
        raise HTTPException(status_code=404, detail="Digest not found")

    return {"digest": digest.model_dump(mode="json")}


@router.patch("/bundles/{bundle_id}")
async def cancel_bundle(
    bundle_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    svc = BundleService(db)
    updated = await svc.cancel_bundle(bundle_id, "system")
    if updated is None:
        raise HTTPException(status_code=400, detail="Cannot cancel this bundle")
    return {"bundle": updated.model_dump(mode="json")}


@router.post("/bundles/{bundle_id}/tasks")
async def add_task_to_bundle(
    bundle_id: str,
    req: AddTaskRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    bundle = await BundleRepo(db).get(bundle_id)
    if bundle is None:
        raise HTTPException(status_code=404, detail="Bundle not found")

    from krewhub.services.task_service import TaskService
    svc = TaskService(db)
    task = await svc.add_task(
        bundle_id=bundle_id,
        title=req.title,
        description=req.description,
        depends_on_task_ids=req.depends_on_task_ids,
    )
    return {"task": task.model_dump(mode="json")}


@router.post("/bundles/{bundle_id}/digest")
async def submit_digest(
    bundle_id: str,
    req: SubmitDigestRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    svc = DigestService(db)
    digest = await svc.submit_digest(
        bundle_id=bundle_id,
        submitted_by=req.submitted_by,
        summary=req.summary,
        task_results=req.task_results,
        facts=req.facts,
        code_refs=req.code_refs,
    )
    if digest is None:
        raise HTTPException(
            status_code=400,
            detail="Cannot submit digest. Ensure all tasks are done/blocked and no digest exists.",
        )
    return {"digest": digest.model_dump(mode="json")}


@router.post("/bundles/{bundle_id}/decision")
async def decide_digest(
    bundle_id: str,
    req: DecisionRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    svc = DigestService(db)
    decision = DigestDecision(req.decision)
    digest = await svc.decide(bundle_id, decision, req.decided_by)
    if digest is None:
        raise HTTPException(
            status_code=400,
            detail="Cannot decide. No pending digest found.",
        )
    return {"digest": digest.model_dump(mode="json")}


@router.get("/recipes/{recipe_id}/digests")
async def list_approved_digests(
    recipe_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    from krewhub.repositories.digest_repo import DigestRepo
    digests = await DigestRepo(db).list_approved_by_recipe(recipe_id)
    return {"digests": [d.model_dump(mode="json") for d in digests]}
