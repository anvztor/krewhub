from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

import aiosqlite

from krewhub.watch.globals import get_watch_service
from krewhub.auth import CallerContext, require_bundle_owner, resolve_caller_or_cookie
from krewhub.config import get_settings
from krewhub.db.connection import get_db
from krewhub.models import DigestDecision
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.services.bundle_service import BundleService, GraphArtifactError
from krewhub.services.deps import get_e2b
from krewhub.services.digest_service import DigestService
from krewhub.services.e2b_client import E2bClient
from krewhub.services.sandbox_service import SandboxService
from krewhub.routes.schemas import (
    AddTaskRequest,
    AttachGraphRequest,
    CreateBundleRequest,
    DecisionRequest,
    SubmitDigestRequest,
)

router = APIRouter(tags=["bundles"], dependencies=[Depends(resolve_caller_or_cookie)])


@router.post("/recipes/{recipe_id}/bundles")
async def create_bundle(
    recipe_id: str,
    req: CreateBundleRequest,
    db: aiosqlite.Connection = Depends(get_db),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    recipe = await RecipeRepo(db).get(recipe_id)
    if recipe is None:
        raise HTTPException(status_code=404, detail="Recipe not found")

    # Use caller identity from JWT, not client-supplied value
    created_by = caller.username or caller.account_id

    svc = BundleService(db, get_watch_service())
    bundle, tasks = await svc.create_bundle(
        recipe_id=recipe_id,
        prompt=req.prompt,
        created_by=created_by,
        tasks=[{**t.model_dump(exclude={"task_id"}), **({"id": t.task_id} if t.task_id else {})} for t in req.tasks],
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


@router.get("/bundles/{bundle_id}/usage")
async def get_bundle_usage(
    bundle_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Aggregate LLM token usage across all tasks in a bundle."""
    cursor = await db.execute(
        """SELECT u.*
           FROM task_usage u
           JOIN tasks t ON t.id = u.task_id
           WHERE t.bundle_id = ?
           ORDER BY u.created_at ASC""",
        (bundle_id,),
    )
    rows = await cursor.fetchall()
    usage_list = [dict(r) for r in rows]

    totals = {
        "input_tokens": sum(r["input_tokens"] or 0 for r in usage_list),
        "output_tokens": sum(r["output_tokens"] or 0 for r in usage_list),
        "cost_usd": sum(r["cost_usd"] or 0 for r in usage_list) if any(r.get("cost_usd") for r in usage_list) else None,
        "duration_ms": sum(r["duration_ms"] or 0 for r in usage_list) if any(r.get("duration_ms") for r in usage_list) else None,
        "task_count": len({r["task_id"] for r in usage_list}),
    }
    return {"usage": usage_list, "totals": totals}


@router.patch("/bundles/{bundle_id}")
async def cancel_bundle(
    bundle_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    svc = BundleService(db, get_watch_service())
    updated = await svc.cancel_bundle(bundle_id, "system")
    if updated is None:
        raise HTTPException(status_code=400, detail="Cannot cancel this bundle")
    return {"bundle": updated.model_dump(mode="json")}


@router.post("/bundles/{bundle_id}/rerun")
async def rerun_blocked_bundle(
    bundle_id: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    svc = BundleService(db, get_watch_service())
    updated = await svc.rerun_blocked_tasks(bundle_id)
    if updated is None:
        raise HTTPException(
            status_code=400,
            detail="No blocked tasks are available to rerun.",
        )
    return {"bundle": updated.model_dump(mode="json")}


@router.post("/bundles/{bundle_id}/graph")
async def attach_bundle_graph(
    bundle_id: str,
    req: AttachGraphRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Attach a validated pydantic-graph artifact to an existing bundle.

    Called by the orchestrator (or by an A2A callback handler that
    relays an orchestrator response). The bundle must exist, must not
    already have graph_code attached, and the code must pass the sandbox.
    On success, the bundle is left in status='open' with graph_code set
    so GraphRunnerController picks it up on the next reconcile.
    """
    svc = BundleService(db, get_watch_service())
    try:
        bundle, tasks = await svc.attach_graph_artifact(
            bundle_id, req.code, created_by=req.created_by,
        )
    except GraphArtifactError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    return {
        "bundle": bundle.model_dump(mode="json"),
        "tasks": [t.model_dump(mode="json") for t in tasks],
    }


@router.post("/bundles/{bundle_id}/tasks")
async def add_task_to_bundle(
    bundle_id: str,
    req: AddTaskRequest,
    db: aiosqlite.Connection = Depends(get_db),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
    e2b: E2bClient = Depends(get_e2b),
):
    """Create a task on a bundle and provision an e2b sandbox for it.

    Auth track A2:
      1. ABAC — caller must own the bundle (require_bundle_owner).
      2. Bundle must have a paired agent (default_agent_runtime_id set);
         otherwise we return 400 no_paired_agent so the UI can prompt
         the user to "Hire an agent first".
      3. Provision an e2b sandbox via SandboxService.create_for_task.
      4. Persist task.assigned_runtime_id + task.sandbox_id so the
         krewcli daemon can pick it up.
    """
    bundle = await require_bundle_owner(bundle_id, caller, db)

    # Legacy API-key callers (acc_legacy_apikey) skip sandbox provisioning
    # entirely — they were not part of the auth journey and existing
    # integrations use POST /bundles/{id}/tasks for orchestration only.
    # Cookie/JWT callers go through the full A2 flow.
    is_legacy_apikey = caller.auth_method == "api_key"

    if not is_legacy_apikey:
        if bundle.default_agent_runtime_id is None:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "no_paired_agent",
                    "message": "Hire an agent first",
                },
            )

    from krewhub.services.task_service import TaskService
    svc = TaskService(db, get_watch_service())
    task = await svc.add_task(
        bundle_id=bundle_id,
        title=req.title,
        description=req.description,
        depends_on_task_ids=req.depends_on_task_ids,
    )

    if is_legacy_apikey or bundle.default_agent_runtime_id is None:
        # Legacy path — no sandbox.
        return {"task": task.model_dump(mode="json")}

    settings = get_settings()
    try:
        sandbox = await SandboxService(db, e2b).create_for_task(
            task_id=task.id,
            owner_account_id=caller.account_id,
            template=settings.e2b_default_template,
        )
    except Exception as exc:
        # Bubble out a structured 503 so the UI can offer a retry button.
        raise HTTPException(
            status_code=503,
            detail={
                "code": "sandbox_provision_timeout",
                "message": str(exc) or "e2b sandbox provisioning failed",
            },
        ) from exc

    # Persist the assignment + sandbox id on the task row.
    await db.execute(
        "UPDATE tasks SET assigned_runtime_id = ?, sandbox_id = ? WHERE id = ?",
        (bundle.default_agent_runtime_id, sandbox.id, task.id),
    )
    await db.commit()

    task_payload = task.model_dump(mode="json")
    task_payload["assigned_runtime_id"] = bundle.default_agent_runtime_id
    task_payload["sandbox_id"] = sandbox.id

    return {
        "task": task_payload,
        "sandbox": sandbox.model_dump(mode="json"),
    }


@router.post("/bundles/{bundle_id}/digest")
async def submit_digest(
    bundle_id: str,
    req: SubmitDigestRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    svc = DigestService(db, get_watch_service())
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
    svc = DigestService(db, get_watch_service())
    decision = DigestDecision(req.decision)
    digest = await svc.decide(bundle_id, decision, req.decided_by, req.note)
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
