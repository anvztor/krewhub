"""Orch mode (O3b): task links — first-class data-flow edges (design §5).

A Link is a directed edge {from_task, to_task, kind, payload_map}:
  pipe      A's output flows to B's prompt when A completes. Creating a
            pipe ALSO appends a dep (B depends_on A) so ordering is
            enforced by the existing dispatch gate — design §5.1 choice B.
  subagent  A delegates a Brief to B (A's sub-agent); B's Report flows
            back onto A's tape. No dep: A is *waiting on* B, not blocked
            by it.

This is the API form of infinite-scroll's `send --text` (pipe payload) and
`new-cell` (inline new_task with provenance created_by_task = A) — see the
parity matrix (design §5.6). Payload firing itself lives in OrchController.

AUTHZ: link creation/revocation mutates both endpoints' world — gated by
require_bundle_owner on the bundle (links never cross bundles in v1).
Reads mirror get_bundle (authenticated; no ownership gate).
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException

from krewhub.auth import CallerContext, require_bundle_owner, resolve_caller_or_cookie
from krewhub.db.connection import get_db
from krewhub.models import TaskStatus, WatchEventType
from krewhub.repositories.task_repo import TaskRepo
from krewhub.routes.schemas import CreateLinkRequest
from krewhub.watch.globals import get_watch_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["links"], dependencies=[Depends(resolve_caller_or_cookie)])

# v3 collapses orchestration to a single 'drives' primitive ("A drives B":
# Brief↓ / Report↑). Legacy 'pipe'/'subagent' rows are still accepted and
# fire under their old semantics so in-flight links don't break.
_VALID_KINDS = ("drives", "pipe", "subagent")
_VALID_SOURCES = ("report", "last_reply")
_VALID_TARGETS = ("followup", "brief_context")


def _row_to_link(row: aiosqlite.Row) -> dict:
    return {
        "id": row["id"],
        "bundle_id": row["bundle_id"],
        "from_task_id": row["from_task_id"],
        "to_task_id": row["to_task_id"],
        "kind": row["kind"],
        "payload_map": json.loads(row["payload_map"] or "{}"),
        "created_by_account": row["created_by_account"],
        "created_by_task": row["created_by_task"],
        "created_at": row["created_at"],
        "fired_at": row["fired_at"],
        "revoked_at": row["revoked_at"],
    }


async def _would_cycle(db, bundle_id: str, from_id: str, to_id: str) -> bool:
    """True if adding dep (to depends-on from) would create a dependency
    cycle. Mirrors the SPA's DFS: walk upward from `from_id`; if we reach
    `to_id`, the new edge closes a loop."""
    cursor = await db.execute(
        "SELECT id, depends_on_task_ids FROM tasks WHERE bundle_id = ?",
        (bundle_id,),
    )
    deps = {
        r["id"]: json.loads(r["depends_on_task_ids"] or "[]")
        for r in await cursor.fetchall()
    }
    stack, seen = [from_id], set()
    while stack:
        cur = stack.pop()
        if cur == to_id:
            return True
        if cur in seen:
            continue
        seen.add(cur)
        stack.extend(deps.get(cur, []))
    return False


async def _would_cycle_links(db, bundle_id: str, from_id: str, to_id: str) -> bool:
    """True if adding the drives edge from_id→to_id would close a cycle in
    the LINK graph. v3 drives links carry no dep edge, so the dep-based
    `_would_cycle` can't see them — and runtime adoption links two
    pre-existing tasks with no provenance — so cascade/cycle/authz must all
    walk the link graph itself (notes 2.2 §2). "A drives B" flows from→to;
    a cycle exists iff `to` can already reach `from` along active edges."""
    cursor = await db.execute(
        "SELECT from_task_id, to_task_id FROM task_links "
        "WHERE bundle_id = ? AND revoked_at IS NULL",
        (bundle_id,),
    )
    adj: dict[str, list[str]] = {}
    for r in await cursor.fetchall():
        adj.setdefault(r["from_task_id"], []).append(r["to_task_id"])
    stack, seen = [to_id], set()
    while stack:
        cur = stack.pop()
        if cur == from_id:
            return True
        if cur in seen:
            continue
        seen.add(cur)
        stack.extend(adj.get(cur, []))
    return False


@router.post("/tasks/{from_task_id}/links")
async def create_link(
    from_task_id: str,
    req: CreateLinkRequest,
    db: aiosqlite.Connection = Depends(get_db),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    kind = (req.kind or "drives").strip().lower()
    if kind not in _VALID_KINDS:
        raise HTTPException(
            status_code=400, detail="kind must be 'drives' (or legacy 'pipe'|'subagent')",
        )
    if (req.to_task_id is None) == (req.new_task is None):
        raise HTTPException(
            status_code=400,
            detail="exactly one of to_task_id or new_task is required",
        )
    pm = req.payload_map or {}
    if pm.get("source") not in (None, *_VALID_SOURCES):
        raise HTTPException(status_code=400, detail="payload_map.source must be report|last_reply")
    if pm.get("target") not in (None, *_VALID_TARGETS):
        raise HTTPException(status_code=400, detail="payload_map.target must be followup|brief_context")

    repo = TaskRepo(db)
    from_task = await repo.get(from_task_id)
    if from_task is None:
        raise HTTPException(status_code=404, detail="from task not found")

    # Owner-gated on the bundle (links mutate both endpoints' world).
    await require_bundle_owner(from_task.bundle_id, caller, db)

    created_task = None
    if req.new_task is not None:
        # Orch's new-cell: A creates its own downstream, provenance-stamped.
        from krewhub.services.task_service import TaskService
        svc = TaskService(db, get_watch_service())
        created_task = await svc.add_task(
            bundle_id=from_task.bundle_id,
            title=req.new_task.title,
            description=req.new_task.description,
            depends_on_task_ids=[],
        )
        to_task_id = created_task.id
        if req.new_task.brief is not None:
            await db.execute(
                "UPDATE tasks SET brief_json = ? WHERE id = ?",
                (req.new_task.brief.model_dump_json(), to_task_id),
            )
    else:
        to_task_id = req.to_task_id
        if to_task_id == from_task_id:
            raise HTTPException(status_code=400, detail="cannot link a task to itself")
        to_task = await repo.get(to_task_id)
        if to_task is None:
            raise HTTPException(status_code=404, detail="to task not found")
        if to_task.bundle_id != from_task.bundle_id:
            raise HTTPException(
                status_code=400, detail="links cannot cross bundles (v1)",
            )
        # v3 runtime adoption: B may be working/claimed — "A drives B" is
        # established live, B is NOT interrupted. No status precondition;
        # B's subsequent Report↑ flows onto A's tape via the new edge
        # (notes 2.2 §3). Terminal targets are pointless but harmless
        # (the link just never fires).

    # Reject duplicate active edge (same direction + kind).
    cursor = await db.execute(
        "SELECT id FROM task_links WHERE from_task_id = ? AND to_task_id = ? "
        "AND kind = ? AND revoked_at IS NULL",
        (from_task_id, to_task_id, kind),
    )
    if await cursor.fetchone() is not None:
        raise HTTPException(status_code=409, detail="link already exists")

    # Cycle guard applies to ANY edge between two EXISTING tasks, every kind
    # (S2 B2, hole #2). It walks BOTH graphs and rejects if either would
    # close a loop: the dep graph (legacy pipe links add a dep) AND the
    # link graph (v3 drives links carry no dep, and runtime adoption has no
    # provenance — notes 2.2 §2). Provenance-spawned children (new_task) are
    # brand-new with no edges, so they can never cycle and are skipped.
    if req.new_task is None and (
        await _would_cycle(db, from_task.bundle_id, from_task_id, to_task_id)
        or await _would_cycle_links(
            db, from_task.bundle_id, from_task_id, to_task_id,
        )
    ):
        raise HTTPException(
            status_code=400, detail="link would create a dependency cycle",
        )

    # pipe implies a dep (B waits for A) — design §5.1 choice B.
    if kind == "pipe":
        to_task = await repo.get(to_task_id)
        if to_task is None:
            raise HTTPException(status_code=404, detail="to task not found")
        dep_ids = list(to_task.depends_on_task_ids or [])
        if from_task_id not in dep_ids:
            updated = await repo.update(
                to_task_id, depends_on_task_ids=dep_ids + [from_task_id],
            )
            if updated is not None:
                await get_watch_service().record_resource(
                    "task", to_task_id, WatchEventType.MODIFIED, updated,
                )

    link_id = f"lnk_{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT INTO task_links (id, bundle_id, from_task_id, to_task_id, kind, "
        "payload_map, created_by_account, created_by_task, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (link_id, from_task.bundle_id, from_task_id, to_task_id, kind,
         json.dumps(pm),
         caller.account_id,
         # Provenance: when A creates its downstream inline, the edge (and
         # the new task) are A-born. Linking two existing tasks is a board
         # gesture, not orch provenance.
         from_task_id if req.new_task is not None else None,
         now),
    )
    if created_task is not None:
        await db.execute(
            "UPDATE tasks SET created_by_task = ? WHERE id = ?",
            (from_task_id, created_task.id),
        )
    await db.commit()

    cursor = await db.execute("SELECT * FROM task_links WHERE id = ?", (link_id,))
    row = await cursor.fetchone()
    fresh_to = await repo.get(to_task_id)
    link = _row_to_link(row)
    # created_by: who drew the edge — "human" (FE Shift gesture, cookie
    # auth) vs "agent" (an orchestrator brain). Provenance lives in
    # created_by_task; this is the actor class (notes 2.2 §3).
    link["created_by"] = caller.principal_type
    return {
        "link": link,
        "to_task": fresh_to.model_dump(mode="json") if fresh_to else None,
    }


@router.get("/bundles/{bundle_id}/links")
async def list_links(
    bundle_id: str,
    include_revoked: bool = False,
    db: aiosqlite.Connection = Depends(get_db),
):
    """List a bundle's links (read mirrors get_bundle: authenticated)."""
    q = "SELECT * FROM task_links WHERE bundle_id = ?"
    if not include_revoked:
        q += " AND revoked_at IS NULL"
    cursor = await db.execute(q + " ORDER BY created_at", (bundle_id,))
    rows = await cursor.fetchall()
    return {"links": [_row_to_link(r) for r in rows]}


@router.delete("/links/{link_id}")
async def revoke_link(
    link_id: str,
    db: aiosqlite.Connection = Depends(get_db),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    """Soft-revoke a link. Owner-only. Revoking a link never deletes tasks
    (design §5.3: 删 Link ≠ 删 task); the pipe-implied dep is removed so B
    stops waiting for A."""
    cursor = await db.execute("SELECT * FROM task_links WHERE id = ?", (link_id,))
    row = await cursor.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="link not found")

    await require_bundle_owner(row["bundle_id"], caller, db)

    if row["revoked_at"] is not None:
        return {"link": _row_to_link(row), "already_revoked": True}

    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "UPDATE task_links SET revoked_at = ? WHERE id = ?", (now, link_id),
    )

    # Unwind the pipe-implied dep (only if no other active pipe edge from
    # the same upstream still requires it).
    if row["kind"] == "pipe":
        cursor = await db.execute(
            "SELECT COUNT(*) AS n FROM task_links WHERE from_task_id = ? "
            "AND to_task_id = ? AND kind = 'pipe' AND revoked_at IS NULL AND id != ?",
            (row["from_task_id"], row["to_task_id"], link_id),
        )
        if (await cursor.fetchone())["n"] == 0:
            repo = TaskRepo(db)
            to_task = await repo.get(row["to_task_id"])
            if to_task is not None and row["from_task_id"] in (to_task.depends_on_task_ids or []):
                updated = await repo.update(
                    row["to_task_id"],
                    depends_on_task_ids=[
                        d for d in to_task.depends_on_task_ids if d != row["from_task_id"]
                    ],
                )
                if updated is not None:
                    await get_watch_service().record_resource(
                        "task", row["to_task_id"], WatchEventType.MODIFIED, updated,
                    )
    await db.commit()

    cursor = await db.execute("SELECT * FROM task_links WHERE id = ?", (link_id,))
    return {"link": _row_to_link(await cursor.fetchone())}


async def cascade_on_task_termination(
    db, task_id: str, _seen: set[str] | None = None,
) -> dict:
    """§5.3 cascade, called when a task is removed/cancelled (parity `close`).
    v3 traverses LINK EDGES, not provenance:
      * all links touching the task are soft-revoked
      * down each outgoing 'drives' edge, a non-terminal `to`-task is
        cancelled — "A drives B": the driver is gone, so the driven task
        dies — and the cascade RECURSES along edges to reclaim the whole
        subtree. This is edge- (not provenance-) based ON PURPOSE: a
        runtime-adopted child has created_by_task != task_id, so a
        provenance walk would MISS it (notes 2.2 §2). Terminal (DONE)
        children are kept — their Report survives (产出物独立存活).
      * LEGACY 'subagent' keeps its provenance gate (created_by_task);
        LEGACY 'pipe' downstream is left alive and only its implied dep on
        the dead upstream is removed so it isn't gated forever.
    Cycles in the link graph are guarded at creation, but ``_seen`` makes
    the recursion terminate defensively regardless. Returns an aggregated
    summary for the route response.
    """
    seen = _seen if _seen is not None else set()
    if task_id in seen:
        return {"links_revoked": 0, "children_cancelled": [], "deps_unblocked": []}
    seen.add(task_id)

    now = datetime.now(timezone.utc).isoformat()
    repo = TaskRepo(db)
    cancelled_children: list[str] = []
    unblocked: list[str] = []
    links_revoked = 0

    cursor = await db.execute(
        "SELECT * FROM task_links WHERE (from_task_id = ? OR to_task_id = ?) "
        "AND revoked_at IS NULL",
        (task_id, task_id),
    )
    links = await cursor.fetchall()
    for row in links:
        await db.execute(
            "UPDATE task_links SET revoked_at = ? WHERE id = ?", (now, row["id"]),
        )
        links_revoked += 1
        if row["from_task_id"] != task_id:
            continue
        child = await repo.get(row["to_task_id"])
        if child is None:
            continue
        kind = row["kind"]
        # v3 'drives' (edge-based, NO provenance) OR legacy 'subagent' that
        # this task provenance-created: the driven/delegated child dies.
        drives_child = kind == "drives" or (
            kind == "subagent" and row["created_by_task"] == task_id
        )
        if drives_child:
            if child.status not in (
                TaskStatus.DONE, TaskStatus.CANCELLED,
            ):
                from krewhub.services.task_service import TaskService
                svc = TaskService(db, get_watch_service())
                if await svc.cancel_task(child.id) is not None:
                    cancelled_children.append(child.id)
                    # Recurse along EDGES: the driver is gone, so the
                    # child's own driven tasks die too — reclaim the whole
                    # subtree (works for adopted children with no provenance).
                    sub = await cascade_on_task_termination(db, child.id, seen)
                    links_revoked += sub["links_revoked"]
                    cancelled_children.extend(sub["children_cancelled"])
                    unblocked.extend(sub["deps_unblocked"])
        elif kind == "pipe":
            deps = list(child.depends_on_task_ids or [])
            if task_id in deps:
                updated = await repo.update(
                    child.id,
                    depends_on_task_ids=[d for d in deps if d != task_id],
                )
                if updated is not None:
                    unblocked.append(child.id)
                    await get_watch_service().record_resource(
                        "task", child.id, WatchEventType.MODIFIED, updated,
                    )
    await db.commit()
    if links:
        logger.info(
            "link cascade for task %s: %d link(s) revoked, %d child cancelled, %d unblocked",
            task_id, links_revoked, len(cancelled_children), len(unblocked),
        )
    return {
        "links_revoked": links_revoked,
        "children_cancelled": cancelled_children,
        "deps_unblocked": unblocked,
    }
