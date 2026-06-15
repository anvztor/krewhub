"""Orch hardening (S2 · Notes 1.2 §A.1 holes #1–#3 + C6 demotion).

These close the three hardening holes the S1 architect found in the
*shipped* O3b code, plus the C6 safety-net demotion:

  B1  recursive cascade   — cancelling A reclaims its whole subagent
                            SUBTREE (grandchildren), not just direct kids.
  B2  universal cycle guard — _would_cycle now covers subagent links
                            between existing tasks AND raw depends_on
                            writes (graph-derived deps).
  B3  decoupled firing    — pipe firing lives in LinkReconcileController,
                            which runs regardless of KREWHUB_ORCH_ENABLED.
  C6  safety-net demotion — OrchController defers Report acceptance to an
                            attached orch-agent brain; mechanical fallback
                            when none is attached (today's behavior).

Maps to eval §D: B1→E? (close/cascade), B2→E5, B3→E3 (flag off), C6→E2.
"""
from __future__ import annotations

import json

import pytest

from krewhub.controllers.link_reconciler import LinkReconcileController
from krewhub.controllers.manager import ControllerManager
from krewhub.controllers.orch_controller import OrchController
from krewhub.db.connection import get_db
from krewhub.repositories.task_repo import TaskRepo
from krewhub.watch.globals import get_watch_service

_BRIEF = {
    "goal": "do the thing", "context": "", "constraints": [],
    "deliverable": "PR", "report_points": [],
}
_REPORT = {
    "status": "done", "artifacts": [], "prs": ["https://x/pr/9"],
    "blockers": [], "decisions_needed": [],
}


async def _bundle_with_tasks(client, n=2) -> tuple[str, list[str]]:
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "harden-cb", "owner_id": "acc_legacy_apikey",
    })
    cookbook_id = cb.json()["cookbook"]["id"]
    bun = await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
        "prompt": "harden bundle",
        "tasks": [{"title": f"t{i}"} for i in range(n)],
    })
    bundle_id = bun.json()["bundle"]["id"]
    ids = [t["id"] for t in bun.json()["tasks"]]
    return bundle_id, ids


async def _force_task(db, task_id, *, status=None, report=None, orch=None, brief=None):
    sets, params = [], []
    if status is not None:
        sets.append("status = ?"); params.append(status)
    if report is not None:
        sets.append("report_json = ?"); params.append(json.dumps(report))
    if orch is not None:
        sets.append("orch_json = ?"); params.append(json.dumps(orch))
    if brief is not None:
        sets.append("brief_json = ?"); params.append(json.dumps(brief))
    params.append(task_id)
    await db.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id = ?", params)
    await db.commit()


def _orch(db) -> OrchController:
    return OrchController(db, get_watch_service(), interval=3600.0)


def _links(db) -> LinkReconcileController:
    return LinkReconcileController(db, get_watch_service(), interval=3600.0)


# ---------------------------------------------------------------------------
# B1 — recursive / subtree cascade (hole #1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cascade_reclaims_whole_subagent_subtree(client):
    """A → B → C subagent chain. Cancelling A must cancel B AND its
    grandchild C — not orphan C (the hole)."""
    db = await get_db()
    _, (a, _) = await _bundle_with_tasks(client)

    made_b = await client.post(f"/api/v1/tasks/{a}/links", json={
        "kind": "subagent", "new_task": {"title": "child B", "brief": _BRIEF},
    })
    b = made_b.json()["to_task"]["id"]
    made_c = await client.post(f"/api/v1/tasks/{b}/links", json={
        "kind": "subagent", "new_task": {"title": "grandchild C", "brief": _BRIEF},
    })
    c = made_c.json()["to_task"]["id"]

    r = await client.post(f"/api/v1/tasks/{a}/cancel")
    assert r.status_code == 200, r.text
    cascade = r.json()["cascade"]

    repo = TaskRepo(db)
    assert (await repo.get(b)).status == "cancelled", "direct child not cancelled"
    assert (await repo.get(c)).status == "cancelled", "grandchild orphaned (hole #1)"
    assert b in cascade["children_cancelled"]
    assert c in cascade["children_cancelled"]

    # All links in the subtree revoked.
    cur = await db.execute("SELECT COUNT(*) AS n FROM task_links WHERE revoked_at IS NULL")
    assert (await cur.fetchone())["n"] == 0


@pytest.mark.asyncio
async def test_cascade_subtree_stops_at_terminal_grandchild(client):
    """A done grandchild's artifacts survive (durable-artifact red line)."""
    db = await get_db()
    _, (a, _) = await _bundle_with_tasks(client)
    made_b = await client.post(f"/api/v1/tasks/{a}/links", json={
        "kind": "subagent", "new_task": {"title": "B", "brief": _BRIEF}})
    b = made_b.json()["to_task"]["id"]
    made_c = await client.post(f"/api/v1/tasks/{b}/links", json={
        "kind": "subagent", "new_task": {"title": "C", "brief": _BRIEF}})
    c = made_c.json()["to_task"]["id"]
    await _force_task(db, c, status="done", report=_REPORT)

    await client.post(f"/api/v1/tasks/{a}/cancel")
    repo = TaskRepo(db)
    assert (await repo.get(b)).status == "cancelled"
    assert (await repo.get(c)).status == "done"  # terminal grandchild preserved


# ---------------------------------------------------------------------------
# B2 — universal cycle guard (hole #2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subagent_cycle_rejected_three_task_chain(client):
    """X→Y→Z via pipe deps; a subagent Z→X closes the loop → 400."""
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "cyc", "owner_id": "acc_legacy_apikey"})
    cookbook_id = cb.json()["cookbook"]["id"]
    bun = await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
        "prompt": "cycle", "tasks": [{"title": "X"}, {"title": "Y"}, {"title": "Z"}]})
    x, y, z = [t["id"] for t in bun.json()["tasks"]]

    assert (await client.post(f"/api/v1/tasks/{x}/links", json={
        "to_task_id": y, "kind": "pipe"})).status_code == 200
    assert (await client.post(f"/api/v1/tasks/{y}/links", json={
        "to_task_id": z, "kind": "pipe"})).status_code == 200

    # Subagent Z→X would close the loop (Z already transitively deps X).
    r = await client.post(f"/api/v1/tasks/{z}/links", json={
        "to_task_id": x, "kind": "subagent"})
    assert r.status_code == 400, r.text
    assert "cycle" in r.text


@pytest.mark.asyncio
async def test_provenance_spawned_subagent_still_allowed(client):
    """New-task subagent children never cycle (acyclic by construction)."""
    _, (a, _) = await _bundle_with_tasks(client)
    r = await client.post(f"/api/v1/tasks/{a}/links", json={
        "kind": "subagent", "new_task": {"title": "fresh child", "brief": _BRIEF}})
    assert r.status_code == 200, r.text


def test_graph_dep_cycle_helper_rejects_loop():
    """B2 raw-dep path: the graph dep builder rejects a cyclic edge set."""
    from krewhub.services.graph_attachment_service import _edges_have_cycle

    # acyclic: a→b→c
    assert _edges_have_cycle(["a", "b", "c"], [("a", "b"), ("b", "c")]) is False
    # cyclic: a→b→c→a
    assert _edges_have_cycle(["a", "b", "c"], [("a", "b"), ("b", "c"), ("c", "a")]) is True
    # self-loop
    assert _edges_have_cycle(["a"], [("a", "a")]) is True


# ---------------------------------------------------------------------------
# B3 — pipe firing decoupled from the orch flag (hole #3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pipe_fires_via_link_reconciler_without_orch(client):
    """A manual pipe fires through LinkReconcileController alone — no
    OrchController involved (proves flag-independence)."""
    db = await get_db()
    _, (p, q) = await _bundle_with_tasks(client)
    await client.post(f"/api/v1/tasks/{p}/links", json={
        "to_task_id": q, "kind": "pipe"})
    await _force_task(db, p, status="done", report=_REPORT)

    # Only the link reconciler runs — never the OrchController.
    await _links(db).reconcile()

    task_q = await TaskRepo(db).get(q)
    assert "UPSTREAM OUTPUT" in (task_q.description or "")
    assert "https://x/pr/9" in task_q.description


@pytest.mark.asyncio
async def test_manager_runs_link_reconciler_even_with_orch_disabled(test_db):
    """With KREWHUB_ORCH_ENABLED=0 the manager still wires the link
    reconciler (no OrchController)."""
    mgr = ControllerManager(test_db, get_watch_service(), orch_enabled=False)
    names = set(mgr.health().keys())
    assert "LinkReconcileController" in names
    assert "OrchController" not in names


@pytest.mark.asyncio
async def test_manager_has_both_when_orch_enabled(test_db):
    mgr = ControllerManager(test_db, get_watch_service(), orch_enabled=True)
    names = set(mgr.health().keys())
    assert "LinkReconcileController" in names
    assert "OrchController" in names


# ---------------------------------------------------------------------------
# C6 — OrchController demoted to safety-net (brain-authoritative when attached)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_orch_defers_acceptance_when_brain_attached(client):
    db = await get_db()
    _, (a, _) = await _bundle_with_tasks(client)
    # Brain-managed: a privileged orch-agent owns this task.
    await _force_task(
        db, a, status="done", report=_REPORT, brief=_BRIEF,
        orch={"managed_by_agent": "rt_brain_1"},
    )
    await _orch(db).reconcile()

    task = await TaskRepo(db).get(a)
    orch = task.orch or {}
    assert orch.get("accepted_at") is None, "safety-net wrongly auto-accepted"
    assert orch.get("awaiting_brain") is True

    # Idempotent: re-reconcile emits no second deferral event.
    await _orch(db).reconcile()
    cur = await db.execute(
        "SELECT COUNT(*) AS n FROM events WHERE task_id = ? AND body LIKE '%orch-agent%'",
        (a,),
    )
    assert (await cur.fetchone())["n"] == 1


@pytest.mark.asyncio
async def test_orch_mechanical_fallback_when_no_brain(client):
    """Default (no brain): mechanical acceptance, exactly today's behavior."""
    db = await get_db()
    _, (a, _) = await _bundle_with_tasks(client)
    await _force_task(db, a, status="done", report=_REPORT, brief=_BRIEF)
    await _orch(db).reconcile()
    task = await TaskRepo(db).get(a)
    assert (task.orch or {}).get("accepted_at") is not None
