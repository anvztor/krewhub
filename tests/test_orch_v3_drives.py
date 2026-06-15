"""Orch v3 — the single 'drives' link primitive (notes 2.2 §krewhub).

"A drives B": A sends Brief↓, B returns Report↑ onto A's tape. v3 collapses
the pipe/subagent split into one kind and routes orchestration by link
topology. These tests cover the four krewhub deltas:

  1. kind convergence  — 'drives' is the default; no implied dep.
  2. Report↑ on every link — a drives link flows B's Report onto A's tape.
  3. cascade + cycle-guard along LINK EDGES (not provenance) — the
     load-bearing change: runtime-adopted children have no created_by_task,
     so a provenance walk would miss them.
  4. runtime-adoption endpoint — a drives link to a WORKING/CLAIMED task,
     established live without interrupting it.

The legacy pipe/subagent paths keep their own coverage in
test_orch_o3b_links.py; this file asserts the v3 behaviour is additive.
"""
from __future__ import annotations

import json

import pytest

from krewhub.controllers.link_reconciler import LinkReconcileController
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
        "name": "v3-cb", "owner_id": "acc_legacy_apikey",
    })
    cookbook_id = cb.json()["cookbook"]["id"]
    bun = await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
        "prompt": "v3 bundle",
        "tasks": [{"title": f"t{i}"} for i in range(n)],
    })
    bundle_id = bun.json()["bundle"]["id"]
    ids = [t["id"] for t in bun.json()["tasks"]]
    return bundle_id, ids


def _link_ctl(db) -> LinkReconcileController:
    return LinkReconcileController(db, get_watch_service(), interval=3600.0)


def _orch_ctl(db) -> OrchController:
    return OrchController(db, get_watch_service(), interval=3600.0)


async def _reconcile(db) -> None:
    await _orch_ctl(db).reconcile()
    await _link_ctl(db).reconcile()


async def _force_task(db, task_id: str, *, status=None, report=None,
                      orch=None, brief=None):
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


# ---------------------------------------------------------------------------
# 1 · kind convergence — 'drives' is the default, no implied dep
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drives_is_default_kind_and_adds_no_dep(client):
    _, (a, b) = await _bundle_with_tasks(client)
    # No kind in the body → server defaults to 'drives'.
    r = await client.post(f"/api/v1/tasks/{a}/links", json={"to_task_id": b})
    assert r.status_code == 200, r.text
    link = r.json()["link"]
    assert link["kind"] == "drives"
    assert link["from_task_id"] == a and link["to_task_id"] == b
    # created_by is the actor class (human via api-key/cookie, agent via brain).
    assert link["created_by"] in ("human", "agent")
    # v3: drives carries NO implied dep (sequencing rides in the Brief).
    assert r.json()["to_task"]["depends_on_task_ids"] == []
    # No provenance for a board-gesture link between two existing tasks.
    assert link["created_by_task"] is None


@pytest.mark.asyncio
async def test_drives_new_task_records_provenance(client):
    _, (a, _) = await _bundle_with_tasks(client)
    r = await client.post(f"/api/v1/tasks/{a}/links", json={
        "new_task": {"title": "child", "brief": _BRIEF},
    })
    assert r.status_code == 200, r.text
    assert r.json()["link"]["kind"] == "drives"
    assert r.json()["link"]["created_by_task"] == a       # edge provenance
    assert r.json()["to_task"]["created_by_task"] == a     # task provenance
    assert r.json()["to_task"]["depends_on_task_ids"] == []


# ---------------------------------------------------------------------------
# 2 · Report↑ on every drives link
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drives_report_flows_up_to_from_task(client):
    db = await get_db()
    _, (a, _) = await _bundle_with_tasks(client)
    made = await client.post(f"/api/v1/tasks/{a}/links", json={
        "new_task": {"title": "child", "brief": _BRIEF},
    })
    child_id = made.json()["to_task"]["id"]
    await _force_task(db, child_id, status="done", report=_REPORT)

    await _reconcile(db)
    await _reconcile(db)  # acceptance pass + flow pass; converges

    cur = await db.execute(
        "SELECT body, payload, actor_type FROM events WHERE task_id = ? "
        "AND type = 'agent_reply' ORDER BY sequence DESC LIMIT 1",
        (a,),
    )
    row = await cur.fetchone()
    assert row is not None, "no Report↑ projection on the from-task tape"
    payload = json.loads(row["payload"])
    assert payload["from_task"] == child_id
    assert payload["report"]["prs"] == ["https://x/pr/9"]
    # Followup convention so A's prompt-builder threads it as an input turn.
    assert row["actor_type"] == "human"

    # Idempotent: link fires once.
    await _reconcile(db)
    cur = await db.execute(
        "SELECT COUNT(*) AS n FROM events WHERE task_id = ? AND "
        "payload LIKE '%\"from_task\"%'", (a,),
    )
    assert (await cur.fetchone())["n"] == 1


# ---------------------------------------------------------------------------
# 3 · cascade + cycle-guard traverse LINK EDGES, not provenance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drives_cascade_cancels_adopted_child_without_provenance(client):
    """THE load-bearing test: a drives B where B is a pre-existing task
    (created_by_task IS NULL — runtime adoption / board gesture). Cancelling
    A must still cancel B, because cascade walks LINK EDGES, not provenance
    (a provenance walk would miss B entirely)."""
    db = await get_db()
    _, (a, b) = await _bundle_with_tasks(client)
    made = await client.post(f"/api/v1/tasks/{a}/links", json={"to_task_id": b})
    assert made.json()["link"]["created_by_task"] is None  # no provenance

    r = await client.post(f"/api/v1/tasks/{a}/cancel")
    assert r.status_code == 200, r.text
    cascade = r.json()["cascade"]
    assert cascade["links_revoked"] == 1
    assert b in cascade["children_cancelled"]
    assert (await TaskRepo(db).get(b)).status == "cancelled"

    cur = await db.execute(
        "SELECT COUNT(*) AS n FROM task_links WHERE revoked_at IS NULL")
    assert (await cur.fetchone())["n"] == 0


@pytest.mark.asyncio
async def test_drives_cascade_recurses_whole_subtree(client):
    """A drives B drives C (all adoption links, no provenance). Cancelling A
    reclaims the entire link subtree: B and C both cancelled."""
    db = await get_db()
    _, (a, b, c) = await _bundle_with_tasks(client, n=3)
    await client.post(f"/api/v1/tasks/{a}/links", json={"to_task_id": b})
    await client.post(f"/api/v1/tasks/{b}/links", json={"to_task_id": c})

    r = await client.post(f"/api/v1/tasks/{a}/cancel")
    assert r.status_code == 200, r.text
    cascade = r.json()["cascade"]
    assert b in cascade["children_cancelled"]
    assert c in cascade["children_cancelled"]   # grandchild via edge recursion
    repo = TaskRepo(db)
    assert (await repo.get(b)).status == "cancelled"
    assert (await repo.get(c)).status == "cancelled"


@pytest.mark.asyncio
async def test_drives_cascade_keeps_done_child(client):
    """Terminal (DONE) driven children are kept — their Report survives
    (产出物独立存活)."""
    db = await get_db()
    _, (a, b) = await _bundle_with_tasks(client)
    await client.post(f"/api/v1/tasks/{a}/links", json={"to_task_id": b})
    await _force_task(db, b, status="done", report=_REPORT)

    r = await client.post(f"/api/v1/tasks/{a}/cancel")
    assert r.status_code == 200
    assert b not in r.json()["cascade"]["children_cancelled"]
    assert (await TaskRepo(db).get(b)).status == "done"


@pytest.mark.asyncio
async def test_drives_link_cycle_rejected_via_link_graph(client):
    """drives links carry no dep, so the cycle must be caught on the LINK
    graph. A drives B; B drives A → 400 (no deps involved at all)."""
    _, (a, b) = await _bundle_with_tasks(client)
    assert (await client.post(
        f"/api/v1/tasks/{a}/links", json={"to_task_id": b})).status_code == 200
    # Confirm no dep edge was created (pure link-graph cycle).
    got = await client.get(f"/api/v1/tasks/{b}")
    assert got.json()["task"]["depends_on_task_ids"] == []

    r = await client.post(f"/api/v1/tasks/{b}/links", json={"to_task_id": a})
    assert r.status_code == 400, r.text
    assert "cycle" in r.text


@pytest.mark.asyncio
async def test_drives_three_node_cycle_rejected(client):
    """A→B→C drives chain; C→A closes a 3-node link cycle → rejected."""
    _, (a, b, c) = await _bundle_with_tasks(client, n=3)
    assert (await client.post(
        f"/api/v1/tasks/{a}/links", json={"to_task_id": b})).status_code == 200
    assert (await client.post(
        f"/api/v1/tasks/{b}/links", json={"to_task_id": c})).status_code == 200
    r = await client.post(f"/api/v1/tasks/{c}/links", json={"to_task_id": a})
    assert r.status_code == 400 and "cycle" in r.text


# ---------------------------------------------------------------------------
# 4 · runtime-adoption endpoint — drives link to a RUNNING task
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_runtime_adoption_links_a_running_task_without_interrupting(client):
    """Shift-connecting two RUNNING tasks establishes "A drives B" live: B
    is not interrupted, the edge is active immediately, and B's later
    Report↑ flows onto A's tape."""
    db = await get_db()
    _, (a, b) = await _bundle_with_tasks(client)
    # B is already executing.
    await _force_task(db, b, status="working")

    r = await client.post(f"/api/v1/tasks/{a}/links", json={"to_task_id": b})
    assert r.status_code == 200, r.text
    assert r.json()["link"]["kind"] == "drives"
    # B is NOT interrupted by the adoption.
    assert (await TaskRepo(db).get(b)).status == "working"

    # B finishes → its Report↑ flows onto A's tape via the adopted edge.
    await _force_task(db, b, status="done", report=_REPORT)
    await _reconcile(db)
    await _reconcile(db)
    cur = await db.execute(
        "SELECT payload FROM events WHERE task_id = ? AND type = 'agent_reply' "
        "ORDER BY sequence DESC LIMIT 1", (a,),
    )
    row = await cur.fetchone()
    assert row is not None and json.loads(row["payload"])["from_task"] == b


@pytest.mark.asyncio
async def test_adoption_to_running_task_still_cycle_guarded(client):
    """Adoption doesn't bypass the cycle guard: if B already drives A,
    adopting A→B is rejected even though B is running."""
    db = await get_db()
    _, (a, b) = await _bundle_with_tasks(client)
    assert (await client.post(
        f"/api/v1/tasks/{b}/links", json={"to_task_id": a})).status_code == 200
    await _force_task(db, b, status="working")
    r = await client.post(f"/api/v1/tasks/{a}/links", json={"to_task_id": b})
    assert r.status_code == 400 and "cycle" in r.text
