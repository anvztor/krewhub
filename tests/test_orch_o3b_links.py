"""Orch mode (O3b) — task links: the data-flow edge layer (design §5).

Covers the parity-matrix gaps this PR closes:
  * new-cell  → POST /tasks/{A}/links with new_task (provenance)
  * send --text → pipe firing (A's output injected into B's prompt) and
                  subagent up-flow (B's Report projected onto A's tape)
  * close     → cascade on task termination

Route tests use the api-key `client` (legacy sentinel = owner-equivalent);
authz negatives use `cookie_client` (different account). Controller tests
drive OrchController.reconcile() directly, as in test_orch_o2_loop.py.
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
        "name": "links-cb", "owner_id": "acc_legacy_apikey",
    })
    cookbook_id = cb.json()["cookbook"]["id"]
    bun = await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
        "prompt": "links bundle",
        "tasks": [{"title": f"t{i}"} for i in range(n)],
    })
    bundle_id = bun.json()["bundle"]["id"]
    ids = [t["id"] for t in bun.json()["tasks"]]
    return bundle_id, ids


def _controller(db) -> OrchController:
    return OrchController(db, get_watch_service(), interval=3600.0)


def _link_ctl(db) -> LinkReconcileController:
    return LinkReconcileController(db, get_watch_service(), interval=3600.0)


async def _reconcile(db) -> None:
    """Run the orch decision pass then the always-on mechanical link pass —
    the two controllers the manager now runs (OrchController gated by the
    orch flag, LinkReconcileController always-on). Link *firing* moved out
    of OrchController in S2 B3."""
    await _controller(db).reconcile()
    await _link_ctl(db).reconcile()


# ---------------------------------------------------------------------------
# Route: create / list / revoke
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_pipe_link_adds_dep(client):
    bundle_id, (a, b) = await _bundle_with_tasks(client)
    r = await client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": b, "kind": "pipe",
    })
    assert r.status_code == 200, r.text
    link = r.json()["link"]
    assert link["kind"] == "pipe" and link["from_task_id"] == a
    # pipe implies dep: B now waits for A (design §5.1 choice B).
    assert a in r.json()["to_task"]["depends_on_task_ids"]

    listed = await client.get(f"/api/v1/bundles/{bundle_id}/links")
    assert [l["id"] for l in listed.json()["links"]] == [link["id"]]


@pytest.mark.asyncio
async def test_pipe_link_cycle_rejected(client):
    _, (a, b) = await _bundle_with_tasks(client)
    assert (await client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": b, "kind": "pipe"})).status_code == 200
    # Reverse edge would close a dependency cycle.
    r = await client.post(f"/api/v1/tasks/{b}/links", json={
        "to_task_id": a, "kind": "pipe",
    })
    assert r.status_code == 400, r.text
    assert "cycle" in r.text


@pytest.mark.asyncio
async def test_duplicate_and_self_and_crossbundle_rejected(client):
    bundle_id, (a, b) = await _bundle_with_tasks(client)
    assert (await client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": b, "kind": "pipe"})).status_code == 200
    assert (await client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": b, "kind": "pipe"})).status_code == 409
    assert (await client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": a, "kind": "pipe"})).status_code == 400
    # Cross-bundle target.
    _, (c, _) = await _bundle_with_tasks(client)
    r = await client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": c, "kind": "pipe",
    })
    assert r.status_code == 400 and "cross" in r.text


@pytest.mark.asyncio
async def test_new_task_subagent_link_records_provenance(client):
    """The parity matrix's new-cell: A creates its own downstream."""
    _, (a, _) = await _bundle_with_tasks(client)
    r = await client.post(f"/api/v1/tasks/{a}/links", json={
        "kind": "subagent",
        "new_task": {"title": "subagent child", "brief": _BRIEF},
    })
    assert r.status_code == 200, r.text
    link = r.json()["link"]
    child = r.json()["to_task"]
    assert link["kind"] == "subagent"
    assert link["created_by_task"] == a            # edge provenance
    assert child["created_by_task"] == a           # task provenance
    assert child["brief"]["goal"] == "do the thing"
    # subagent adds NO dep (A waits on B, not blocked by it).
    assert child["depends_on_task_ids"] == []


@pytest.mark.asyncio
async def test_link_mutations_denied_for_non_owner(client, cookie_client):
    _, (a, b) = await _bundle_with_tasks(client)  # owned by acc_legacy_apikey
    r = await cookie_client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": b, "kind": "pipe",
    })
    assert r.status_code == 403, r.text

    made = await client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": b, "kind": "pipe"})
    link_id = made.json()["link"]["id"]
    r = await cookie_client.delete(f"/api/v1/links/{link_id}")
    assert r.status_code == 403, r.text


@pytest.mark.asyncio
async def test_revoke_pipe_link_removes_dep(client):
    _, (a, b) = await _bundle_with_tasks(client)
    made = await client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": b, "kind": "pipe"})
    link_id = made.json()["link"]["id"]

    r = await client.delete(f"/api/v1/links/{link_id}")
    assert r.status_code == 200, r.text
    assert r.json()["link"]["revoked_at"] is not None

    got = await client.get(f"/api/v1/tasks/{b}")
    assert a not in got.json()["task"]["depends_on_task_ids"]

    # Idempotent re-revoke.
    again = await client.delete(f"/api/v1/links/{link_id}")
    assert again.json().get("already_revoked") is True


# ---------------------------------------------------------------------------
# Controller: pipe firing (send --text, API-fied)
# ---------------------------------------------------------------------------


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


@pytest.mark.asyncio
async def test_pipe_fires_upstream_report_into_downstream(client):
    db = await get_db()
    _, (a, b) = await _bundle_with_tasks(client)
    await client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": b, "kind": "pipe"})
    # Plain upstream (no brief): DONE suffices to fire.
    await _force_task(db, a, status="done", report=_REPORT)

    ctl = _link_ctl(db)
    await ctl.reconcile()

    task_b = await TaskRepo(db).get(b)
    assert "UPSTREAM OUTPUT" in (task_b.description or "")
    assert "https://x/pr/9" in task_b.description
    # fired_at set → second reconcile injects nothing twice.
    await ctl.reconcile()
    assert task_b.description.count("UPSTREAM OUTPUT") == 1
    fresh_b = await TaskRepo(db).get(b)
    assert fresh_b.description.count("UPSTREAM OUTPUT") == 1


@pytest.mark.asyncio
async def test_pipe_waits_for_orch_acceptance_on_brief_upstream(client):
    db = await get_db()
    _, (a, b) = await _bundle_with_tasks(client)
    await client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": b, "kind": "pipe"})
    # Brief-managed upstream, done but NOT yet orch-accepted.
    await _force_task(db, a, status="done", report=_REPORT, brief=_BRIEF)

    # Orch accepts A (decision), then the link reconciler fires the pipe
    # (mechanical). Two passes converge regardless of ordering.
    await _reconcile(db)
    await _reconcile(db)
    task_b = await TaskRepo(db).get(b)
    assert "UPSTREAM OUTPUT" in (task_b.description or "")


@pytest.mark.asyncio
async def test_pipe_does_not_fire_before_done(client):
    db = await get_db()
    _, (a, b) = await _bundle_with_tasks(client)
    await client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": b, "kind": "pipe"})
    await _link_ctl(db).reconcile()
    task_b = await TaskRepo(db).get(b)
    assert "UPSTREAM OUTPUT" not in (task_b.description or "")


@pytest.mark.asyncio
async def test_pipe_brief_context_target(client):
    db = await get_db()
    _, (a, b) = await _bundle_with_tasks(client)
    await _force_task(db, b, brief=_BRIEF)
    await client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": b, "kind": "pipe",
        "payload_map": {"source": "report", "target": "brief_context"},
    })
    await _force_task(db, a, status="done", report=_REPORT)
    await _link_ctl(db).reconcile()

    task_b = await TaskRepo(db).get(b)
    assert "UPSTREAM OUTPUT" in task_b.brief["context"]
    assert "UPSTREAM OUTPUT" not in (task_b.description or "")


# ---------------------------------------------------------------------------
# Controller: subagent up-flow (Report back onto the delegator's tape)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subagent_report_flows_back_to_parent_tape(client):
    db = await get_db()
    _, (a, _) = await _bundle_with_tasks(client)
    made = await client.post(f"/api/v1/tasks/{a}/links", json={
        "kind": "subagent",
        "new_task": {"title": "child", "brief": _BRIEF},
    })
    child_id = made.json()["to_task"]["id"]
    # Child completes with a valid Report; orch accepts, then flows it up.
    await _force_task(db, child_id, status="done", report=_REPORT)

    await _reconcile(db)
    await _reconcile(db)  # acceptance pass + flow pass; converges

    cur = await db.execute(
        "SELECT body, payload, actor_type FROM events WHERE task_id = ? "
        "AND type = 'agent_reply' ORDER BY sequence DESC LIMIT 1",
        (a,),
    )
    row = await cur.fetchone()
    assert row is not None, "no projection event on parent tape"
    assert "SUBAGENT REPORT" in row["body"]
    payload = json.loads(row["payload"])
    assert payload["kind"] == "subagent_report"
    assert payload["from_task"] == child_id
    # Followup convention: actor_type=human so A's prompt-builder threads it.
    assert row["actor_type"] == "human"

    # Idempotent: link fired once.
    await _reconcile(db)
    cur = await db.execute(
        "SELECT COUNT(*) AS n FROM events WHERE task_id = ? AND "
        "payload LIKE '%subagent_report%'", (a,),
    )
    assert (await cur.fetchone())["n"] == 1


# ---------------------------------------------------------------------------
# Cascade (the parity matrix's `close`)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_parent_cascades_subagent_child_but_keeps_pipe_target(client):
    db = await get_db()
    _, (a, b) = await _bundle_with_tasks(client)
    # A → B pipe (independent downstream), A → C subagent (A-born child).
    await client.post(f"/api/v1/tasks/{a}/links", json={
        "to_task_id": b, "kind": "pipe"})
    made = await client.post(f"/api/v1/tasks/{a}/links", json={
        "kind": "subagent", "new_task": {"title": "child", "brief": _BRIEF},
    })
    c = made.json()["to_task"]["id"]

    r = await client.post(f"/api/v1/tasks/{a}/cancel")
    assert r.status_code == 200, r.text
    cascade = r.json()["cascade"]
    assert cascade["links_revoked"] == 2
    assert c in cascade["children_cancelled"]
    assert b in cascade["deps_unblocked"]

    repo = TaskRepo(db)
    assert (await repo.get(c)).status == "cancelled"      # A-born child dies
    task_b = await repo.get(b)
    assert task_b.status == "open"                        # pipe target lives
    assert a not in (task_b.depends_on_task_ids or [])    # and is unblocked

    cur = await db.execute(
        "SELECT COUNT(*) AS n FROM task_links WHERE revoked_at IS NULL",
    )
    assert (await cur.fetchone())["n"] == 0


@pytest.mark.asyncio
async def test_cascade_keeps_done_subagent_child(client):
    db = await get_db()
    _, (a, _) = await _bundle_with_tasks(client)
    made = await client.post(f"/api/v1/tasks/{a}/links", json={
        "kind": "subagent", "new_task": {"title": "done child"},
    })
    c = made.json()["to_task"]["id"]
    await _force_task(db, c, status="done", report=_REPORT)

    r = await client.post(f"/api/v1/tasks/{a}/cancel")
    assert r.status_code == 200
    assert c not in r.json()["cascade"]["children_cancelled"]
    assert (await TaskRepo(db).get(c)).status == "done"   # Report survives
