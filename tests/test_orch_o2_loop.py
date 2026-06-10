"""Orch mode (O2) — minimal orchestration loop (OrchController).

Acceptance (spec §4.4 O2): killing the worker self-heals — the task is
re-dispatched with the Brief replayed. Plus: completion → Report validation
→ accept/retire; invalid Report → needs_human escalation; respawn cap →
blocker + parked BLOCKED; legacy (brief-less) tasks untouched; event-driven
fast path reacts without waiting for the reconcile tick.

Tests drive the controller's reconcile() directly against the in-memory DB
(the same way controller tests work elsewhere in the suite), plus one
started-controller test for the watch fast path.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from krewhub.controllers.orch_controller import OrchController
from krewhub.db.connection import get_db
from krewhub.models import WatchEventType
from krewhub.repositories.task_repo import TaskRepo
from krewhub.watch.globals import get_watch_service

_BRIEF = json.dumps({
    "goal": "ship it", "context": "", "constraints": [],
    "deliverable": "PR", "report_points": [],
})
_VALID_REPORT = json.dumps({
    "status": "done", "artifacts": [], "prs": ["https://x/pr/1"],
    "blockers": [], "decisions_needed": [],
})

_NOW = lambda: datetime.now(timezone.utc)  # noqa: E731


async def _seed(
    db,
    *,
    task_status: str = "working",
    brief: str | None = _BRIEF,
    report: str | None = None,
    orch: str | None = None,
    runtime_last_seen: datetime | None = None,
    runtime_status: str = "online",
    with_runtime: bool = True,
    suffix: str = "1",
) -> str:
    """Seed cookbook/bundle/task (+ optionally a runtime) and return task_id."""
    cb, bun, task, rt = f"cb_{suffix}", f"bun_{suffix}", f"task_{suffix}", f"rt_{suffix}"
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?,?,?,?)",
        (cb, "c", "acc_owner", _NOW().isoformat()),
    )
    await db.execute(
        "INSERT INTO bundles (id, cookbook_id, prompt, status, created_by, "
        "created_at, owner_account_id) VALUES (?,?,?,?,?,?,?)",
        (bun, cb, "p", "open", "acc_owner", _NOW().isoformat(), "acc_owner"),
    )
    if with_runtime:
        last_seen = (runtime_last_seen or _NOW()).isoformat()
        await db.execute(
            "INSERT INTO agent_runtimes (id, agent_id, account_id, host_info, "
            "status, last_seen_at, started_at) VALUES (?,?,?,?,?,?,?)",
            (rt, "agent_a", "acc_owner", "{}", runtime_status, last_seen, last_seen),
        )
    await db.execute(
        "INSERT INTO tasks (id, bundle_id, title, status, depends_on_task_ids, "
        "resource_version, generation, assigned_runtime_id, brief_json, "
        "report_json, orch_json, claimed_by_agent_id) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (task, bun, "T", task_status, "[]", 1, 1,
         rt if with_runtime else None, brief, report, orch,
         "agent_a" if task_status in ("claimed", "working") else None),
    )
    await db.commit()
    return task


def _controller(db, **kw) -> OrchController:
    return OrchController(
        db, get_watch_service(),
        interval=kw.pop("interval", 3600.0),  # tests drive reconcile directly
        liveness_timeout=kw.pop("liveness_timeout", 60.0),
        max_respawns=kw.pop("max_respawns", 3),
    )


async def _events_of(db, task_id: str) -> list[tuple[str, str]]:
    cur = await db.execute(
        "SELECT type, body FROM events WHERE task_id = ? ORDER BY sequence",
        (task_id,),
    )
    return [(r["type"], r["body"]) for r in await cur.fetchall()]


# ---------------------------------------------------------------------------
# Acceptance: kill worker → self-heal redispatch with Brief replay
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dead_worker_respawns_task_with_brief_replay(_setup_db):
    db = await get_db()
    stale = _NOW() - timedelta(seconds=300)  # "killed" daemon: 5min silent
    task_id = await _seed(db, task_status="working", runtime_last_seen=stale)

    await _controller(db).reconcile()

    task = await TaskRepo(db).get(task_id)
    # Self-healed: back to open, claim cleared → TaskDispatchController
    # re-pushes it on the next tick.
    assert task.status == "open"
    assert task.claimed_by_agent_id is None
    # Brief replay: the brief survives the respawn verbatim.
    assert task.brief is not None and task.brief["goal"] == "ship it"
    # Bookkeeping + narration.
    assert task.orch["respawns"] == 1
    types = [t for t, _ in await _events_of(db, task_id)]
    assert "log" in types, types


@pytest.mark.asyncio
async def test_offline_runtime_also_triggers_respawn(_setup_db):
    db = await get_db()
    task_id = await _seed(
        db, task_status="claimed",
        runtime_last_seen=_NOW(), runtime_status="offline",
    )
    await _controller(db).reconcile()
    task = await TaskRepo(db).get(task_id)
    assert task.status == "open"
    assert task.orch["respawns"] == 1


@pytest.mark.asyncio
async def test_alive_worker_not_respawned(_setup_db):
    db = await get_db()
    task_id = await _seed(db, task_status="working", runtime_last_seen=_NOW())
    await _controller(db).reconcile()
    task = await TaskRepo(db).get(task_id)
    assert task.status == "working"
    assert task.orch is None  # untouched


@pytest.mark.asyncio
async def test_respawn_cap_parks_task_with_blocker(_setup_db):
    db = await get_db()
    stale = _NOW() - timedelta(seconds=300)
    task_id = await _seed(
        db, task_status="working", runtime_last_seen=stale,
        orch=json.dumps({"respawns": 3}),  # already at the cap
    )
    ctl = _controller(db, max_respawns=3)
    await ctl.reconcile()

    task = await TaskRepo(db).get(task_id)
    assert task.status == "blocked"
    assert "respawn limit" in (task.blocked_reason or "")
    assert task.orch["halted"] is True
    types = [t for t, _ in await _events_of(db, task_id)]
    assert types.count("blocker") == 1

    # Idempotent: halted tasks are skipped — no duplicate blocker.
    await ctl.reconcile()
    types = [t for t, _ in await _events_of(db, task_id)]
    assert types.count("blocker") == 1


# ---------------------------------------------------------------------------
# completion → validate(Report) → retire | escalate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_done_with_valid_report_is_accepted_and_retired(_setup_db):
    db = await get_db()
    task_id = await _seed(db, task_status="done", report=_VALID_REPORT)
    ctl = _controller(db)
    await ctl.reconcile()

    task = await TaskRepo(db).get(task_id)
    assert task.orch["accepted_at"] is not None
    events = await _events_of(db, task_id)
    accepted = [(t, b) for t, b in events if t == "milestone" and "accepted" in b]
    assert len(accepted) == 1, events

    # Idempotent: accepted tasks are skipped — no duplicate milestone.
    await ctl.reconcile()
    events = await _events_of(db, task_id)
    accepted = [(t, b) for t, b in events if t == "milestone" and "accepted" in b]
    assert len(accepted) == 1


@pytest.mark.asyncio
async def test_done_without_report_escalates_needs_human_once(_setup_db):
    db = await get_db()
    task_id = await _seed(db, task_status="done", report=None)
    ctl = _controller(db)
    await ctl.reconcile()
    await ctl.reconcile()  # idempotency

    task = await TaskRepo(db).get(task_id)
    assert task.orch["report_invalid"] is True
    assert task.status == "done"  # left terminal; a human decides
    types = [t for t, _ in await _events_of(db, task_id)]
    assert types.count("needs_human") == 1


@pytest.mark.asyncio
async def test_done_with_schema_invalid_report_escalates(_setup_db):
    db = await get_db()
    task_id = await _seed(
        db, task_status="done",
        report=json.dumps({"artifacts": []}),  # missing required `status`
    )
    await _controller(db).reconcile()
    task = await TaskRepo(db).get(task_id)
    assert task.orch["report_invalid"] is True


# ---------------------------------------------------------------------------
# Scope guards
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_brief_less_tasks_are_never_touched(_setup_db):
    db = await get_db()
    stale = _NOW() - timedelta(seconds=300)
    task_id = await _seed(
        db, task_status="working", brief=None, runtime_last_seen=stale,
    )
    await _controller(db).reconcile()
    task = await TaskRepo(db).get(task_id)
    assert task.status == "working"  # legacy task: orch keeps hands off
    assert task.orch is None
    assert await _events_of(db, task_id) == []


@pytest.mark.asyncio
async def test_task_without_runtime_is_skipped(_setup_db):
    db = await get_db()
    task_id = await _seed(db, task_status="working", with_runtime=False)
    await _controller(db).reconcile()
    task = await TaskRepo(db).get(task_id)
    assert task.status == "working"  # legacy claim flow — not orch's beat


@pytest.mark.asyncio
async def test_cancelled_tasks_ignored(_setup_db):
    db = await get_db()
    task_id = await _seed(db, task_status="cancelled", report=None)
    await _controller(db).reconcile()
    task = await TaskRepo(db).get(task_id)
    assert task.orch is None


# ---------------------------------------------------------------------------
# Event-driven fast path (started controller; huge interval so only the
# watch subscription can have acted)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_watch_fast_path_accepts_report_without_tick(_setup_db):
    db = await get_db()
    watch = get_watch_service()
    task_id = await _seed(db, task_status="done", report=_VALID_REPORT)

    ctl = _controller(db, interval=3600.0)
    await ctl.start()
    try:
        # Give the start()-tick reconcile a moment, then verify; if the
        # initial tick already accepted it, that's fine — the point is no
        # *second* tick is needed. To isolate the watch path, reset the
        # bookkeeping and fire a watch event.
        await asyncio.sleep(0.05)
        await db.execute(
            "UPDATE tasks SET orch_json = NULL WHERE id = ?", (task_id,),
        )
        await db.commit()
        # Delete the prior acceptance narration so we assert on fresh state.
        await db.execute(
            "DELETE FROM events WHERE task_id = ? AND type = 'milestone'",
            (task_id,),
        )
        await db.commit()

        task = await TaskRepo(db).get(task_id)
        await watch.record_resource(
            "task", task_id, WatchEventType.MODIFIED, task,
        )
        # The watch consumer should reconcile this task well before any
        # 3600s tick.
        for _ in range(40):
            await asyncio.sleep(0.05)
            fresh = await TaskRepo(db).get(task_id)
            if fresh.orch and fresh.orch.get("accepted_at"):
                break
        fresh = await TaskRepo(db).get(task_id)
        assert fresh.orch and fresh.orch.get("accepted_at"), (
            "watch fast path did not accept the report"
        )
    finally:
        await ctl.stop()
