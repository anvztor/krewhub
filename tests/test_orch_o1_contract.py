"""Orch mode (O1) — contract layer.

Covers the four contract pieces added for orchestrated sub-tasks:
  1. Brief / Report JSON Schema — validated on task-create / completion,
     stored, surfaced; absent ⇒ legacy behavior (backward compat).
  2. Semantic event types — progress/blocker/needs_review/needs_human/log
     accepted by POST /tasks/{id}/events[:batch].
  3. needs_review state — a needs_review event parks the task at
     blocked_on_review; POST /tasks/{id}/review (approve/reject + audit).
  4. Migrations — the events.type / tasks.status CHECK rebuilds upgrade an
     existing DB in place without losing data.

`client` = X-API-Key (acc_legacy_apikey, owner-equivalent via the legacy
sentinel bypass). `cookie_client` = acc_test_cookie (a different account).
"""
from __future__ import annotations

import pytest


async def _setup_task(client) -> tuple[str, str, str]:
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "orch-cb", "owner_id": "acc_legacy_apikey",
    })
    cookbook_id = cb.json()["cookbook"]["id"]
    bun = await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
        "prompt": "orch bundle", "tasks": [{"title": "t"}],
    })
    bundle_id = bun.json()["bundle"]["id"]
    task_id = bun.json()["tasks"][0]["id"]
    return cookbook_id, bundle_id, task_id


_VALID_BRIEF = {
    "goal": "Ship the widget",
    "context": "repo X, branch main",
    "constraints": ["no force-push", "GitOps only"],
    "deliverable": "a merged PR",
    "report_points": ["PR url", "test results"],
    "pre_auth": ["run tests", "open PR"],
}

_VALID_REPORT = {
    "status": "done",
    "artifacts": ["dist/widget.js"],
    "prs": ["https://github.com/x/y/pull/1"],
    "blockers": [],
    "decisions_needed": [],
}


# ---------------------------------------------------------------------------
# 1. Brief / Report schema — validate, store, surface, backward-compat
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_task_with_valid_brief_stores_and_surfaces(client):
    _, bundle_id, _ = await _setup_task(client)
    r = await client.post(f"/api/v1/bundles/{bundle_id}/tasks", json={
        "title": "briefed task", "brief": _VALID_BRIEF,
    })
    assert r.status_code == 200, r.text
    task_id = r.json()["task"]["id"]

    got = await client.get(f"/api/v1/tasks/{task_id}")
    assert got.status_code == 200
    assert got.json()["task"]["brief"]["goal"] == "Ship the widget"
    assert got.json()["task"]["brief"]["constraints"] == ["no force-push", "GitOps only"]


@pytest.mark.asyncio
async def test_add_task_with_invalid_brief_rejected(client):
    """A brief missing the required `goal` is a 422 (contract enforced)."""
    _, bundle_id, _ = await _setup_task(client)
    bad = {k: v for k, v in _VALID_BRIEF.items() if k != "goal"}
    r = await client.post(f"/api/v1/bundles/{bundle_id}/tasks", json={
        "title": "bad brief", "brief": bad,
    })
    assert r.status_code == 422, r.text


@pytest.mark.asyncio
async def test_add_task_without_brief_is_backward_compatible(client):
    _, bundle_id, _ = await _setup_task(client)
    r = await client.post(f"/api/v1/bundles/{bundle_id}/tasks", json={
        "title": "no brief",
    })
    assert r.status_code == 200, r.text
    task_id = r.json()["task"]["id"]
    got = await client.get(f"/api/v1/tasks/{task_id}")
    assert got.json()["task"]["brief"] is None


@pytest.mark.asyncio
async def test_completion_with_valid_report_stored(client):
    _, _, task_id = await _setup_task(client)
    r = await client.post(f"/api/v1/tasks/{task_id}/completion", json={
        "session_id": "s1", "report": _VALID_REPORT,
    })
    assert r.status_code == 200, r.text
    got = await client.get(f"/api/v1/tasks/{task_id}")
    assert got.json()["task"]["report"]["status"] == "done"
    assert got.json()["task"]["report"]["prs"] == ["https://github.com/x/y/pull/1"]


@pytest.mark.asyncio
async def test_completion_with_invalid_report_rejected(client):
    _, _, task_id = await _setup_task(client)
    r = await client.post(f"/api/v1/tasks/{task_id}/completion", json={
        "report": {"artifacts": []},  # missing required `status`
    })
    assert r.status_code == 422, r.text


@pytest.mark.asyncio
async def test_completion_without_report_is_backward_compatible(client):
    _, _, task_id = await _setup_task(client)
    r = await client.post(f"/api/v1/tasks/{task_id}/completion", json={
        "session_id": "s1", "work_dir": "/tmp",
    })
    assert r.status_code == 200, r.text
    got = await client.get(f"/api/v1/tasks/{task_id}")
    assert got.json()["task"]["report"] is None


# ---------------------------------------------------------------------------
# 2. Semantic event types accepted by the events route
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("etype", ["progress", "blocker", "needs_human", "log", "milestone"])
async def test_new_event_types_accepted(client, etype):
    _, _, task_id = await _setup_task(client)
    r = await client.post(f"/api/v1/tasks/{task_id}/events", json={
        "type": etype, "actor_id": "a", "actor_type": "agent", "body": etype,
    })
    assert r.status_code == 200, f"{etype}: {r.text}"


@pytest.mark.asyncio
async def test_events_batch_accepts_new_types(client):
    _, _, task_id = await _setup_task(client)
    r = await client.post(f"/api/v1/tasks/{task_id}/events:batch", json={
        "events": [
            {"type": "log", "actor_id": "a", "actor_type": "agent", "body": "x"},
            {"type": "progress", "actor_id": "a", "actor_type": "agent", "body": "y"},
        ],
    })
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 2


# ---------------------------------------------------------------------------
# 3. needs_review state machine + review gate
# ---------------------------------------------------------------------------


async def _park_for_review(client, task_id: str) -> None:
    r = await client.post(f"/api/v1/tasks/{task_id}/events", json={
        "type": "needs_review", "actor_id": "a", "actor_type": "agent",
        "body": "please review my PR",
    })
    assert r.status_code == 200, r.text


@pytest.mark.asyncio
async def test_needs_review_event_parks_task(client):
    _, _, task_id = await _setup_task(client)
    await _park_for_review(client, task_id)
    got = await client.get(f"/api/v1/tasks/{task_id}")
    assert got.json()["task"]["status"] == "blocked_on_review"


@pytest.mark.asyncio
async def test_review_approve_resumes_and_audits(client, test_db):
    _, _, task_id = await _setup_task(client)
    await _park_for_review(client, task_id)

    r = await client.post(f"/api/v1/tasks/{task_id}/review", json={
        "action": "approve", "diff_summary": "looks good, 3 files",
    })
    assert r.status_code == 200, r.text
    assert r.json()["task"]["status"] == "working"
    assert r.json()["review"]["action"] == "approve"
    assert r.json()["review"]["decided_by"] == "acc_legacy_apikey"

    # Audit row persisted.
    cur = await test_db.execute(
        "SELECT action, decided_by, diff_summary FROM task_reviews WHERE task_id = ?",
        (task_id,),
    )
    row = await cur.fetchone()
    assert row is not None
    assert row["action"] == "approve"
    assert row["decided_by"] == "acc_legacy_apikey"
    assert row["diff_summary"] == "looks good, 3 files"


@pytest.mark.asyncio
async def test_review_reject_carries_reason_into_description(client):
    _, _, task_id = await _setup_task(client)
    await _park_for_review(client, task_id)

    r = await client.post(f"/api/v1/tasks/{task_id}/review", json={
        "action": "reject", "reason": "tests are failing, fix and resubmit",
    })
    assert r.status_code == 200, r.text
    assert r.json()["task"]["status"] == "working"
    assert "REVIEW REJECTED" in (r.json()["task"]["description"] or "")
    assert "tests are failing" in (r.json()["task"]["description"] or "")


@pytest.mark.asyncio
async def test_review_invalid_action_rejected(client):
    _, _, task_id = await _setup_task(client)
    await _park_for_review(client, task_id)
    r = await client.post(f"/api/v1/tasks/{task_id}/review", json={
        "action": "maybe",
    })
    assert r.status_code == 400, r.text


@pytest.mark.asyncio
async def test_review_on_non_parked_task_rejected(client):
    """A task that isn't blocked_on_review can't be reviewed → 400."""
    _, _, task_id = await _setup_task(client)  # status 'open'
    r = await client.post(f"/api/v1/tasks/{task_id}/review", json={
        "action": "approve",
    })
    assert r.status_code == 400, r.text


@pytest.mark.asyncio
async def test_review_denied_for_non_owner(client, cookie_client):
    """Review gate is owner-only — a worker / other account can't self-approve."""
    _, _, task_id = await _setup_task(client)  # bundle owned by acc_legacy_apikey
    await _park_for_review(client, task_id)

    r = await cookie_client.post(f"/api/v1/tasks/{task_id}/review", json={
        "action": "approve",
    })
    assert r.status_code == 403, r.text


# ---------------------------------------------------------------------------
# 4. Migration rebuilds upgrade an existing DB in place (no data loss)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_migration_widens_tasks_status_check_preserving_rows():
    import aiosqlite
    from krewhub.db.migrations import _migrate_tasks_add_blocked_on_review_status

    async with aiosqlite.connect(":memory:") as db:
        db.row_factory = aiosqlite.Row
        # Parent table for the rebuilt tasks_new FK (bundle_id REFERENCES
        # bundles); the migration re-enables foreign_keys at the end.
        await db.executescript(
            "CREATE TABLE bundles (id TEXT PRIMARY KEY);"
            "INSERT INTO bundles (id) VALUES ('b1');"
        )
        # Old-shape tasks table: status CHECK WITHOUT blocked_on_review,
        # and WITHOUT the O1 brief/report columns.
        await db.executescript(
            """
            CREATE TABLE tasks (
                id TEXT PRIMARY KEY,
                bundle_id TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                status TEXT NOT NULL DEFAULT 'open'
                    CHECK(status IN ('open','claimed','working','done','blocked','cancelled')),
                depends_on_task_ids TEXT NOT NULL DEFAULT '[]',
                assigned_agent_id TEXT, claimed_by_agent_id TEXT, claimed_at TEXT,
                completed_at TEXT, blocked_reason TEXT, graph_node_id TEXT,
                resource_version INTEGER NOT NULL DEFAULT 1,
                generation INTEGER NOT NULL DEFAULT 1,
                progress_json TEXT, session_id TEXT, work_dir TEXT, artifacts_json TEXT,
                session_token TEXT, assigned_runtime_id TEXT, sandbox_id TEXT,
                updated_at TEXT
            );
            INSERT INTO tasks (id, bundle_id, title, status, depends_on_task_ids)
                VALUES ('t_keep', 'b1', 'survivor', 'working', '[]');
            """
        )
        # Pre-req for the rebuild: the O1 additive columns exist first.
        await db.execute("ALTER TABLE tasks ADD COLUMN brief_json TEXT")
        await db.execute("ALTER TABLE tasks ADD COLUMN report_json TEXT")
        await db.commit()

        await _migrate_tasks_add_blocked_on_review_status(db)

        sql = (await (await db.execute(
            "SELECT sql FROM sqlite_master WHERE name='tasks'")).fetchone())["sql"]
        assert "blocked_on_review" in sql
        # Row preserved.
        row = await (await db.execute(
            "SELECT title, status FROM tasks WHERE id='t_keep'")).fetchone()
        assert row["title"] == "survivor" and row["status"] == "working"
        # New status value now insertable.
        await db.execute(
            "INSERT INTO tasks (id, bundle_id, title, status, depends_on_task_ids) "
            "VALUES ('t_new', 'b1', 'parked', 'blocked_on_review', '[]')"
        )
        await db.commit()

        # Idempotent: a second run is a no-op (guard short-circuits).
        await _migrate_tasks_add_blocked_on_review_status(db)


@pytest.mark.asyncio
async def test_migration_widens_events_type_check_preserving_rows():
    import aiosqlite
    from krewhub.db.migrations import _migrate_events_add_orch_types

    async with aiosqlite.connect(":memory:") as db:
        db.row_factory = aiosqlite.Row
        # Parent tables for the rebuilt events_new FKs (cookbook_id /
        # bundle_id); the migration re-enables foreign_keys at the end.
        await db.executescript(
            "CREATE TABLE cookbooks (id TEXT PRIMARY KEY);"
            "CREATE TABLE bundles (id TEXT PRIMARY KEY);"
        )
        # Old-shape events table: type CHECK WITHOUT the orch semantic types.
        await db.executescript(
            """
            CREATE TABLE events (
                id TEXT PRIMARY KEY,
                cookbook_id TEXT,
                bundle_id TEXT,
                task_id TEXT,
                type TEXT NOT NULL
                    CHECK(type IN ('prompt','plan','task_claimed','task_working',
                        'milestone','fact_added','code_pushed',
                        'bundle_closed','bundle_reopened',
                        'session_start','session_end','tool_use','tool_result',
                        'agent_reply','thinking')),
                actor_id TEXT NOT NULL,
                actor_type TEXT NOT NULL CHECK(actor_type IN ('human','agent','system','hook')),
                body TEXT NOT NULL DEFAULT '',
                payload TEXT NOT NULL DEFAULT '{}',
                sequence INTEGER NOT NULL DEFAULT 0,
                facts TEXT NOT NULL DEFAULT '[]',
                code_refs TEXT NOT NULL DEFAULT '[]',
                visibility TEXT NOT NULL DEFAULT 'system',
                created_at TEXT NOT NULL,
                expires_at TEXT
            );
            INSERT INTO events (id, type, actor_id, actor_type, created_at)
                VALUES ('e_keep', 'milestone', 'a', 'agent', 't0');
            """
        )
        await db.commit()

        await _migrate_events_add_orch_types(db)

        sql = (await (await db.execute(
            "SELECT sql FROM sqlite_master WHERE name='events'")).fetchone())["sql"]
        assert "needs_review" in sql
        row = await (await db.execute(
            "SELECT type FROM events WHERE id='e_keep'")).fetchone()
        assert row["type"] == "milestone"
        # New type now insertable.
        await db.execute(
            "INSERT INTO events (id, type, actor_id, actor_type, created_at) "
            "VALUES ('e_new', 'needs_review', 'a', 'agent', 't1')"
        )
        await db.commit()

        # Idempotent.
        await _migrate_events_add_orch_types(db)
