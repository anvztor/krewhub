"""Auth track A2 SSE event-kind contract tests.

We document the canonical event kinds that producers should emit:
    sandbox.attached, agent.output.line, task.completed

The post_task_event route stamps the event row with whatever kind the
producer supplies; this test guards against accidental regression of
the existing free-form contract.
"""
from __future__ import annotations

import pytest


async def _seed_minimal_task(db, task_id: str = "tev1") -> str:
    await db.execute(
        "INSERT OR IGNORE INTO cookbooks (id, name, owner_id, created_at) "
        "VALUES (?,?,?,?)",
        ("cb_ev", "x", "alice", "2026-01-01"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO bundles (id, cookbook_id, prompt, status, "
        "created_by, created_at) VALUES (?,?,?,?,?,?)",
        ("b_ev", "cb_ev", "p", "open", "alice", "2026-01-01"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO tasks (id, bundle_id, title, description, "
        "status, depends_on_task_ids, resource_version, generation) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (task_id, "b_ev", "x", "", "open", "[]", 1, 1),
    )
    await db.commit()
    return task_id


@pytest.mark.asyncio
async def test_post_event_accepts_agent_reply_kind(client):
    from krewhub.db.connection import get_db
    db = await get_db()
    await _seed_minimal_task(db, "tev1")
    r = await client.post(
        "/api/v1/tasks/tev1/events",
        json={
            "type": "agent_reply",
            "actor_id": "agent_a",
            "actor_type": "agent",
            "body": "hi",
        },
    )
    assert r.status_code == 200, r.text


@pytest.mark.asyncio
async def test_stream_route_returns_404_for_missing_task(client):
    """Verifies the /tasks/{id}/stream route is wired (returns 404, not 405)."""
    r = await client.get("/api/v1/tasks/does_not_exist/stream")
    assert r.status_code == 404
