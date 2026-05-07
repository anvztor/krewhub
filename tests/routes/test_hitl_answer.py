"""Tests for POST /api/v1/tasks/{id}/hitl/answer.

When a task lands in `blocked` status (CLI timeout, agent gave up,
etc.) the cookrew-beta SPA renders an HITL clickbar chip. Clicking
opens a textarea where the operator types guidance. The submit
hits this endpoint to:

  1. Append the answer to the task description so the agent sees it
     on the next attempt.
  2. Drop a HITL `prompt` event onto the recipe stream so the SPA's
     event feed surfaces what the operator said.
  3. Reset task from blocked → open + clear claim so
     TaskDispatchController re-dispatches.
"""
from __future__ import annotations

import pytest
from httpx import AsyncClient

from krewhub.db.connection import get_db


async def _seed_blocked_task(
    cookie_client: AsyncClient,
    *,
    blocked_reason: str = "execution_timeout",
) -> tuple[str, str]:
    """Create a recipe + bundle + blocked task. Returns (recipe_id, task_id)."""
    init = await cookie_client.post("/api/v1/me/init-workspace")
    recipe_id = init.json()["recipe"]["id"]
    bundle = await cookie_client.post(
        f"/api/v1/recipes/{recipe_id}/bundles",
        json={"prompt": "", "requested_by": "tester", "tasks": []},
    )
    bundle_id = bundle.json()["bundle"]["id"]
    # Direct DB insert of a blocked task; bypasses the bundle/runtime
    # binding requirement of /bundles/{id}/tasks since the test only
    # needs the row in `blocked` state.
    db = await get_db()
    task_id = "task_hitltest"
    await db.execute(
        """INSERT INTO tasks (
            id, bundle_id, title, description, status,
            depends_on_task_ids, claimed_by_agent_id, claimed_at,
            blocked_reason, resource_version, generation
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1)""",
        (
            task_id, bundle_id, "Implement feature", "initial spec",
            "blocked", "[]",
            "claude@krew", "2026-05-07T05:00:00+00:00", blocked_reason,
        ),
    )
    await db.commit()
    return recipe_id, task_id


@pytest.mark.asyncio
async def test_hitl_answer_resets_blocked_task_to_open(cookie_client: AsyncClient):
    _recipe, task_id = await _seed_blocked_task(cookie_client)

    r = await cookie_client.post(
        f"/api/v1/tasks/{task_id}/hitl/answer",
        json={"answer": "Increase the timeout to 5 minutes and retry."},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["task"]["status"] == "open"
    assert body["task"]["claimed_by_agent_id"] is None
    assert body["task"]["claimed_at"] is None
    assert body["task"]["blocked_reason"] is None
    # Description carries the operator's answer for the daemon to read.
    assert "OPERATOR" in body["task"]["description"]
    assert "Increase the timeout" in body["task"]["description"]


@pytest.mark.asyncio
async def test_hitl_answer_emits_prompt_event(cookie_client: AsyncClient):
    """The answer surfaces in the event feed as a HITL prompt event."""
    recipe_id, task_id = await _seed_blocked_task(cookie_client)

    answer = "Skip this step and move on."
    await cookie_client.post(
        f"/api/v1/tasks/{task_id}/hitl/answer",
        json={"answer": answer},
    )

    db = await get_db()
    cur = await db.execute(
        "SELECT type, body, payload FROM events "
        "WHERE task_id = ? AND type = 'prompt' ORDER BY created_at DESC LIMIT 1",
        (task_id,),
    )
    row = await cur.fetchone()
    assert row is not None, "expected a prompt event for the HITL answer"
    assert answer in row["body"]
    import json as _json
    assert _json.loads(row["payload"]).get("hitl") is True


@pytest.mark.asyncio
async def test_hitl_answer_rejects_non_blocked_task(cookie_client: AsyncClient):
    init = await cookie_client.post("/api/v1/me/init-workspace")
    recipe_id = init.json()["recipe"]["id"]
    bundle = await cookie_client.post(
        f"/api/v1/recipes/{recipe_id}/bundles",
        json={"prompt": "", "requested_by": "t", "tasks": []},
    )
    bundle_id = bundle.json()["bundle"]["id"]
    db = await get_db()
    await db.execute(
        """INSERT INTO tasks (id, bundle_id, title, status, depends_on_task_ids,
            resource_version, generation) VALUES (?, ?, ?, ?, '[]', 1, 1)""",
        ("task_open", bundle_id, "Open", "open"),
    )
    await db.commit()

    r = await cookie_client.post(
        "/api/v1/tasks/task_open/hitl/answer",
        json={"answer": "anything"},
    )
    assert r.status_code == 400
    assert "blocked" in r.json()["detail"]


@pytest.mark.asyncio
async def test_hitl_answer_rejects_empty(cookie_client: AsyncClient):
    _recipe, task_id = await _seed_blocked_task(cookie_client)
    r = await cookie_client.post(
        f"/api/v1/tasks/{task_id}/hitl/answer",
        json={"answer": "   "},
    )
    assert r.status_code == 400
    assert r.json()["detail"] == "empty_answer"


@pytest.mark.asyncio
async def test_hitl_answer_404_on_missing(cookie_client: AsyncClient):
    r = await cookie_client.post(
        "/api/v1/tasks/task_does_not_exist/hitl/answer",
        json={"answer": "hi"},
    )
    assert r.status_code == 404
