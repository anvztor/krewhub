"""Non-blocking delegate — PR1 projection.

When POST /api/v1/invocations/:id/result fires on a task-scoped
invocation, the terminal envelope must be projected as a synthetic
`agent_reply` event (actor_type='human', payload.kind='delegate_answer')
onto the task's events tape so the brain's prompt builder
(`_build_prompt_with_context`) can thread the operator's answer as a
HUMAN turn on the next re-entry.

Covers:
- accept envelope → body == content text
- decline envelope → body summarizes the decline reason
- idempotent re-submit → no dupe events
- non-task-scoped invocation → no projection (legacy A2A flow stays clean)
"""
from __future__ import annotations

import asyncio
import json

import pytest

from krewhub.db.connection import get_db


async def _seed_task(db) -> tuple[str, str, str]:
    """Seed a cookbook + bundle + task; return (cookbook_id, bundle_id, task_id).

    Post step-(e) schema: recipes table is gone, bundles point at
    cookbooks directly.
    """
    import uuid
    cb_id = f"cb_{uuid.uuid4().hex[:8]}"
    bundle_id = f"bun_{uuid.uuid4().hex[:8]}"
    task_id = f"task_{uuid.uuid4().hex[:8]}"
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) "
        "VALUES (?, 'cb', 'dev-user-1', '2026-05-13T00:00:00')",
        (cb_id,),
    )
    await db.execute(
        "INSERT INTO bundles (id, cookbook_id, prompt, status, created_by, "
        "created_at, owner_account_id) "
        "VALUES (?, ?, 'p', 'open', 'dev-user-1', '2026-05-13T00:00:00', "
        "'dev-user-1')",
        (bundle_id, cb_id),
    )
    await db.execute(
        "INSERT INTO tasks (id, bundle_id, title, status, depends_on_task_ids, "
        "resource_version, generation) "
        "VALUES (?, ?, 't', 'working', '[]', 1, 1)",
        (task_id, bundle_id),
    )
    await db.commit()
    return cb_id, bundle_id, task_id


async def _wait_for_running(inv_client, inv_id: str) -> None:
    """The FakeHand starts in 'pending' then flips to 'running' once
    execute() awaits the cancel token. Give it a tick."""
    for _ in range(50):
        resp = await inv_client.get(f"/api/v1/invocations/{inv_id}")
        if resp.status_code == 200:
            inv = resp.json().get("invocation", resp.json())
            if inv.get("status") in ("running", "completed", "cancelled", "errored"):
                return
        await asyncio.sleep(0.02)


@pytest.mark.asyncio
async def test_accept_projects_content_as_human_turn(inv_client, _install_fake_hand):
    """Operator's `accept` envelope lands on the task's events tape as
    actor_type='human' + payload.kind='delegate_answer'."""
    import asyncio as _asyncio
    from tests.test_invocation_service import FakeHand
    from krewhub.services.invocation_service import InvocationService
    from krewhub.watch.globals import get_watch_service

    app, _ = _install_fake_hand
    db = await get_db()
    _, _, task_id = await _seed_task(db)

    # Use a blocking HumanHand so POST /result is what closes the inv.
    block = _asyncio.Event()
    blocking = FakeHand(target_type="human", block_until_event=block)
    app.state.invocations = InvocationService(
        db, hands={"human": blocking}, watch=get_watch_service(),
    )

    create = await inv_client.post("/api/v1/invocations", json={
        "target": "human",
        "input": "Which MBTI?",
        "task_id": task_id,
    })
    assert create.status_code == 200, create.text
    inv_id = create.json()["invocation_id"]
    await _wait_for_running(inv_client, inv_id)

    submit = await inv_client.post(
        f"/api/v1/invocations/{inv_id}/result",
        json={"action": "accept", "content": "INTJ"},
    )
    assert submit.status_code == 200, submit.text

    # Inspect the task tape — exactly one new human event with the
    # delegate_answer marker.
    cur = await db.execute(
        "SELECT type, actor_type, body, payload FROM events "
        "WHERE task_id = ? AND actor_type = 'human'",
        (task_id,),
    )
    rows = await cur.fetchall()
    assert len(rows) == 1, f"expected 1 projected event, got {len(rows)}"
    etype, actor_type, body, payload_json = rows[0]
    assert etype == "agent_reply"
    assert actor_type == "human"
    assert body == "INTJ"
    payload = json.loads(payload_json)
    assert payload["kind"] == "delegate_answer"
    assert payload["invocation_id"] == inv_id
    assert payload["action"] == "accept"


@pytest.mark.asyncio
async def test_decline_projects_reason(inv_client, _install_fake_hand):
    """Operator's `decline` envelope projects a `[decline] reason` body
    so the brain sees *what happened*, not silence."""
    import asyncio as _asyncio
    from tests.test_invocation_service import FakeHand
    from krewhub.services.invocation_service import InvocationService
    from krewhub.watch.globals import get_watch_service

    app, _ = _install_fake_hand
    db = await get_db()
    _, _, task_id = await _seed_task(db)

    block = _asyncio.Event()
    blocking = FakeHand(target_type="human", block_until_event=block)
    app.state.invocations = InvocationService(
        db, hands={"human": blocking}, watch=get_watch_service(),
    )

    create = await inv_client.post("/api/v1/invocations", json={
        "target": "human",
        "input": "approve?",
        "task_id": task_id,
    })
    inv_id = create.json()["invocation_id"]
    await _wait_for_running(inv_client, inv_id)

    submit = await inv_client.post(
        f"/api/v1/invocations/{inv_id}/result",
        json={"action": "decline", "reason": "not now"},
    )
    assert submit.status_code == 200, submit.text

    cur = await db.execute(
        "SELECT body, payload FROM events "
        "WHERE task_id = ? AND actor_type = 'human'",
        (task_id,),
    )
    rows = await cur.fetchall()
    assert len(rows) == 1
    body, payload_json = rows[0]
    assert "decline" in body.lower()
    assert "not now" in body
    payload = json.loads(payload_json)
    assert payload["action"] == "decline"


@pytest.mark.asyncio
async def test_no_task_id_means_no_projection(inv_client, _install_fake_hand):
    """Legacy A2A invocations without task_id must NOT project anything —
    they're recipe-scoped, not task-scoped, and stuffing them onto a
    random task tape would be wrong."""
    import asyncio as _asyncio
    from tests.test_invocation_service import FakeHand
    from krewhub.services.invocation_service import InvocationService
    from krewhub.watch.globals import get_watch_service

    app, _ = _install_fake_hand
    db = await get_db()
    _, _, task_id = await _seed_task(db)

    block = _asyncio.Event()
    blocking = FakeHand(target_type="human", block_until_event=block)
    app.state.invocations = InvocationService(
        db, hands={"human": blocking}, watch=get_watch_service(),
    )

    # No task_id in body — invocation is recipe-scoped
    create = await inv_client.post("/api/v1/invocations", json={
        "target": "human",
        "input": "free-standing question",
    })
    inv_id = create.json()["invocation_id"]
    await _wait_for_running(inv_client, inv_id)

    submit = await inv_client.post(
        f"/api/v1/invocations/{inv_id}/result",
        json={"action": "accept", "content": "ok"},
    )
    assert submit.status_code == 200, submit.text

    cur = await db.execute(
        "SELECT COUNT(*) FROM events WHERE actor_type = 'human' AND task_id = ?",
        (task_id,),
    )
    (count,) = await cur.fetchone()
    assert count == 0, "non-task-scoped invocation should not project to any task"
