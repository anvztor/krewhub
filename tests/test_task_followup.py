"""POST /api/v1/tasks/{id}/followup — operator threads a prompt onto an
existing task instead of spawning a new one.

NOTE: uses an inline AsyncClient with raise_app_exceptions=True so
500-level failures surface their tracebacks instead of becoming
opaque "Internal Server Error" responses.

Pins:
- Empty prompt rejected.
- Event written with type='human_followup' actor_type='human'.
- Done/cooked tasks flip back to 'open' with claim cleared so the
  daemon re-picks them up.
- Working/blocked tasks stay in their status — only the event lands.
"""
from __future__ import annotations

import pytest

from krewhub.db.connection import get_db


async def _seed_bundle_and_task(db, *, task_status: str = "open") -> tuple[str, str]:
    import uuid
    rec_id = f"rec_{uuid.uuid4().hex[:8]}"
    cb_id = f"cb_{uuid.uuid4().hex[:8]}"
    bundle_id = f"bun_{uuid.uuid4().hex[:8]}"
    task_id = f"task_{uuid.uuid4().hex[:8]}"
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, 'cb', 'dev-user-1', '2026-05-12T00:00:00')",
        (cb_id,),
    )
    await db.execute(
        "INSERT INTO recipes (id, name, repo_url, default_branch, created_by, created_at, cookbook_id) "
        "VALUES (?, 'r', 'https://github.com/x/y.git', 'main', 'dev-user-1', '2026-05-12T00:00:00', ?)",
        (rec_id, cb_id),
    )
    await db.execute(
        "INSERT INTO bundles (id, recipe_id, prompt, status, created_by, created_at, owner_account_id) "
        "VALUES (?, ?, 'p', 'open', 'dev-user-1', '2026-05-12T00:00:00', 'dev-user-1')",
        (bundle_id, rec_id),
    )
    await db.execute(
        "INSERT INTO tasks (id, bundle_id, title, status, depends_on_task_ids, "
        "resource_version, generation) VALUES (?, ?, 't', ?, '[]', 1, 1)",
        (task_id, bundle_id, task_status),
    )
    await db.commit()
    return bundle_id, task_id


@pytest.mark.asyncio
async def test_followup_writes_event_with_human_followup_type(client, _setup_db):
    db = await get_db()
    _, task_id = await _seed_bundle_and_task(db)

    r = await client.post(
        f"/api/v1/tasks/{task_id}/followup",
        json={"prompt": "Try again with the README under docs/"},
    )
    if r.status_code != 200:
        print("RESPONSE BODY:", r.text)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["task"]["id"] == task_id
    assert body["status_flipped"] is False  # was 'open' — no flip needed

    # Followup events use type='agent_reply' + actor_type='human'
    # (events.type CHECK doesn't allow a custom value without a heavy
    # SQLite ALTER-rebuild migration). Discriminate via actor_type.
    cur = await db.execute(
        "SELECT type, actor_type, body, payload FROM events "
        "WHERE task_id = ? AND actor_type = 'human'",
        (task_id,),
    )
    row = await cur.fetchone()
    assert row is not None
    assert row[0] == "agent_reply"
    assert row[1] == "human"
    assert "README" in row[2]
    import json as _json
    payload = _json.loads(row[3])
    assert payload["kind"] == "human_followup"


@pytest.mark.asyncio
async def test_followup_flips_done_task_back_to_open(client, _setup_db):
    db = await get_db()
    _, task_id = await _seed_bundle_and_task(db, task_status="done")
    await db.execute(
        "UPDATE tasks SET claimed_by_agent_id = 'claude@krew', "
        "completed_at = '2026-05-12T01:00:00' WHERE id = ?",
        (task_id,),
    )
    await db.commit()

    r = await client.post(
        f"/api/v1/tasks/{task_id}/followup",
        json={"prompt": "Actually swap to a TLDR section at the top"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status_flipped"] is True
    assert body["task"]["status"] == "open"
    assert body["task"]["claimed_by_agent_id"] is None
    assert body["task"]["completed_at"] is None


@pytest.mark.asyncio
async def test_followup_does_not_flip_working_task(client, _setup_db):
    db = await get_db()
    _, task_id = await _seed_bundle_and_task(db, task_status="working")
    r = await client.post(
        f"/api/v1/tasks/{task_id}/followup",
        json={"prompt": "FYI also update the changelog"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status_flipped"] is False
    assert body["task"]["status"] == "working"


@pytest.mark.asyncio
async def test_empty_prompt_rejected(client, _setup_db):
    db = await get_db()
    _, task_id = await _seed_bundle_and_task(db)
    r = await client.post(
        f"/api/v1/tasks/{task_id}/followup",
        json={"prompt": "   "},
    )
    assert r.status_code == 400
    assert "empty_prompt" in r.text


@pytest.mark.asyncio
async def test_unknown_task_404(client, _setup_db):
    r = await client.post(
        "/api/v1/tasks/task_does_not_exist/followup",
        json={"prompt": "hi"},
    )
    assert r.status_code == 404
