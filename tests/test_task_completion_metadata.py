"""Tests for task completion metadata (session_id, work_dir, artifacts)."""
from __future__ import annotations

import pytest


async def _setup_task(client) -> tuple[str, str]:
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "meta-test", "description": "x", "owner_id": "u1",
    })
    cb_id = cb.json()["cookbook"]["id"]
    rec = await client.post("/api/v1/recipes", json={
        "cookbook_id": cb_id, "name": "r",
        "repo_url": "https://example.com/x.git", "created_by": "u1",
    })
    rec_id = rec.json()["recipe"]["id"]
    bun = await client.post(f"/api/v1/recipes/{rec_id}/bundles", json={
        "prompt": "p", "requested_by": "u1",
    })
    bun_id = bun.json()["bundle"]["id"]
    task = await client.post(f"/api/v1/bundles/{bun_id}/tasks", json={"title": "t"})
    return bun_id, task.json()["task"]["id"]


@pytest.mark.asyncio
async def test_post_completion_metadata(client):
    _, task_id = await _setup_task(client)

    resp = await client.post(f"/api/v1/tasks/{task_id}/completion", json={
        "session_id": "sess_abc123",
        "work_dir": "/Users/alice/workspace/project",
        "artifacts": {
            "files_written": ["src/api.py", "tests/test_api.py"],
            "commits": ["abc1234: add api endpoint"],
            "pr_url": "https://github.com/x/y/pull/42",
        },
    })
    assert resp.status_code == 200
    data = resp.json()["task"]
    assert data["session_id"] == "sess_abc123"
    assert data["work_dir"] == "/Users/alice/workspace/project"
    assert "files_written" in data["artifacts"]
    assert len(data["artifacts"]["files_written"]) == 2


@pytest.mark.asyncio
async def test_completion_metadata_in_task_get(client):
    _, task_id = await _setup_task(client)

    await client.post(f"/api/v1/tasks/{task_id}/completion", json={
        "session_id": "sess_xyz",
        "work_dir": "/tmp/work",
        "artifacts": {"pr_url": "https://github.com/x/y/pull/1"},
    })

    resp = await client.get(f"/api/v1/tasks/{task_id}")
    task = resp.json()["task"]
    assert task["session_id"] == "sess_xyz"
    assert task["work_dir"] == "/tmp/work"
    assert task["artifacts"]["pr_url"] == "https://github.com/x/y/pull/1"


@pytest.mark.asyncio
async def test_completion_metadata_partial(client):
    """Only session_id is common; work_dir and artifacts optional."""
    _, task_id = await _setup_task(client)

    resp = await client.post(f"/api/v1/tasks/{task_id}/completion", json={
        "session_id": "sess_only",
    })
    assert resp.status_code == 200
    task = resp.json()["task"]
    assert task["session_id"] == "sess_only"
    assert task["work_dir"] is None
    assert task["artifacts"] == {}


@pytest.mark.asyncio
async def test_completion_metadata_unknown_task(client):
    resp = await client.post("/api/v1/tasks/task_nope/completion", json={
        "session_id": "x",
    })
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_completion_metadata_overwrites(client):
    _, task_id = await _setup_task(client)

    await client.post(f"/api/v1/tasks/{task_id}/completion", json={
        "session_id": "sess1", "work_dir": "/a",
    })
    await client.post(f"/api/v1/tasks/{task_id}/completion", json={
        "session_id": "sess2", "work_dir": "/b",
    })

    resp = await client.get(f"/api/v1/tasks/{task_id}")
    task = resp.json()["task"]
    assert task["session_id"] == "sess2"
    assert task["work_dir"] == "/b"
