"""Tests for task progress endpoint."""
from __future__ import annotations

import pytest


async def _setup_task(client) -> tuple[str, str]:
    """Create a cookbook+recipe+bundle+task, return (task_id, bundle_id)."""
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "progress-test", "description": "x", "owner_id": "u1",
    })
    cb_id = cb.json()["cookbook"]["id"]

    rec = await client.post("/api/v1/recipes", json={
        "cookbook_id": cb_id, "name": "r1",
        "repo_url": "https://example.com/x.git",
        "created_by": "u1",
    })
    rec_id = rec.json()["recipe"]["id"]

    bundle = await client.post(f"/api/v1/recipes/{rec_id}/bundles", json={
        "prompt": "Do the thing", "requested_by": "u1",
    })
    bundle_id = bundle.json()["bundle"]["id"]

    task = await client.post(f"/api/v1/bundles/{bundle_id}/tasks", json={
        "title": "Run tests",
    })
    task_id = task.json()["task"]["id"]
    return task_id, bundle_id


@pytest.mark.asyncio
async def test_post_progress_step_total(client):
    task_id, _ = await _setup_task(client)

    resp = await client.post(f"/api/v1/tasks/{task_id}/progress", json={
        "summary": "Running tests",
        "step": 3,
        "total": 10,
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["task_id"] == task_id
    assert body["progress"]["summary"] == "Running tests"
    assert body["progress"]["step"] == 3
    assert body["progress"]["total"] == 10
    # percent derived when step+total given
    assert body["progress"]["percent"] == pytest.approx(0.3, abs=0.01)


@pytest.mark.asyncio
async def test_post_progress_percent_only(client):
    task_id, _ = await _setup_task(client)

    resp = await client.post(f"/api/v1/tasks/{task_id}/progress", json={
        "summary": "Compiling",
        "percent": 0.65,
    })
    assert resp.status_code == 200
    assert resp.json()["progress"]["percent"] == 0.65


@pytest.mark.asyncio
async def test_post_progress_summary_only(client):
    task_id, _ = await _setup_task(client)

    resp = await client.post(f"/api/v1/tasks/{task_id}/progress", json={
        "summary": "Thinking hard",
    })
    assert resp.status_code == 200
    assert resp.json()["progress"]["summary"] == "Thinking hard"
    assert resp.json()["progress"]["step"] is None
    assert resp.json()["progress"]["total"] is None


@pytest.mark.asyncio
async def test_progress_overwrites_previous(client):
    task_id, _ = await _setup_task(client)

    await client.post(f"/api/v1/tasks/{task_id}/progress", json={
        "summary": "Step 1", "step": 1, "total": 5,
    })
    resp = await client.post(f"/api/v1/tasks/{task_id}/progress", json={
        "summary": "Step 2", "step": 2, "total": 5,
    })
    assert resp.json()["progress"]["summary"] == "Step 2"
    assert resp.json()["progress"]["step"] == 2


@pytest.mark.asyncio
async def test_progress_for_unknown_task_returns_404(client):
    resp = await client.post("/api/v1/tasks/task_nonexistent/progress", json={
        "summary": "test",
    })
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_progress_validates_percent_range(client):
    task_id, _ = await _setup_task(client)

    # percent > 1.0 should be rejected
    resp = await client.post(f"/api/v1/tasks/{task_id}/progress", json={
        "summary": "x", "percent": 1.5,
    })
    assert resp.status_code == 422

    # negative percent rejected
    resp = await client.post(f"/api/v1/tasks/{task_id}/progress", json={
        "summary": "x", "percent": -0.1,
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_progress_included_in_task_get(client):
    task_id, _ = await _setup_task(client)

    await client.post(f"/api/v1/tasks/{task_id}/progress", json={
        "summary": "Halfway", "step": 5, "total": 10,
    })

    resp = await client.get(f"/api/v1/tasks/{task_id}")
    assert resp.status_code == 200
    task = resp.json()["task"]
    assert task.get("progress") is not None
    assert task["progress"]["summary"] == "Halfway"
    assert task["progress"]["step"] == 5
