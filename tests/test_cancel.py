"""Tests for task/bundle cancellation propagation."""
from __future__ import annotations

import pytest


async def _setup_task(client, *, status: str = "open") -> tuple[str, str, str]:
    """Create cookbook + recipe + bundle + task. Return (recipe_id, bundle_id, task_id)."""
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "cancel-test", "description": "x", "owner_id": "u1",
    })
    cb_id = cb.json()["cookbook"]["id"]
    rec = await client.post("/api/v1/recipes", json={
        "cookbook_id": cb_id, "name": "r",
        "repo_url": "https://example.com/x.git", "created_by": "u1",
    })
    rec_id = rec.json()["recipe"]["id"]
    bun = await client.post(f"/api/v1/recipes/{rec_id}/bundles", json={
        "prompt": "do it", "requested_by": "u1",
    })
    bun_id = bun.json()["bundle"]["id"]
    task = await client.post(f"/api/v1/bundles/{bun_id}/tasks", json={
        "title": "run",
    })
    task_id = task.json()["task"]["id"]

    if status != "open":
        # transition through valid path
        if status in ("claimed", "working", "done", "blocked", "cancelled"):
            await client.post(f"/api/v1/tasks/{task_id}/claim", json={
                "agent_id": "agent1",
            })
    return rec_id, bun_id, task_id


class TestTaskCancel:
    @pytest.mark.asyncio
    async def test_cancel_open_task(self, client):
        _, _, task_id = await _setup_task(client)

        resp = await client.post(f"/api/v1/tasks/{task_id}/cancel")
        assert resp.status_code == 200
        assert resp.json()["task"]["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_cancel_claimed_task(self, client):
        _, _, task_id = await _setup_task(client, status="claimed")

        resp = await client.post(f"/api/v1/tasks/{task_id}/cancel")
        assert resp.status_code == 200
        assert resp.json()["task"]["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_cancel_already_done_task_rejected(self, client):
        _, _, task_id = await _setup_task(client, status="claimed")

        await client.patch(f"/api/v1/tasks/{task_id}/status", json={"status": "done"})

        resp = await client.post(f"/api/v1/tasks/{task_id}/cancel")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_task(self, client):
        resp = await client.post("/api/v1/tasks/task_nope/cancel")
        assert resp.status_code == 404


class TestCancelStatus:
    @pytest.mark.asyncio
    async def test_cancel_status_not_cancelled(self, client):
        _, _, task_id = await _setup_task(client)

        resp = await client.get(f"/api/v1/tasks/{task_id}/cancel-status")
        assert resp.status_code == 200
        assert resp.json()["cancelled"] is False
        assert resp.json()["task_id"] == task_id

    @pytest.mark.asyncio
    async def test_cancel_status_after_cancel(self, client):
        _, _, task_id = await _setup_task(client)

        await client.post(f"/api/v1/tasks/{task_id}/cancel")

        resp = await client.get(f"/api/v1/tasks/{task_id}/cancel-status")
        assert resp.status_code == 200
        assert resp.json()["cancelled"] is True

    @pytest.mark.asyncio
    async def test_cancel_status_via_bundle_cancel(self, client):
        """Cancelling the bundle should cascade to task → cancel-status returns true."""
        _, bundle_id, task_id = await _setup_task(client)

        await client.patch(f"/api/v1/bundles/{bundle_id}")

        resp = await client.get(f"/api/v1/tasks/{task_id}/cancel-status")
        assert resp.status_code == 200
        assert resp.json()["cancelled"] is True

    @pytest.mark.asyncio
    async def test_cancel_status_unknown_task(self, client):
        resp = await client.get("/api/v1/tasks/task_nope/cancel-status")
        assert resp.status_code == 404


class TestBundleCancelCascade:
    @pytest.mark.asyncio
    async def test_bundle_cancel_cancels_all_open_tasks(self, client):
        _, bundle_id, task_id = await _setup_task(client)

        # Add a second task
        task2 = await client.post(f"/api/v1/bundles/{bundle_id}/tasks", json={
            "title": "task2",
        })
        task2_id = task2.json()["task"]["id"]

        await client.patch(f"/api/v1/bundles/{bundle_id}")

        # Both tasks should be cancelled
        for tid in (task_id, task2_id):
            status = await client.get(f"/api/v1/tasks/{tid}/cancel-status")
            assert status.json()["cancelled"] is True, f"{tid} not cancelled"

    @pytest.mark.asyncio
    async def test_bundle_cancel_preserves_done_tasks(self, client):
        """Tasks already done should stay done, not be marked cancelled."""
        _, bundle_id, task_id = await _setup_task(client, status="claimed")

        await client.patch(f"/api/v1/tasks/{task_id}/status", json={"status": "done"})

        await client.patch(f"/api/v1/bundles/{bundle_id}")

        resp = await client.get(f"/api/v1/tasks/{task_id}")
        assert resp.json()["task"]["status"] == "done"
