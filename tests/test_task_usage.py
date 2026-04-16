"""Tests for task token usage tracking."""
from __future__ import annotations

import pytest


async def _setup_task(client) -> tuple[str, str, str]:
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "usage-test", "description": "x", "owner_id": "u1",
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
    return rec_id, bun_id, task.json()["task"]["id"]


class TestPostUsage:
    @pytest.mark.asyncio
    async def test_post_basic_usage(self, client):
        _, _, task_id = await _setup_task(client)

        resp = await client.post(f"/api/v1/tasks/{task_id}/usage", json={
            "input_tokens": 1500,
            "output_tokens": 800,
            "model": "claude-opus-4",
            "cost_usd": 0.042,
            "duration_ms": 3200,
        })
        assert resp.status_code == 200
        usage = resp.json()["usage"]
        assert usage["task_id"] == task_id
        assert usage["input_tokens"] == 1500
        assert usage["output_tokens"] == 800
        assert usage["model"] == "claude-opus-4"
        assert usage["cost_usd"] == 0.042
        assert usage["duration_ms"] == 3200

    @pytest.mark.asyncio
    async def test_post_partial_usage(self, client):
        """Only tokens required; cost/model/duration optional."""
        _, _, task_id = await _setup_task(client)

        resp = await client.post(f"/api/v1/tasks/{task_id}/usage", json={
            "input_tokens": 100,
            "output_tokens": 50,
        })
        assert resp.status_code == 200
        usage = resp.json()["usage"]
        assert usage["input_tokens"] == 100
        assert usage["cost_usd"] is None

    @pytest.mark.asyncio
    async def test_post_usage_unknown_task(self, client):
        resp = await client.post("/api/v1/tasks/task_nope/usage", json={
            "input_tokens": 1, "output_tokens": 1,
        })
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_multiple_usage_posts_accumulate(self, client):
        """Multiple POSTs for the same task accumulate — useful for multi-turn runs."""
        _, _, task_id = await _setup_task(client)

        await client.post(f"/api/v1/tasks/{task_id}/usage", json={
            "input_tokens": 100, "output_tokens": 50, "cost_usd": 0.01,
        })
        await client.post(f"/api/v1/tasks/{task_id}/usage", json={
            "input_tokens": 200, "output_tokens": 150, "cost_usd": 0.025,
        })

        resp = await client.get(f"/api/v1/tasks/{task_id}/usage")
        assert resp.status_code == 200
        rows = resp.json()["usage"]
        assert len(rows) == 2
        assert sum(r["input_tokens"] for r in rows) == 300
        assert sum(r["output_tokens"] for r in rows) == 200

    @pytest.mark.asyncio
    async def test_get_usage_includes_totals(self, client):
        _, _, task_id = await _setup_task(client)

        await client.post(f"/api/v1/tasks/{task_id}/usage", json={
            "input_tokens": 100, "output_tokens": 50, "cost_usd": 0.01,
        })
        await client.post(f"/api/v1/tasks/{task_id}/usage", json={
            "input_tokens": 200, "output_tokens": 150, "cost_usd": 0.02,
        })

        resp = await client.get(f"/api/v1/tasks/{task_id}/usage")
        data = resp.json()
        assert data["totals"]["input_tokens"] == 300
        assert data["totals"]["output_tokens"] == 200
        assert data["totals"]["cost_usd"] == pytest.approx(0.03)


class TestBundleUsageAggregate:
    @pytest.mark.asyncio
    async def test_bundle_usage_aggregates_across_tasks(self, client):
        _, bundle_id, task_id = await _setup_task(client)

        # Second task in same bundle
        task2 = await client.post(f"/api/v1/bundles/{bundle_id}/tasks", json={"title": "t2"})
        task2_id = task2.json()["task"]["id"]

        await client.post(f"/api/v1/tasks/{task_id}/usage", json={
            "input_tokens": 100, "output_tokens": 50, "cost_usd": 0.01,
        })
        await client.post(f"/api/v1/tasks/{task2_id}/usage", json={
            "input_tokens": 200, "output_tokens": 100, "cost_usd": 0.02,
        })

        resp = await client.get(f"/api/v1/bundles/{bundle_id}/usage")
        assert resp.status_code == 200
        data = resp.json()
        assert data["totals"]["input_tokens"] == 300
        assert data["totals"]["output_tokens"] == 150
        assert data["totals"]["cost_usd"] == pytest.approx(0.03)
