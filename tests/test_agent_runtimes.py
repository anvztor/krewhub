"""Tests for agent_runtimes table + daemon health tracking."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


class TestRuntimeRegister:
    @pytest.mark.asyncio
    async def test_register_runtime(self, client):
        resp = await client.post("/api/v1/agents/runtime/register", json={
            "agent_id": "claude@alice",
            "account_id": "acc_1",
            "daemon_version": "0.2.0",
            "provider": "claude",
            "host_info": {"hostname": "laptop.local", "os": "macOS"},
        })
        assert resp.status_code == 200
        runtime = resp.json()["runtime"]
        assert runtime["id"].startswith("rt_")
        assert runtime["agent_id"] == "claude@alice"
        assert runtime["account_id"] == "acc_1"
        assert runtime["daemon_version"] == "0.2.0"
        assert runtime["provider"] == "claude"
        assert runtime["status"] == "online"

    @pytest.mark.asyncio
    async def test_register_requires_agent_id(self, client):
        resp = await client.post("/api/v1/agents/runtime/register", json={
            "account_id": "acc_1",
        })
        assert resp.status_code == 422


class TestRuntimeHeartbeat:
    @pytest.mark.asyncio
    async def test_heartbeat_updates_last_seen(self, client):
        reg = await client.post("/api/v1/agents/runtime/register", json={
            "agent_id": "agent1",
            "account_id": "acc_1",
            "daemon_version": "0.2.0",
            "provider": "claude",
        })
        rt_id = reg.json()["runtime"]["id"]
        first_seen = reg.json()["runtime"]["last_seen_at"]

        # Heartbeat
        resp = await client.post(f"/api/v1/agents/runtime/{rt_id}/heartbeat")
        assert resp.status_code == 200

        updated = resp.json()["runtime"]["last_seen_at"]
        assert updated >= first_seen
        assert resp.json()["runtime"]["status"] == "online"

    @pytest.mark.asyncio
    async def test_heartbeat_unknown_runtime(self, client):
        resp = await client.post("/api/v1/agents/runtime/rt_nope/heartbeat")
        assert resp.status_code == 404


class TestListRuntimes:
    @pytest.mark.asyncio
    async def test_list_runtimes_empty(self, client):
        resp = await client.get("/api/v1/agents/runtimes")
        assert resp.status_code == 200
        assert resp.json()["runtimes"] == []

    @pytest.mark.asyncio
    async def test_list_runtimes_after_register(self, client):
        await client.post("/api/v1/agents/runtime/register", json={
            "agent_id": "a1", "account_id": "acc_1", "daemon_version": "0.2", "provider": "claude",
        })
        await client.post("/api/v1/agents/runtime/register", json={
            "agent_id": "a2", "account_id": "acc_1", "daemon_version": "0.2", "provider": "codex",
        })

        resp = await client.get("/api/v1/agents/runtimes")
        assert resp.status_code == 200
        runtimes = resp.json()["runtimes"]
        assert len(runtimes) == 2
        providers = {r["provider"] for r in runtimes}
        assert providers == {"claude", "codex"}

    @pytest.mark.asyncio
    async def test_list_runtimes_filtered_by_account(self, client):
        await client.post("/api/v1/agents/runtime/register", json={
            "agent_id": "a1", "account_id": "acc_alice", "daemon_version": "0.2", "provider": "claude",
        })
        await client.post("/api/v1/agents/runtime/register", json={
            "agent_id": "a2", "account_id": "acc_bob", "daemon_version": "0.2", "provider": "claude",
        })

        resp = await client.get("/api/v1/agents/runtimes?account_id=acc_alice")
        runtimes = resp.json()["runtimes"]
        assert len(runtimes) == 1
        assert runtimes[0]["account_id"] == "acc_alice"


class TestStaleMarking:
    @pytest.mark.asyncio
    async def test_stale_runtime_marked_offline(self, client):
        """A runtime whose last_seen_at is > 60s ago should be offline."""
        reg = await client.post("/api/v1/agents/runtime/register", json={
            "agent_id": "a1", "account_id": "acc_1", "daemon_version": "0.2", "provider": "claude",
        })
        rt_id = reg.json()["runtime"]["id"]

        # Manually backdate last_seen_at via direct DB write
        from krewhub.db.connection import get_db
        db = await get_db()
        old = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        await db.execute(
            "UPDATE agent_runtimes SET last_seen_at = ? WHERE id = ?",
            (old, rt_id),
        )
        await db.commit()

        # Trigger sweep
        resp = await client.post("/api/v1/agents/runtime/sweep")
        assert resp.status_code == 200
        assert resp.json()["marked_offline"] >= 1

        # Verify
        lst = await client.get("/api/v1/agents/runtimes")
        rt = next(r for r in lst.json()["runtimes"] if r["id"] == rt_id)
        assert rt["status"] == "offline"
