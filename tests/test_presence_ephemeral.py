"""R1 (alert#34): agent heartbeats must not bloat the durable watch_log.

Presence is current-state telemetry — the agent_presence row is the source
of truth and the UI gets live updates via an in-memory ephemeral channel.
A durable watch_log entry is appended only on a status transition, not on
every heartbeat (heartbeats were 84% of watch_log volume).
"""

from __future__ import annotations

import pytest

from krewhub.db.connection import get_db
from krewhub.models import WatchEventType
from krewhub.watch.service import WatchService


async def _agent_watch_rows(db) -> int:
    cur = await db.execute(
        "SELECT COUNT(*) FROM watch_log WHERE resource_type = 'agent'"
    )
    return (await cur.fetchone())[0]


class TestEphemeralNotify:
    @pytest.mark.asyncio
    async def test_notify_ephemeral_does_not_persist_but_notifies(self):
        db = await get_db()
        watch = WatchService(db)
        before_seq = await watch.latest_seq()
        q = watch.subscribe()

        event = await watch.notify_ephemeral(
            "agent", "agent_x", WatchEventType.MODIFIED,
            {"resource_version": 7, "agent_id": "agent_x"},
        )

        # Subscriber received it live...
        assert q.get_nowait().resource_id == "agent_x"
        assert event.resource_version == 7
        # ...but nothing was persisted: seq unchanged, no watch_log row.
        assert await watch.latest_seq() == before_seq
        assert await _agent_watch_rows(db) == 0


class TestHeartbeatPersistence:
    async def _heartbeat(self, client, cookbook_id, *, agent="agent_a", task=None):
        body = {
            "agent_id": agent,
            "cookbook_id": cookbook_id,
            "display_name": "A",
            "capabilities": ["claim"],
        }
        if task:
            body["current_task_id"] = task
        return await client.post("/api/v1/agents/heartbeat", json=body)

    @pytest.mark.asyncio
    async def test_repeated_same_status_heartbeat_does_not_grow_watch_log(self, client):
        resp = await client.post("/api/v1/cookbooks",
                                 json={"name": "cb", "owner_id": "human_1"})
        cookbook_id = resp.json()["cookbook"]["id"]
        db = await get_db()

        # First heartbeat: None -> online is a transition, persists 1 row.
        await self._heartbeat(client, cookbook_id)
        after_first = await _agent_watch_rows(db)
        assert after_first == 1

        # Three more identical heartbeats: no status change -> ephemeral only.
        for _ in range(3):
            await self._heartbeat(client, cookbook_id)
        assert await _agent_watch_rows(db) == after_first, (
            "same-status heartbeats must not append to the durable watch_log"
        )

    @pytest.mark.asyncio
    async def test_status_transition_persists(self, client):
        resp = await client.post("/api/v1/cookbooks",
                                 json={"name": "cb2", "owner_id": "human_1"})
        cookbook_id = resp.json()["cookbook"]["id"]
        db = await get_db()

        await self._heartbeat(client, cookbook_id)            # -> online (persist)
        await self._heartbeat(client, cookbook_id)            # online (ephemeral)
        baseline = await _agent_watch_rows(db)
        await self._heartbeat(client, cookbook_id, task="t1")  # online -> busy (persist)
        assert await _agent_watch_rows(db) == baseline + 1
