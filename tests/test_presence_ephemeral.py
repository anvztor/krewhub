"""R1 (alert#34): agent heartbeats must not bloat the durable watch_log.

Presence is current-state telemetry — the agent_presence row is the source
of truth and the UI gets live updates via an in-memory ephemeral channel.
A durable watch_log entry is appended only on a status transition, not on
every heartbeat (heartbeats were 84% of watch_log volume).
"""
from __future__ import annotations

import pytest

from krewhub.db.connection import get_db
from krewhub.models import AgentPresence, AgentStatus, WatchEventType
from krewhub.watch.service import WatchService
from krewhub.watch.types import WatchOptions


async def _agent_watch_rows(db) -> int:
    cur = await db.execute(
        "SELECT COUNT(*) FROM watch_log WHERE resource_type = 'agent'"
    )
    return (await cur.fetchone())[0]


def _presence(status: AgentStatus = AgentStatus.ONLINE) -> AgentPresence:
    from datetime import datetime, timezone
    return AgentPresence(
        agent_id="agent_eph",
        cookbook_id="cb_eph",
        display_name="Eph",
        capabilities=["claim"],
        status=status,
        last_heartbeat_at=datetime.now(timezone.utc),
    )


class TestEphemeralNotify:
    @pytest.mark.asyncio
    async def test_notify_ephemeral_does_not_persist_but_notifies(self):
        db = await get_db()
        watch = WatchService(db)
        before = await _agent_watch_rows(db)

        queue = watch.subscribe(WatchOptions(resource_type="agent"))
        event = await watch.notify_ephemeral(
            "agent", "agent_eph", WatchEventType.MODIFIED, _presence(),
        )

        # Live subscriber received it...
        assert queue.get_nowait().resource_id == "agent_eph"
        assert event.channel  # a channel was derived
        # ...but nothing was written to the durable replay buffer.
        assert await _agent_watch_rows(db) == before

    @pytest.mark.asyncio
    async def test_ephemeral_seq_does_not_advance_cursor(self):
        db = await get_db()
        watch = WatchService(db)
        latest = await watch.latest_seq()
        event = await watch.notify_ephemeral(
            "agent", "agent_eph", WatchEventType.MODIFIED, _presence(),
        )
        # Carries the current max durable seq — never advances the cursor.
        assert event.seq == latest
        assert await watch.latest_seq() == latest


class TestHeartbeatPersistsOnlyOnTransition:
    async def _cookbook(self, client) -> str:
        resp = await client.post("/api/v1/cookbooks", json={
            "name": "eph-cb", "owner_id": "human_1",
        })
        return resp.json()["cookbook"]["id"]

    async def _beat(self, client, cookbook_id, *, task=None):
        return await client.post("/api/v1/agents/heartbeat", json={
            "agent_id": "agent_hb",
            "cookbook_id": cookbook_id,
            "display_name": "HB",
            "capabilities": ["claim"],
            **({"current_task_id": task} if task else {}),
        })

    @pytest.mark.asyncio
    async def test_repeat_heartbeats_same_status_do_not_grow_watch_log(self, client):
        db = await get_db()
        cookbook_id = await self._cookbook(client)

        await self._beat(client, cookbook_id)            # None -> online (transition)
        after_first = await _agent_watch_rows(db)
        assert after_first >= 1

        await self._beat(client, cookbook_id)            # online -> online (ephemeral)
        await self._beat(client, cookbook_id)            # online -> online (ephemeral)
        assert await _agent_watch_rows(db) == after_first

        # Status transition (online -> busy) persists exactly one more row.
        await self._beat(client, cookbook_id, task="task_1")
        assert await _agent_watch_rows(db) == after_first + 1

        # Busy -> busy is ephemeral again.
        await self._beat(client, cookbook_id, task="task_1")
        assert await _agent_watch_rows(db) == after_first + 1
