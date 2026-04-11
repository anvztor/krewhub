from __future__ import annotations

import json
from datetime import datetime

import aiosqlite

from krewhub.models import AgentPresence


class AgentRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def upsert_presence(self, presence: AgentPresence) -> AgentPresence:
        await self._db.execute(
            """INSERT INTO agent_presence
               (agent_id, cookbook_id, display_name, capabilities,
                max_concurrent_tasks, endpoint_url, status,
                last_heartbeat_at, current_task_id, resource_version)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
               ON CONFLICT(agent_id, cookbook_id) DO UPDATE SET
                display_name = excluded.display_name,
                capabilities = excluded.capabilities,
                max_concurrent_tasks = excluded.max_concurrent_tasks,
                endpoint_url = excluded.endpoint_url,
                status = excluded.status,
                last_heartbeat_at = excluded.last_heartbeat_at,
                current_task_id = excluded.current_task_id,
                resource_version = agent_presence.resource_version + 1""",
            (presence.agent_id, presence.cookbook_id, presence.display_name,
             json.dumps(presence.capabilities), presence.max_concurrent_tasks,
             presence.endpoint_url, presence.status,
             presence.last_heartbeat_at.isoformat(), presence.current_task_id),
        )
        await self._db.commit()
        return await self.get(presence.agent_id, presence.cookbook_id) or presence

    async def list_by_cookbook(self, cookbook_id: str) -> list[AgentPresence]:
        cursor = await self._db.execute(
            "SELECT * FROM agent_presence WHERE cookbook_id = ?",
            (cookbook_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_presence(r) for r in rows]

    async def list_by_recipe(self, recipe_id: str) -> list[AgentPresence]:
        """List agents available for a recipe via its cookbook."""
        cursor = await self._db.execute(
            """SELECT ap.* FROM agent_presence ap
               JOIN recipes r ON r.cookbook_id = ap.cookbook_id
               WHERE r.id = ?""",
            (recipe_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_presence(r) for r in rows]

    async def get(self, agent_id: str, cookbook_id: str) -> AgentPresence | None:
        cursor = await self._db.execute(
            "SELECT * FROM agent_presence WHERE agent_id = ? AND cookbook_id = ?",
            (agent_id, cookbook_id),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_presence(row)

    async def mark_offline_stale(self, cutoff: datetime) -> int:
        cursor = await self._db.execute(
            """UPDATE agent_presence
               SET status = 'offline',
                   current_task_id = NULL,
                   resource_version = resource_version + 1
               WHERE last_heartbeat_at < ? AND status != 'offline'""",
            (cutoff.isoformat(),),
        )
        await self._db.commit()
        return cursor.rowcount


def _row_to_presence(row: aiosqlite.Row) -> AgentPresence:
    # owner_username may not exist in old DBs
    try:
        owner = row["owner_username"]
    except (IndexError, KeyError):
        owner = None

    return AgentPresence(
        agent_id=row["agent_id"],
        cookbook_id=row["cookbook_id"],
        display_name=row["display_name"],
        capabilities=json.loads(row["capabilities"]),
        max_concurrent_tasks=row["max_concurrent_tasks"],
        endpoint_url=row["endpoint_url"],
        status=row["status"],
        last_heartbeat_at=datetime.fromisoformat(row["last_heartbeat_at"]),
        current_task_id=row["current_task_id"],
        resource_version=row["resource_version"],
        owner_username=owner,
    )
