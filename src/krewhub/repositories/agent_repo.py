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
               (agent_id, recipe_id, display_name, capabilities, status,
                last_heartbeat_at, current_task_id)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(agent_id, recipe_id) DO UPDATE SET
                display_name = excluded.display_name,
                capabilities = excluded.capabilities,
                status = excluded.status,
                last_heartbeat_at = excluded.last_heartbeat_at,
                current_task_id = excluded.current_task_id""",
            (presence.agent_id, presence.recipe_id, presence.display_name,
             json.dumps(presence.capabilities), presence.status,
             presence.last_heartbeat_at.isoformat(), presence.current_task_id),
        )
        await self._db.commit()
        return presence

    async def list_by_recipe(self, recipe_id: str) -> list[AgentPresence]:
        cursor = await self._db.execute(
            "SELECT * FROM agent_presence WHERE recipe_id = ?",
            (recipe_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_presence(r) for r in rows]

    async def get(self, agent_id: str, recipe_id: str) -> AgentPresence | None:
        cursor = await self._db.execute(
            "SELECT * FROM agent_presence WHERE agent_id = ? AND recipe_id = ?",
            (agent_id, recipe_id),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_presence(row)

    async def mark_offline_stale(self, cutoff: datetime) -> int:
        cursor = await self._db.execute(
            """UPDATE agent_presence SET status = 'offline', current_task_id = NULL
               WHERE last_heartbeat_at < ? AND status != 'offline'""",
            (cutoff.isoformat(),),
        )
        await self._db.commit()
        return cursor.rowcount


def _row_to_presence(row: aiosqlite.Row) -> AgentPresence:
    return AgentPresence(
        agent_id=row["agent_id"],
        recipe_id=row["recipe_id"],
        display_name=row["display_name"],
        capabilities=json.loads(row["capabilities"]),
        status=row["status"],
        last_heartbeat_at=datetime.fromisoformat(row["last_heartbeat_at"]),
        current_task_id=row["current_task_id"],
    )
