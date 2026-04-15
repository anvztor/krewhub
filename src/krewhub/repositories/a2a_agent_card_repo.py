from __future__ import annotations

import json
from datetime import datetime, timezone

import aiosqlite


class A2AAgentCardRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def get_card_json(self, owner: str, agent_name: str) -> str | None:
        cursor = await self._db.execute(
            "SELECT card_json FROM a2a_agent_cards WHERE owner = ? AND agent_name = ?",
            (owner, agent_name),
        )
        row = await cursor.fetchone()
        return row["card_json"] if row else None

    async def upsert(self, owner: str, agent_name: str, card_json: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "INSERT INTO a2a_agent_cards (owner, agent_name, card_json, updated_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(owner, agent_name) DO UPDATE SET card_json = excluded.card_json, updated_at = excluded.updated_at",
            (owner, agent_name, card_json, now),
        )
        await self._db.commit()

    async def is_agent_online(self, agent_name: str) -> bool:
        agent_id_pattern = f"{agent_name}@%"
        cursor = await self._db.execute(
            "SELECT status FROM agent_presence WHERE agent_id LIKE ? AND status != 'offline' LIMIT 1",
            (agent_id_pattern,),
        )
        return await cursor.fetchone() is not None

    async def get_agent_status(self, agent_name: str) -> str:
        agent_id_pattern = f"{agent_name}@%"
        cursor = await self._db.execute(
            "SELECT status FROM agent_presence WHERE agent_id LIKE ? ORDER BY last_heartbeat_at DESC LIMIT 1",
            (agent_id_pattern,),
        )
        row = await cursor.fetchone()
        return row["status"] if row else "offline"
