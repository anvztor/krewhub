from __future__ import annotations

from datetime import datetime

import aiosqlite

from krewhub.models import Cookbook


class CookbookRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(self, cookbook: Cookbook) -> Cookbook:
        await self._db.execute(
            "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
            (cookbook.id, cookbook.name, cookbook.owner_id, cookbook.created_at.isoformat()),
        )
        await self._db.commit()
        return cookbook

    async def get(self, cookbook_id: str) -> Cookbook | None:
        cursor = await self._db.execute(
            "SELECT * FROM cookbooks WHERE id = ?", (cookbook_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_cookbook(row)

    async def list_all(self) -> list[Cookbook]:
        cursor = await self._db.execute("SELECT * FROM cookbooks ORDER BY created_at")
        rows = await cursor.fetchall()
        return [_row_to_cookbook(r) for r in rows]

    async def find_by_name_and_owner(self, name: str, owner_id: str) -> Cookbook | None:
        cursor = await self._db.execute(
            "SELECT * FROM cookbooks WHERE name = ? AND owner_id = ?",
            (name, owner_id),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_cookbook(row)

    async def list_by_owner(self, owner_id: str) -> list[Cookbook]:
        cursor = await self._db.execute(
            "SELECT * FROM cookbooks WHERE owner_id = ? ORDER BY created_at",
            (owner_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_cookbook(r) for r in rows]


def _row_to_cookbook(row: aiosqlite.Row) -> Cookbook:
    return Cookbook(
        id=row["id"],
        name=row["name"],
        owner_id=row["owner_id"],
        created_at=datetime.fromisoformat(row["created_at"]),
    )
