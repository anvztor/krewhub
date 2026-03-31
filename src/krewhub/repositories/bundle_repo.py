from __future__ import annotations

import json
from datetime import datetime

import aiosqlite

from krewhub.models import Bundle, BundleStatus


class BundleRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(self, bundle: Bundle) -> Bundle:
        await self._db.execute(
            """INSERT INTO bundles
               (id, recipe_id, prompt, status, created_by, created_at,
                claimed_at, cooked_at, digested_at, blocked_reason)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (bundle.id, bundle.recipe_id, bundle.prompt, bundle.status,
             bundle.created_by, bundle.created_at.isoformat(),
             bundle.claimed_at.isoformat() if bundle.claimed_at else None,
             bundle.cooked_at.isoformat() if bundle.cooked_at else None,
             bundle.digested_at.isoformat() if bundle.digested_at else None,
             bundle.blocked_reason),
        )
        await self._db.commit()
        return bundle

    async def get(self, bundle_id: str) -> Bundle | None:
        cursor = await self._db.execute(
            "SELECT * FROM bundles WHERE id = ?", (bundle_id,)
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_bundle(row)

    async def list_by_recipe(self, recipe_id: str) -> list[Bundle]:
        cursor = await self._db.execute(
            "SELECT * FROM bundles WHERE recipe_id = ? ORDER BY created_at DESC",
            (recipe_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_bundle(r) for r in rows]

    async def update_status(
        self,
        bundle_id: str,
        status: BundleStatus,
        *,
        claimed_at: datetime | None = None,
        cooked_at: datetime | None = None,
        digested_at: datetime | None = None,
        blocked_reason: str | None = None,
    ) -> Bundle | None:
        parts: list[str] = ["status = ?"]
        params: list[object] = [status]

        if claimed_at is not None:
            parts.append("claimed_at = ?")
            params.append(claimed_at.isoformat())
        if cooked_at is not None:
            parts.append("cooked_at = ?")
            params.append(cooked_at.isoformat())
        if digested_at is not None:
            parts.append("digested_at = ?")
            params.append(digested_at.isoformat())
        if blocked_reason is not None:
            parts.append("blocked_reason = ?")
            params.append(blocked_reason)

        params.append(bundle_id)
        await self._db.execute(
            f"UPDATE bundles SET {', '.join(parts)} WHERE id = ?",
            params,
        )
        await self._db.commit()
        return await self.get(bundle_id)

    async def reopen_for_rerun(self, bundle_id: str) -> Bundle | None:
        await self._db.execute(
            """UPDATE bundles
               SET status = 'open',
                   blocked_reason = NULL
               WHERE id = ?""",
            (bundle_id,),
        )
        await self._db.commit()
        return await self.get(bundle_id)


def _row_to_bundle(row: aiosqlite.Row) -> Bundle:
    return Bundle(
        id=row["id"],
        recipe_id=row["recipe_id"],
        prompt=row["prompt"],
        status=row["status"],
        created_by=row["created_by"],
        created_at=datetime.fromisoformat(row["created_at"]),
        claimed_at=datetime.fromisoformat(row["claimed_at"]) if row["claimed_at"] else None,
        cooked_at=datetime.fromisoformat(row["cooked_at"]) if row["cooked_at"] else None,
        digested_at=datetime.fromisoformat(row["digested_at"]) if row["digested_at"] else None,
        blocked_reason=row["blocked_reason"],
    )
