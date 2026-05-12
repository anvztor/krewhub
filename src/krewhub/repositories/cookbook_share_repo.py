from __future__ import annotations

from datetime import datetime

import aiosqlite

from krewhub.models import CookbookShare, ShareRole


class CookbookShareRepo:
    """CRUD for cookbook_shares (cookbook-level RBAC).

    Soft-delete via revoked_at — keeps audit trail of who used to have
    access. A share with revoked_at set should be treated as not
    granting access; UNIQUE(cookbook_id, shared_with_account_id) means
    reshare goes via update rather than re-insert.
    """

    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(self, share: CookbookShare) -> CookbookShare:
        await self._db.execute(
            """INSERT INTO cookbook_shares
                 (id, cookbook_id, shared_with_account_id, role,
                  shared_by_account_id, shared_at, revoked_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                share.id, share.cookbook_id, share.shared_with_account_id,
                share.role, share.shared_by_account_id,
                share.shared_at.isoformat(),
                share.revoked_at.isoformat() if share.revoked_at else None,
            ),
        )
        await self._db.commit()
        return share

    async def get(self, share_id: str) -> CookbookShare | None:
        cursor = await self._db.execute(
            "SELECT * FROM cookbook_shares WHERE id = ?", (share_id,),
        )
        row = await cursor.fetchone()
        return _row_to_share(row) if row else None

    async def get_active_for(
        self, cookbook_id: str, account_id: str,
    ) -> CookbookShare | None:
        cursor = await self._db.execute(
            """SELECT * FROM cookbook_shares
               WHERE cookbook_id = ? AND shared_with_account_id = ?
                 AND revoked_at IS NULL""",
            (cookbook_id, account_id),
        )
        row = await cursor.fetchone()
        return _row_to_share(row) if row else None

    async def list_by_cookbook(self, cookbook_id: str) -> list[CookbookShare]:
        cursor = await self._db.execute(
            """SELECT * FROM cookbook_shares
               WHERE cookbook_id = ? AND revoked_at IS NULL
               ORDER BY shared_at""",
            (cookbook_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_share(r) for r in rows]

    async def list_by_account(self, account_id: str) -> list[CookbookShare]:
        cursor = await self._db.execute(
            """SELECT * FROM cookbook_shares
               WHERE shared_with_account_id = ? AND revoked_at IS NULL
               ORDER BY shared_at""",
            (account_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_share(r) for r in rows]

    async def revoke(
        self, share_id: str, *, at: datetime,
    ) -> CookbookShare | None:
        await self._db.execute(
            """UPDATE cookbook_shares
               SET revoked_at = ?
               WHERE id = ? AND revoked_at IS NULL""",
            (at.isoformat(), share_id),
        )
        await self._db.commit()
        return await self.get(share_id)

    async def update_role(
        self, share_id: str, role: ShareRole,
    ) -> CookbookShare | None:
        await self._db.execute(
            """UPDATE cookbook_shares SET role = ?
               WHERE id = ? AND revoked_at IS NULL""",
            (role, share_id),
        )
        await self._db.commit()
        return await self.get(share_id)


def _row_to_share(row: aiosqlite.Row) -> CookbookShare:
    return CookbookShare(
        id=row["id"],
        cookbook_id=row["cookbook_id"],
        shared_with_account_id=row["shared_with_account_id"],
        role=ShareRole(row["role"]),
        shared_by_account_id=row["shared_by_account_id"],
        shared_at=datetime.fromisoformat(row["shared_at"]),
        revoked_at=(
            datetime.fromisoformat(row["revoked_at"])
            if row["revoked_at"] else None
        ),
    )
