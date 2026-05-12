from __future__ import annotations

from datetime import datetime

import aiosqlite

from krewhub.models import RepoGrant, RepoProvider


class RepoGrantRepo:
    """CRUD for repo_grants — per-cookbook OAuth scopes used at JIT
    repo materialization time. Soft-delete via revoked_at."""

    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(self, grant: RepoGrant) -> RepoGrant:
        await self._db.execute(
            """INSERT INTO repo_grants
                 (id, cookbook_id, provider, scope, token_ref,
                  granted_by_account_id, granted_at, revoked_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                grant.id, grant.cookbook_id, grant.provider, grant.scope,
                grant.token_ref, grant.granted_by_account_id,
                grant.granted_at.isoformat(),
                grant.revoked_at.isoformat() if grant.revoked_at else None,
            ),
        )
        await self._db.commit()
        return grant

    async def get(self, grant_id: str) -> RepoGrant | None:
        cursor = await self._db.execute(
            "SELECT * FROM repo_grants WHERE id = ?", (grant_id,),
        )
        row = await cursor.fetchone()
        return _row_to_grant(row) if row else None

    async def list_by_cookbook(
        self, cookbook_id: str, *, include_revoked: bool = False,
    ) -> list[RepoGrant]:
        if include_revoked:
            sql = (
                "SELECT * FROM repo_grants WHERE cookbook_id = ? "
                "ORDER BY granted_at"
            )
        else:
            sql = (
                "SELECT * FROM repo_grants WHERE cookbook_id = ? "
                "AND revoked_at IS NULL ORDER BY granted_at"
            )
        cursor = await self._db.execute(sql, (cookbook_id,))
        rows = await cursor.fetchall()
        return [_row_to_grant(r) for r in rows]

    async def revoke(
        self, grant_id: str, *, at: datetime,
    ) -> RepoGrant | None:
        await self._db.execute(
            """UPDATE repo_grants SET revoked_at = ?
               WHERE id = ? AND revoked_at IS NULL""",
            (at.isoformat(), grant_id),
        )
        await self._db.commit()
        return await self.get(grant_id)

    async def find_covering(
        self,
        cookbook_id: str,
        provider: RepoProvider,
        owner: str,
        repo: str,
    ) -> RepoGrant | None:
        """Return an active grant on this cookbook that covers
        provider:owner/repo. Used by the JIT repo materialization
        path. Scope matching:
            "owner/repo" — exact match
            "owner/*"    — any repo under owner
            "owner"      — same as "owner/*"
        First active match wins; ordered by granted_at so the earliest
        grant takes precedence (callers can revoke + regrant to change).
        """
        cursor = await self._db.execute(
            """SELECT * FROM repo_grants
               WHERE cookbook_id = ? AND provider = ? AND revoked_at IS NULL
               ORDER BY granted_at""",
            (cookbook_id, provider),
        )
        rows = await cursor.fetchall()
        target_exact = f"{owner}/{repo}"
        target_wild = f"{owner}/*"
        for row in rows:
            scope = row["scope"]
            if scope in (target_exact, target_wild, owner):
                return _row_to_grant(row)
        return None


def _row_to_grant(row: aiosqlite.Row) -> RepoGrant:
    return RepoGrant(
        id=row["id"],
        cookbook_id=row["cookbook_id"],
        provider=RepoProvider(row["provider"]),
        scope=row["scope"],
        token_ref=row["token_ref"],
        granted_by_account_id=row["granted_by_account_id"],
        granted_at=datetime.fromisoformat(row["granted_at"]),
        revoked_at=(
            datetime.fromisoformat(row["revoked_at"])
            if row["revoked_at"] else None
        ),
    )
