from __future__ import annotations

import json
from datetime import datetime

import aiosqlite

from krewhub.models import (
    CodeRef,
    Digest,
    DigestDecision,
    DigestTaskResult,
    FactRef,
)


class DigestRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(self, digest: Digest) -> Digest:
        await self._db.execute(
            """INSERT INTO digests
               (id, recipe_id, bundle_id, summary, task_results, facts, code_refs,
                submitted_by, submitted_at, decision, decided_by, decided_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (digest.id, digest.recipe_id, digest.bundle_id, digest.summary,
             json.dumps([tr.model_dump() for tr in digest.task_results]),
             json.dumps([f.model_dump() for f in digest.facts]),
             json.dumps([c.model_dump() for c in digest.code_refs]),
             digest.submitted_by, digest.submitted_at.isoformat(),
             digest.decision, digest.decided_by,
             digest.decided_at.isoformat() if digest.decided_at else None),
        )
        await self._db.commit()
        return digest

    async def get(self, digest_id: str) -> Digest | None:
        cursor = await self._db.execute(
            "SELECT * FROM digests WHERE id = ?", (digest_id,)
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_digest(row)

    async def get_by_bundle(self, bundle_id: str) -> Digest | None:
        cursor = await self._db.execute(
            "SELECT * FROM digests WHERE bundle_id = ?", (bundle_id,)
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_digest(row)

    async def list_approved_by_recipe(self, recipe_id: str) -> list[Digest]:
        cursor = await self._db.execute(
            """SELECT * FROM digests
               WHERE recipe_id = ? AND decision = 'approved'
               ORDER BY decided_at DESC""",
            (recipe_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_digest(r) for r in rows]

    async def update_decision(
        self,
        digest_id: str,
        decision: DigestDecision,
        decided_by: str,
        decided_at: datetime,
    ) -> Digest | None:
        await self._db.execute(
            "UPDATE digests SET decision = ?, decided_by = ?, decided_at = ? WHERE id = ?",
            (decision, decided_by, decided_at.isoformat(), digest_id),
        )
        await self._db.commit()
        return await self.get(digest_id)


def _row_to_digest(row: aiosqlite.Row) -> Digest:
    return Digest(
        id=row["id"],
        recipe_id=row["recipe_id"],
        bundle_id=row["bundle_id"],
        summary=row["summary"],
        task_results=[DigestTaskResult(**tr) for tr in json.loads(row["task_results"])],
        facts=[FactRef(**f) for f in json.loads(row["facts"])],
        code_refs=[CodeRef(**c) for c in json.loads(row["code_refs"])],
        submitted_by=row["submitted_by"],
        submitted_at=datetime.fromisoformat(row["submitted_at"]),
        decision=row["decision"],
        decided_by=row["decided_by"],
        decided_at=datetime.fromisoformat(row["decided_at"]) if row["decided_at"] else None,
    )
