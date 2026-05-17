"""ElicitRepo — durable elicit tracking with reservation semantics.

State machine: pending → injecting (with lease) → resolved.
Lease expiry: a background sweeper flips stuck 'injecting' rows back to
'pending' if `injecting_until < now`. This is the v5 design — a transient
sandbox failure doesn't burn the elicit.
"""
from __future__ import annotations

from dataclasses import dataclass

import aiosqlite


@dataclass(frozen=True)
class ElicitRow:
    id: str
    invocation_id: str
    op: str
    payload_json: str
    status: str            # 'pending' | 'injecting' | 'resolved' | 'expired'
    created_at: str | None = None
    resolved_at: str | None = None
    injecting_until: str | None = None


class ElicitRepo:
    def __init__(self, conn: aiosqlite.Connection):
        self._db = conn

    async def put(self, row: ElicitRow) -> None:
        """Insert; no-op on conflict (idempotent emission)."""
        await self._db.execute(
            """
            INSERT INTO elicits (id, invocation_id, op, payload_json, status)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO NOTHING
            """,
            (row.id, row.invocation_id, row.op, row.payload_json, row.status),
        )
        await self._db.commit()

    async def get_pending(self, *, invocation_id: str, elicit_id: str) -> ElicitRow | None:
        cur = await self._db.execute(
            "SELECT id, invocation_id, op, payload_json, status, created_at, resolved_at, injecting_until "
            "FROM elicits WHERE invocation_id = ? AND id = ? AND status = 'pending'",
            (invocation_id, elicit_id),
        )
        r = await cur.fetchone()
        return None if not r else ElicitRow(
            id=r[0], invocation_id=r[1], op=r[2], payload_json=r[3], status=r[4],
            created_at=r[5], resolved_at=r[6], injecting_until=r[7],
        )

    async def latest_pending_auth_required(self, *, invocation_id: str) -> ElicitRow | None:
        cur = await self._db.execute(
            "SELECT id, invocation_id, op, payload_json, status, created_at, resolved_at, injecting_until "
            "FROM elicits WHERE invocation_id = ? AND op = 'auth_required' AND status = 'pending' "
            "ORDER BY created_at DESC LIMIT 1",
            (invocation_id,),
        )
        r = await cur.fetchone()
        return None if not r else ElicitRow(
            id=r[0], invocation_id=r[1], op=r[2], payload_json=r[3], status=r[4],
            created_at=r[5], resolved_at=r[6], injecting_until=r[7],
        )

    async def reserve(self, *, invocation_id: str, elicit_id: str, lease_s: int) -> bool:
        """Atomic pending → injecting. Returns True iff this caller won the
        reservation (rowcount == 1)."""
        cur = await self._db.execute(
            """
            UPDATE elicits
               SET status = 'injecting',
                   injecting_until = datetime('now', '+' || ? || ' seconds')
             WHERE invocation_id = ? AND id = ? AND status = 'pending'
            """,
            (lease_s, invocation_id, elicit_id),
        )
        await self._db.commit()
        return (cur.rowcount or 0) > 0

    async def finalize(self, *, invocation_id: str, elicit_id: str) -> bool:
        """Atomic injecting → resolved. Returns False if not in 'injecting'
        (e.g., sweeper already reverted it after a lease expiry)."""
        cur = await self._db.execute(
            """
            UPDATE elicits
               SET status = 'resolved',
                   resolved_at = datetime('now'),
                   injecting_until = NULL
             WHERE invocation_id = ? AND id = ? AND status = 'injecting'
            """,
            (invocation_id, elicit_id),
        )
        await self._db.commit()
        return (cur.rowcount or 0) > 0

    async def sweep_expired_leases(self) -> int:
        """Flip injecting → pending when lease has expired. Returns count."""
        cur = await self._db.execute(
            """
            UPDATE elicits
               SET status = 'pending', injecting_until = NULL
             WHERE status = 'injecting' AND injecting_until < datetime('now')
            """,
        )
        await self._db.commit()
        return cur.rowcount or 0
