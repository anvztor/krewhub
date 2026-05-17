"""Background loop that sweeps expired elicit leases.

When a credential-relay inject call times out, the elicit row is left in
'injecting' status with an `injecting_until` timestamp. This sweeper
periodically flips those expired rows back to 'pending' so the SPA can
retry without manual intervention.
"""
from __future__ import annotations

import asyncio
import logging

log = logging.getLogger(__name__)


async def run_elicit_sweep_loop(db_factory, interval_s: float = 10.0) -> None:
    """Periodic sweep of expired elicit leases. Cancellable via task.cancel()."""
    while True:
        try:
            await asyncio.sleep(interval_s)
            db = await db_factory()
            from krewhub.repositories.elicit_repo import ElicitRepo
            n = await ElicitRepo(db).sweep_expired_leases()
            if n:
                log.info("swept %d expired elicit leases", n)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("elicit sweep iteration failed; continuing")
