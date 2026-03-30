from __future__ import annotations

from datetime import datetime, timezone

import aiosqlite

from krewhub.repositories.event_repo import EventRepo


class RetentionService:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._events = EventRepo(db)

    async def cleanup_expired(self) -> int:
        now = datetime.now(timezone.utc)
        return await self._events.delete_expired(now)
