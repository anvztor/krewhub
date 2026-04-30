"""Background sweeper: terminates idle / aged sandboxes.

A sandbox row is sweep-eligible when it is in a non-terminal status
(provisioning / ready / running) AND either:
  - last_event_at is older than `idle_seconds`, OR
  - created_at is older than `max_age_seconds`.

Each tick: list eligible rows; for each call SandboxService.terminate.
A failure on one sandbox doesn't block the rest.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

import aiosqlite

from krewhub.repositories.sandbox_repo import SandboxRepo
from krewhub.services.e2b_client import E2bClient
from krewhub.services.sandbox_service import SandboxService

logger = logging.getLogger(__name__)


GetDb = Callable[[], Awaitable[aiosqlite.Connection]]


class SandboxSweeper:
    def __init__(
        self,
        *,
        get_db: GetDb,
        e2b: E2bClient,
        idle_seconds: int,
        max_age_seconds: int,
        interval_seconds: int = 60,
    ) -> None:
        self._get_db = get_db
        self._e2b = e2b
        self._idle = idle_seconds
        self._max_age = max_age_seconds
        self._interval = interval_seconds
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def tick(self) -> int:
        """Run one sweep pass. Returns the number of sandboxes terminated."""
        db = await self._get_db()
        repo = SandboxRepo(db)
        stale = await repo.list_idle_or_expired(
            idle_seconds=self._idle, max_age_seconds=self._max_age,
        )
        terminated = 0
        for sandbox in stale:
            try:
                await SandboxService(db, self._e2b).terminate(sandbox.id)
                terminated += 1
                logger.info("sweeper terminated sandbox %s", sandbox.id)
            except Exception:
                logger.exception("sweeper failed on sandbox %s", sandbox.id)
        return terminated

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.tick()
            except Exception:
                logger.exception("sweeper tick failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass

    def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await self._task
            except Exception:
                logger.exception("sweeper stop: task raised")
            self._task = None
