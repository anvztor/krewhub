from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod

import aiosqlite

from krewhub.watch.service import WatchService

logger = logging.getLogger(__name__)


class BaseController(ABC):
    """Base class for K8s-style reconciliation controllers.

    Each controller runs an async loop that periodically calls
    reconcile(). The loop is level-triggered: reconcile() examines
    current state and converges it toward desired state. If the
    controller crashes and restarts, it catches up by reading
    current state — no event replay needed for correctness.
    """

    def __init__(
        self,
        db: aiosqlite.Connection,
        watch: WatchService,
        *,
        interval: float = 2.0,
        max_backoff: float = 30.0,
    ) -> None:
        self._db = db
        self._watch = watch
        self._interval = interval
        self._max_backoff = max_backoff
        self._task: asyncio.Task | None = None
        self._running = False
        self._consecutive_errors = 0

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    async def reconcile(self) -> None:
        """Examine current state and reconcile toward desired state.

        Implementations must be idempotent and safe to call repeatedly.
        """
        ...

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name=f"controller:{self.name}")
        logger.info("Controller %s started (interval=%.1fs)", self.name, self._interval)

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Controller %s stopped", self.name)

    @property
    def is_running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    async def _loop(self) -> None:
        while self._running:
            try:
                await self.reconcile()
                self._consecutive_errors = 0
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                break
            except Exception:
                self._consecutive_errors += 1
                backoff = min(
                    self._interval * (2 ** self._consecutive_errors),
                    self._max_backoff,
                )
                logger.exception(
                    "Controller %s reconcile failed (attempt %d), backing off %.1fs",
                    self.name,
                    self._consecutive_errors,
                    backoff,
                )
                await asyncio.sleep(backoff)
