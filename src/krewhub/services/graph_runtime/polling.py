"""Poll a krewhub task row until it reaches a terminal state.

Used by dispatch_cycle to wait for an agent to finish a dispatched task.
The cycle owns retry logic — this helper is purely a "wait until done or
timeout" primitive.
"""

from __future__ import annotations

import asyncio
import logging

from krewhub.models import Task, TaskStatus
from krewhub.repositories.task_repo import TaskRepo

logger = logging.getLogger(__name__)


_TERMINAL_STATUSES: frozenset[TaskStatus] = frozenset({
    TaskStatus.DONE,
    TaskStatus.BLOCKED,
    TaskStatus.CANCELLED,
})


async def wait_for_task_terminal(
    task_repo: TaskRepo,
    task_id: str,
    *,
    poll_interval: float = 2.0,
    timeout: float = 300.0,
) -> Task:
    """Poll the task row until status is terminal, then return it.

    Args:
        task_repo: repository scoped to the same db connection as the dispatcher.
        task_id: the krewhub task id to watch.
        poll_interval: seconds between reads. Keep small enough that cycle
            latency stays acceptable but large enough to avoid hammering sqlite.
        timeout: hard wall-clock ceiling. Raises asyncio.TimeoutError on expiry.

    Returns:
        The Task row in its final terminal state.

    Raises:
        asyncio.TimeoutError: if the task never reaches a terminal state.
        ValueError: if the task disappears mid-wait (caller should treat as fatal).
    """
    async def _wait() -> Task:
        while True:
            task = await task_repo.get(task_id)
            if task is None:
                raise ValueError(f"task {task_id} disappeared during polling")
            if task.status in _TERMINAL_STATUSES:
                return task
            await asyncio.sleep(poll_interval)

    return await asyncio.wait_for(_wait(), timeout=timeout)
