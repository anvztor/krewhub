"""Poll krewhub task rows until they reach terminal / accepted state.

Used by dispatch_cycle to (a) wait for an agent to finish a dispatched
task, and (b) enforce fanin semantics by waiting for every upstream
dependency to reach DONE before dispatching the current step.

The cycle owns retry logic — these helpers are "wait until done or
timeout" primitives.
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


class DependencyFailedError(RuntimeError):
    """Raised when an upstream dependency reached a non-DONE terminal state.

    Carries the offending upstream task id and its terminal status so the
    caller can build a user-facing failure summary.
    """

    def __init__(self, dep_task_id: str, status: TaskStatus, reason: str) -> None:
        self.dep_task_id = dep_task_id
        self.status = status
        self.reason = reason
        super().__init__(
            f"dependency {dep_task_id} ended with {status}: {reason or 'no reason'}"
        )


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


async def wait_for_dependencies(
    task_repo: TaskRepo,
    dep_task_ids: list[str],
    *,
    poll_interval: float = 2.0,
    timeout: float = 300.0,
    accept_when: TaskStatus = TaskStatus.DONE,
) -> None:
    """Wait until every task in `dep_task_ids` reaches `accept_when`.

    Enforces fanin semantics at the runtime layer. pydantic-graph's beta
    GraphBuilder fires a downstream step as soon as any single predecessor
    completes (unless an explicit Join node was added), so without this
    guard a step with N predecessors can run before the other N-1 are
    finished — exactly the bug cookrew exhibited on fanin graphs.

    Args:
        task_repo: repository scoped to the same db connection as the
            dispatcher.
        dep_task_ids: upstream task ids from `task.depends_on_task_ids`.
            Empty list returns immediately.
        poll_interval: seconds between reads.
        timeout: wall-clock ceiling across all deps.
        accept_when: the terminal status that counts as "satisfied"
            (default DONE — matches dispatch_cycle's default).

    Raises:
        asyncio.TimeoutError: if the wait exceeds `timeout`.
        DependencyFailedError: if any dep reaches a terminal state other
            than `accept_when` (e.g. BLOCKED, CANCELLED). The caller
            should fail the current step — there is no point dispatching
            work whose inputs are missing.
        ValueError: if a dep task id doesn't exist in the db (caller
            should treat as fatal — the bundle is in an inconsistent state).
    """
    pending: list[str] = [tid for tid in dep_task_ids if tid]
    if not pending:
        return

    async def _wait() -> None:
        while pending:
            next_round: list[str] = []
            for dep_id in pending:
                dep = await task_repo.get(dep_id)
                if dep is None:
                    raise ValueError(
                        f"dependency {dep_id} not found in db"
                    )
                if dep.status == accept_when:
                    continue
                if dep.status in _TERMINAL_STATUSES:
                    # Terminal but not accepted — upstream failed.
                    raise DependencyFailedError(
                        dep_id, dep.status, dep.blocked_reason or "",
                    )
                next_round.append(dep_id)
            if not next_round:
                return
            pending[:] = next_round
            await asyncio.sleep(poll_interval)

    await asyncio.wait_for(_wait(), timeout=timeout)
