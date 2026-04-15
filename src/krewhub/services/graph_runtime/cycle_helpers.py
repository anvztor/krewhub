"""Helper functions extracted from dispatch_cycle.

These are internal helpers used by the agentic retry loop in cycle.py.
They handle prompt building, attempt recording, success/failure tracking,
fact collection, and task claiming.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from krewhub.models import TaskStatus, WatchEventType
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.services.graph_runtime.state import (
    AttemptRecord,
    OrchestratorDeps,
    OrchestratorState,
    TaskNodeResult,
)

if TYPE_CHECKING:
    from krewhub.models import AgentPresence

logger = logging.getLogger(__name__)


def _build_prompt(base: str, instruction: str, last_summary: str, attempt: int) -> str:
    parts = [
        f"[Attempt {attempt}] {instruction}",
        f"Context: {base}",
    ]
    if last_summary:
        parts.append(f"Prior failure: {last_summary}")
    return "\n\n".join(parts)


def _refine(original: str, failure_summary: str) -> str:
    return (
        f"{original}\n\nThe previous attempt failed: {failure_summary}\n"
        "Please address the failure and try again."
    )


def _append_attempt(
    state: OrchestratorState,
    node_id: str,
    task_id: str,
    attempt: AttemptRecord,
) -> None:
    record = state.task_results.get(node_id)
    if record is None:
        record = TaskNodeResult(
            node_id=node_id, task_id=task_id, success=False, summary="",
        )
        state.task_results[node_id] = record
    record.attempts.append(attempt)


def _record_success(
    state: OrchestratorState,
    node_id: str,
    task_id: str,
    *,
    agent_id: str,
    iteration: int,
    summary: str,
) -> None:
    record = state.task_results.get(node_id)
    if record is None:
        record = TaskNodeResult(
            node_id=node_id, task_id=task_id, success=True, summary=summary,
        )
        state.task_results[node_id] = record
    else:
        record.success = True
        record.summary = summary
    logger.info(
        "dispatch_cycle: success node=%s task=%s agent=%s iter=%d",
        node_id, task_id, agent_id, iteration,
    )


def _record_failure(
    state: OrchestratorState,
    node_id: str,
    *,
    task_id: str,
    agent_id: str,
    iteration: int,
    status: str,
    summary: str,
) -> str:
    record = state.task_results.get(node_id)
    if record is None:
        record = TaskNodeResult(
            node_id=node_id, task_id=task_id, success=False, summary=summary,
        )
        state.task_results[node_id] = record
    else:
        record.success = False
        record.summary = summary
    if iteration > 0:
        record.attempts.append(
            AttemptRecord(
                iteration=iteration, agent_id=agent_id, status=status,
                summary=summary,
                started_at=datetime.now(timezone.utc),
                ended_at=datetime.now(timezone.utc),
            )
        )
    logger.warning(
        "dispatch_cycle: failure node=%s task=%s status=%s summary=%s",
        node_id, task_id, status, summary,
    )
    return f"error: {summary}"


async def _collect_facts_and_code_refs(
    state: OrchestratorState,
    deps: OrchestratorDeps,
    node_id: str,
    task_id: str,
) -> None:
    """Read task events and accumulate facts/code_refs on the result record."""
    record = state.task_results.get(node_id)
    if record is None:
        return
    events = await EventRepo(deps.db).list_by_task(task_id)
    for evt in events:
        record.facts.extend(f.model_dump() for f in evt.facts)
        record.code_refs.extend(c.model_dump() for c in evt.code_refs)


async def _mark_claimed(
    task_repo: TaskRepo,
    deps: OrchestratorDeps,
    task_id: str,
    agent: "AgentPresence",
) -> None:
    """Update the task row to claimed/working and emit a watch event."""
    now = datetime.now(timezone.utc)
    updated = await task_repo.update(
        task_id,
        status=TaskStatus.CLAIMED,
        assigned_agent_id=agent.agent_id,
        claimed_by_agent_id=agent.agent_id,
        claimed_at=now,
    )
    if updated is not None:
        # The bundle's recipe_id is needed for the watch index; pull from deps.
        recipe_id = deps.recipe_meta.get("recipe_id", "") if deps.recipe_meta else ""
        await deps.watch.record_resource(
            "task", task_id, WatchEventType.MODIFIED, updated,
            recipe_id=recipe_id,
        )
