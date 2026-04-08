"""dispatch_cycle — the agentic retry loop embedded in every graph step.

This is the function the LLM-generated graph code calls inside each
@g.step body. It owns the entire "dispatch this step's work to an A2A
agent, retry on failure, record what happened" lifecycle. The LLM only
declares *intent* (node_id, task_kind, instruction); the cycle picks
the agent, formats the prompt, dispatches, polls, and retries.

Trust model:
    - The exec'd graph code calls dispatch_cycle by name (it's injected
      into the sandbox namespace by execute_graph_code).
    - Validator + sandbox prevent the graph code from doing anything else.
    - This module is the only place that touches httpx, the db, or the
      watch service from inside a step.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from krewhub.models import TaskStatus, WatchEventType
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.services.graph_runtime.a2a import dispatch_to_gateway
from krewhub.services.graph_runtime.agent_picker import pick_agent_for_kind
from krewhub.services.graph_runtime.polling import wait_for_task_terminal
from krewhub.services.graph_runtime.state import (
    AttemptRecord,
    OrchestratorDeps,
    OrchestratorState,
    TaskNodeResult,
)

if TYPE_CHECKING:
    from krewhub.models import AgentPresence

logger = logging.getLogger(__name__)


async def dispatch_cycle(
    ctx: Any,  # pydantic_graph StepContext[OrchestratorState, OrchestratorDeps, ...]
    *,
    node_id: str,
    task_kind: str,
    instruction: str,
    max_iterations: int = 3,
    accept_when: TaskStatus = TaskStatus.DONE,
) -> str:
    """Run the agentic retry loop for one graph step.

    Each iteration:
        1. Pick a fresh gateway suitable for `task_kind` (excluding agents
           that already failed this cycle).
        2. POST the task to that gateway via A2A message/send.
        3. Poll the krewhub task row until it reaches a terminal status.
        4. If terminal status matches `accept_when` → record success, return.
        5. Otherwise refine the instruction with the failure summary and
           try a different agent.

    Args:
        ctx: pydantic-graph StepContext. Must expose `.state` (OrchestratorState)
            and `.deps` (OrchestratorDeps).
        node_id: graph step name; used as the lookup key into ctx.deps.task_id_map.
        task_kind: a free-form skill name matched against agent capabilities.
        instruction: human-readable description of what this step should do.
        max_iterations: hard ceiling on retry count.
        accept_when: which terminal TaskStatus counts as success (default DONE).

    Returns:
        A short status string the graph step can return as its node output.
        Format: "done: <agent_id>" on success, "error: <reason>" on failure.

    Side effects:
        - Records an AttemptRecord on ctx.state.task_results[node_id] for
          every iteration (success or failure).
        - Updates the krewhub task row's status/blocked_reason via task_repo.
        - Emits MODIFIED watch events on the task so cookrew SSE picks them up.
    """
    state: OrchestratorState = ctx.state
    deps: OrchestratorDeps = ctx.deps

    task_id = deps.task_id_map.get(node_id, "")
    if not task_id:
        return _record_failure(
            state, node_id, task_id="",
            agent_id="", iteration=1, status="no_task",
            summary=f"no task_id mapped for node {node_id!r}",
        )

    task_repo = TaskRepo(deps.db)
    agent_repo = AgentRepo(deps.db)

    task = await task_repo.get(task_id)
    if task is None:
        return _record_failure(
            state, node_id, task_id=task_id,
            agent_id="", iteration=1, status="missing_task",
            summary=f"task {task_id} not found in db",
        )

    # Resume guard: if a previous run already finished this task, skip.
    if task.status == accept_when:
        _record_success(state, node_id, task_id, agent_id=task.claimed_by_agent_id or "",
                        iteration=0, summary="already complete")
        return f"done: {task.claimed_by_agent_id or 'cached'}"

    tried: set[str] = set()
    last_summary = ""
    refined_instruction = instruction

    # In-flight guard: if another party (a stray legacy dispatcher, a prior
    # run of this cycle after a crash, or a parallel runner) has already
    # claimed the task, don't re-dispatch — that would execute the step
    # twice. Wait for whoever owns it to finish, then treat the terminal
    # state as the result of attempt 0. If the outcome is BLOCKED/CANCELLED
    # the normal retry loop below will still get a chance with a fresh agent.
    if task.status in (TaskStatus.CLAIMED, TaskStatus.WORKING):
        started_at = datetime.now(timezone.utc)
        owning_agent = task.claimed_by_agent_id or ""
        try:
            final = await wait_for_task_terminal(
                task_repo, task_id,
                poll_interval=deps.poll_interval,
                timeout=deps.task_timeout,
            )
        except TimeoutError:
            ended = datetime.now(timezone.utc)
            _append_attempt(
                state, node_id, task_id,
                AttemptRecord(
                    iteration=0, agent_id=owning_agent, status="timeout",
                    summary=(
                        f"already in-flight on entry; timed out after "
                        f"{deps.task_timeout}s waiting for terminal"
                    ),
                    started_at=started_at, ended_at=ended,
                ),
            )
            last_summary = f"timeout waiting for {owning_agent or 'existing run'}"
            refined_instruction = _refine(instruction, last_summary)
        else:
            ended = datetime.now(timezone.utc)
            if final.status == accept_when:
                _append_attempt(
                    state, node_id, task_id,
                    AttemptRecord(
                        iteration=0, agent_id=owning_agent, status="done",
                        summary=(
                            f"task was already in-flight on entry; "
                            f"{owning_agent or 'existing run'} completed it"
                        ),
                        started_at=started_at, ended_at=ended,
                    ),
                )
                _record_success(
                    state, node_id, task_id,
                    agent_id=owning_agent, iteration=0,
                    summary=f"adopted existing run by {owning_agent or 'unknown'}",
                )
                return f"done: {owning_agent or 'adopted'}"

            # Terminal but not DONE → record + fall through to retry loop.
            reason = final.blocked_reason or f"status={final.status}"
            _append_attempt(
                state, node_id, task_id,
                AttemptRecord(
                    iteration=0, agent_id=owning_agent, status=str(final.status),
                    summary=f"in-flight on entry, ended with {reason}",
                    started_at=started_at, ended_at=ended,
                ),
            )
            last_summary = reason
            refined_instruction = _refine(instruction, last_summary)
            # Reopen the task so the retry loop can re-dispatch it.
            await task_repo.reopen_for_rerun(task_id)

    for attempt in range(1, max_iterations + 1):
        started_at = datetime.now(timezone.utc)

        agents = await agent_repo.list_by_cookbook(deps.cookbook_id)
        chosen = pick_agent_for_kind(agents, task_kind, exclude=tried)
        if chosen is None:
            # No fresh agent — fall back to *any* eligible agent (ignore exclude).
            chosen = pick_agent_for_kind(agents, task_kind, exclude=set())
            if chosen is None:
                ended = datetime.now(timezone.utc)
                _append_attempt(
                    state, node_id, task_id,
                    AttemptRecord(
                        iteration=attempt, agent_id="", status="no_agent",
                        summary="no eligible gateway in pool",
                        started_at=started_at, ended_at=ended,
                    ),
                )
                last_summary = "no eligible gateway"
                break

        tried.add(chosen.agent_id)
        prompt = _build_prompt(state.prompt, refined_instruction, last_summary, attempt)

        accepted = await dispatch_to_gateway(
            deps.http, agent=chosen, task=task,
            prompt=prompt, attempt=attempt,
            recipe_meta=deps.recipe_meta,
        )

        if not accepted:
            ended = datetime.now(timezone.utc)
            _append_attempt(
                state, node_id, task_id,
                AttemptRecord(
                    iteration=attempt, agent_id=chosen.agent_id, status="rejected",
                    summary=f"gateway {chosen.agent_id} rejected dispatch",
                    started_at=started_at, ended_at=ended,
                ),
            )
            last_summary = f"gateway {chosen.agent_id} rejected"
            refined_instruction = _refine(instruction, last_summary)
            continue

        await _mark_claimed(task_repo, deps, task_id, chosen)

        try:
            final = await wait_for_task_terminal(
                task_repo, task_id,
                poll_interval=deps.poll_interval,
                timeout=deps.task_timeout,
            )
        except TimeoutError:
            ended = datetime.now(timezone.utc)
            _append_attempt(
                state, node_id, task_id,
                AttemptRecord(
                    iteration=attempt, agent_id=chosen.agent_id, status="timeout",
                    summary=f"timed out after {deps.task_timeout}s",
                    started_at=started_at, ended_at=ended,
                ),
            )
            last_summary = f"timeout on {chosen.agent_id}"
            refined_instruction = _refine(instruction, last_summary)
            continue

        ended = datetime.now(timezone.utc)
        if final.status == accept_when:
            _append_attempt(
                state, node_id, task_id,
                AttemptRecord(
                    iteration=attempt, agent_id=chosen.agent_id, status="done",
                    summary=f"{chosen.agent_id} completed in attempt {attempt}",
                    started_at=started_at, ended_at=ended,
                ),
            )
            _record_success(
                state, node_id, task_id,
                agent_id=chosen.agent_id, iteration=attempt,
                summary=f"completed by {chosen.agent_id} in {attempt} attempt(s)",
            )
            return f"done: {chosen.agent_id}"

        # Terminal but not the accepted state (blocked / cancelled).
        reason = final.blocked_reason or f"status={final.status}"
        _append_attempt(
            state, node_id, task_id,
            AttemptRecord(
                iteration=attempt, agent_id=chosen.agent_id, status=str(final.status),
                summary=reason,
                started_at=started_at, ended_at=ended,
            ),
        )
        last_summary = reason
        refined_instruction = _refine(instruction, last_summary)
        # Reopen the task so the next iteration can re-dispatch it.
        await task_repo.reopen_for_rerun(task_id)

    return _record_failure(
        state, node_id, task_id=task_id,
        agent_id="", iteration=max_iterations,
        status="exhausted",
        summary=f"exhausted {max_iterations} attempt(s); last: {last_summary}",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
