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

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from krewhub.models import TaskStatus, WatchEventType
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.services.graph_runtime.a2a import dispatch_to_gateway
from krewhub.services.graph_runtime.agent_picker import pick_agent_for_kind
from krewhub.services.graph_runtime.polling import (
    DependencyFailedError,
    wait_for_dependencies,
    wait_for_task_terminal,
)
from krewhub.services.graph_runtime.state import (
    AttemptRecord,
    OrchestratorDeps,
    OrchestratorState,
    TaskNodeResult,
)
from krewhub.tape.manager import TapeManager

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
        await _collect_facts_and_code_refs(state, deps, node_id, task_id)
        return f"done: {task.claimed_by_agent_id or 'cached'}"

    # Fanin guard: pydantic-graph beta's GraphBuilder fires a downstream
    # step as soon as *any one* predecessor completes (unless the LLM emits
    # an explicit Join node, which it doesn't). Without this wait, a step
    # with N upstream dependencies can race ahead while N-1 siblings are
    # still running. depends_on_task_ids is the source of truth — bundle
    # creation populates it from the graph edges, so it always matches the
    # rendered topology even when the generated code omits joins.
    if task.depends_on_task_ids:
        try:
            await wait_for_dependencies(
                task_repo, task.depends_on_task_ids,
                poll_interval=deps.poll_interval,
                timeout=deps.task_timeout,
                accept_when=accept_when,
            )
        except TimeoutError:
            return _record_failure(
                state, node_id, task_id=task_id,
                agent_id="", iteration=0, status="dep_timeout",
                summary=(
                    f"timed out after {deps.task_timeout}s waiting for "
                    f"{len(task.depends_on_task_ids)} upstream dependency(ies)"
                ),
            )
        except DependencyFailedError as exc:
            return _record_failure(
                state, node_id, task_id=task_id,
                agent_id="", iteration=0, status="dep_failed",
                summary=f"upstream failure: {exc}",
            )
        except ValueError as exc:
            return _record_failure(
                state, node_id, task_id=task_id,
                agent_id="", iteration=0, status="dep_missing",
                summary=f"dependency lookup failed: {exc}",
            )
        # Re-fetch after the wait — another runner may have touched the row
        # (e.g. a sibling fire of the same step via a parallel predecessor
        # path) while we were waiting. If it's now DONE, adopt the result.
        refreshed = await task_repo.get(task_id)
        if refreshed is not None:
            task = refreshed
            if task.status == accept_when:
                _record_success(
                    state, node_id, task_id,
                    agent_id=task.claimed_by_agent_id or "",
                    iteration=0,
                    summary="already complete (won race with sibling fire)",
                )
                await _collect_facts_and_code_refs(state, deps, node_id, task_id)
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
                await _collect_facts_and_code_refs(state, deps, node_id, task_id)
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

    # Assemble upstream handoff context (once, reused across retries).
    upstream_context = await _assemble_upstream_context(
        deps, state, task.depends_on_task_ids,
    )

    for attempt in range(1, max_iterations + 1):
        started_at = datetime.now(timezone.utc)

        agents = await agent_repo.list_by_cookbook(deps.cookbook_id)
        # Three-tier pick:
        #   1. fresh for this cycle AND not yet used by this bundle
        #      (best: maximizes diversity across sibling tasks)
        #   2. fresh for this cycle (any agent this cycle hasn't tried)
        #   3. any eligible agent (reuse, last resort)
        chosen = _pick_with_diversity(
            agents, task_kind,
            cycle_tried=tried,
            bundle_used=state.bundle_dispatched_agents,
        )
        if chosen is None:
            # Wait for an agent to come back online (e.g. finishing
            # another step and heartbeating back).  Poll up to 60 s
            # before consuming this attempt — enough for two full
            # heartbeat cycles (30 s timeout).
            _GATEWAY_WAIT = 60.0
            _GATEWAY_POLL = deps.poll_interval
            waited = 0.0
            while waited < _GATEWAY_WAIT:
                await asyncio.sleep(_GATEWAY_POLL)
                waited += _GATEWAY_POLL
                agents = await agent_repo.list_by_cookbook(deps.cookbook_id)
                chosen = _pick_with_diversity(
                    agents, task_kind,
                    cycle_tried=tried,
                    bundle_used=state.bundle_dispatched_agents,
                )
                if chosen is not None:
                    break

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
        state.bundle_dispatched_agents.add(chosen.agent_id)
        prompt = _build_prompt(
            state.prompt, refined_instruction, last_summary, attempt,
            upstream_context=upstream_context,
        )

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
            await _collect_facts_and_code_refs(state, deps, node_id, task_id)
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


def _pick_with_diversity(
    agents: list["AgentPresence"],
    task_kind: str,
    *,
    cycle_tried: set[str],
    bundle_used: set[str],
) -> "AgentPresence | None":
    """Pick an agent preferring per-bundle diversity.

    Tier 1: fresh for this cycle AND not yet used by this bundle —
            maximizes multi-agent participation across sibling tasks.
    Tier 2: fresh for this cycle (any agent this cycle hasn't tried) —
            necessary when the bundle has exhausted the pool.
    Tier 3: any eligible agent — unavoidable reuse; better than blocking.
    """
    chosen = pick_agent_for_kind(
        agents, task_kind, exclude=cycle_tried | bundle_used,
    )
    if chosen is not None:
        return chosen
    chosen = pick_agent_for_kind(agents, task_kind, exclude=cycle_tried)
    if chosen is not None:
        return chosen
    return pick_agent_for_kind(agents, task_kind, exclude=set())


def _build_prompt(
    base: str,
    instruction: str,
    last_summary: str,
    attempt: int,
    *,
    upstream_context: str = "",
) -> str:
    parts = [f"[Attempt {attempt}] {instruction}"]
    if upstream_context:
        parts.append(upstream_context)
    parts.append(f"Context: {base}")
    if last_summary:
        parts.append(f"Prior failure: {last_summary}")
    return "\n\n".join(parts)


async def _assemble_upstream_context(
    deps: OrchestratorDeps,
    state: OrchestratorState,
    upstream_task_ids: list[str],
) -> str:
    """Read handoff anchors from upstream fork tapes and format as context.

    Returns a Markdown section that tells the downstream agent what
    upstream tasks discovered, decided, and built — so it doesn't
    start from scratch.
    """
    if not upstream_task_ids:
        return ""

    # Reverse map: task_id → node_id
    tid_to_nid = {v: k for k, v in deps.task_id_map.items()}

    sections: list[str] = []
    # Phase 12: cookbook-scoped tape; legacy recipe_id is None for new bundles.
    tape_name = (
        f"recipe:{state.recipe_id}" if state.recipe_id
        else f"cookbook:{deps.cookbook_id}"
    )
    tape = TapeManager(deps.db, tape_name)

    for task_id in upstream_task_ids:
        node_id = tid_to_nid.get(task_id, task_id)

        # First, try fork tape handoff anchors (richest context).
        try:
            fork_entries = await tape.get_fork_entries(state.bundle_id, task_id)
            anchors = [e for e in fork_entries if e.kind == "anchor"]
            if anchors:
                anchor = anchors[-1]
                p = anchor.payload
                section = f"### {node_id} ({task_id}): {p.get('summary', 'completed')}\n"
                if p.get("facts"):
                    section += "**Facts:**\n"
                    for f in p["facts"]:
                        claim = f.get("claim", f) if isinstance(f, dict) else str(f)
                        section += f"- {claim}\n"
                if p.get("decisions"):
                    section += "**Decisions:**\n"
                    for d in p["decisions"]:
                        section += f"- {d}\n"
                if p.get("code_ref"):
                    cr = p["code_ref"]
                    paths = ", ".join(cr.get("paths", [])) if cr.get("paths") else ""
                    section += f"**Code:** branch `{cr.get('branch', '?')}` {paths}\n"
                if p.get("next_steps"):
                    section += "**Next steps:**\n"
                    for ns in p["next_steps"]:
                        section += f"- {ns}\n"
                sections.append(section)
                continue
        except Exception:
            pass

        # Fallback: use in-memory task_results (facts/code_refs from events).
        result = state.task_results.get(node_id)
        if result is not None:
            section = f"### {node_id} ({task_id}): {result.summary}\n"
            if result.facts:
                section += "**Facts:**\n"
                for f in result.facts[:10]:
                    claim = f.get("claim", "") if isinstance(f, dict) else str(f)
                    if claim:
                        section += f"- {claim}\n"
            if result.code_refs:
                section += "**Code changes:**\n"
                for cr in result.code_refs[:5]:
                    paths = ", ".join(cr.get("paths", [])) if isinstance(cr, dict) else ""
                    section += f"- {paths}\n"
            sections.append(section)

    if not sections:
        return ""
    return "## Prior work from upstream tasks\n\n" + "\n".join(sections)


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
        )
