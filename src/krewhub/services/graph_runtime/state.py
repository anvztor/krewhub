"""In-memory state and dependency carriers for an executing graph.

These objects are passed to every step of a krewhub-side pydantic-graph run.
They are *not* the same as the persisted krewhub domain models — they hold
transient runtime context that lives for the lifetime of one bundle's
graph.iter() invocation.

Why dataclasses (not pydantic):
    - State is mutated in-place by dispatch_cycle (append AttemptRecord, set
      task_results entries). Pydantic frozen models would force a copy on
      every update, which fights pydantic-graph's contract that the same
      `state` object flows through every node.
    - Deps is constructed once per graph run; immutability is desirable but
      a frozen dataclass is enough — we don't need pydantic validation here.
    - Persistence to the krewhub db happens through the repos, not by
      serializing these objects. Keep them dependency-free.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import aiosqlite
    import httpx

    from krewhub.watch import WatchService


# ---------------------------------------------------------------------------
# Result records (per-step bookkeeping)
# ---------------------------------------------------------------------------


@dataclass
class AttemptRecord:
    """One iteration of a node's dispatch cycle.

    Multiple AttemptRecords accumulate on a TaskNodeResult when the cycle
    retries — e.g. when an agent rejects, times out, or returns blocked.
    Cookrew renders these as the "three cards while agent is working" stack.
    """

    iteration: int
    agent_id: str
    status: str  # "dispatched" | "timeout" | "rejected" | "blocked" | "done" | "no_agent"
    summary: str
    started_at: datetime
    ended_at: datetime


@dataclass
class TaskNodeResult:
    """Final + cumulative state for one graph step.

    `attempts` grows monotonically as the cycle iterates. `success` and
    `summary` reflect the *last* attempt — older outcomes are preserved in
    `attempts` so the UI and post-mortems can replay the history.
    """

    node_id: str
    task_id: str
    success: bool
    summary: str
    attempts: list[AttemptRecord] = field(default_factory=list)


# ---------------------------------------------------------------------------
# State (mutable, flows through every node)
# ---------------------------------------------------------------------------


@dataclass
class OrchestratorState:
    """Per-bundle execution state, mutated by dispatch_cycle on each step."""

    prompt: str
    bundle_id: str
    recipe_id: str
    iteration: int = 0  # global tick counter, advanced by Orchestrator if used
    task_results: dict[str, TaskNodeResult] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Deps (constructed once, read-only during execution)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrchestratorDeps:
    """Read-only dependencies threaded through every step.

    The graph runner constructs this once per bundle (in
    GraphRunnerController, step 4) and passes it to graph.iter(...).
    dispatch_cycle reads from it; nothing in this class is mutated.

    Fields:
        db:           sqlite connection — passed to fresh repo instances
                      per call (don't cache repos here, the connection is
                      shared and repos are cheap).
        http:         shared httpx client for A2A POSTs.
        watch:        krewhub WatchService for emitting MODIFIED events on
                      task rows so cookrew SSE picks up state changes.
        task_id_map:  node_id (graph step name) → krewhub task row id.
                      Built when the bundle's tasks are created from the
                      validated graph structure.
        cookbook_id:  scopes agent discovery to a single cookbook.
        recipe_meta:  metadata forwarded in A2A message.metadata so the
                      gateway can resolve the repo/branch context.
        poll_interval: seconds between task-status reads in the cycle.
        task_timeout:  hard ceiling per attempt before timeout.
    """

    db: "aiosqlite.Connection"
    http: "httpx.AsyncClient"
    watch: "WatchService"
    task_id_map: dict[str, str]
    cookbook_id: str
    recipe_meta: dict[str, str] = field(default_factory=dict)
    poll_interval: float = 2.0
    task_timeout: float = 300.0
