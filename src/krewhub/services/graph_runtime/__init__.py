"""Runtime side of krewhub graph execution.

graph_sandbox builds Graph objects from LLM source. graph_runtime runs them:

    - state.py        : OrchestratorState/Deps + result records
    - agent_picker.py : pick_agent_for_kind ranking
    - a2a.py          : dispatch_to_gateway JSON-RPC POST
    - polling.py      : wait_for_task_terminal
    - cycle.py        : dispatch_cycle — the per-step retry loop

Bundle service composition (forthcoming step 4):

    deps = OrchestratorDeps(db=..., http=..., watch=..., task_id_map=..., ...)
    state = OrchestratorState(prompt=..., bundle_id=...,)
    graph = execute_graph_code(code, ..., dispatch_cycle=dispatch_cycle)
    async with graph.iter(state=state, deps=deps) as run:
        async for node in run:
            ...
"""

from krewhub.services.graph_runtime.agent_picker import pick_agent_for_kind
from krewhub.services.graph_runtime.cycle import dispatch_cycle
from krewhub.services.graph_runtime.state import (
    AttemptRecord,
    OrchestratorDeps,
    OrchestratorState,
    TaskNodeResult,
)

__all__ = [
    "AttemptRecord",
    "OrchestratorDeps",
    "OrchestratorState",
    "TaskNodeResult",
    "dispatch_cycle",
    "pick_agent_for_kind",
]
