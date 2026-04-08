"""Restricted exec wrapper for LLM-generated pydantic-graph code.

Order of operations in execute_graph_code:

    1. validate_graph_code(code)        — AST allowlist (raises GraphValidationError)
    2. exec(code, namespace)            — runs in a stripped namespace
    3. assert `graph` is a pydantic-graph Graph
    4. return graph

Trust model — read this carefully:

The **validator** is the only real security boundary. It is an AST allowlist
that rejects any reference to `os`, `sys`, `httpx`, `__import__`, `eval`,
`open`, dunder/private attributes, imports, classes, try/except, etc. before
any code runs.

The **exec namespace** is *not* a sandbox. We deliberately pass the full
`builtins` module because pydantic-graph's own internals call `__import__`
(via typing/inspect/dataclass machinery) while building the Graph. Restricted
builtins broke the library without adding real safety — Python's `exec` has
never been a security boundary, only the AST validator is. Defense in depth
here means: keep the validator strict, never weaken it.

What we *do* control via the namespace:
    - Inject GraphBuilder, StepContext, reduce_list_append (the API)
    - Inject OrchestratorState/Deps and dispatch_cycle (the data + side effect)
    - Don't inject anything else — names like `os`, `httpx`, `subprocess`
      remain unbound, so even though `__import__` exists in builtins, the
      validator already rejected any user reference to it.
"""

from __future__ import annotations

import builtins
from typing import Any, Awaitable, Callable

from pydantic_graph.beta import GraphBuilder, StepContext
from pydantic_graph.beta.join import reduce_list_append

from krewhub.services.graph_sandbox.errors import GraphExecError
from krewhub.services.graph_sandbox.validator import (
    INJECTED_NAMES,
    validate_graph_code,
)

# Type alias for the dispatch_cycle helper signature. The real implementation
# lives in krewhub.services.graph_sandbox.dispatch (forthcoming step 3) — for
# now we accept any async callable so unit tests can inject a fake.
DispatchCycle = Callable[..., Awaitable[str]]


async def _default_dispatch_cycle_stub(*_args: Any, **_kwargs: Any) -> str:
    """Placeholder so validated code can be exec'd in tests without a real cycle.

    Once the real dispatch_cycle (which talks to A2A gateways) lands in step 3,
    callers will pass it explicitly via the `dispatch_cycle` parameter.
    """
    return "stub: dispatch_cycle not configured"


def _build_namespace(
    *,
    orchestrator_state_cls: type,
    orchestrator_deps_cls: type,
    dispatch_cycle: DispatchCycle,
) -> dict[str, Any]:
    """Construct the exec globals dict given the injected dependencies.

    `__builtins__` is intentionally the full module — see the module docstring
    for why. The validator is the gate that prevents user code from reaching
    anything dangerous in builtins.
    """
    namespace: dict[str, Any] = {
        "__builtins__": builtins.__dict__,
        "GraphBuilder": GraphBuilder,
        "StepContext": StepContext,
        "reduce_list_append": reduce_list_append,
        "dispatch_cycle": dispatch_cycle,
        "OrchestratorState": orchestrator_state_cls,
        "OrchestratorDeps": orchestrator_deps_cls,
    }
    # Sanity check: every name in INJECTED_NAMES must be present so the
    # validator's reference checks line up with the runtime namespace.
    missing = INJECTED_NAMES - set(namespace.keys())
    if missing:
        raise GraphExecError(f"namespace missing injected names: {sorted(missing)}")
    return namespace


def execute_graph_code(
    code: str,
    *,
    orchestrator_state_cls: type,
    orchestrator_deps_cls: type,
    dispatch_cycle: DispatchCycle | None = None,
) -> Any:
    """Validate and exec LLM-generated graph code, return the built Graph.

    Args:
        code: Python source defining a `graph` variable via GraphBuilder.
        orchestrator_state_cls: dataclass exposed as `OrchestratorState` in code.
        orchestrator_deps_cls: dataclass exposed as `OrchestratorDeps` in code.
        dispatch_cycle: async callable exposed as `dispatch_cycle` in code.
            If None, a stub is injected (tests / dry runs only).

    Raises:
        GraphValidationError: AST validation failed.
        GraphExecError:       exec failed, or `graph` was not produced /
                              wasn't a pydantic-graph Graph.
    """
    validate_graph_code(code)

    namespace = _build_namespace(
        orchestrator_state_cls=orchestrator_state_cls,
        orchestrator_deps_cls=orchestrator_deps_cls,
        dispatch_cycle=dispatch_cycle or _default_dispatch_cycle_stub,
    )

    try:
        compiled = compile(code, "<graph_sandbox>", "exec")
        exec(compiled, namespace)  # noqa: S102 — sandboxed by validator + restricted globals
    except GraphExecError:
        raise
    except Exception as exc:
        raise GraphExecError(f"graph code execution failed: {exc}") from exc

    graph = namespace.get("graph")
    if graph is None:
        raise GraphExecError("graph code did not assign a `graph` variable")

    if not _looks_like_graph(graph):
        raise GraphExecError(
            f"`graph` is not a pydantic-graph Graph (got {type(graph).__name__})"
        )

    return graph


def _looks_like_graph(obj: Any) -> bool:
    """Duck-type check for a built pydantic-graph Graph.

    We don't import the concrete class because the beta API may evolve;
    instead we look for the structural surface we rely on downstream
    (nodes dict + an iter/run method).
    """
    has_nodes = hasattr(obj, "nodes") and isinstance(getattr(obj, "nodes"), dict)
    has_run = hasattr(obj, "run") or hasattr(obj, "iter")
    return has_nodes and has_run
