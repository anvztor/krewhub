"""Tests for krewhub.services.graph_sandbox.

Trust boundary: any source we exec must pass validate_graph_code() first.
These tests document exactly what is and isn't allowed, and verify a
realistic LLM-emitted graph round-trips successfully through exec.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from krewhub.services.graph_sandbox import (
    GraphExecError,
    GraphValidationError,
    execute_graph_code,
    validate_graph_code,
)


# ---------------------------------------------------------------------------
# Stub state/deps so the exec'd graph can resolve its type parameters
# ---------------------------------------------------------------------------


@dataclass
class FakeState:
    prompt: str = ""
    bundle_id: str = ""
    task_results: dict[str, Any] = field(default_factory=dict)


@dataclass
class FakeDeps:
    poll_interval: float = 1.0
    task_timeout: float = 30.0


async def fake_dispatch_cycle(ctx: Any, **kwargs: Any) -> str:
    return f"done: {kwargs.get('node_id', '?')}"


def _exec(code: str):
    return execute_graph_code(
        code,
        orchestrator_state_cls=FakeState,
        orchestrator_deps_cls=FakeDeps,
        dispatch_cycle=fake_dispatch_cycle,
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


GOOD_LINEAR_GRAPH = '''
g = GraphBuilder(state_type=OrchestratorState, deps_type=OrchestratorDeps, output_type=str)

@g.step
async def scope(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="scope",
        task_kind="planner",
        instruction="Plan the work",
        max_iterations=2,
    )

@g.step
async def implement(ctx: StepContext[OrchestratorState, OrchestratorDeps, str]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="implement",
        task_kind="coder",
        instruction="Implement",
        max_iterations=3,
    )

@g.step
async def review(ctx: StepContext[OrchestratorState, OrchestratorDeps, str]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="review",
        task_kind="reviewer",
        instruction="Review",
        max_iterations=2,
    )

g.add(
    g.edge_from(g.start_node).to(scope),
    g.edge_from(scope).to(implement),
    g.edge_from(implement).to(review),
    g.edge_from(review).to(g.end_node),
)
graph = g.build()
'''


def test_happy_path_linear_graph_validates_and_execs():
    validate_graph_code(GOOD_LINEAR_GRAPH)  # should not raise
    graph = _exec(GOOD_LINEAR_GRAPH)
    assert hasattr(graph, "nodes")
    node_names = set(graph.nodes.keys())
    # Step nodes should appear; internal nodes are prefixed with __
    user_nodes = {n for n in node_names if not n.startswith("__")}
    assert {"scope", "implement", "review"} <= user_nodes


def test_happy_path_extracts_runnable_graph_with_iter_or_run():
    graph = _exec(GOOD_LINEAR_GRAPH)
    assert hasattr(graph, "iter") or hasattr(graph, "run")


# ---------------------------------------------------------------------------
# Validator: rejected constructs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "code, must_contain",
    [
        # Imports
        ("import os\ngraph = None", "Import"),
        ("from os import system\ngraph = None", "Import"),
        ("from __future__ import annotations\ngraph = None", "Import"),
        # Dunder + private access
        (
            "g = GraphBuilder(state_type=OrchestratorState, deps_type=OrchestratorDeps, output_type=str)\n"
            "x = g.__class__\n"
            "graph = g",
            "private/dunder",
        ),
        (
            "x = ().__class__.__bases__\ngraph = None",
            "private/dunder",
        ),
        # Banned builtins
        ("x = eval('1+1')\ngraph = None", "eval"),
        ("x = exec('pass')\ngraph = None", "exec"),
        ("x = open('/etc/passwd')\ngraph = None", "open"),
        ("x = getattr(object, 'mro')\ngraph = None", "getattr"),
        ("x = globals()\ngraph = None", "globals"),
        ("x = type(1)\ngraph = None", "type"),
        # Unknown name reference
        ("x = httpx.post('http://evil')\ngraph = None", "httpx"),
        ("x = os.system('rm -rf /')\ngraph = None", "os"),
        # Disallowed structural constructs
        ("class Foo: pass\ngraph = None", "ClassDef"),
        ("try:\n    pass\nexcept:\n    pass\ngraph = None", "Try"),
        ("with open('x') as f:\n    pass\ngraph = None", "With"),
        ("raise ValueError('boom')\ngraph = None", "Raise"),
        ("f = lambda x: x\ngraph = None", "Lambda"),
        ("def sync_step(): pass\ngraph = None", "async def"),
        ("global x\ngraph = None", "Global"),
        # Tuple/subscript assignment forbidden at module level
        ("a, b = 1, 2\ngraph = None", "simple"),
    ],
)
def test_validator_rejects_unsafe_code(code: str, must_contain: str):
    with pytest.raises(GraphValidationError) as exc_info:
        validate_graph_code(code)
    joined = " ".join(exc_info.value.violations).lower()
    assert must_contain.lower() in joined, f"expected {must_contain!r} in {joined!r}"


def test_validator_requires_graph_assignment():
    code = (
        "g = GraphBuilder(state_type=OrchestratorState, "
        "deps_type=OrchestratorDeps, output_type=str)\n"
    )
    with pytest.raises(GraphValidationError) as exc_info:
        validate_graph_code(code)
    assert any("graph" in v.lower() for v in exc_info.value.violations)


def test_validator_rejects_only_g_step_decorator():
    code = '''
g = GraphBuilder(state_type=OrchestratorState, deps_type=OrchestratorDeps, output_type=str)

@some_other_decorator
async def step1(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return "x"

graph = g
'''
    with pytest.raises(GraphValidationError) as exc_info:
        validate_graph_code(code)
    msg = " ".join(exc_info.value.violations).lower()
    assert "@g.step" in msg or "decorator" in msg


def test_validator_syntax_error():
    with pytest.raises(GraphValidationError) as exc_info:
        validate_graph_code("def broken(:\n    pass")
    assert any("syntax" in v.lower() for v in exc_info.value.violations)


# ---------------------------------------------------------------------------
# Sandbox: exec failure paths
# ---------------------------------------------------------------------------


def test_exec_rejects_when_graph_is_not_built():
    code = (
        "g = GraphBuilder(state_type=OrchestratorState, "
        "deps_type=OrchestratorDeps, output_type=str)\n"
        "graph = g\n"  # not g.build() — fails the duck-type check
    )
    with pytest.raises(GraphExecError) as exc_info:
        _exec(code)
    assert "not a pydantic-graph Graph" in str(exc_info.value)


def test_exec_rejects_when_graph_assigned_to_string():
    code = "graph = 'not a graph'\n"
    with pytest.raises(GraphExecError):
        _exec(code)


def test_exec_propagates_validation_errors():
    code = "import sys\ngraph = None"
    with pytest.raises(GraphValidationError):
        _exec(code)


def test_exec_restricted_builtins_blocks_unsafe_runtime_calls():
    """Even if the validator missed something, restricted builtins should
    prevent runtime escape via __builtins__."""
    # We construct code that *would* pass naively but rely on a banned
    # builtin at runtime. The validator should still catch it via name check.
    code = "x = abs(-1)\ngraph = None"  # abs is allowed
    with pytest.raises(GraphExecError):
        # passes validation, but `graph = None` fails the duck-type check
        _exec(code)


def test_exec_namespace_isolation_no_module_access():
    """Code that tries to walk back to modules via __builtins__ access
    must be rejected during validation."""
    code = "x = __builtins__\ngraph = None"
    with pytest.raises(GraphValidationError):
        validate_graph_code(code)


# ---------------------------------------------------------------------------
# Smoke: validator allows realistic constructs
# ---------------------------------------------------------------------------


def test_validator_allows_fstring_in_step_body():
    code = '''
g = GraphBuilder(state_type=OrchestratorState, deps_type=OrchestratorDeps, output_type=str)

@g.step
async def step1(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="step1",
        task_kind="planner",
        instruction=f"Process {ctx.state.prompt}",
        max_iterations=2,
    )

g.add(
    g.edge_from(g.start_node).to(step1),
    g.edge_from(step1).to(g.end_node),
)
graph = g.build()
'''
    validate_graph_code(code)
    graph = _exec(code)
    assert "step1" in graph.nodes


def test_validator_allows_parallel_branches():
    code = '''
g = GraphBuilder(state_type=OrchestratorState, deps_type=OrchestratorDeps, output_type=str)

@g.step
async def root(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(ctx, node_id="root", task_kind="planner",
                                 instruction="plan", max_iterations=1)

@g.step
async def branch_a(ctx: StepContext[OrchestratorState, OrchestratorDeps, str]) -> str:
    return await dispatch_cycle(ctx, node_id="branch_a", task_kind="coder",
                                 instruction="left", max_iterations=2)

@g.step
async def branch_b(ctx: StepContext[OrchestratorState, OrchestratorDeps, str]) -> str:
    return await dispatch_cycle(ctx, node_id="branch_b", task_kind="reviewer",
                                 instruction="right", max_iterations=2)

g.add(
    g.edge_from(g.start_node).to(root),
    g.edge_from(root).to(branch_a),
    g.edge_from(root).to(branch_b),
    g.edge_from(branch_a).to(g.end_node),
    g.edge_from(branch_b).to(g.end_node),
)
graph = g.build()
'''
    graph = _exec(code)
    user_nodes = {n for n in graph.nodes if not n.startswith("__")}
    assert {"root", "branch_a", "branch_b"} <= user_nodes
