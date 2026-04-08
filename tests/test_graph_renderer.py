"""Tests for krewhub.services.graph_sandbox.renderer.

These exercise the structure-extraction + mermaid-rendering surface against
real built Graphs from execute_graph_code, so any drift in the pydantic-graph
beta API surfaces here rather than in production.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from krewhub.services.graph_sandbox import (
    RenderedGraph,
    execute_graph_code,
    extract_graph_structure,
    render_graph,
)
from krewhub.services.graph_sandbox.renderer import (
    _escape_mermaid,
    _humanize,
    _safe_id,
)


# ---------------------------------------------------------------------------
# Stubs
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
# Fixtures: realistic graphs
# ---------------------------------------------------------------------------


LINEAR_GRAPH = '''
g = GraphBuilder(state_type=OrchestratorState, deps_type=OrchestratorDeps, output_type=str)

@g.step
async def scope(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(ctx, node_id="scope", task_kind="planner",
                                 instruction="plan", max_iterations=1)

@g.step
async def implement(ctx: StepContext[OrchestratorState, OrchestratorDeps, str]) -> str:
    return await dispatch_cycle(ctx, node_id="implement", task_kind="coder",
                                 instruction="impl", max_iterations=1)

@g.step
async def review(ctx: StepContext[OrchestratorState, OrchestratorDeps, str]) -> str:
    return await dispatch_cycle(ctx, node_id="review", task_kind="reviewer",
                                 instruction="review", max_iterations=1)

g.add(
    g.edge_from(g.start_node).to(scope),
    g.edge_from(scope).to(implement),
    g.edge_from(implement).to(review),
    g.edge_from(review).to(g.end_node),
)
graph = g.build()
'''


PARALLEL_GRAPH = '''
g = GraphBuilder(state_type=OrchestratorState, deps_type=OrchestratorDeps, output_type=str)

@g.step
async def root(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(ctx, node_id="root", task_kind="planner",
                                 instruction="plan", max_iterations=1)

@g.step
async def branch_a(ctx: StepContext[OrchestratorState, OrchestratorDeps, str]) -> str:
    return await dispatch_cycle(ctx, node_id="branch_a", task_kind="coder",
                                 instruction="left", max_iterations=1)

@g.step
async def branch_b(ctx: StepContext[OrchestratorState, OrchestratorDeps, str]) -> str:
    return await dispatch_cycle(ctx, node_id="branch_b", task_kind="reviewer",
                                 instruction="right", max_iterations=1)

g.add(
    g.edge_from(g.start_node).to(root),
    g.edge_from(root).to(branch_a),
    g.edge_from(root).to(branch_b),
    g.edge_from(branch_a).to(g.end_node),
    g.edge_from(branch_b).to(g.end_node),
)
graph = g.build()
'''


SINGLE_NODE_GRAPH = '''
g = GraphBuilder(state_type=OrchestratorState, deps_type=OrchestratorDeps, output_type=str)

@g.step
async def only(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(ctx, node_id="only", task_kind="planner",
                                 instruction="solo", max_iterations=1)

g.add(
    g.edge_from(g.start_node).to(only),
    g.edge_from(only).to(g.end_node),
)
graph = g.build()
'''


# ---------------------------------------------------------------------------
# extract_graph_structure
# ---------------------------------------------------------------------------


def test_extract_linear_returns_user_nodes_only():
    graph = _exec(LINEAR_GRAPH)
    node_ids, _edges = extract_graph_structure(graph)
    assert set(node_ids) == {"scope", "implement", "review"}
    # No __start__/__end__/Fork/Join leakage
    assert all(not nid.startswith("__") for nid in node_ids)


def test_extract_linear_edges_form_a_chain():
    graph = _exec(LINEAR_GRAPH)
    _node_ids, edges = extract_graph_structure(graph)
    edge_set = set(edges)
    assert ("scope", "implement") in edge_set
    assert ("implement", "review") in edge_set
    # No edges into/out of start/end (those endpoints were filtered)
    for src, dst in edges:
        assert not src.startswith("__")
        assert not dst.startswith("__")


def test_extract_parallel_branches_have_correct_fanout():
    graph = _exec(PARALLEL_GRAPH)
    node_ids, edges = extract_graph_structure(graph)
    assert set(node_ids) == {"root", "branch_a", "branch_b"}
    edge_set = set(edges)
    assert ("root", "branch_a") in edge_set
    assert ("root", "branch_b") in edge_set


def test_extract_single_node_has_no_user_edges():
    graph = _exec(SINGLE_NODE_GRAPH)
    node_ids, edges = extract_graph_structure(graph)
    assert node_ids == ["only"]
    # The only edges are start→only and only→end, both filtered out.
    for src, dst in edges:
        assert src in {"only"}
        assert dst in {"only"}


def test_extract_handles_object_with_no_nodes_attr():
    """Defensive: passing something that isn't a Graph returns empty tuples."""
    node_ids, edges = extract_graph_structure(object())
    assert node_ids == []
    assert edges == []


# ---------------------------------------------------------------------------
# render_graph
# ---------------------------------------------------------------------------


def test_render_linear_produces_flowchart_with_nodes_and_arrows():
    graph = _exec(LINEAR_GRAPH)
    rendered = render_graph(graph, direction="LR")
    assert isinstance(rendered, RenderedGraph)
    assert rendered.mermaid.startswith("flowchart LR")
    assert rendered.node_count == 3
    assert rendered.edge_count >= 2  # at least scope→implement and implement→review

    body = rendered.mermaid
    # Each user node appears as `id["Label"]`
    assert 'scope["Scope"]' in body
    assert 'implement["Implement"]' in body
    assert 'review["Review"]' in body
    # Arrows between user nodes
    assert "scope --> implement" in body
    assert "implement --> review" in body


def test_render_parallel_marks_root_and_leaves():
    graph = _exec(PARALLEL_GRAPH)
    rendered = render_graph(graph, direction="TD")
    body = rendered.mermaid
    # root has no incoming user edges → marked as root
    assert "classDef root" in body
    assert "class root root" in body
    # branch_a and branch_b have no outgoing user edges → both marked leaf
    assert "classDef leaf" in body
    assert "branch_a" in body
    assert "branch_b" in body


def test_render_single_node_marks_it_as_both_root_and_leaf():
    graph = _exec(SINGLE_NODE_GRAPH)
    rendered = render_graph(graph)
    body = rendered.mermaid
    assert 'only["Only"]' in body
    # No user-to-user edges → it's both a root and a leaf
    assert "classDef root" in body
    assert "classDef leaf" in body
    assert "class only root" in body
    assert "class only leaf" in body
    assert rendered.node_count == 1
    assert rendered.edge_count == 0


def test_render_default_direction_is_TD():
    graph = _exec(LINEAR_GRAPH)
    rendered = render_graph(graph)
    assert rendered.mermaid.startswith("flowchart TD")


@pytest.mark.parametrize("direction", ["TD", "TB", "BT", "LR", "RL"])
def test_render_accepts_all_valid_directions(direction: str):
    graph = _exec(LINEAR_GRAPH)
    rendered = render_graph(graph, direction=direction)
    assert rendered.mermaid.startswith(f"flowchart {direction}")


@pytest.mark.parametrize("direction", ["XX", "down", "td", "", "TDB"])
def test_render_rejects_invalid_direction(direction: str):
    graph = _exec(LINEAR_GRAPH)
    with pytest.raises(ValueError, match="invalid direction"):
        render_graph(graph, direction=direction)


def test_render_empty_structure_returns_minimal_flowchart():
    """A graph with no extractable user nodes still renders a valid header."""
    rendered = render_graph(_FakeEmptyGraph(), direction="LR")
    assert rendered.mermaid == "flowchart LR"
    assert rendered.node_count == 0
    assert rendered.edge_count == 0


class _FakeEmptyGraph:
    """Stand-in for a Graph whose only nodes are internal scaffolding."""

    nodes: dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Label helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("scope", "Scope"),
        ("scope_review", "Scope Review"),
        ("FeatureScope", "Feature Scope"),
        ("HTTPRequest", "H T T P Request"),  # acronyms split per uppercase boundary
        ("multi_word_step", "Multi Word Step"),
        ("a", "A"),
    ],
)
def test_humanize_handles_snake_and_camel(raw: str, expected: str):
    assert _humanize(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("scope", "scope"),
        ("scope-1", "scope_1"),
        ("scope.review", "scope_review"),
        ("scope review", "scope_review"),
        ("a/b/c", "a_b_c"),
    ],
)
def test_safe_id_strips_special_chars(raw: str, expected: str):
    assert _safe_id(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ('plain', 'plain'),
        ('with "quotes"', 'with \\"quotes\\"'),
        (r"path\with\backslashes", r"path\\with\\backslashes"),
    ],
)
def test_escape_mermaid_quotes_and_backslashes(raw: str, expected: str):
    assert _escape_mermaid(raw) == expected


# ---------------------------------------------------------------------------
# End-to-end: validate → exec → extract → render
# ---------------------------------------------------------------------------


def test_e2e_compile_render_roundtrip():
    """The full pipeline a bundle_service will run on every codegen artifact."""
    graph = _exec(PARALLEL_GRAPH)
    node_ids, edges = extract_graph_structure(graph)
    rendered = render_graph(graph, direction="LR")

    # Every edge endpoint must be in node_ids (orphan-free)
    node_set = set(node_ids)
    for src, dst in edges:
        assert src in node_set
        assert dst in node_set

    # Mermaid string must mention every node
    for nid in node_ids:
        assert nid in rendered.mermaid

    # Edge count consistency between extraction and rendering
    assert rendered.edge_count == len(edges)
    assert rendered.node_count == len(node_ids)
