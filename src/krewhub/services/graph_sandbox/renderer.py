"""Mermaid flowchart renderer for pydantic-graph (beta) Graph objects.

Two responsibilities:

    1. extract_graph_structure(graph) → (node_ids, edges)
       Pulls the runnable step nodes + their edges out of a built Graph,
       skipping internal __start__/__end__/Fork/Join scaffolding.

    2. render_graph(graph, *, direction) → RenderedGraph
       Produces a mermaid flowchart string suitable for cookrew display,
       with root/leaf styling.

These are deliberately separate functions: the bundle service needs the
(node_ids, edges) tuple to create krewhub task rows with proper dependencies,
and *also* needs the mermaid string for cookrew. Both come from the same
extraction pass — the renderer just composes them.

This module is the krewhub-side equivalent of
krewcli/workflows/graph_renderer.py. The TaskSpec-based path was dropped
because TaskSpec is a krewcli domain object that has no place in krewhub.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RenderedGraph:
    """Output of mermaid rendering."""

    mermaid: str
    node_count: int
    edge_count: int


# ---------------------------------------------------------------------------
# Direction validation
# ---------------------------------------------------------------------------


_VALID_DIRECTIONS: frozenset[str] = frozenset({"TD", "TB", "BT", "LR", "RL"})


# ---------------------------------------------------------------------------
# Structure extraction
# ---------------------------------------------------------------------------


# Internal node names that pydantic-graph beta uses for scaffolding.
_INTERNAL_NODE_NAMES: frozenset[str] = frozenset({"__start__", "__end__"})

# Internal node *types* (by class name) that we skip even if they have a
# user-visible name. Fork/Join nodes are graph topology auto-inserted by the
# beta GraphBuilder when a step has multiple outgoing or incoming edges.
_INTERNAL_NODE_TYPES: frozenset[str] = frozenset({
    "StartNode",
    "EndNode",
    "Fork",
    "Join",
})


def extract_graph_structure(graph: Any) -> tuple[list[str], list[tuple[str, str]]]:
    """Extract user-step node IDs and their edges from a built Graph.

    Returns:
        (node_ids, edges) where edges are (source, target) name tuples and
        both source and target are guaranteed to be in node_ids.

    Internal scaffolding (start/end/fork/join) is filtered out. When a user
    step's outgoing edge passes through a fork/join before reaching another
    user step, the fork/join is collapsed and the edge is recorded as a
    direct user→user link. This matches what cookrew should display.
    """
    if not (hasattr(graph, "nodes") and isinstance(getattr(graph, "nodes"), dict)):
        logger.warning("graph %s has no nodes dict; cannot extract structure", type(graph))
        return [], []

    node_ids: list[str] = []
    for name, node in graph.nodes.items():
        if _is_internal(name, node):
            continue
        node_ids.append(name)

    edges = _extract_edges(graph, set(node_ids))
    return node_ids, edges


def _is_internal(name: str, node: Any) -> bool:
    """Return True if a (name, node) pair is graph scaffolding, not a user step."""
    if name in _INTERNAL_NODE_NAMES:
        return True
    if type(node).__name__ in _INTERNAL_NODE_TYPES:
        return True
    return False


def _extract_edges(graph: Any, user_nodes: set[str]) -> list[tuple[str, str]]:
    """Pull user→user edges out of a beta GraphBuilder graph.

    Walks `graph.edges_by_source` and, whenever an edge points to an internal
    fork/join node, transitively follows that node's outgoing edges until it
    reaches user steps. The result is the collapsed user-facing topology.

    Deduplicates edges (a fanout through a fork can otherwise produce the
    same (src, dst) pair multiple times) and preserves first-seen order so
    cookrew renders consistently across runs.
    """
    edges_by_source = getattr(graph, "edges_by_source", None)
    if not isinstance(edges_by_source, dict):
        return []

    seen: set[tuple[str, str]] = set()
    edges: list[tuple[str, str]] = []

    for src in user_nodes:
        for dst in _resolve_user_destinations(graph, edges_by_source, src):
            if dst in user_nodes:
                pair = (src, dst)
                if pair not in seen:
                    seen.add(pair)
                    edges.append(pair)

    return edges


def _resolve_user_destinations(
    graph: Any,
    edges_by_source: dict[str, Any],
    source_id: str,
) -> list[str]:
    """Resolve a source's outgoing destinations to the nearest user steps.

    For each Path leaving `source_id`, take its terminal DestinationMarker
    and either (a) accept it if it's a user step, or (b) recurse through it
    if it's an internal Fork/Join/Start/End. Cycles are guarded by `visited`.
    """
    visited: set[str] = set()
    results: list[str] = []

    def walk(current: str) -> None:
        if current in visited:
            return
        visited.add(current)

        for path in edges_by_source.get(current, []) or []:
            dest_id = _destination_of_path(path)
            if dest_id is None:
                continue
            dest_node = graph.nodes.get(dest_id)
            if dest_node is None:
                continue
            if _is_internal(dest_id, dest_node):
                walk(dest_id)
            else:
                results.append(dest_id)

    # Start one step out from the source — we don't recurse back through
    # source itself.
    for path in edges_by_source.get(source_id, []) or []:
        dest_id = _destination_of_path(path)
        if dest_id is None:
            continue
        dest_node = graph.nodes.get(dest_id)
        if dest_node is None:
            continue
        if _is_internal(dest_id, dest_node):
            walk(dest_id)
        else:
            results.append(dest_id)

    return results


def _destination_of_path(path: Any) -> str | None:
    """Extract the terminal DestinationMarker.destination_id from a Path.

    Beta API: `Path(items=[..., DestinationMarker(destination_id="...")])`.
    We take the last item with a `destination_id` attribute, which is the
    actual edge target. Returns None if the path shape is unexpected.
    """
    items = getattr(path, "items", None)
    if not items:
        return None
    for item in reversed(list(items)):
        dest = getattr(item, "destination_id", None)
        if isinstance(dest, str):
            return dest
    return None


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def render_graph(graph: Any, *, direction: str = "TD") -> RenderedGraph:
    """Render a built Graph as a mermaid flowchart with root/leaf styling.

    Args:
        graph: a pydantic-graph (beta) Graph object.
        direction: mermaid flowchart direction — TD/TB/BT/LR/RL.

    Raises:
        ValueError: if direction is invalid.

    Returns:
        RenderedGraph with mermaid source, node count, and edge count.
    """
    if direction not in _VALID_DIRECTIONS:
        raise ValueError(
            f"invalid direction {direction!r}; must be one of {sorted(_VALID_DIRECTIONS)}"
        )

    node_ids, edges = extract_graph_structure(graph)

    if not node_ids:
        return RenderedGraph(mermaid=f"flowchart {direction}", node_count=0, edge_count=0)

    lines: list[str] = [f"flowchart {direction}"]

    incoming: dict[str, list[str]] = {nid: [] for nid in node_ids}
    outgoing: dict[str, list[str]] = {nid: [] for nid in node_ids}
    for src, dst in edges:
        outgoing[src].append(dst)
        incoming[dst].append(src)

    for nid in node_ids:
        label = _escape_mermaid(_humanize(nid))
        lines.append(f'    {_safe_id(nid)}["{label}"]')

    for src, dst in edges:
        lines.append(f"    {_safe_id(src)} --> {_safe_id(dst)}")

    roots = [nid for nid in node_ids if not incoming[nid]]
    leaves = [nid for nid in node_ids if not outgoing[nid]]

    if roots:
        lines.append("    classDef root fill:#e8f5e9,stroke:#4caf50,stroke-width:2px")
        lines.append(f"    class {','.join(_safe_id(r) for r in roots)} root")

    if leaves:
        lines.append("    classDef leaf fill:#e3f2fd,stroke:#2196f3,stroke-width:2px")
        lines.append(f"    class {','.join(_safe_id(leaf) for leaf in leaves)} leaf")

    return RenderedGraph(
        mermaid="\n".join(lines),
        node_count=len(node_ids),
        edge_count=len(edges),
    )


# ---------------------------------------------------------------------------
# Label helpers
# ---------------------------------------------------------------------------


def _humanize(node_id: str) -> str:
    """Convert `scope_review` or `FeatureScope` into `Scope Review` / `Feature Scope`."""
    spaced = re.sub(r"(?<!^)(?=[A-Z])", " ", node_id)
    return spaced.replace("_", " ").title()


def _safe_id(node_id: str) -> str:
    """Coerce a node id into a mermaid-safe identifier (alphanumeric + underscore)."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", node_id)


def _escape_mermaid(text: str) -> str:
    """Escape backslashes and double quotes for mermaid label strings."""
    return text.replace("\\", "\\\\").replace('"', '\\"')
