"""Sandboxed validator + executor + renderer for LLM-generated pydantic-graph code.

Public API:
    - validate_graph_code:     AST-only check, no execution
    - execute_graph_code:      validate + restricted exec, returns the built Graph
    - extract_graph_structure: pull (node_ids, edges) out of a built Graph
    - render_graph:            render a built Graph as mermaid flowchart
    - RenderedGraph:           dataclass returned by render_graph
    - GraphValidationError, GraphExecError

The trust boundary: any Python source coming from an LLM must pass
validate_graph_code() before reaching exec(). The validator uses an
allowlist-based AST walker — anything not explicitly permitted is rejected.

Typical bundle_service usage:

    graph = execute_graph_code(code, ..., dispatch_cycle=cycle)
    node_ids, edges = extract_graph_structure(graph)
    rendered = render_graph(graph, direction="LR")
    # → create task rows from node_ids + edges
    # → publish rendered.mermaid to cookrew via SSE
"""

from krewhub.services.graph_sandbox.errors import (
    GraphExecError,
    GraphValidationError,
)
from krewhub.services.graph_sandbox.renderer import (
    RenderedGraph,
    extract_graph_structure,
    render_graph,
)
from krewhub.services.graph_sandbox.sandbox import execute_graph_code
from krewhub.services.graph_sandbox.validator import validate_graph_code

__all__ = [
    "GraphExecError",
    "GraphValidationError",
    "RenderedGraph",
    "execute_graph_code",
    "extract_graph_structure",
    "render_graph",
    "validate_graph_code",
]
