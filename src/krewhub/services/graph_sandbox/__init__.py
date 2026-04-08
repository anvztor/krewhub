"""Sandboxed validator + executor for LLM-generated pydantic-graph code.

Public API:
    - validate_graph_code: AST-only check, no execution
    - execute_graph_code:  validate + restricted exec, returns the built Graph
    - GraphValidationError, GraphExecError

The trust boundary: any Python source coming from an LLM must pass
validate_graph_code() before reaching exec(). The validator uses an
allowlist-based AST walker — anything not explicitly permitted is rejected.
"""

from krewhub.services.graph_sandbox.errors import (
    GraphExecError,
    GraphValidationError,
)
from krewhub.services.graph_sandbox.sandbox import execute_graph_code
from krewhub.services.graph_sandbox.validator import validate_graph_code

__all__ = [
    "GraphExecError",
    "GraphValidationError",
    "execute_graph_code",
    "validate_graph_code",
]
