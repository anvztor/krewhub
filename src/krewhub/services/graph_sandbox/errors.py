"""Errors raised by the graph sandbox."""

from __future__ import annotations


class GraphValidationError(Exception):
    """Raised when LLM-generated graph code fails the AST allowlist check.

    Attributes:
        violations: list of human-readable rule violations
        line: first offending line number, if any
    """

    def __init__(self, violations: list[str], line: int | None = None) -> None:
        self.violations = list(violations)
        self.line = line
        head = self.violations[0] if self.violations else "unknown violation"
        more = f" (+{len(self.violations) - 1} more)" if len(self.violations) > 1 else ""
        loc = f" at line {line}" if line is not None else ""
        super().__init__(f"graph code validation failed{loc}: {head}{more}")


class GraphExecError(Exception):
    """Raised when validated graph code fails to exec or doesn't yield a Graph."""
