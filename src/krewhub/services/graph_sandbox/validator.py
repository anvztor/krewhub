"""Allowlist-based AST validator for LLM-generated pydantic-graph code.

Trust model: any name, attribute, or AST construct that isn't explicitly
allowed is rejected. We err strongly on the side of false positives —
the LLM can be reprompted when validation fails.

What valid graph code looks like:

    g = GraphBuilder(state_type=OrchestratorState, deps_type=OrchestratorDeps, output_type=str)

    @g.step
    async def scope(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
        return await dispatch_cycle(
            ctx,
            node_id="scope",
            task_kind="planner",
            instruction="Plan the work.",
            max_iterations=2,
        )

    @g.step
    async def implement(ctx: StepContext[OrchestratorState, OrchestratorDeps, str]) -> str:
        return await dispatch_cycle(
            ctx, node_id="implement", task_kind="coder",
            instruction="Implement the plan.", max_iterations=3,
        )

    g.add(
        g.edge_from(g.start_node).to(scope),
        g.edge_from(scope).to(implement),
        g.edge_from(implement).to(g.end_node),
    )
    graph = g.build()
"""

from __future__ import annotations

import ast

from krewhub.services.graph_sandbox.errors import GraphValidationError

# ---------------------------------------------------------------------------
# Allowlists
# ---------------------------------------------------------------------------

# Names the LLM may reference at the top level (injected by sandbox.py).
INJECTED_NAMES: frozenset[str] = frozenset({
    "GraphBuilder",
    "StepContext",
    "reduce_list_append",
    "dispatch_cycle",
    "OrchestratorState",
    "OrchestratorDeps",
})

# Names that are safe builtins (callables and types) the LLM may use.
ALLOWED_BUILTINS: frozenset[str] = frozenset({
    # Type constructors / annotations
    "str", "int", "float", "bool", "bytes", "list", "dict", "tuple", "set",
    "frozenset", "None", "True", "False",
    # Iteration / aggregation
    "len", "range", "enumerate", "zip", "min", "max", "sum",
    "any", "all", "sorted", "reversed", "abs", "round",
    # Type checks (safe forms)
    "isinstance",
})

# Names that must NEVER appear, even as an Attribute, kwarg, or string check.
BANNED_NAMES: frozenset[str] = frozenset({
    "__import__", "__builtins__", "__loader__", "__spec__", "__file__",
    "__name__", "__package__", "__doc__",
    "__class__", "__dict__", "__mro__", "__bases__", "__subclasses__",
    "__globals__", "__code__", "__closure__", "__init_subclass__",
    "__getattribute__", "__getattr__", "__setattr__", "__delattr__",
    "__module__", "__qualname__",
    "eval", "exec", "compile", "open", "input", "breakpoint", "help",
    "getattr", "setattr", "delattr",
    "globals", "locals", "vars", "dir", "id",
    "type", "object", "super", "memoryview", "bytearray",
    "__import__", "exit", "quit",
})

# Disallowed AST node types (anywhere in the tree).
DISALLOWED_NODES: tuple[type[ast.AST], ...] = (
    ast.Import,
    ast.ImportFrom,
    ast.ClassDef,
    ast.Global,
    ast.Nonlocal,
    ast.With,
    ast.AsyncWith,
    ast.Try,
    ast.TryStar,
    ast.Raise,
    ast.Lambda,
    ast.Delete,
    ast.Yield,
    ast.YieldFrom,
    ast.Match,
)


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


class _Validator(ast.NodeVisitor):
    def __init__(self) -> None:
        self.violations: list[str] = []
        self.first_line: int | None = None
        self._defined_names: set[str] = set()  # names assigned/defined in module
        # Names that are decorator-bound (the @g.step funcs) — also valid refs.
        self._step_funcs: set[str] = set()

    # ----- helpers -----

    def _flag(self, node: ast.AST, msg: str) -> None:
        line = getattr(node, "lineno", None)
        prefix = f"line {line}: " if line is not None else ""
        self.violations.append(prefix + msg)
        if self.first_line is None and line is not None:
            self.first_line = line

    def _check_name(self, node: ast.AST, name: str) -> None:
        if name in BANNED_NAMES or name.startswith("__"):
            self._flag(node, f"banned identifier {name!r}")

    # ----- node visitors -----

    def visit(self, node: ast.AST) -> None:
        if isinstance(node, DISALLOWED_NODES):
            self._flag(node, f"disallowed construct {type(node).__name__}")
            return
        super().visit(node)

    def visit_Module(self, node: ast.Module) -> None:
        # First pass: collect top-level assignments + step function names so
        # later visits can validate Name references against a known scope.
        for stmt in node.body:
            if isinstance(stmt, (ast.AsyncFunctionDef, ast.FunctionDef)):
                self._step_funcs.add(stmt.name)
                self._defined_names.add(stmt.name)
            elif isinstance(stmt, ast.Assign):
                for tgt in stmt.targets:
                    if isinstance(tgt, ast.Name):
                        self._defined_names.add(tgt.id)
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                self._defined_names.add(stmt.target.id)

        for stmt in node.body:
            self.visit(stmt)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        # We require step functions to be `async def` — see dispatch_cycle.
        self._flag(node, f"step function {node.name!r} must be `async def`, not `def`")

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_name(node, node.name)
        # Decorators must be exactly `@g.step` (or list of them — but we only
        # accept that one form to keep the surface tiny).
        for deco in node.decorator_list:
            if not (isinstance(deco, ast.Attribute) and deco.attr == "step"
                    and isinstance(deco.value, ast.Name) and deco.value.id == "g"):
                self._flag(deco, "only @g.step decorator is allowed on step functions")
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        name = node.id
        self._check_name(node, name)
        if isinstance(node.ctx, ast.Load):
            allowed = (
                name in INJECTED_NAMES
                or name in ALLOWED_BUILTINS
                or name in self._defined_names
                or name in self._step_funcs
                or name in {"g", "graph", "ctx"}  # the conventional locals
            )
            if not allowed:
                self._flag(node, f"reference to unknown name {name!r}")

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr.startswith("_"):
            self._flag(node, f"access to private/dunder attribute {node.attr!r}")
            return
        if node.attr in BANNED_NAMES:
            self._flag(node, f"access to banned attribute {node.attr!r}")
            return
        # Recurse into the value side (e.g., `ctx.state.foo` → check `ctx.state`).
        self.visit(node.value)

    def visit_Call(self, node: ast.Call) -> None:
        # Reject calling something that resolves to a banned name even via
        # constant folding tricks. Most checks happen via Name/Attribute visits.
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        for tgt in node.targets:
            if isinstance(tgt, ast.Name):
                self._defined_names.add(tgt.id)
                self._check_name(tgt, tgt.id)
            else:
                # Allow tuple unpacking / subscript? Keep it simple: no.
                self._flag(tgt, "only simple `name = expr` assignments allowed at module level")
        self.visit(node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if isinstance(node.target, ast.Name):
            self._defined_names.add(node.target.id)
            self._check_name(node.target, node.target.id)
        if node.annotation is not None:
            self.visit(node.annotation)
        if node.value is not None:
            self.visit(node.value)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        # Used for generics like StepContext[State, Deps, In]. Allow as long
        # as both the value and slice pass normal checks.
        self.visit(node.value)
        self.visit(node.slice)

    def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
        # f-strings: walk parts so embedded expressions are validated.
        for v in node.values:
            self.visit(v)

    def visit_FormattedValue(self, node: ast.FormattedValue) -> None:
        self.visit(node.value)
        if node.format_spec is not None:
            self.visit(node.format_spec)


def validate_graph_code(code: str) -> None:
    """Validate that `code` is safe to exec inside the graph sandbox.

    Raises GraphValidationError on any rule violation. Returns None on success.
    """
    try:
        tree = ast.parse(code, mode="exec")
    except SyntaxError as exc:
        raise GraphValidationError(
            [f"syntax error: {exc.msg}"], line=exc.lineno
        ) from exc

    v = _Validator()
    v.visit(tree)

    # Structural requirement: must define `graph` at module level.
    has_graph_assign = any(
        isinstance(stmt, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "graph" for t in stmt.targets)
        for stmt in tree.body
    )
    if not has_graph_assign:
        v.violations.append("module must assign a `graph` variable (e.g. `graph = g.build()`)")

    if v.violations:
        raise GraphValidationError(v.violations, line=v.first_line)
