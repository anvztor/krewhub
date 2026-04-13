"""Built-in graph templates — predefined workflows that skip LLM generation.

Each template is a function that returns graph code as a string.
Templates can be parameterized (e.g., branch name, file paths).

Usage:
    POST /recipes/{id}/bundles with {"prompt": "...", "template": "code-review"}
    → krewhub creates the bundle AND attaches graph code immediately
    → GraphRunnerController picks it up on next reconcile (no planner dispatch)
"""

from __future__ import annotations

TEMPLATES: dict[str, "GraphTemplate"] = {}


class GraphTemplate:
    """A registered graph template."""

    def __init__(
        self,
        name: str,
        description: str,
        graph_code: str,
    ) -> None:
        self.name = name
        self.description = description
        self.graph_code = graph_code


def _register(name: str, description: str, code: str) -> None:
    TEMPLATES[name] = GraphTemplate(name=name, description=description, graph_code=code)


def get_template(name: str) -> GraphTemplate | None:
    return TEMPLATES.get(name)


def list_templates() -> list[dict[str, str]]:
    return [{"name": t.name, "description": t.description} for t in TEMPLATES.values()]


# ---------------------------------------------------------------------------
# Built-in templates
# ---------------------------------------------------------------------------

_register(
    "code-review",
    "Review code quality, security, and architecture of the current branch",
    """\
g = GraphBuilder(
    state_type=OrchestratorState,
    deps_type=OrchestratorDeps,
    output_type=str,
)

@g.step
async def scan_changes(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="scan_changes",
        task_kind="coder",
        instruction="List all modified files in the current branch compared to the default branch. Summarize what changed in each file.",
        max_iterations=2,
    )

@g.step
async def review_quality(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="review_quality",
        task_kind="reviewer",
        instruction="Review the code changes for quality issues: naming, readability, complexity, error handling, dead code, and adherence to project conventions.",
        max_iterations=2,
    )

@g.step
async def review_security(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="review_security",
        task_kind="reviewer",
        instruction="Review the code changes for security vulnerabilities: injection, XSS, auth bypass, secrets exposure, unsafe deserialization, and OWASP Top 10.",
        max_iterations=2,
    )

@g.step
async def synthesize_review(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="synthesize_review",
        task_kind="reviewer",
        instruction="Combine the quality and security review findings into a single review summary with severity ratings (critical/high/medium/low) and actionable recommendations.",
        max_iterations=2,
    )

g.add(
    g.edge_from(g.start_node).to(scan_changes),
    g.edge_from(scan_changes).to(review_quality),
    g.edge_from(scan_changes).to(review_security),
    g.edge_from(review_quality).to(synthesize_review),
    g.edge_from(review_security).to(synthesize_review),
    g.edge_from(synthesize_review).to(g.end_node),
)

graph = g.build()
""",
)

_register(
    "test-coverage",
    "Analyze test coverage gaps and generate missing tests",
    """\
g = GraphBuilder(
    state_type=OrchestratorState,
    deps_type=OrchestratorDeps,
    output_type=str,
)

@g.step
async def analyze_coverage(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="analyze_coverage",
        task_kind="tester",
        instruction="Run existing tests and analyze coverage. Identify untested functions, branches, and edge cases.",
        max_iterations=2,
    )

@g.step
async def generate_tests(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="generate_tests",
        task_kind="tester",
        instruction="Write tests for the identified coverage gaps. Focus on critical paths and edge cases.",
        max_iterations=3,
    )

@g.step
async def verify_tests(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="verify_tests",
        task_kind="tester",
        instruction="Run all tests including the newly generated ones. Verify they pass and improve coverage.",
        max_iterations=2,
    )

g.add(
    g.edge_from(g.start_node).to(analyze_coverage),
    g.edge_from(analyze_coverage).to(generate_tests),
    g.edge_from(generate_tests).to(verify_tests),
    g.edge_from(verify_tests).to(g.end_node),
)

graph = g.build()
""",
)

_register(
    "refactor",
    "Analyze and refactor code for maintainability",
    """\
g = GraphBuilder(
    state_type=OrchestratorState,
    deps_type=OrchestratorDeps,
    output_type=str,
)

@g.step
async def identify_smells(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="identify_smells",
        task_kind="reviewer",
        instruction="Analyze the codebase for code smells: long methods, large classes, duplicated code, deep nesting, tight coupling, and unused code.",
        max_iterations=2,
    )

@g.step
async def plan_refactor(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="plan_refactor",
        task_kind="planner",
        instruction="Create a prioritized refactoring plan based on the identified code smells. Focus on high-impact, low-risk changes first.",
        max_iterations=2,
    )

@g.step
async def execute_refactor(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="execute_refactor",
        task_kind="coder",
        instruction="Execute the top-priority refactoring changes from the plan. Ensure all existing tests still pass.",
        max_iterations=3,
    )

@g.step
async def verify_refactor(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx,
        node_id="verify_refactor",
        task_kind="reviewer",
        instruction="Review the refactored code. Verify it maintains the same behavior, tests pass, and the code quality improved.",
        max_iterations=2,
    )

g.add(
    g.edge_from(g.start_node).to(identify_smells),
    g.edge_from(identify_smells).to(plan_refactor),
    g.edge_from(plan_refactor).to(execute_refactor),
    g.edge_from(execute_refactor).to(verify_refactor),
    g.edge_from(verify_refactor).to(g.end_node),
)

graph = g.build()
""",
)
