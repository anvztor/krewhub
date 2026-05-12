"""Tests for GraphRunnerController.

These wire the sandbox + runtime + controller end-to-end against a real
sqlite database. httpx is mocked so per-step A2A dispatches don't hit
the network. We seed each test with a cookbook + recipe + bundle (with
graph_code attached) + tasks (with graph_node_id) + an online gateway,
then drive reconcile() and assert the bundle ends up in the expected
terminal state.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from krewhub.controllers.graph_runner import GraphRunnerController
from krewhub.db.connection import get_db
from krewhub.models import (
    AgentPresence,
    AgentStatus,
    Bundle,
    BundleStatus,
    Recipe,
    Task,
    TaskStatus,
)
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.watch.globals import get_watch_service


# ---------------------------------------------------------------------------
# Fixtures: a working linear graph the runner can exec
# ---------------------------------------------------------------------------


GOOD_GRAPH_CODE = '''
g = GraphBuilder(state_type=OrchestratorState, deps_type=OrchestratorDeps, output_type=str)

@g.step
async def step_a(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx, node_id="step_a", task_kind="coder",
        instruction="do A", max_iterations=2,
    )

@g.step
async def step_b(ctx: StepContext[OrchestratorState, OrchestratorDeps, str]) -> str:
    return await dispatch_cycle(
        ctx, node_id="step_b", task_kind="reviewer",
        instruction="do B", max_iterations=2,
    )

g.add(
    g.edge_from(g.start_node).to(step_a),
    g.edge_from(step_a).to(step_b),
    g.edge_from(step_b).to(g.end_node),
)
graph = g.build()
'''


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_seed_counter = 0


def _next_suffix() -> str:
    global _seed_counter
    _seed_counter += 1
    return f"runner-{_seed_counter:04d}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _mock_post_response(*, status_code: int = 200, state: str = "submitted") -> Mock:
    resp = Mock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = {
        "result": {"id": "any", "status": {"state": state}},
    }
    resp.text = "ok"
    return resp


async def _seed_runnable_bundle(
    *,
    graph_code: str | None = GOOD_GRAPH_CODE,
    node_ids: tuple[str, ...] = ("step_a", "step_b"),
    agent_capabilities: tuple[str, ...] = ("coder", "reviewer"),
    with_agent: bool = True,
    suffix: str | None = None,
) -> tuple[str, list[str]]:
    """Seed db with cookbook+recipe+bundle (graph attached) + tasks + agent.

    Returns (bundle_id, [task_ids]).
    """
    suffix = suffix or _next_suffix()
    db = await get_db()

    # Cookbook
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
        (f"cb-{suffix}", f"cb-{suffix}", "human", _now().isoformat()),
    )
    await db.commit()

    recipe_repo = RecipeRepo(db)
    bundle_repo = BundleRepo(db)
    task_repo = TaskRepo(db)
    agent_repo = AgentRepo(db)

    recipe = await recipe_repo.create(
        Recipe(
            id=f"r-{suffix}", name=f"test/{suffix}",
            repo_url="git@x:y.git", default_branch="main",
            created_by="human", created_at=_now(),
            cookbook_id=f"cb-{suffix}",
        )
    )
    bundle = await bundle_repo.create(
        Bundle(
            id=f"b-{suffix}", recipe_id=recipe.id, prompt="run graph",
            status=BundleStatus.OPEN, created_by="human",
            created_at=_now(),
            graph_code=graph_code, graph_mermaid="flowchart TD",
        )
    )

    task_ids: list[str] = []
    for i, node_id in enumerate(node_ids):
        task = await task_repo.create(
            Task(
                id=f"t-{suffix}-{i}", bundle_id=bundle.id,
                title=f"Task {node_id}", status=TaskStatus.OPEN,
                graph_node_id=node_id,
            )
        )
        task_ids.append(task.id)

    if with_agent:
        await agent_repo.upsert_presence(
            AgentPresence(
                agent_id=f"gw-{suffix}",
                cookbook_id=f"cb-{suffix}",
                display_name="GW",
                capabilities=list(agent_capabilities),
                max_concurrent_tasks=4,
                endpoint_url="http://gw/api",
                status=AgentStatus.ONLINE,
                last_heartbeat_at=_now(),
            )
        )

    return bundle.id, task_ids


async def _wait_for_bundle_status(
    bundle_id: str,
    target: BundleStatus,
    *,
    timeout: float = 3.0,
) -> Bundle:
    """Poll the bundle row until it reaches `target` or times out."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        bundle = await BundleRepo(await get_db()).get(bundle_id)
        if bundle is not None and bundle.status == target:
            return bundle
        await asyncio.sleep(0.02)
    bundle = await BundleRepo(await get_db()).get(bundle_id)
    raise AssertionError(
        f"bundle {bundle_id} never reached {target}; "
        f"last status={bundle.status if bundle else 'missing'}, "
        f"reason={bundle.blocked_reason if bundle else None!r}"
    )


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------


class TestRunnerEligibility:
    @pytest.mark.asyncio
    async def test_skips_bundle_without_graph_code(self):
        bundle_id, _task_ids = await _seed_runnable_bundle(graph_code=None)
        db = await get_db()

        runner = GraphRunnerController(db, get_watch_service(), interval=0.05)
        try:
            await runner.reconcile()
            # Give any spawned task a moment — but none should have spawned.
            await asyncio.sleep(0.05)
            assert bundle_id not in runner._in_flight
            bundle = await BundleRepo(db).get(bundle_id)
            assert bundle is not None
            assert bundle.status == BundleStatus.OPEN
        finally:
            await runner.stop()

    @pytest.mark.asyncio
    async def test_skips_bundle_already_in_flight(self):
        bundle_id, _task_ids = await _seed_runnable_bundle()
        db = await get_db()
        runner = GraphRunnerController(db, get_watch_service(), interval=0.05)
        try:
            runner._in_flight.add(bundle_id)  # pretend already running
            spawn_count_before = len(runner._runner_tasks)
            await runner.reconcile()
            assert len(runner._runner_tasks) == spawn_count_before
        finally:
            runner._in_flight.discard(bundle_id)
            await runner.stop()

    @pytest.mark.asyncio
    async def test_max_concurrent_caps_spawns(self):
        # Seed 3 bundles with cap=2
        ids: list[str] = []
        for _ in range(3):
            bid, _ = await _seed_runnable_bundle()
            ids.append(bid)
        db = await get_db()
        runner = GraphRunnerController(db, get_watch_service(), max_concurrent=2)
        try:
            # Seed _in_flight with 2 to simulate full capacity
            runner._in_flight.update(ids[:2])
            await runner.reconcile()
            # No new spawn — third bundle stays out
            assert ids[2] not in runner._in_flight
        finally:
            runner._in_flight.clear()
            await runner.stop()


# ---------------------------------------------------------------------------
# Per-bundle execution
# ---------------------------------------------------------------------------


class TestRunnerExecution:
    @pytest.mark.asyncio
    async def test_runs_linear_graph_to_cooked(self):
        bundle_id, task_ids = await _seed_runnable_bundle()
        db = await get_db()

        runner = GraphRunnerController(
            db, get_watch_service(),
            poll_interval=0.01, task_timeout=2.0,
        )
        # Mock httpx so dispatch is "accepted" and arrange for tasks to flip
        # to DONE shortly after each dispatch attempt.
        runner._http = AsyncMock(spec=httpx.AsyncClient)
        runner._http.post.return_value = _mock_post_response(state="working")
        runner._http.aclose = AsyncMock()

        async def flipper():
            task_repo = TaskRepo(db)
            for tid in task_ids:
                # Wait until the task transitions to CLAIMED, then mark DONE.
                for _ in range(200):
                    t = await task_repo.get(tid)
                    if t and t.status == TaskStatus.CLAIMED:
                        break
                    await asyncio.sleep(0.01)
                await task_repo.update(tid, status=TaskStatus.DONE)

        flip_task = asyncio.create_task(flipper())

        try:
            await runner._execute_bundle(bundle_id)
            await flip_task

            bundle = await BundleRepo(db).get(bundle_id)
            assert bundle is not None
            # Step (d.1): bundle stays OPEN. Success surfaces as a
            # MILESTONE event, not a status flip.
            assert bundle.status == BundleStatus.OPEN
            cursor = await db.execute(
                "SELECT body FROM events WHERE bundle_id = ? AND type = 'milestone'",
                (bundle_id,),
            )
            milestones = await cursor.fetchall()
            assert any("completed" in (m["body"] or "") for m in milestones)

            # Both tasks should have been dispatched
            assert runner._http.post.await_count >= 2
        finally:
            await runner.stop()

    @pytest.mark.asyncio
    async def test_emits_failure_milestone_when_step_fails(self):
        """Step (d.1): on failure, graph runner emits a MILESTONE event
        instead of marking the bundle BLOCKED. Bundle stays OPEN."""
        bundle_id, _task_ids = await _seed_runnable_bundle()
        db = await get_db()

        runner = GraphRunnerController(
            db, get_watch_service(),
            poll_interval=0.01, task_timeout=0.2,
        )
        runner._http = AsyncMock(spec=httpx.AsyncClient)
        runner._http.post.return_value = _mock_post_response(status_code=500)
        runner._http.aclose = AsyncMock()

        try:
            await runner._execute_bundle(bundle_id)
            bundle = await BundleRepo(db).get(bundle_id)
            assert bundle is not None
            # Bundle stays OPEN — no BLOCKED transition.
            assert bundle.status == BundleStatus.OPEN
            # A failure milestone event was emitted.
            cursor = await db.execute(
                "SELECT body, payload FROM events "
                "WHERE bundle_id = ? AND type = 'milestone'",
                (bundle_id,),
            )
            rows = await cursor.fetchall()
            assert len(rows) >= 1
        finally:
            await runner.stop()

    @pytest.mark.asyncio
    async def test_invalid_graph_code_keeps_bundle_open(self):
        """Step (d.1): graph compile failure no longer writes BLOCKED."""
        bad_code = "import os\ngraph = None"
        bundle_id, _task_ids = await _seed_runnable_bundle(graph_code=bad_code)
        db = await get_db()

        runner = GraphRunnerController(db, get_watch_service())
        try:
            await runner._execute_bundle(bundle_id)
            bundle = await BundleRepo(db).get(bundle_id)
            assert bundle is not None
            assert bundle.status == BundleStatus.OPEN
        finally:
            await runner.stop()

    @pytest.mark.asyncio
    async def test_bundle_with_no_graph_node_tasks_keeps_open(self):
        """Step (d.1): orphan tasks don't flip the bundle to BLOCKED."""
        suffix = _next_suffix()
        db = await get_db()
        await db.execute(
            "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
            (f"cb-{suffix}", "x", "h", _now().isoformat()),
        )
        await db.commit()
        recipe = await RecipeRepo(db).create(
            Recipe(
                id=f"r-{suffix}", name=f"t/{suffix}", repo_url="g@x:e.git",
                default_branch="main", created_by="h",
                created_at=_now(), cookbook_id=f"cb-{suffix}",
            )
        )
        bundle = await BundleRepo(db).create(
            Bundle(
                id=f"b-{suffix}", recipe_id=recipe.id, prompt="x",
                status=BundleStatus.OPEN, created_by="h",
                created_at=_now(),
                graph_code=GOOD_GRAPH_CODE, graph_mermaid="x",
            )
        )
        await TaskRepo(db).create(
            Task(
                id=f"t-{suffix}", bundle_id=bundle.id, title="orphan",
                status=TaskStatus.OPEN,
            )
        )

        runner = GraphRunnerController(db, get_watch_service())
        try:
            await runner._execute_bundle(bundle.id)
            updated = await BundleRepo(db).get(bundle.id)
            assert updated is not None
            assert updated.status == BundleStatus.OPEN
        finally:
            await runner.stop()


# ---------------------------------------------------------------------------
# Reconcile spawning
# ---------------------------------------------------------------------------


class TestReconcileSpawning:
    @pytest.mark.asyncio
    async def test_reconcile_spawns_runnable_bundle(self):
        bundle_id, task_ids = await _seed_runnable_bundle()
        db = await get_db()

        runner = GraphRunnerController(
            db, get_watch_service(),
            poll_interval=0.01, task_timeout=2.0,
        )
        runner._http = AsyncMock(spec=httpx.AsyncClient)
        runner._http.post.return_value = _mock_post_response(state="working")
        runner._http.aclose = AsyncMock()

        async def flipper():
            task_repo = TaskRepo(db)
            for tid in task_ids:
                for _ in range(200):
                    t = await task_repo.get(tid)
                    if t and t.status == TaskStatus.CLAIMED:
                        break
                    await asyncio.sleep(0.01)
                await task_repo.update(tid, status=TaskStatus.DONE)

        flip_task = asyncio.create_task(flipper())

        try:
            await runner.reconcile()
            assert bundle_id in runner._in_flight
            # Wait for the spawned task to complete. Bundle stays OPEN
            # under step (d.1); we wait for the milestone event instead.
            await flip_task
            db = await get_db()
            deadline = asyncio.get_event_loop().time() + 5.0
            while asyncio.get_event_loop().time() < deadline:
                cursor = await db.execute(
                    "SELECT 1 FROM events WHERE bundle_id = ? AND type = 'milestone'",
                    (bundle_id,),
                )
                if await cursor.fetchone():
                    break
                await asyncio.sleep(0.05)
            else:
                pytest.fail("graph runner did not emit milestone event")
        finally:
            await runner.stop()

    @pytest.mark.asyncio
    async def test_reconcile_does_not_double_spawn(self):
        bundle_id, _task_ids = await _seed_runnable_bundle()
        db = await get_db()

        runner = GraphRunnerController(db, get_watch_service())
        runner._http = AsyncMock(spec=httpx.AsyncClient)
        runner._http.aclose = AsyncMock()
        # Pretend the runner task is in-flight forever (don't actually run it)
        runner._in_flight.add(bundle_id)
        try:
            await runner.reconcile()
            await runner.reconcile()
            # Should still be just the one we manually added
            assert len(runner._runner_tasks) == 0
        finally:
            runner._in_flight.discard(bundle_id)
            await runner.stop()
