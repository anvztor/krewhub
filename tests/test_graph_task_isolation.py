"""Regression tests for the graph-vs-legacy-dispatcher race.

The bug: ControllerManager runs TaskDispatchController and GraphRunnerController
side by side. When BundleService.attach_graph_artifact creates the graph's tasks,
they're OPEN — and TaskDispatchController was happily claiming them before the
GraphRunnerController reached the bundle. The root step could execute twice and
outside the graph's edge ordering.

The two fixes:
    1. TaskRepo.list_open_by_recipe filters out tasks with graph_node_id set,
       so the legacy dispatcher can't even see them.
    2. dispatch_cycle treats CLAIMED/WORKING tasks as in-flight and waits for
       terminal rather than re-dispatching (belt-and-suspenders).

These tests lock both behaviors in.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from krewhub.controllers.task_dispatch import TaskDispatchController
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
from krewhub.services.graph_runtime import (
    OrchestratorDeps,
    OrchestratorState,
    dispatch_cycle,
)
from krewhub.watch.globals import get_watch_service


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_seed_counter = 0


def _next_suffix() -> str:
    global _seed_counter
    _seed_counter += 1
    return f"iso-{_seed_counter:04d}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _mock_post_response(*, status_code: int = 200, state: str = "working") -> Mock:
    resp = Mock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = {"result": {"id": "x", "status": {"state": state}}}
    resp.text = "ok"
    return resp


async def _seed_recipe() -> tuple[str, str]:
    """Insert a cookbook + recipe. Returns (cookbook_id, recipe_id)."""
    suffix = _next_suffix()
    cookbook_id = f"cb-{suffix}"
    db = await get_db()
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
        (cookbook_id, cookbook_id, "human", _now().isoformat()),
    )
    await db.commit()
    recipe = await RecipeRepo(db).create(
        Recipe(
            id=f"r-{suffix}", name=f"test/{suffix}",
            repo_url="git@x:y.git", default_branch="main",
            created_by="human", created_at=_now(),
            cookbook_id=cookbook_id,
        )
    )
    return cookbook_id, recipe.id


async def _seed_bundle(recipe_id: str, *, graph_code: str | None = None) -> str:
    suffix = _next_suffix()
    db = await get_db()
    bundle = await BundleRepo(db).create(
        Bundle(
            id=f"b-{suffix}", recipe_id=recipe_id, prompt="p",
            status=BundleStatus.OPEN, created_by="h",
            created_at=_now(),
            graph_code=graph_code,
            graph_mermaid="flowchart TD" if graph_code else None,
        )
    )
    return bundle.id


async def _seed_task(
    bundle_id: str,
    *,
    graph_node_id: str | None = None,
    status: TaskStatus = TaskStatus.OPEN,
    claimed_by: str | None = None,
) -> str:
    suffix = _next_suffix()
    db = await get_db()
    task = await TaskRepo(db).create(
        Task(
            id=f"t-{suffix}", bundle_id=bundle_id,
            title=f"Task {suffix}", status=status,
            graph_node_id=graph_node_id,
            claimed_by_agent_id=claimed_by,
            claimed_at=_now() if claimed_by else None,
            assigned_agent_id=claimed_by,
        )
    )
    return task.id


def _make_ctx(state: OrchestratorState, deps: OrchestratorDeps):
    return SimpleNamespace(state=state, deps=deps)


# ---------------------------------------------------------------------------
# Fix 1: TaskRepo.list_open_by_recipe excludes graph tasks
# ---------------------------------------------------------------------------


class TestListOpenExcludesGraphTasks:
    @pytest.mark.asyncio
    async def test_graph_task_is_hidden_from_list_open_by_recipe(self):
        _cb, recipe_id = await _seed_recipe()
        bundle_id = await _seed_bundle(recipe_id, graph_code="graph = ...")
        await _seed_task(bundle_id, graph_node_id="root")

        db = await get_db()
        open_tasks = await TaskRepo(db).list_open_by_recipe(recipe_id)
        assert open_tasks == [], (
            f"graph-backed task leaked into list_open_by_recipe: {open_tasks}"
        )

    @pytest.mark.asyncio
    async def test_non_graph_task_still_visible(self):
        _cb, recipe_id = await _seed_recipe()
        bundle_id = await _seed_bundle(recipe_id)  # no graph_code
        task_id = await _seed_task(bundle_id, graph_node_id=None)

        db = await get_db()
        open_tasks = await TaskRepo(db).list_open_by_recipe(recipe_id)
        assert [t.id for t in open_tasks] == [task_id]

    @pytest.mark.asyncio
    async def test_mixed_bundle_only_returns_non_graph_task(self):
        """A bundle with both graph tasks and a legacy task should expose
        only the legacy one via list_open_by_recipe."""
        _cb, recipe_id = await _seed_recipe()
        bundle_id = await _seed_bundle(recipe_id, graph_code="graph = ...")
        _graph_task = await _seed_task(bundle_id, graph_node_id="root")
        legacy_task = await _seed_task(bundle_id, graph_node_id=None)

        db = await get_db()
        open_tasks = await TaskRepo(db).list_open_by_recipe(recipe_id)
        assert [t.id for t in open_tasks] == [legacy_task]


# ---------------------------------------------------------------------------
# Fix 1 (integration): TaskDispatchController doesn't touch graph tasks
# ---------------------------------------------------------------------------


class TestTaskDispatchControllerSkipsGraphTasks:
    @pytest.mark.asyncio
    async def test_reconcile_does_not_dispatch_graph_tasks(self):
        cookbook_id, recipe_id = await _seed_recipe()
        bundle_id = await _seed_bundle(recipe_id, graph_code="g = ...")
        graph_task_id = await _seed_task(bundle_id, graph_node_id="root")

        # Register an online gateway so capacity isn't the reason we skip.
        db = await get_db()
        await AgentRepo(db).upsert_presence(
            AgentPresence(
                agent_id="gw1", cookbook_id=cookbook_id,
                display_name="GW", capabilities=["coder"],
                max_concurrent_tasks=4,
                endpoint_url="http://gw1/api",
                status=AgentStatus.ONLINE,
                last_heartbeat_at=_now(),
            )
        )

        controller = TaskDispatchController(db, get_watch_service(), interval=0.05)
        controller._http = AsyncMock(spec=httpx.AsyncClient)
        controller._http.post.return_value = _mock_post_response()
        controller._http.aclose = AsyncMock()

        try:
            await controller.reconcile()
            controller._http.post.assert_not_called()

            # Task should remain OPEN — nobody claimed it.
            task = await TaskRepo(db).get(graph_task_id)
            assert task is not None
            assert task.status == TaskStatus.OPEN
            assert task.claimed_by_agent_id is None
        finally:
            await controller.stop()

    @pytest.mark.asyncio
    async def test_legacy_non_graph_tasks_still_dispatched(self):
        """Regression guard: the filter must not break the legacy dispatch
        path that existed before graph tasks."""
        cookbook_id, recipe_id = await _seed_recipe()
        bundle_id = await _seed_bundle(recipe_id)  # no graph_code
        legacy_task_id = await _seed_task(bundle_id, graph_node_id=None)

        db = await get_db()
        await AgentRepo(db).upsert_presence(
            AgentPresence(
                agent_id="gw1", cookbook_id=cookbook_id,
                display_name="GW", capabilities=["coder"],
                max_concurrent_tasks=4,
                endpoint_url="http://gw1/api",
                status=AgentStatus.ONLINE,
                last_heartbeat_at=_now(),
            )
        )

        controller = TaskDispatchController(db, get_watch_service(), interval=0.05)
        controller._http = AsyncMock(spec=httpx.AsyncClient)
        controller._http.post.return_value = _mock_post_response()
        controller._http.aclose = AsyncMock()

        try:
            await controller.reconcile()
            # The legacy dispatcher should have POSTed once to the gateway.
            assert controller._http.post.await_count == 1

            task = await TaskRepo(db).get(legacy_task_id)
            assert task is not None
            assert task.status == TaskStatus.CLAIMED
            assert task.claimed_by_agent_id == "gw1"
        finally:
            await controller.stop()


# ---------------------------------------------------------------------------
# Fix 2: dispatch_cycle in-flight guard
# ---------------------------------------------------------------------------


class TestDispatchCycleInFlightGuard:
    @pytest.mark.asyncio
    async def test_adopts_existing_run_when_task_already_claimed(self):
        """If dispatch_cycle enters on a CLAIMED task, it should wait for
        the existing run and record success without re-dispatching."""
        cookbook_id, recipe_id = await _seed_recipe()
        bundle_id = await _seed_bundle(recipe_id, graph_code="g = ...")
        task_id = await _seed_task(
            bundle_id, graph_node_id="step1",
            status=TaskStatus.CLAIMED,
            claimed_by="gw1",
        )

        db = await get_db()
        await AgentRepo(db).upsert_presence(
            AgentPresence(
                agent_id="gw1", cookbook_id=cookbook_id,
                display_name="GW", capabilities=["coder"],
                max_concurrent_tasks=4,
                endpoint_url="http://gw1/api",
                status=AgentStatus.ONLINE,
                last_heartbeat_at=_now(),
            )
        )

        http = AsyncMock(spec=httpx.AsyncClient)
        # Flip the task to DONE to simulate the in-flight run completing.
        async def flip_done():
            await asyncio.sleep(0.02)
            await TaskRepo(db).update(task_id, status=TaskStatus.DONE)

        flipper = asyncio.create_task(flip_done())

        state = OrchestratorState(prompt="p", bundle_id=bundle_id, recipe_id=recipe_id)
        deps = OrchestratorDeps(
            db=db, http=http, watch=get_watch_service(),
            task_id_map={"step1": task_id},
            cookbook_id=cookbook_id,
            poll_interval=0.01, task_timeout=1.0,
        )
        result = await dispatch_cycle(
            _make_ctx(state, deps),
            node_id="step1", task_kind="coder",
            instruction="do it", max_iterations=2,
        )
        await flipper

        assert result.startswith("done:")
        # Zero dispatches — we adopted the existing run.
        http.post.assert_not_called()
        record = state.task_results["step1"]
        assert record.success is True
        assert len(record.attempts) == 1
        assert record.attempts[0].iteration == 0
        assert record.attempts[0].agent_id == "gw1"
        assert record.attempts[0].status == "done"

    @pytest.mark.asyncio
    async def test_adopts_existing_run_when_task_already_working(self):
        cookbook_id, recipe_id = await _seed_recipe()
        bundle_id = await _seed_bundle(recipe_id, graph_code="g = ...")
        task_id = await _seed_task(
            bundle_id, graph_node_id="step1",
            status=TaskStatus.WORKING,
            claimed_by="gw1",
        )

        db = await get_db()
        await AgentRepo(db).upsert_presence(
            AgentPresence(
                agent_id="gw1", cookbook_id=cookbook_id,
                display_name="GW", capabilities=["coder"],
                max_concurrent_tasks=4,
                endpoint_url="http://gw1/api",
                status=AgentStatus.ONLINE,
                last_heartbeat_at=_now(),
            )
        )

        http = AsyncMock(spec=httpx.AsyncClient)

        async def flip_done():
            await asyncio.sleep(0.02)
            await TaskRepo(db).update(task_id, status=TaskStatus.DONE)

        flipper = asyncio.create_task(flip_done())

        state = OrchestratorState(prompt="p", bundle_id=bundle_id, recipe_id=recipe_id)
        deps = OrchestratorDeps(
            db=db, http=http, watch=get_watch_service(),
            task_id_map={"step1": task_id},
            cookbook_id=cookbook_id,
            poll_interval=0.01, task_timeout=1.0,
        )
        result = await dispatch_cycle(
            _make_ctx(state, deps),
            node_id="step1", task_kind="coder",
            instruction="do it", max_iterations=2,
        )
        await flipper

        assert result.startswith("done:")
        http.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_in_flight_blocked_falls_through_to_retry_loop(self):
        """If the in-flight run ends BLOCKED, the cycle should reopen the
        task and try a fresh agent via the normal retry loop."""
        cookbook_id, recipe_id = await _seed_recipe()
        bundle_id = await _seed_bundle(recipe_id, graph_code="g = ...")
        task_id = await _seed_task(
            bundle_id, graph_node_id="step1",
            status=TaskStatus.CLAIMED,
            claimed_by="gw_bad",
        )

        db = await get_db()
        # Register a second gateway for the retry.
        await AgentRepo(db).upsert_presence(
            AgentPresence(
                agent_id="gw_good", cookbook_id=cookbook_id,
                display_name="GW Good", capabilities=["coder"],
                max_concurrent_tasks=4,
                endpoint_url="http://gw_good/api",
                status=AgentStatus.ONLINE,
                last_heartbeat_at=_now(),
            )
        )

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response()

        async def driver():
            # Step 1: flip the in-flight run to BLOCKED.
            await asyncio.sleep(0.02)
            await TaskRepo(db).update(
                task_id, status=TaskStatus.BLOCKED,
                blocked_reason="existing run failed",
            )
            # Step 2: wait for the cycle to reopen + re-dispatch, then mark DONE.
            for _ in range(200):
                if http.post.await_count >= 1:
                    break
                await asyncio.sleep(0.01)
            await TaskRepo(db).update(task_id, status=TaskStatus.DONE)

        driver_task = asyncio.create_task(driver())

        state = OrchestratorState(prompt="p", bundle_id=bundle_id, recipe_id=recipe_id)
        deps = OrchestratorDeps(
            db=db, http=http, watch=get_watch_service(),
            task_id_map={"step1": task_id},
            cookbook_id=cookbook_id,
            poll_interval=0.01, task_timeout=2.0,
        )
        result = await dispatch_cycle(
            _make_ctx(state, deps),
            node_id="step1", task_kind="coder",
            instruction="do it", max_iterations=3,
        )
        await driver_task

        assert result.startswith("done:")
        record = state.task_results["step1"]
        assert record.success is True
        # Two attempts: 0 (adopted, failed) + 1 (our own dispatch, succeeded)
        assert len(record.attempts) == 2
        assert record.attempts[0].iteration == 0
        assert "blocked" in record.attempts[0].status.lower() or record.attempts[0].status == "blocked"
        assert record.attempts[1].iteration == 1
        assert record.attempts[1].status == "done"
