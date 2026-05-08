"""Tests for PlannerDispatchController.

Drives the controller against a real sqlite db with mocked httpx so we
exercise: empty-bundle eligibility, planner agent matching, A2A POST
shape, and the in-flight dedup set.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from krewhub.controllers.planner_dispatch import (
    PLANNER_CAPABILITY,
    PlannerDispatchController,
)
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
# Helpers
# ---------------------------------------------------------------------------


_seed_counter = 0


def _next_suffix() -> str:
    global _seed_counter
    _seed_counter += 1
    return f"pd-{_seed_counter:04d}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _mock_post_response(status_code: int = 200) -> Mock:
    resp = Mock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = {"result": {"id": "any", "status": {"state": "submitted"}}}
    resp.text = "ok"
    return resp


async def _seed_cookbook_recipe(suffix: str | None = None) -> tuple[str, str]:
    """Insert cookbook + recipe and return (cookbook_id, recipe_id)."""
    suffix = suffix or _next_suffix()
    db = await get_db()
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
        (f"cb-{suffix}", f"cb-{suffix}", "human", _now().isoformat()),
    )
    await db.commit()

    recipe = await RecipeRepo(db).create(
        Recipe(
            id=f"r-{suffix}", name=f"test/{suffix}",
            repo_url="git@x:y.git", default_branch="main",
            created_by="human", created_at=_now(),
            cookbook_id=f"cb-{suffix}",
        )
    )
    return f"cb-{suffix}", recipe.id


async def _seed_empty_bundle(
    *,
    suffix: str | None = None,
    with_planner: bool = True,
    autoplan_enabled: bool = True,
    planner_capabilities: tuple[str, ...] = (PLANNER_CAPABILITY,),
    planner_status: AgentStatus = AgentStatus.ONLINE,
    planner_endpoint: str | None = "http://planner/api",
) -> tuple[str, str, str]:
    """Seed an empty bundle (no graph_code, no tasks). Returns (bundle_id, cookbook_id, recipe_id)."""
    suffix = suffix or _next_suffix()
    cookbook_id, recipe_id = await _seed_cookbook_recipe(suffix)

    db = await get_db()
    bundle = await BundleRepo(db).create(
        Bundle(
            id=f"b-{suffix}", recipe_id=recipe_id, prompt="please plan",
            status=BundleStatus.OPEN, created_by="human",
            created_at=_now(),
        )
    )
    if autoplan_enabled:
        await db.execute(
            "UPDATE bundles SET autoplan_enabled = 1 WHERE id = ?",
            (bundle.id,),
        )
        await db.commit()

    if with_planner:
        await AgentRepo(db).upsert_presence(
            AgentPresence(
                agent_id=f"planner-{suffix}",
                cookbook_id=cookbook_id,
                display_name="Planner",
                capabilities=list(planner_capabilities),
                max_concurrent_tasks=2,
                endpoint_url=planner_endpoint,
                status=planner_status,
                last_heartbeat_at=_now(),
            )
        )

    return bundle.id, cookbook_id, recipe_id


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------


class TestEligibility:
    @pytest.mark.asyncio
    async def test_skips_bundle_with_graph_code(self):
        suffix = _next_suffix()
        cookbook_id, recipe_id = await _seed_cookbook_recipe(suffix)
        db = await get_db()
        await BundleRepo(db).create(
            Bundle(
                id=f"b-{suffix}", recipe_id=recipe_id, prompt="x",
                status=BundleStatus.OPEN, created_by="h",
                created_at=_now(),
                graph_code="g = ...", graph_mermaid="flowchart",
            )
        )

        http = AsyncMock(spec=httpx.AsyncClient)
        controller = PlannerDispatchController(
            db, get_watch_service(), interval=0.05, http=http,
        )
        try:
            await controller.reconcile()
            http.post.assert_not_called()
        finally:
            await controller.stop()

    @pytest.mark.asyncio
    async def test_skips_bundle_with_existing_tasks(self):
        suffix = _next_suffix()
        cookbook_id, recipe_id = await _seed_cookbook_recipe(suffix)
        db = await get_db()
        bundle = await BundleRepo(db).create(
            Bundle(
                id=f"b-{suffix}", recipe_id=recipe_id, prompt="x",
                status=BundleStatus.OPEN, created_by="h",
                created_at=_now(),
            )
        )
        await TaskRepo(db).create(
            Task(
                id=f"t-{suffix}", bundle_id=bundle.id, title="manual",
                status=TaskStatus.OPEN,
            )
        )

        http = AsyncMock(spec=httpx.AsyncClient)
        controller = PlannerDispatchController(
            db, get_watch_service(), interval=0.05, http=http,
        )
        try:
            await controller.reconcile()
            http.post.assert_not_called()
        finally:
            await controller.stop()

    @pytest.mark.asyncio
    async def test_skips_terminal_bundle(self):
        suffix = _next_suffix()
        cookbook_id, recipe_id = await _seed_cookbook_recipe(suffix)
        db = await get_db()
        await BundleRepo(db).create(
            Bundle(
                id=f"b-{suffix}", recipe_id=recipe_id, prompt="x",
                status=BundleStatus.CANCELLED, created_by="h",
                created_at=_now(),
            )
        )

        http = AsyncMock(spec=httpx.AsyncClient)
        controller = PlannerDispatchController(
            db, get_watch_service(), interval=0.05, http=http,
        )
        try:
            await controller.reconcile()
            http.post.assert_not_called()
        finally:
            await controller.stop()


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    @pytest.mark.asyncio
    async def test_dispatches_empty_bundle_to_planner(self):
        bundle_id, cookbook_id, recipe_id = await _seed_empty_bundle()
        db = await get_db()

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response()

        controller = PlannerDispatchController(
            db, get_watch_service(), interval=0.05, http=http,
        )
        try:
            await controller.reconcile()
            assert http.post.await_count == 1
            assert bundle_id in controller._dispatched
        finally:
            await controller.stop()

    @pytest.mark.asyncio
    async def test_request_payload_carries_bundle_metadata(self):
        bundle_id, cookbook_id, recipe_id = await _seed_empty_bundle()
        db = await get_db()

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response()

        controller = PlannerDispatchController(
            db, get_watch_service(), interval=0.05, http=http,
        )
        try:
            await controller.reconcile()
            call = http.post.await_args
            suffix = bundle_id.removeprefix("b-")
            assert call.args[0] == (
                f"http://127.0.0.1:8420/a2a/planner-{suffix}/planner-{suffix}"
            )
            payload = call.kwargs["json"]
            assert payload["method"] == "message/send"
            metadata = payload["params"]["message"]["metadata"]
            assert metadata["bundle_id"] == bundle_id
            assert metadata["cookbook_id"] == cookbook_id
            assert metadata["recipe_id"] == recipe_id
            assert metadata["branch"] == "main"
            # Prompt forwarded as the message text
            text = payload["params"]["message"]["parts"][0]["text"]
            assert text == "please plan"
        finally:
            await controller.stop()

    @pytest.mark.asyncio
    async def test_does_not_double_dispatch_same_bundle(self):
        bundle_id, _cookbook_id, _recipe_id = await _seed_empty_bundle()
        db = await get_db()

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response()

        controller = PlannerDispatchController(
            db, get_watch_service(), interval=0.05, http=http,
        )
        try:
            await controller.reconcile()
            await controller.reconcile()
            await controller.reconcile()
            assert http.post.await_count == 1  # only first reconcile dispatched
            assert bundle_id in controller._dispatched
        finally:
            await controller.stop()

    @pytest.mark.asyncio
    async def test_no_planner_in_pool_leaves_bundle_alone(self):
        bundle_id, _cookbook_id, _recipe_id = await _seed_empty_bundle(
            with_planner=False,
        )
        db = await get_db()

        http = AsyncMock(spec=httpx.AsyncClient)
        controller = PlannerDispatchController(
            db, get_watch_service(), interval=0.05, http=http,
        )
        try:
            await controller.reconcile()
            http.post.assert_not_called()
            # Bundle should remain OPEN, not BLOCKED
            bundle = await BundleRepo(db).get(bundle_id)
            assert bundle is not None
            assert bundle.status == BundleStatus.OPEN
            assert bundle.blocked_reason is None
            assert bundle_id not in controller._dispatched
        finally:
            await controller.stop()

    @pytest.mark.asyncio
    async def test_offline_planner_is_skipped(self):
        bundle_id, _cookbook_id, _recipe_id = await _seed_empty_bundle(
            planner_status=AgentStatus.OFFLINE,
        )
        db = await get_db()

        http = AsyncMock(spec=httpx.AsyncClient)
        controller = PlannerDispatchController(
            db, get_watch_service(), interval=0.05, http=http,
        )
        try:
            await controller.reconcile()
            http.post.assert_not_called()
            assert bundle_id not in controller._dispatched
        finally:
            await controller.stop()

    @pytest.mark.asyncio
    async def test_agent_without_planner_capability_is_skipped(self):
        bundle_id, _cookbook_id, _recipe_id = await _seed_empty_bundle(
            planner_capabilities=("coder",),  # not generate-graph
        )
        db = await get_db()

        http = AsyncMock(spec=httpx.AsyncClient)
        controller = PlannerDispatchController(
            db, get_watch_service(), interval=0.05, http=http,
        )
        try:
            await controller.reconcile()
            http.post.assert_not_called()
            assert bundle_id not in controller._dispatched
        finally:
            await controller.stop()

    @pytest.mark.asyncio
    async def test_post_failure_does_not_mark_dispatched(self):
        bundle_id, _cookbook_id, _recipe_id = await _seed_empty_bundle()
        db = await get_db()

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.side_effect = httpx.ConnectError("planner down")

        controller = PlannerDispatchController(
            db, get_watch_service(), interval=0.05, http=http,
        )
        try:
            await controller.reconcile()
            assert http.post.await_count == 1
            assert bundle_id not in controller._dispatched
            # Bundle still OPEN, retry-eligible
            bundle = await BundleRepo(db).get(bundle_id)
            assert bundle is not None
            assert bundle.status == BundleStatus.OPEN
        finally:
            await controller.stop()

    @pytest.mark.asyncio
    async def test_post_4xx_does_not_mark_dispatched(self):
        bundle_id, _cookbook_id, _recipe_id = await _seed_empty_bundle()
        db = await get_db()

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response(status_code=503)

        controller = PlannerDispatchController(
            db, get_watch_service(), interval=0.05, http=http,
        )
        try:
            await controller.reconcile()
            assert http.post.await_count == 1
            assert bundle_id not in controller._dispatched
            # Next reconcile retries
            await controller.reconcile()
            assert http.post.await_count == 2
        finally:
            await controller.stop()

    @pytest.mark.asyncio
    async def test_read_timeout_keeps_slot_reserved(self):
        """Regression: long-running gateway codegen (CLI-driven) causes
        the A2A message/send POST to read-timeout on our side while the
        gateway keeps running server-side. We must NOT free the
        reservation in that case — otherwise the next reconcile dispatches
        a second codegen that races the first and ends up with HTTP 409
        'already has graph_code attached', trapping the planner in a loop.
        """
        bundle_id, _cookbook_id, _recipe_id = await _seed_empty_bundle()
        db = await get_db()

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.side_effect = httpx.ReadTimeout("codegen still running")

        controller = PlannerDispatchController(
            db, get_watch_service(), interval=0.05, http=http,
        )
        try:
            await controller.reconcile()
            assert http.post.await_count == 1
            # Reservation MUST stick — work is in flight server-side.
            assert bundle_id in controller._dispatched

            # A second reconcile while the bundle is still empty must
            # NOT re-dispatch; the first codegen is still running.
            await controller.reconcile()
            assert http.post.await_count == 1
            assert bundle_id in controller._dispatched
        finally:
            await controller.stop()

    @pytest.mark.asyncio
    async def test_dispatched_set_purges_when_bundle_no_longer_empty(self):
        """When the bundle gains tasks (planner attached), the dispatched
        slot should free for future bundles."""
        bundle_id, _cookbook_id, _recipe_id = await _seed_empty_bundle()
        db = await get_db()

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response()

        controller = PlannerDispatchController(
            db, get_watch_service(), interval=0.05, http=http,
        )
        try:
            await controller.reconcile()
            assert bundle_id in controller._dispatched

            # Simulate planner POSTing back: attach a task
            await TaskRepo(db).create(
                Task(
                    id=f"t-attached", bundle_id=bundle_id,
                    title="step", status=TaskStatus.OPEN,
                    graph_node_id="step",
                )
            )

            await controller.reconcile()
            assert bundle_id not in controller._dispatched
        finally:
            await controller.stop()
