"""Tests for the BundleService.attach_graph_artifact pipeline + HTTP route.

Covers:
    - happy path: code → tasks with deps → mermaid → bundle row updated
    - already-attached bundle is rejected (409)
    - invalid code is rejected (422) and bundle is marked BLOCKED
    - HTTP route returns the right status codes
    - end-to-end: attach + GraphRunnerController.reconcile() → COOKED
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
    TaskStatus,
)
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.services.bundle_service import BundleService, GraphArtifactError
from krewhub.watch.globals import get_watch_service


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


PARALLEL_GRAPH = '''
g = GraphBuilder(state_type=OrchestratorState, deps_type=OrchestratorDeps, output_type=str)

@g.step
async def root(ctx: StepContext[OrchestratorState, OrchestratorDeps, None]) -> str:
    return await dispatch_cycle(
        ctx, node_id="root", task_kind="planner",
        instruction="plan", max_iterations=1,
    )

@g.step
async def left(ctx: StepContext[OrchestratorState, OrchestratorDeps, str]) -> str:
    return await dispatch_cycle(
        ctx, node_id="left", task_kind="coder",
        instruction="left work", max_iterations=1,
    )

@g.step
async def right(ctx: StepContext[OrchestratorState, OrchestratorDeps, str]) -> str:
    return await dispatch_cycle(
        ctx, node_id="right", task_kind="reviewer",
        instruction="right work", max_iterations=1,
    )

g.add(
    g.edge_from(g.start_node).to(root),
    g.edge_from(root).to(left),
    g.edge_from(root).to(right),
    g.edge_from(left).to(g.end_node),
    g.edge_from(right).to(g.end_node),
)
graph = g.build()
'''


_seed_counter = 0


def _next_suffix() -> str:
    global _seed_counter
    _seed_counter += 1
    return f"bg-{_seed_counter:04d}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _mock_post_response(*, status_code: int = 200, state: str = "submitted") -> Mock:
    resp = Mock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = {"result": {"id": "any", "status": {"state": state}}}
    resp.text = "ok"
    return resp


async def _seed_empty_bundle(suffix: str | None = None) -> tuple[str, str, str]:
    """Create a cookbook-scoped bundle. Returns (bundle_id, "", cookbook_id)."""
    suffix = suffix or _next_suffix()
    db = await get_db()
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
        (f"cb-{suffix}", f"cb-{suffix}", "human", _now().isoformat()),
    )
    await db.commit()

    bundle = await BundleRepo(db).create(
        Bundle(
            id=f"b-{suffix}", cookbook_id=f"cb-{suffix}", prompt="run something",
            status=BundleStatus.OPEN, created_by="human",
            created_at=_now(),
        )
    )
    return bundle.id, "", f"cb-{suffix}"


# ---------------------------------------------------------------------------
# Service: happy path
# ---------------------------------------------------------------------------


class TestAttachHappyPath:
    @pytest.mark.asyncio
    async def test_attaches_code_and_mermaid(self):
        bundle_id, _r, _c = await _seed_empty_bundle()
        db = await get_db()
        svc = BundleService(db, get_watch_service())

        bundle, tasks = await svc.attach_graph_artifact(bundle_id, PARALLEL_GRAPH)

        assert bundle.graph_code == PARALLEL_GRAPH
        assert bundle.graph_mermaid is not None
        assert bundle.graph_mermaid.startswith("flowchart LR")
        assert "root" in bundle.graph_mermaid
        assert "left" in bundle.graph_mermaid
        assert len(tasks) == 3

    @pytest.mark.asyncio
    async def test_creates_one_task_per_node_with_graph_node_id(self):
        bundle_id, _r, _c = await _seed_empty_bundle()
        db = await get_db()
        svc = BundleService(db, get_watch_service())

        _bundle, tasks = await svc.attach_graph_artifact(bundle_id, PARALLEL_GRAPH)

        node_ids = {t.graph_node_id for t in tasks}
        assert node_ids == {"root", "left", "right"}
        # Titles humanized from node ids
        titles = {t.title for t in tasks}
        assert titles == {"Root", "Left", "Right"}

    @pytest.mark.asyncio
    async def test_task_dependencies_match_graph_edges(self):
        bundle_id, _r, _c = await _seed_empty_bundle()
        db = await get_db()
        svc = BundleService(db, get_watch_service())

        _bundle, tasks = await svc.attach_graph_artifact(bundle_id, PARALLEL_GRAPH)

        by_node = {t.graph_node_id: t for t in tasks}
        # root has no graph predecessors → no deps
        assert by_node["root"].depends_on_task_ids == []
        # left and right both depend on root
        assert by_node["left"].depends_on_task_ids == [by_node["root"].id]
        assert by_node["right"].depends_on_task_ids == [by_node["root"].id]

    @pytest.mark.asyncio
    async def test_records_plan_event_with_graph_payload(self):
        bundle_id, recipe_id, _c = await _seed_empty_bundle()
        db = await get_db()
        svc = BundleService(db, get_watch_service())

        await svc.attach_graph_artifact(bundle_id, PARALLEL_GRAPH, created_by="orchestrator-1")

        # Walk events table directly
        cursor = await db.execute(
            "SELECT * FROM events WHERE bundle_id = ? AND type = 'plan' ORDER BY created_at DESC",
            (bundle_id,),
        )
        rows = await cursor.fetchall()
        assert len(rows) >= 1
        plan = rows[0]
        assert plan["actor_id"] == "orchestrator-1"
        assert "3 steps" in plan["body"]


# ---------------------------------------------------------------------------
# Service: rejection paths
# ---------------------------------------------------------------------------


class TestAttachRejection:
    @pytest.mark.asyncio
    async def test_404_when_bundle_missing(self):
        db = await get_db()
        svc = BundleService(db, get_watch_service())
        with pytest.raises(GraphArtifactError) as exc_info:
            await svc.attach_graph_artifact("nonexistent", PARALLEL_GRAPH)
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_409_when_already_attached(self):
        bundle_id, _r, _c = await _seed_empty_bundle()
        db = await get_db()
        svc = BundleService(db, get_watch_service())
        await svc.attach_graph_artifact(bundle_id, PARALLEL_GRAPH)
        with pytest.raises(GraphArtifactError) as exc_info:
            await svc.attach_graph_artifact(bundle_id, PARALLEL_GRAPH)
        assert exc_info.value.status_code == 409

    @pytest.mark.asyncio
    async def test_422_on_invalid_code_leaves_bundle_open(self):
        """Step (d.1): graph rejection raises 422 but bundle stays
        OPEN. The failure belongs in the caller's response, not the
        bundle FSM."""
        bundle_id, _r, _c = await _seed_empty_bundle()
        db = await get_db()
        svc = BundleService(db, get_watch_service())

        bad_code = "import os\ngraph = None"
        with pytest.raises(GraphArtifactError) as exc_info:
            await svc.attach_graph_artifact(bundle_id, bad_code)
        assert exc_info.value.status_code == 422

        bundle = await BundleRepo(db).get(bundle_id)
        assert bundle is not None
        assert bundle.status == BundleStatus.OPEN

    @pytest.mark.asyncio
    async def test_422_when_graph_has_no_user_steps(self):
        empty_graph = '''
g = GraphBuilder(state_type=OrchestratorState, deps_type=OrchestratorDeps, output_type=str)
g.add(g.edge_from(g.start_node).to(g.end_node))
graph = g.build()
'''
        bundle_id, _r, _c = await _seed_empty_bundle()
        db = await get_db()
        svc = BundleService(db, get_watch_service())

        with pytest.raises(GraphArtifactError) as exc_info:
            await svc.attach_graph_artifact(bundle_id, empty_graph)
        assert exc_info.value.status_code == 422


# ---------------------------------------------------------------------------
# HTTP route
# ---------------------------------------------------------------------------


class TestAttachRoute:
    @pytest.mark.asyncio
    async def test_post_attaches_and_returns_bundle_plus_tasks(self, client):
        resp = await client.post("/api/v1/cookbooks", json={
            "name": "graph-route-cb", "owner_id": "acc_legacy_apikey",
        })
        cookbook_id = resp.json()["cookbook"]["id"]
        resp = await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
            "prompt": "do work", "tasks": [],
        })
        bundle_id = resp.json()["bundle"]["id"]

        resp = await client.post(
            f"/api/v1/bundles/{bundle_id}/graph",
            json={"code": PARALLEL_GRAPH, "created_by": "orchestrator"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["bundle"]["graph_code"] == PARALLEL_GRAPH
        assert body["bundle"]["graph_mermaid"].startswith("flowchart LR")
        assert len(body["tasks"]) == 3
        assert {t["graph_node_id"] for t in body["tasks"]} == {"root", "left", "right"}

    @pytest.mark.asyncio
    async def test_post_404_when_bundle_missing(self, client):
        resp = await client.post(
            "/api/v1/bundles/nope/graph",
            json={"code": PARALLEL_GRAPH},
        )
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_post_422_on_bad_code(self, client):
        resp = await client.post("/api/v1/cookbooks", json={
            "name": "bad-cb", "owner_id": "acc_legacy_apikey",
        })
        cookbook_id = resp.json()["cookbook"]["id"]
        resp = await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
            "prompt": "x", "tasks": [],
        })
        bundle_id = resp.json()["bundle"]["id"]

        resp = await client.post(
            f"/api/v1/bundles/{bundle_id}/graph",
            json={"code": "import os\ngraph = None"},
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# End-to-end: attach → GraphRunnerController → COOKED
# ---------------------------------------------------------------------------


class TestAttachThenRun:
    @pytest.mark.asyncio
    async def test_attach_then_runner_picks_up_and_cooks(self):
        bundle_id, _r, cookbook_id = await _seed_empty_bundle()
        db = await get_db()

        # Register an online gateway with both capabilities so each step
        # finds an agent.
        await AgentRepo(db).upsert_presence(
            AgentPresence(
                agent_id="gw1", cookbook_id=cookbook_id,
                display_name="Gateway", capabilities=["planner", "coder", "reviewer"],
                max_concurrent_tasks=4, endpoint_url="http://gw1/api",
                status=AgentStatus.ONLINE, last_heartbeat_at=_now(),
            )
        )

        svc = BundleService(db, get_watch_service())
        _bundle, tasks = await svc.attach_graph_artifact(bundle_id, PARALLEL_GRAPH)
        task_ids = [t.id for t in tasks]

        runner = GraphRunnerController(
            db, get_watch_service(), poll_interval=0.01, task_timeout=2.0,
        )
        runner._http = AsyncMock(spec=httpx.AsyncClient)
        runner._http.post.return_value = _mock_post_response(state="working")
        runner._http.aclose = AsyncMock()

        # Flip each task to DONE shortly after it's claimed.
        async def flipper():
            task_repo = TaskRepo(db)
            done: set[str] = set()
            for _ in range(500):
                if len(done) == len(task_ids):
                    break
                for tid in task_ids:
                    if tid in done:
                        continue
                    t = await task_repo.get(tid)
                    if t and t.status == TaskStatus.CLAIMED:
                        await task_repo.update(tid, status=TaskStatus.DONE)
                        done.add(tid)
                await asyncio.sleep(0.005)

        flip_task = asyncio.create_task(flipper())
        try:
            await runner._execute_bundle(bundle_id)
            await flip_task

            bundle = await BundleRepo(db).get(bundle_id)
            assert bundle is not None
            assert bundle.status == BundleStatus.COOKED, bundle.blocked_reason
        finally:
            await runner.stop()
