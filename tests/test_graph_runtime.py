"""Tests for krewhub.services.graph_runtime.

Layered:
    - agent_picker  : pure ranking, no fixtures
    - a2a           : mocked httpx client
    - polling       : real sqlite via the existing _setup_db fixture
    - dispatch_cycle: real sqlite + watch + mocked httpx
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from krewhub.db.connection import get_db
from krewhub.models import (
    AgentPresence,
    AgentStatus,
    Bundle,
    BundleStatus,
    Task,
    TaskStatus,
)
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.services.graph_runtime import (
    AttemptRecord,
    OrchestratorDeps,
    OrchestratorState,
    dispatch_cycle,
    pick_agent_for_kind,
)
from krewhub.services.graph_runtime.a2a import dispatch_to_gateway
from krewhub.services.graph_runtime.polling import (
    DependencyFailedError,
    wait_for_dependencies,
    wait_for_task_terminal,
)
from krewhub.watch.globals import get_watch_service


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _agent(
    agent_id: str,
    *,
    capabilities: list[str] | None = None,
    status: AgentStatus = AgentStatus.ONLINE,
    endpoint_url: str | None = "http://gw/x",
    cookbook_id: str = "cb1",
) -> AgentPresence:
    return AgentPresence(
        agent_id=agent_id,
        cookbook_id=cookbook_id,
        display_name=agent_id,
        capabilities=capabilities or [],
        max_concurrent_tasks=1,
        endpoint_url=endpoint_url,
        status=status,
        last_heartbeat_at=datetime.now(timezone.utc),
    )


def _task(task_id: str = "t1", bundle_id: str = "b1") -> Task:
    return Task(
        id=task_id,
        bundle_id=bundle_id,
        title="Test task",
        description="A test task",
        status=TaskStatus.OPEN,
    )


# ---------------------------------------------------------------------------
# agent_picker
# ---------------------------------------------------------------------------


class TestAgentPicker:
    def test_picks_capability_match_over_others(self):
        agents = [
            _agent("a1", capabilities=["other"]),
            _agent("a2", capabilities=["coder"]),
            _agent("a3", capabilities=["planner"]),
        ]
        chosen = pick_agent_for_kind(agents, "coder")
        assert chosen is not None
        assert chosen.agent_id == "a2"

    def test_capability_match_is_case_insensitive(self):
        agents = [_agent("a1", capabilities=["CODER"])]
        chosen = pick_agent_for_kind(agents, "coder")
        assert chosen is not None
        assert chosen.agent_id == "a1"

    def test_falls_back_to_non_matching_when_no_match(self):
        agents = [
            _agent("a1", capabilities=["other"]),
            _agent("a2", capabilities=["different"]),
        ]
        chosen = pick_agent_for_kind(agents, "coder")
        assert chosen is not None
        # Both lack capability — deterministic pick by agent_id
        assert chosen.agent_id == "a1"

    def test_skips_offline_agents(self):
        agents = [
            _agent("a1", capabilities=["coder"], status=AgentStatus.OFFLINE),
            _agent("a2", capabilities=["coder"], status=AgentStatus.ONLINE),
        ]
        chosen = pick_agent_for_kind(agents, "coder")
        assert chosen is not None
        assert chosen.agent_id == "a2"

    def test_skips_agents_without_endpoint_url(self):
        agents = [
            _agent("a1", capabilities=["coder"], endpoint_url=None),
            _agent("a2", capabilities=["coder"]),
        ]
        chosen = pick_agent_for_kind(agents, "coder")
        assert chosen is not None
        assert chosen.agent_id == "a2"

    def test_excludes_set_is_honored(self):
        agents = [
            _agent("a1", capabilities=["coder"]),
            _agent("a2", capabilities=["coder"]),
        ]
        chosen = pick_agent_for_kind(agents, "coder", exclude={"a1"})
        assert chosen is not None
        assert chosen.agent_id == "a2"

    def test_returns_none_when_pool_empty(self):
        assert pick_agent_for_kind([], "coder") is None

    def test_returns_none_when_all_excluded(self):
        agents = [_agent("a1"), _agent("a2")]
        assert pick_agent_for_kind(agents, "coder", exclude={"a1", "a2"}) is None

    def test_prefers_online_over_busy_on_tiebreak(self):
        agents = [
            _agent("a1", capabilities=["coder"], status=AgentStatus.BUSY),
            _agent("a2", capabilities=["coder"], status=AgentStatus.ONLINE),
        ]
        chosen = pick_agent_for_kind(agents, "coder")
        assert chosen is not None
        assert chosen.agent_id == "a2"

    def test_deterministic_lex_tiebreak(self):
        agents = [
            _agent("zeta", capabilities=["coder"]),
            _agent("alpha", capabilities=["coder"]),
        ]
        chosen = pick_agent_for_kind(agents, "coder")
        assert chosen is not None
        assert chosen.agent_id == "alpha"


# ---------------------------------------------------------------------------
# a2a.dispatch_to_gateway
# ---------------------------------------------------------------------------


def _mock_post_response(status_code: int = 200, json_body: dict | None = None) -> Mock:
    resp = Mock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_body or {}
    resp.text = "ok"
    return resp


class TestA2ADispatch:
    @pytest.mark.asyncio
    async def test_accepts_when_state_is_submitted(self):
        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response(
            json_body={"result": {"id": "t1", "status": {"state": "submitted"}}},
        )
        ok = await dispatch_to_gateway(
            http, agent=_agent("g1"), task=_task(),
            prompt="do it", attempt=1,
        )
        assert ok is True
        http.post.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_accepts_when_state_is_working(self):
        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response(
            json_body={"result": {"id": "t1", "status": {"state": "working"}}},
        )
        assert await dispatch_to_gateway(
            http, agent=_agent("g1"), task=_task(), prompt="x", attempt=1,
        ) is True

    @pytest.mark.asyncio
    async def test_accepts_when_only_id_present(self):
        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response(
            json_body={"result": {"id": "t1"}},
        )
        assert await dispatch_to_gateway(
            http, agent=_agent("g1"), task=_task(), prompt="x", attempt=1,
        ) is True

    @pytest.mark.asyncio
    async def test_rejects_on_4xx(self):
        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response(status_code=400)
        assert await dispatch_to_gateway(
            http, agent=_agent("g1"), task=_task(), prompt="x", attempt=1,
        ) is False

    @pytest.mark.asyncio
    async def test_rejects_on_unknown_state(self):
        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response(
            json_body={"result": {"status": {"state": "rejected"}}},
        )
        assert await dispatch_to_gateway(
            http, agent=_agent("g1"), task=_task(), prompt="x", attempt=1,
        ) is False

    @pytest.mark.asyncio
    async def test_rejects_on_network_error(self):
        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.side_effect = httpx.ConnectError("boom")
        assert await dispatch_to_gateway(
            http, agent=_agent("g1"), task=_task(), prompt="x", attempt=1,
        ) is False

    @pytest.mark.asyncio
    async def test_carries_attempt_in_metadata(self):
        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response(
            json_body={"result": {"id": "t1"}},
        )
        await dispatch_to_gateway(
            http, agent=_agent("g1"), task=_task(),
            prompt="x", attempt=3,
            recipe_meta={"recipe_id": "r1", "branch": "main"},
        )
        call = http.post.call_args
        payload = call.kwargs.get("json") or call.args[1] if len(call.args) > 1 else call.kwargs["json"]
        meta = payload["params"]["message"]["metadata"]
        assert meta["attempt"] == 3
        assert meta["recipe_id"] == "r1"
        assert meta["branch"] == "main"
        assert meta["task_id"] == "t1"


# ---------------------------------------------------------------------------
# polling.wait_for_task_terminal
# ---------------------------------------------------------------------------


_seed_counter = 0


def _next_suffix() -> str:
    global _seed_counter
    _seed_counter += 1
    return f"{_seed_counter:04d}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _create_seed_task(
    *, status: TaskStatus = TaskStatus.OPEN, suffix: str | None = None,
) -> tuple[str, str]:
    """Insert a real bundle + task and return (bundle_id, task_id)."""
    suffix = suffix or _next_suffix()
    db = await get_db()
    bundle_repo = BundleRepo(db)
    task_repo = TaskRepo(db)

    cookbook_id = f"cb-{suffix}"
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
        (cookbook_id, f"test-cb-{suffix}", "human_1", _now().isoformat()),
    )
    await db.commit()

    bundle = await bundle_repo.create(
        Bundle(
            id=f"b-{suffix}", cookbook_id=cookbook_id, prompt="hi",
            status=BundleStatus.OPEN, created_by="human_1",
            created_at=_now(),
        )
    )
    task = await task_repo.create(
        Task(id=f"t-{suffix}", bundle_id=bundle.id, title="seed", status=status)
    )
    return bundle.id, task.id


class TestPolling:
    @pytest.mark.asyncio
    async def test_returns_immediately_when_already_terminal(self):
        _bundle_id, task_id = await _create_seed_task(status=TaskStatus.DONE)
        db = await get_db()
        repo = TaskRepo(db)
        task = await wait_for_task_terminal(repo, task_id, poll_interval=0.01, timeout=1.0)
        assert task.status == TaskStatus.DONE

    @pytest.mark.asyncio
    async def test_returns_when_status_transitions_to_terminal(self):
        _bundle_id, task_id = await _create_seed_task()
        db = await get_db()
        repo = TaskRepo(db)

        async def flip_to_done():
            await asyncio.sleep(0.02)
            await repo.update(task_id, status=TaskStatus.DONE)

        flipper = asyncio.create_task(flip_to_done())
        task = await wait_for_task_terminal(repo, task_id, poll_interval=0.01, timeout=1.0)
        await flipper
        assert task.status == TaskStatus.DONE

    @pytest.mark.asyncio
    async def test_raises_timeout_when_never_terminal(self):
        _bundle_id, task_id = await _create_seed_task()
        db = await get_db()
        repo = TaskRepo(db)
        with pytest.raises(asyncio.TimeoutError):
            await wait_for_task_terminal(
                repo, task_id, poll_interval=0.01, timeout=0.05,
            )

    @pytest.mark.asyncio
    async def test_raises_when_task_disappears(self):
        _bundle_id, task_id = await _create_seed_task()
        db = await get_db()
        repo = TaskRepo(db)
        await repo.delete(task_id)
        with pytest.raises(ValueError, match="disappeared"):
            await wait_for_task_terminal(repo, task_id, poll_interval=0.01, timeout=1.0)


# ---------------------------------------------------------------------------
# polling.wait_for_dependencies
# ---------------------------------------------------------------------------


class TestWaitForDependencies:
    @pytest.mark.asyncio
    async def test_empty_dep_list_returns_immediately(self):
        db = await get_db()
        repo = TaskRepo(db)
        # Must return without raising and without sleeping.
        await wait_for_dependencies(repo, [], poll_interval=10.0, timeout=0.01)

    @pytest.mark.asyncio
    async def test_all_deps_already_done_returns_immediately(self):
        _b1, dep1 = await _create_seed_task(status=TaskStatus.DONE)
        _b2, dep2 = await _create_seed_task(status=TaskStatus.DONE)
        db = await get_db()
        repo = TaskRepo(db)
        await wait_for_dependencies(
            repo, [dep1, dep2], poll_interval=0.01, timeout=0.5,
        )

    @pytest.mark.asyncio
    async def test_waits_until_last_dep_done(self):
        _b1, dep1 = await _create_seed_task(status=TaskStatus.DONE)
        _b2, dep2 = await _create_seed_task(status=TaskStatus.WORKING)
        db = await get_db()
        repo = TaskRepo(db)

        async def flip_dep2():
            await asyncio.sleep(0.03)
            await repo.update(dep2, status=TaskStatus.DONE)

        flipper = asyncio.create_task(flip_dep2())
        await wait_for_dependencies(
            repo, [dep1, dep2], poll_interval=0.01, timeout=1.0,
        )
        await flipper

    @pytest.mark.asyncio
    async def test_raises_when_dep_ends_blocked(self):
        _b1, dep1 = await _create_seed_task(status=TaskStatus.BLOCKED)
        db = await get_db()
        repo = TaskRepo(db)
        # Seed a reason so the caller's summary is useful.
        await repo.update(dep1, blocked_reason="upstream failure")
        with pytest.raises(DependencyFailedError) as excinfo:
            await wait_for_dependencies(
                repo, [dep1], poll_interval=0.01, timeout=0.5,
            )
        assert excinfo.value.dep_task_id == dep1
        assert excinfo.value.status == TaskStatus.BLOCKED

    @pytest.mark.asyncio
    async def test_raises_timeout_when_dep_never_finishes(self):
        _b1, dep1 = await _create_seed_task(status=TaskStatus.WORKING)
        db = await get_db()
        repo = TaskRepo(db)
        with pytest.raises(asyncio.TimeoutError):
            await wait_for_dependencies(
                repo, [dep1], poll_interval=0.01, timeout=0.05,
            )

    @pytest.mark.asyncio
    async def test_raises_when_dep_missing(self):
        db = await get_db()
        repo = TaskRepo(db)
        with pytest.raises(ValueError, match="not found"):
            await wait_for_dependencies(
                repo, ["nope"], poll_interval=0.01, timeout=0.5,
            )


# ---------------------------------------------------------------------------
# dispatch_cycle
# ---------------------------------------------------------------------------


async def _seed_with_agent(
    *,
    agent_capabilities: list[str],
    task_status: TaskStatus = TaskStatus.OPEN,
) -> tuple[str, str, str]:
    """Insert a cookbook + recipe + bundle + task + one online agent.

    Returns (cookbook_id, task_id, recipe_id).
    """
    suffix = _next_suffix()
    cookbook_id = f"cb-{suffix}"
    db = await get_db()
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
        (cookbook_id, f"cb-{suffix}", "human_1", _now().isoformat()),
    )
    await db.commit()

    bundle_repo = BundleRepo(db)
    task_repo = TaskRepo(db)
    agent_repo = AgentRepo(db)

    bundle = await bundle_repo.create(
        Bundle(
            id=f"b-{suffix}", cookbook_id=cookbook_id, prompt="run",
            status=BundleStatus.OPEN, created_by="human_1",
            created_at=_now(),
        )
    )
    task = await task_repo.create(
        Task(
            id=f"t-{suffix}", bundle_id=bundle.id, title="cycle task",
            status=task_status,
        )
    )
    await agent_repo.upsert_presence(
        AgentPresence(
            agent_id="gw1",
            cookbook_id=cookbook_id,
            display_name="Gateway One",
            capabilities=agent_capabilities,
            max_concurrent_tasks=2,
            endpoint_url="http://gw1/api",
            status=AgentStatus.ONLINE,
            last_heartbeat_at=_now(),
        )
    )
    return cookbook_id, task.id, None


def _make_ctx(state: OrchestratorState, deps: OrchestratorDeps):
    return SimpleNamespace(state=state, deps=deps)


class TestDispatchCycle:
    @pytest.mark.asyncio
    async def test_happy_path_single_iteration(self):
        cookbook_id, task_id, recipe_id = await _seed_with_agent(
            agent_capabilities=["coder"],
        )
        db = await get_db()
        watch = get_watch_service()

        # Mock httpx so the gateway "accepts", and arrange for the task to
        # transition to DONE shortly after dispatch.
        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response(
            json_body={"result": {"id": task_id, "status": {"state": "submitted"}}},
        )

        async def flip_done():
            await asyncio.sleep(0.02)
            await TaskRepo(db).update(task_id, status=TaskStatus.DONE)

        flipper = asyncio.create_task(flip_done())

        state = OrchestratorState(prompt="build x", bundle_id="b-cycle", recipe_id=recipe_id)
        deps = OrchestratorDeps(
            db=db, http=http, watch=watch,
            task_id_map={"step1": task_id},
            cookbook_id=cookbook_id,
            recipe_meta={"recipe_id": recipe_id},
            poll_interval=0.01, task_timeout=1.0,
        )
        result = await dispatch_cycle(
            _make_ctx(state, deps),
            node_id="step1", task_kind="coder",
            instruction="please code", max_iterations=2,
        )
        await flipper

        assert result.startswith("done: gw1")
        record = state.task_results["step1"]
        assert record.success is True
        assert record.task_id == task_id
        assert len(record.attempts) == 1
        assert record.attempts[0].agent_id == "gw1"
        assert record.attempts[0].status == "done"
        http.post.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_missing_task_id_fails_fast(self):
        cookbook_id, _task_id, recipe_id = await _seed_with_agent(
            agent_capabilities=["coder"],
        )
        db = await get_db()
        watch = get_watch_service()
        http = AsyncMock(spec=httpx.AsyncClient)

        state = OrchestratorState(prompt="x", bundle_id="b-cycle", recipe_id=recipe_id)
        deps = OrchestratorDeps(
            db=db, http=http, watch=watch,
            task_id_map={},  # empty — node not mapped
            cookbook_id=cookbook_id,
            poll_interval=0.01, task_timeout=1.0,
        )

        result = await dispatch_cycle(
            _make_ctx(state, deps),
            node_id="missing", task_kind="coder",
            instruction="x", max_iterations=2,
        )
        assert result.startswith("error:")
        assert "no task_id mapped" in result
        http.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_agents_fails_with_recorded_attempt(self):
        db = await get_db()
        await db.execute(
            "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
            ("cb-empty", "empty", "h", datetime.now(timezone.utc).isoformat()),
        )
        await db.commit()
        bundle = await BundleRepo(db).create(
            Bundle(
                id="b-empty", cookbook_id="cb-empty", prompt="x",
                status=BundleStatus.OPEN, created_by="h",
                created_at=datetime.now(timezone.utc),
            )
        )
        task = await TaskRepo(db).create(
            Task(id="t-empty", bundle_id=bundle.id, title="t", status=TaskStatus.OPEN)
        )

        watch = get_watch_service()
        http = AsyncMock(spec=httpx.AsyncClient)
        state = OrchestratorState(prompt="p", bundle_id=bundle.id)
        deps = OrchestratorDeps(
            db=db, http=http, watch=watch,
            task_id_map={"step1": task.id},
            cookbook_id="cb-empty",
            poll_interval=0.01, task_timeout=1.0,
        )
        result = await dispatch_cycle(
            _make_ctx(state, deps),
            node_id="step1", task_kind="coder",
            instruction="x", max_iterations=2,
        )
        assert result.startswith("error:")
        record = state.task_results["step1"]
        assert record.success is False
        assert any(a.status == "no_agent" for a in record.attempts)
        http.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_retries_on_gateway_rejection_then_succeeds(self):
        cookbook_id, task_id, recipe_id = await _seed_with_agent(
            agent_capabilities=["coder"],
        )
        # Add a second agent so retry has a fresh option.
        db = await get_db()
        await AgentRepo(db).upsert_presence(
            AgentPresence(
                agent_id="gw2", cookbook_id=cookbook_id,
                display_name="Gateway Two", capabilities=["coder"],
                max_concurrent_tasks=2,
                endpoint_url="http://gw2/api",
                status=AgentStatus.ONLINE,
                last_heartbeat_at=datetime.now(timezone.utc),
            )
        )

        watch = get_watch_service()
        http = AsyncMock(spec=httpx.AsyncClient)
        # First call rejects (4xx), second accepts.
        http.post.side_effect = [
            _mock_post_response(status_code=400),
            _mock_post_response(json_body={"result": {"id": task_id, "status": {"state": "working"}}}),
        ]

        async def flip_done_after_second_call():
            # Wait until two posts have happened, then mark done.
            for _ in range(50):
                if http.post.await_count >= 2:
                    break
                await asyncio.sleep(0.01)
            await TaskRepo(db).update(task_id, status=TaskStatus.DONE)

        flipper = asyncio.create_task(flip_done_after_second_call())

        state = OrchestratorState(prompt="x", bundle_id="b-cycle", recipe_id=recipe_id)
        deps = OrchestratorDeps(
            db=db, http=http, watch=watch,
            task_id_map={"step1": task_id},
            cookbook_id=cookbook_id,
            poll_interval=0.01, task_timeout=2.0,
        )
        result = await dispatch_cycle(
            _make_ctx(state, deps),
            node_id="step1", task_kind="coder",
            instruction="build", max_iterations=3,
        )
        await flipper

        assert result.startswith("done:")
        record = state.task_results["step1"]
        assert record.success is True
        assert len(record.attempts) == 2
        assert record.attempts[0].status == "rejected"
        assert record.attempts[1].status == "done"
        # Different agents picked across retries
        assert record.attempts[0].agent_id != record.attempts[1].agent_id

    @pytest.mark.asyncio
    async def test_exhausts_max_iterations_when_all_fail(self):
        cookbook_id, task_id, recipe_id = await _seed_with_agent(
            agent_capabilities=["coder"],
        )
        watch = get_watch_service()
        db = await get_db()
        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response(status_code=500)

        state = OrchestratorState(prompt="x", bundle_id="b-cycle", recipe_id=recipe_id)
        deps = OrchestratorDeps(
            db=db, http=http, watch=watch,
            task_id_map={"step1": task_id},
            cookbook_id=cookbook_id,
            poll_interval=0.01, task_timeout=1.0,
        )
        result = await dispatch_cycle(
            _make_ctx(state, deps),
            node_id="step1", task_kind="coder",
            instruction="x", max_iterations=2,
        )
        assert result.startswith("error:")
        record = state.task_results["step1"]
        assert record.success is False
        # 2 rejection attempts + 1 final "exhausted" record
        assert len(record.attempts) == 3
        assert record.attempts[-1].status == "exhausted"

    @pytest.mark.asyncio
    async def test_waits_for_all_dependencies_before_dispatching(self):
        """Regression: fanin steps must wait for every predecessor.

        The LLM-generated graphs omit explicit Join nodes, so pydantic-graph
        fires a downstream step as soon as one predecessor finishes. The
        runtime must consult task.depends_on_task_ids and hold dispatch
        until all siblings reach DONE.
        """
        cookbook_id, task_id, recipe_id = await _seed_with_agent(
            agent_capabilities=["coder"],
        )
        db = await get_db()
        repo = TaskRepo(db)

        # Seed two upstream deps in the same bundle — one already done,
        # one still working.
        seed_task = await repo.get(task_id)
        assert seed_task is not None
        bundle_id = seed_task.bundle_id
        dep_done = await repo.create(
            Task(id=f"{task_id}-d1", bundle_id=bundle_id, title="d1",
                 status=TaskStatus.DONE)
        )
        dep_working = await repo.create(
            Task(id=f"{task_id}-d2", bundle_id=bundle_id, title="d2",
                 status=TaskStatus.WORKING)
        )
        await repo.update(
            task_id, depends_on_task_ids=[dep_done.id, dep_working.id],
        )

        watch = get_watch_service()
        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = _mock_post_response(
            json_body={"result": {"id": task_id, "status": {"state": "submitted"}}},
        )

        # Flip dep2 to DONE after a short wait, then flip the target task
        # to DONE once it has been dispatched.
        flip_log: list[str] = []

        async def orchestrate():
            await asyncio.sleep(0.05)
            await repo.update(dep_working.id, status=TaskStatus.DONE)
            flip_log.append("dep_done_flipped")
            # Wait for dispatch to happen, then flip target to DONE.
            for _ in range(100):
                if http.post.await_count >= 1:
                    break
                await asyncio.sleep(0.01)
            await repo.update(task_id, status=TaskStatus.DONE)
            flip_log.append("target_flipped")

        flipper = asyncio.create_task(orchestrate())

        state = OrchestratorState(
            prompt="x", bundle_id=bundle_id, recipe_id=recipe_id,
        )
        deps = OrchestratorDeps(
            db=db, http=http, watch=watch,
            task_id_map={"fanin": task_id},
            cookbook_id=cookbook_id,
            recipe_meta={"recipe_id": recipe_id},
            poll_interval=0.01, task_timeout=3.0,
        )
        result = await dispatch_cycle(
            _make_ctx(state, deps),
            node_id="fanin", task_kind="coder",
            instruction="join", max_iterations=2,
        )
        await flipper

        assert result.startswith("done:")
        # http.post was called exactly once — dep guard held it until both
        # upstream deps were DONE.
        assert http.post.await_count == 1
        assert "dep_done_flipped" in flip_log
        assert "target_flipped" in flip_log

    @pytest.mark.asyncio
    async def test_fails_when_upstream_dependency_blocked(self):
        """If a predecessor ends BLOCKED, the downstream step must not run."""
        cookbook_id, task_id, recipe_id = await _seed_with_agent(
            agent_capabilities=["coder"],
        )
        db = await get_db()
        repo = TaskRepo(db)
        seed_task = await repo.get(task_id)
        assert seed_task is not None
        bundle_id = seed_task.bundle_id
        dep_blocked = await repo.create(
            Task(id=f"{task_id}-db", bundle_id=bundle_id, title="db",
                 status=TaskStatus.BLOCKED)
        )
        await repo.update(dep_blocked.id, blocked_reason="upstream failed")
        await repo.update(task_id, depends_on_task_ids=[dep_blocked.id])

        watch = get_watch_service()
        http = AsyncMock(spec=httpx.AsyncClient)

        state = OrchestratorState(
            prompt="x", bundle_id=bundle_id, recipe_id=recipe_id,
        )
        deps = OrchestratorDeps(
            db=db, http=http, watch=watch,
            task_id_map={"downstream": task_id},
            cookbook_id=cookbook_id,
            recipe_meta={"recipe_id": recipe_id},
            poll_interval=0.01, task_timeout=0.5,
        )
        result = await dispatch_cycle(
            _make_ctx(state, deps),
            node_id="downstream", task_kind="coder",
            instruction="won't run", max_iterations=2,
        )
        assert result.startswith("error:")
        assert "upstream failure" in result
        http.post.assert_not_called()
        record = state.task_results["downstream"]
        assert record.success is False
        assert "upstream failure" in record.summary

    @pytest.mark.asyncio
    async def test_already_done_task_short_circuits(self):
        cookbook_id, task_id, recipe_id = await _seed_with_agent(
            agent_capabilities=["coder"],
            task_status=TaskStatus.DONE,
        )
        db = await get_db()
        watch = get_watch_service()
        http = AsyncMock(spec=httpx.AsyncClient)

        state = OrchestratorState(prompt="x", bundle_id="b-cycle", recipe_id=recipe_id)
        deps = OrchestratorDeps(
            db=db, http=http, watch=watch,
            task_id_map={"step1": task_id},
            cookbook_id=cookbook_id,
            poll_interval=0.01, task_timeout=1.0,
        )
        result = await dispatch_cycle(
            _make_ctx(state, deps),
            node_id="step1", task_kind="coder",
            instruction="x", max_iterations=2,
        )
        assert result.startswith("done:")
        http.post.assert_not_called()
        assert state.task_results["step1"].success is True


# ---------------------------------------------------------------------------
# State / record dataclass smoke
# ---------------------------------------------------------------------------


def test_attempt_record_is_a_plain_dataclass():
    rec = AttemptRecord(
        iteration=1, agent_id="g", status="done", summary="ok",
        started_at=datetime.now(timezone.utc),
        ended_at=datetime.now(timezone.utc),
    )
    assert rec.iteration == 1
    assert rec.agent_id == "g"


def test_orchestrator_state_mutable_dict():
    s = OrchestratorState(prompt="p", bundle_id="b", recipe_id="r")
    assert s.task_results == {}
    s.task_results["x"] = "anything"  # type: ignore[assignment]
    assert s.task_results["x"] == "anything"
