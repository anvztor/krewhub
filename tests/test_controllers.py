from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from krewhub.controllers.bundle_controller import BundleController, _compute_bundle_phase
from krewhub.controllers.presence_controller import PresenceController
from krewhub.controllers.manager import ControllerManager
from krewhub.db.connection import get_db
from krewhub.models import BundleStatus, TaskStatus, AgentStatus
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.watch.globals import get_watch_service


# --- BundleController tests ---


@pytest.mark.asyncio
async def test_compute_bundle_phase_all_done():
    """Pure function: all tasks done → cooked."""
    from krewhub.models import Task

    tasks = [
        Task(id="t1", bundle_id="b1", title="A", status=TaskStatus.DONE),
        Task(id="t2", bundle_id="b1", title="B", status=TaskStatus.DONE),
    ]
    assert _compute_bundle_phase(tasks) == BundleStatus.COOKED


@pytest.mark.asyncio
async def test_compute_bundle_phase_any_blocked():
    from krewhub.models import Task

    tasks = [
        Task(id="t1", bundle_id="b1", title="A", status=TaskStatus.DONE),
        Task(id="t2", bundle_id="b1", title="B", status=TaskStatus.BLOCKED, blocked_reason="oops"),
    ]
    assert _compute_bundle_phase(tasks) == BundleStatus.BLOCKED


@pytest.mark.asyncio
async def test_compute_bundle_phase_any_claimed():
    from krewhub.models import Task

    tasks = [
        Task(id="t1", bundle_id="b1", title="A", status=TaskStatus.WORKING),
        Task(id="t2", bundle_id="b1", title="B", status=TaskStatus.OPEN),
    ]
    assert _compute_bundle_phase(tasks) == BundleStatus.CLAIMED


@pytest.mark.asyncio
async def test_compute_bundle_phase_all_open():
    from krewhub.models import Task

    tasks = [
        Task(id="t1", bundle_id="b1", title="A", status=TaskStatus.OPEN),
        Task(id="t2", bundle_id="b1", title="B", status=TaskStatus.OPEN),
    ]
    assert _compute_bundle_phase(tasks) == BundleStatus.OPEN


@pytest.mark.asyncio
async def test_bundle_controller_reconciles_status(client):
    """BundleController should update bundle status when tasks change."""
    db = await get_db()
    watch = get_watch_service()

    # Create a recipe and bundle via API
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/ctrl",
        "repo_url": "git@github.com:test/ctrl.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Test controller reconciliation",
        "requested_by": "human_1",
        "tasks": [{"title": "Task A"}, {"title": "Task B"}],
    })
    bundle_id = resp.json()["bundle"]["id"]
    tasks = resp.json()["tasks"]

    # Manually mark both tasks as done in the repo (bypassing service layer)
    task_repo = TaskRepo(db)
    now = datetime.now(timezone.utc)
    for t in tasks:
        await task_repo.update(t["id"], status=TaskStatus.DONE, completed_at=now)

    # Bundle should still be "open" because no one recomputed yet
    bundle = await BundleRepo(db).get(bundle_id)
    assert bundle.status == BundleStatus.OPEN

    # Run the controller reconcile
    controller = BundleController(db, watch)
    await controller.reconcile()

    # Now bundle should be "cooked"
    bundle = await BundleRepo(db).get(bundle_id)
    assert bundle.status == BundleStatus.COOKED
    assert bundle.cooked_at is not None


@pytest.mark.asyncio
async def test_bundle_controller_skips_terminal_bundles(client):
    """Controller should not touch cancelled/digested/rejected bundles."""
    db = await get_db()
    watch = get_watch_service()

    resp = await client.post("/api/v1/recipes", json={
        "name": "test/ctrl-terminal",
        "repo_url": "git@github.com:test/ctrl-terminal.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Test cancelled bundle",
        "requested_by": "human_1",
        "tasks": [{"title": "Task A"}],
    })
    bundle_id = resp.json()["bundle"]["id"]

    # Cancel the bundle
    await client.patch(f"/api/v1/bundles/{bundle_id}")

    bundle = await BundleRepo(db).get(bundle_id)
    assert bundle.status == BundleStatus.CANCELLED

    # Reconcile should not change it
    controller = BundleController(db, watch)
    await controller.reconcile()

    bundle = await BundleRepo(db).get(bundle_id)
    assert bundle.status == BundleStatus.CANCELLED


# --- PresenceController tests ---


@pytest.mark.asyncio
async def test_presence_controller_marks_stale_agents_offline(client):
    """Agents with expired heartbeats should be marked offline."""
    db = await get_db()
    watch = get_watch_service()

    resp = await client.post("/api/v1/recipes", json={
        "name": "test/presence",
        "repo_url": "git@github.com:test/presence.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    # Register agent via heartbeat
    await client.post("/api/v1/agents/heartbeat", json={
        "agent_id": "agent_stale",
        "recipe_id": recipe_id,
        "display_name": "Stale Agent",
        "capabilities": ["claim"],
    })

    agent_repo = AgentRepo(db)
    agent = await agent_repo.get("agent_stale", recipe_id)
    assert agent.status == AgentStatus.ONLINE

    # Backdate the heartbeat to simulate staleness
    old_time = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()
    await db.execute(
        "UPDATE agent_presence SET last_heartbeat_at = ? WHERE agent_id = ?",
        (old_time, "agent_stale"),
    )
    await db.commit()

    # Run presence controller with a short timeout
    controller = PresenceController(db, watch, heartbeat_timeout=30.0)
    await controller.reconcile()

    agent = await agent_repo.get("agent_stale", recipe_id)
    assert agent.status == AgentStatus.OFFLINE


@pytest.mark.asyncio
async def test_presence_controller_releases_tasks_from_offline_agents(client):
    """Tasks held by agents that go offline should be released to open."""
    db = await get_db()
    watch = get_watch_service()

    resp = await client.post("/api/v1/recipes", json={
        "name": "test/presence-release",
        "repo_url": "git@github.com:test/presence-release.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    # Create a bundle and claim a task
    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Test task release",
        "requested_by": "human_1",
        "tasks": [{"title": "Releasable task"}],
    })
    task_id = resp.json()["tasks"][0]["id"]

    await client.post("/api/v1/agents/heartbeat", json={
        "agent_id": "agent_release",
        "recipe_id": recipe_id,
        "display_name": "Release Agent",
        "capabilities": ["claim"],
    })

    await client.post(f"/api/v1/tasks/{task_id}/claim", json={
        "agent_id": "agent_release",
    })

    task_repo = TaskRepo(db)
    task = await task_repo.get(task_id)
    assert task.status == TaskStatus.CLAIMED

    # Backdate heartbeat
    old_time = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()
    await db.execute(
        "UPDATE agent_presence SET last_heartbeat_at = ? WHERE agent_id = ?",
        (old_time, "agent_release"),
    )
    await db.commit()

    # Run presence controller
    controller = PresenceController(db, watch, heartbeat_timeout=30.0)
    await controller.reconcile()

    # Task should be released back to open
    task = await task_repo.get(task_id)
    assert task.status == TaskStatus.OPEN
    assert task.claimed_by_agent_id is None


# --- ControllerManager tests ---


@pytest.mark.asyncio
async def test_controller_manager_start_stop():
    """Manager should start and stop controllers cleanly."""
    db = await get_db()
    watch = get_watch_service()

    manager = ControllerManager(db, watch)
    await manager.start_all()

    health = manager.health()
    assert all(running for running in health.values())
    assert "BundleController" in health
    assert "PresenceController" in health

    await manager.stop_all()

    health = manager.health()
    assert all(not running for running in health.values())
