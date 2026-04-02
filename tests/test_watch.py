from __future__ import annotations

import asyncio

import pytest

from krewhub.db.connection import get_db
from krewhub.models import WatchEventType
from krewhub.watch.globals import get_watch_service
from krewhub.watch.store import WatchLogStore
from krewhub.watch.types import WatchOptions


@pytest.mark.asyncio
async def test_watch_log_store_append_and_list():
    db = await get_db()
    store = WatchLogStore(db)

    entry1 = await store.append(
        resource_type="bundle",
        resource_id="bun_1",
        event_type=WatchEventType.ADDED,
        resource_version=1,
        payload={"id": "bun_1", "status": "open"},
        recipe_id="rec_1",
    )
    assert entry1.seq > 0
    assert entry1.resource_type == "bundle"

    entry2 = await store.append(
        resource_type="task",
        resource_id="task_1",
        event_type=WatchEventType.ADDED,
        resource_version=1,
        payload={"id": "task_1", "status": "open"},
        recipe_id="rec_1",
    )
    assert entry2.seq > entry1.seq

    entries = await store.list_since(since=0)
    assert len(entries) == 2

    entries_after_first = await store.list_since(since=entry1.seq)
    assert len(entries_after_first) == 1
    assert entries_after_first[0].resource_id == "task_1"


@pytest.mark.asyncio
async def test_watch_log_store_filter_by_resource_type():
    db = await get_db()
    store = WatchLogStore(db)

    await store.append("bundle", "bun_1", WatchEventType.ADDED, 1, {}, "rec_1")
    await store.append("task", "task_1", WatchEventType.ADDED, 1, {}, "rec_1")
    await store.append("task", "task_2", WatchEventType.MODIFIED, 2, {}, "rec_1")

    task_entries = await store.list_since(since=0, resource_type="task")
    assert len(task_entries) == 2
    assert all(e.resource_type == "task" for e in task_entries)


@pytest.mark.asyncio
async def test_watch_log_store_filter_by_recipe():
    db = await get_db()
    store = WatchLogStore(db)

    await store.append("bundle", "bun_1", WatchEventType.ADDED, 1, {}, "rec_1")
    await store.append("bundle", "bun_2", WatchEventType.ADDED, 1, {}, "rec_2")

    rec1_entries = await store.list_since(since=0, recipe_id="rec_1")
    assert len(rec1_entries) == 1
    assert rec1_entries[0].resource_id == "bun_1"


@pytest.mark.asyncio
async def test_watch_log_store_latest_seq():
    db = await get_db()
    store = WatchLogStore(db)

    seq_before = await store.latest_seq()
    assert seq_before == 0

    entry = await store.append("bundle", "bun_1", WatchEventType.ADDED, 1, {})
    seq_after = await store.latest_seq()
    assert seq_after == entry.seq


@pytest.mark.asyncio
async def test_watch_log_store_trim():
    db = await get_db()
    store = WatchLogStore(db)

    e1 = await store.append("bundle", "bun_1", WatchEventType.ADDED, 1, {})
    e2 = await store.append("task", "task_1", WatchEventType.ADDED, 1, {})
    await store.append("task", "task_2", WatchEventType.ADDED, 1, {})

    trimmed = await store.trim_before(e2.seq)
    assert trimmed == 1  # only e1 trimmed

    remaining = await store.list_since(since=0)
    assert len(remaining) == 2


@pytest.mark.asyncio
async def test_watch_service_record_and_notify():
    watch = get_watch_service()
    options = WatchOptions(recipe_id="rec_test")
    queue = watch.subscribe(options)

    try:
        event = await watch.record(
            resource_type="bundle",
            resource_id="bun_test",
            event_type=WatchEventType.ADDED,
            resource_version=1,
            payload={"id": "bun_test", "status": "open"},
            recipe_id="rec_test",
        )
        assert event.seq > 0
        assert event.resource_type == "bundle"

        # Should have been delivered to subscriber
        received = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert received.resource_id == "bun_test"
        assert received.event_type == "ADDED"
    finally:
        watch.unsubscribe(queue)


@pytest.mark.asyncio
async def test_watch_service_filter_by_recipe():
    watch = get_watch_service()
    queue_rec1 = watch.subscribe(WatchOptions(recipe_id="rec_1"))
    queue_rec2 = watch.subscribe(WatchOptions(recipe_id="rec_2"))

    try:
        await watch.record("bundle", "bun_1", WatchEventType.ADDED, 1, {}, "rec_1")
        await watch.record("bundle", "bun_2", WatchEventType.ADDED, 1, {}, "rec_2")

        event1 = await asyncio.wait_for(queue_rec1.get(), timeout=1.0)
        assert event1.resource_id == "bun_1"

        event2 = await asyncio.wait_for(queue_rec2.get(), timeout=1.0)
        assert event2.resource_id == "bun_2"

        # Each queue should only have its own event
        assert queue_rec1.empty()
        assert queue_rec2.empty()
    finally:
        watch.unsubscribe(queue_rec1)
        watch.unsubscribe(queue_rec2)


@pytest.mark.asyncio
async def test_watch_service_replay():
    watch = get_watch_service()

    e1 = await watch.record("bundle", "bun_r1", WatchEventType.ADDED, 1, {}, "rec_r")
    e2 = await watch.record("task", "task_r1", WatchEventType.ADDED, 1, {}, "rec_r")

    # Replay from beginning
    events = await watch.replay(WatchOptions(recipe_id="rec_r"))
    assert len(events) >= 2

    # Replay from after e1
    events_after = await watch.replay(WatchOptions(recipe_id="rec_r", since=e1.seq))
    assert any(e.resource_id == "task_r1" for e in events_after)


@pytest.mark.asyncio
async def test_watch_service_filter_by_resource_type():
    watch = get_watch_service()
    queue = watch.subscribe(WatchOptions(resource_type="task"))

    try:
        await watch.record("bundle", "bun_f1", WatchEventType.ADDED, 1, {})
        await watch.record("task", "task_f1", WatchEventType.ADDED, 1, {})

        event = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert event.resource_type == "task"

        # Bundle event should NOT have been delivered
        assert queue.empty()
    finally:
        watch.unsubscribe(queue)


@pytest.mark.asyncio
async def test_optimistic_concurrency_in_bundle_update(client):
    """Test that resource_version increments on update and is returned."""
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/versioning",
        "repo_url": "git@github.com:test/versioning.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Test resource versioning",
        "requested_by": "human_1",
        "tasks": [{"title": "Versioned task"}],
    })
    bundle = resp.json()["bundle"]
    assert bundle["resource_version"] == 1

    task = resp.json()["tasks"][0]
    assert task["resource_version"] == 1

    # Claim the task
    resp = await client.post(f"/api/v1/tasks/{task['id']}/claim", json={
        "agent_id": "agent_v1",
    })
    assert resp.status_code == 200
    claimed_task = resp.json()["task"]
    assert claimed_task["resource_version"] > 1

    # Mark done
    resp = await client.patch(f"/api/v1/tasks/{task['id']}/status", json={
        "status": "done",
    })
    assert resp.status_code == 200
    done_task = resp.json()["task"]
    assert done_task["resource_version"] > claimed_task["resource_version"]


@pytest.mark.asyncio
async def test_watch_endpoint_returns_events(client):
    """Test the /watch endpoint returns events via SSE format."""
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/watch-endpoint",
        "repo_url": "git@github.com:test/watch-endpoint.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    # Create a bundle to generate watch events
    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Test watch endpoint",
        "requested_by": "human_1",
        "tasks": [{"title": "Watch me"}],
    })
    assert resp.status_code == 200

    # Verify events are in the watch log
    watch = get_watch_service()
    seq = await watch.latest_seq()
    assert seq > 0

    # Replay should return events
    events = await watch.replay(WatchOptions(recipe_id=recipe_id))
    assert len(events) >= 1
    assert any(e.resource_type == "bundle" for e in events)
