"""Integration tests for watch service channel filtering."""
from __future__ import annotations

import asyncio

import pytest

from krewhub.models import WatchEventType
from krewhub.watch.service import WatchService
from krewhub.watch.types import WatchOptions


@pytest.mark.asyncio
async def test_watch_event_has_channel_field(client):
    """A recorded event should have its channel derived."""
    from krewhub.db.connection import get_db
    db = await get_db()

    svc = WatchService(db)
    event = await svc.record(
        resource_type="task",
        resource_id="task_test",
        event_type=WatchEventType.MODIFIED,
        resource_version=1,
        payload={"status": "done", "id": "task_test"},
        recipe_id="rec_test",
    )

    assert event.channel == "task:completed"


@pytest.mark.asyncio
async def test_watch_event_for_digest_submitted(client):
    from krewhub.db.connection import get_db
    db = await get_db()

    svc = WatchService(db)
    event = await svc.record(
        resource_type="digest",
        resource_id="dig_test",
        event_type=WatchEventType.ADDED,
        resource_version=1,
        payload={"decision": "pending"},
        recipe_id="rec_test",
    )

    assert event.channel == "digest:submitted"


@pytest.mark.asyncio
async def test_channel_filter_delivers_matching(client):
    from krewhub.db.connection import get_db
    db = await get_db()

    svc = WatchService(db)
    opts = WatchOptions(channel_prefixes=["task:completed"])
    queue = svc.subscribe(opts)

    try:
        # This matches
        await svc.record(
            resource_type="task",
            resource_id="task_1",
            event_type=WatchEventType.MODIFIED,
            resource_version=1,
            payload={"status": "done"},
        )
        # This does not match
        await svc.record(
            resource_type="task",
            resource_id="task_2",
            event_type=WatchEventType.MODIFIED,
            resource_version=1,
            payload={"status": "claimed"},
        )

        # Should only receive the first one
        event1 = await asyncio.wait_for(queue.get(), timeout=2.0)
        assert event1.channel == "task:completed"
        assert event1.resource_id == "task_1"

        # Queue should be empty
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(queue.get(), timeout=0.5)
    finally:
        svc.unsubscribe(queue)


@pytest.mark.asyncio
async def test_channel_wildcard_filter(client):
    from krewhub.db.connection import get_db
    db = await get_db()

    svc = WatchService(db)
    opts = WatchOptions(channel_prefixes=["task:*"])
    queue = svc.subscribe(opts)

    try:
        # Both match task:*
        await svc.record(
            resource_type="task",
            resource_id="task_a",
            event_type=WatchEventType.MODIFIED,
            resource_version=1,
            payload={"status": "claimed"},
        )
        await svc.record(
            resource_type="task",
            resource_id="task_b",
            event_type=WatchEventType.MODIFIED,
            resource_version=1,
            payload={"status": "done"},
        )
        # This does NOT match task:*
        await svc.record(
            resource_type="digest",
            resource_id="dig_x",
            event_type=WatchEventType.ADDED,
            resource_version=1,
            payload={"decision": "pending"},
        )

        event1 = await asyncio.wait_for(queue.get(), timeout=2.0)
        assert event1.channel.startswith("task:")
        event2 = await asyncio.wait_for(queue.get(), timeout=2.0)
        assert event2.channel.startswith("task:")

        # No more
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(queue.get(), timeout=0.5)
    finally:
        svc.unsubscribe(queue)


@pytest.mark.asyncio
async def test_no_channel_filter_delivers_all(client):
    from krewhub.db.connection import get_db
    db = await get_db()

    svc = WatchService(db)
    opts = WatchOptions()  # no filter
    queue = svc.subscribe(opts)

    try:
        await svc.record(
            resource_type="task",
            resource_id="task_x",
            event_type=WatchEventType.MODIFIED,
            resource_version=1,
            payload={"status": "done"},
        )
        await svc.record(
            resource_type="bundle",
            resource_id="bun_x",
            event_type=WatchEventType.MODIFIED,
            resource_version=1,
            payload={"status": "cooked"},
        )

        e1 = await asyncio.wait_for(queue.get(), timeout=2.0)
        e2 = await asyncio.wait_for(queue.get(), timeout=2.0)
        assert {e1.channel, e2.channel} == {"task:completed", "bundle:cooked"}
    finally:
        svc.unsubscribe(queue)
