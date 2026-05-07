"""Slice 1 — SSE / WatchService delivery for invocation events.

Pins down: the InvocationService writes events through `WatchService`
with `resource_type='invocation'`, so:
- Subscribers attached BEFORE create() get the `started` event.
- Subscribers attached AFTER create() can replay from a sequence
  number and receive the missed events.
- Terminal `done` is the last event delivered.

Status: RED.
"""
from __future__ import annotations

import asyncio

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _make_service(hands: dict | None = None):
    from krewhub.db.connection import get_db
    from krewhub.services.invocation_service import InvocationService
    from krewhub.watch.globals import get_watch_service

    db = await get_db()
    return InvocationService(db, hands=hands or {}, watch=get_watch_service())


# ---------------------------------------------------------------------------
# WatchService integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invocation_service_writes_to_watch_log():
    """Every event written via TapeWriter should land in the watch_log
    with resource_type='invocation'."""
    from krewhub.models.invocation import InvocationRequest
    from krewhub.watch.globals import get_watch_service
    from krewhub.watch.types import WatchOptions
    from tests.test_invocation_service import FakeHand

    hand = FakeHand(events=[
        ("output", {"stream": "stdout", "chunk": "hello"}),
    ])
    svc = await _make_service({"fake": hand})

    watch = get_watch_service()
    seq_before = await watch.latest_seq()

    inv = await svc.create(
        InvocationRequest(target="fake:abc", input="x"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=2.0)

    seq_after = await watch.latest_seq()
    assert seq_after > seq_before

    events = await watch.replay(WatchOptions(resource_type="invocation"))
    inv_events = [e for e in events if e.resource_id == inv.tape_id]
    kinds = [e.object.get("kind") for e in inv_events]
    assert "started" in kinds
    assert "output" in kinds
    assert "done" in kinds


@pytest.mark.asyncio
async def test_replay_from_sequence_returns_missed_events():
    """Subscriber connecting late can replay from a known seq."""
    from krewhub.models.invocation import InvocationRequest
    from krewhub.watch.globals import get_watch_service
    from krewhub.watch.types import WatchOptions
    from tests.test_invocation_service import FakeHand

    svc = await _make_service({"fake": FakeHand()})
    watch = get_watch_service()

    seq_baseline = await watch.latest_seq()

    inv = await svc.create(
        InvocationRequest(target="fake:abc", input="x"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=2.0)

    # Replay everything since baseline — should include this invocation's events
    new_events = await watch.replay(
        WatchOptions(resource_type="invocation", since=seq_baseline),
    )
    tape_events = [e for e in new_events if e.resource_id == inv.tape_id]
    assert any(e.object.get("kind") == "started" for e in tape_events)
    assert any(e.object.get("kind") == "done" for e in tape_events)


# ---------------------------------------------------------------------------
# In-process subscription via WatchService
# ---------------------------------------------------------------------------


async def _drain_until(queue: asyncio.Queue, predicate, timeout: float = 2.0) -> list:
    """Read from a WatchService subscriber Queue until predicate hits or
    timeout; return all events seen."""
    seen: list = []
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            break
        try:
            ev = await asyncio.wait_for(queue.get(), timeout=remaining)
        except asyncio.TimeoutError:
            break
        seen.append(ev)
        if predicate(ev):
            break
    return seen


@pytest.mark.asyncio
async def test_subscribe_before_create_receives_started_event():
    """A subscriber attached before invocation creation must observe the
    `started` event in real time."""
    from krewhub.models.invocation import InvocationRequest
    from krewhub.watch.globals import get_watch_service
    from krewhub.watch.types import WatchOptions
    from tests.test_invocation_service import FakeHand

    block = asyncio.Event()  # keep the Hand alive long enough to see started
    hand = FakeHand(block_until_event=block)
    svc = await _make_service({"fake": hand})

    watch = get_watch_service()
    queue = watch.subscribe(WatchOptions(resource_type="invocation"))

    inv = await svc.create(
        InvocationRequest(target="fake:abc", input="x"),
        caller_account_id="acc_test",
    )

    seen = await _drain_until(
        queue,
        lambda ev: (ev.object.get("kind") == "started"
                    and ev.resource_id == inv.tape_id),
    )
    assert any(
        ev.object.get("kind") == "started" and ev.resource_id == inv.tape_id
        for ev in seen
    ), f"subscriber did not receive 'started' event; got {[e.object.get('kind') for e in seen]}"

    # Cleanup the blocking Hand
    await svc.cancel(inv.id, reason="test_done")
    await svc.wait_for_terminal(inv.id, timeout=2.0)
    watch.unsubscribe(queue)


@pytest.mark.asyncio
async def test_subscribe_receives_done_as_last_event():
    """`done` should be the terminal event for a tape (contract §6
    constraint: exactly one `done` per tape)."""
    from krewhub.models.invocation import InvocationRequest
    from krewhub.watch.globals import get_watch_service
    from krewhub.watch.types import WatchOptions
    from tests.test_invocation_service import FakeHand

    svc = await _make_service({"fake": FakeHand()})
    watch = get_watch_service()
    queue = watch.subscribe(WatchOptions(resource_type="invocation"))

    inv = await svc.create(
        InvocationRequest(target="fake:abc", input="x"),
        caller_account_id="acc_test",
    )

    seen = await _drain_until(
        queue,
        lambda ev: (ev.object.get("kind") == "done"
                    and ev.resource_id == inv.tape_id),
    )
    tape_events = [e for e in seen if e.resource_id == inv.tape_id]
    assert tape_events, "no events for our tape received"
    assert tape_events[-1].object.get("kind") == "done"
    watch.unsubscribe(queue)


# ---------------------------------------------------------------------------
# /api/v1/invocations/:id/events long-poll behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_events_long_poll_returns_immediately_when_data_ready(
    _install_fake_hand,
):
    """When events already exist >after, the endpoint must return without
    blocking the full long-poll window."""
    import time
    from httpx import ASGITransport, AsyncClient

    app, _ = _install_fake_hand
    transport = ASGITransport(app=app, raise_app_exceptions=False)

    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-API-Key": "test-key"},
    ) as ac:
        create = await ac.post("/api/v1/invocations", json={
            "target": "fake:abc",
            "input": "x",
        })
        inv_id = create.json()["invocation_id"]
        await asyncio.sleep(0.1)

        t0 = time.monotonic()
        resp = await ac.get(
            f"/api/v1/invocations/{inv_id}/events",
        )
        elapsed = time.monotonic() - t0
        assert resp.status_code == 200
        assert elapsed < 1.0  # fast — events already exist


# ---------------------------------------------------------------------------
# SSE endpoint shape (text/event-stream)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sse_endpoint_serves_text_event_stream(_install_fake_hand):
    """`GET /api/v1/invocations/:id/stream` returns a text/event-stream
    response with at least one `data:` line per event."""
    from httpx import ASGITransport, AsyncClient

    app, _ = _install_fake_hand
    transport = ASGITransport(app=app, raise_app_exceptions=False)

    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-API-Key": "test-key"},
    ) as ac:
        create = await ac.post("/api/v1/invocations", json={
            "target": "fake:abc",
            "input": "x",
        })
        inv_id = create.json()["invocation_id"]

        async with ac.stream(
            "GET", f"/api/v1/invocations/{inv_id}/stream",
        ) as resp:
            assert resp.status_code == 200
            assert "text/event-stream" in resp.headers.get("content-type", "")
            saw_data = False
            saw_done = False
            async for line in resp.aiter_lines():
                if line.startswith("data:"):
                    saw_data = True
                    if '"done"' in line:
                        saw_done = True
                        break
            assert saw_data
            assert saw_done
