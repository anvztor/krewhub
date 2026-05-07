"""Slice 1 — HTTP routes for /api/v1/invocations (contract §9).

Tests the full HTTP surface end-to-end via the existing `client`
fixture (X-API-Key auth) and `cookie_client` fixture (session cookie):

  POST   /api/v1/invocations
  GET    /api/v1/invocations/:id
  GET    /api/v1/invocations/:id/events?after=N
  POST   /api/v1/invocations/:id/result
  POST   /api/v1/invocations/:id/cancel

A test FakeHand is registered into the service so the routes have
something to dispatch to without slice 2's e2b dependency.

Status: RED.
"""
from __future__ import annotations

import asyncio

import pytest

# `_install_fake_hand` and `inv_client` fixtures are provided by
# tests/conftest_invocations.py (re-exported through tests/conftest.py).


# ---------------------------------------------------------------------------
# POST /api/v1/invocations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_invocation_returns_running(inv_client):
    resp = await inv_client.post("/api/v1/invocations", json={
        "target": "fake:abc",
        "input": "hi",
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["invocation_id"].startswith("inv_")
    assert body["tape_id"].startswith("tape_")
    assert body["status"] in ("running", "completed")  # may finish before response


@pytest.mark.asyncio
async def test_post_invocation_with_dict_input(inv_client):
    resp = await inv_client.post("/api/v1/invocations", json={
        "target": "fake:abc",
        "input": {"command": "ls", "cwd": "/tmp"},
        "label": "list /tmp",
        "deadline_s": 60,
    })
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_post_invocation_with_schema(inv_client):
    resp = await inv_client.post("/api/v1/invocations", json={
        "target": "fake:abc",
        "input": "give me a name",
        "schema": {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
        },
    })
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_post_invocation_bad_target_returns_400(inv_client):
    resp = await inv_client.post("/api/v1/invocations", json={
        "target": "human:should_not_have_id",
        "input": "x",
    })
    assert resp.status_code == 400
    assert "target" in resp.text.lower()


@pytest.mark.asyncio
async def test_post_invocation_unknown_target_type_returns_400(inv_client):
    resp = await inv_client.post("/api/v1/invocations", json={
        "target": "workflow:abc",
        "input": "x",
    })
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_post_invocation_nested_schema_returns_400(inv_client):
    resp = await inv_client.post("/api/v1/invocations", json={
        "target": "fake:abc",
        "input": "x",
        "schema": {
            "type": "object",
            "properties": {
                "address": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                },
            },
        },
    })
    assert resp.status_code == 400
    assert "nested" in resp.text.lower() or "schema" in resp.text.lower()


@pytest.mark.asyncio
async def test_post_invocation_deadline_out_of_bounds_returns_422(inv_client):
    resp = await inv_client.post("/api/v1/invocations", json={
        "target": "fake:abc",
        "input": "x",
        "deadline_s": 0,
    })
    assert resp.status_code in (400, 422)


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_idempotent_post_returns_existing_invocation(inv_client):
    body = {
        "target": "fake:abc",
        "input": "hi",
        "parent_tape_id": "tape_parent_xyz",
        "idempotency_key": "key_42",
    }
    r1 = await inv_client.post("/api/v1/invocations", json=body)
    r2 = await inv_client.post("/api/v1/invocations", json=body)
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["invocation_id"] == r2.json()["invocation_id"]


# ---------------------------------------------------------------------------
# GET /api/v1/invocations/:id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_invocation_returns_full_state(inv_client):
    create = await inv_client.post("/api/v1/invocations", json={
        "target": "fake:abc",
        "input": "hi",
    })
    inv_id = create.json()["invocation_id"]

    # Poll for terminal
    for _ in range(50):
        resp = await inv_client.get(f"/api/v1/invocations/{inv_id}")
        body = resp.json()
        if body["status"] in ("completed", "errored", "cancelled"):
            break
        await asyncio.sleep(0.02)
    else:
        pytest.fail("invocation never reached terminal state")

    assert "invocation" in body or "status" in body
    # Acceptable shapes: {invocation: {...}} or flat {id, status, ...}.
    inv = body.get("invocation", body)
    assert inv["status"] == "completed"


@pytest.mark.asyncio
async def test_get_unknown_invocation_returns_404(inv_client):
    resp = await inv_client.get("/api/v1/invocations/inv_does_not_exist")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/v1/invocations/:id/events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_events_returns_started_and_done(inv_client):
    create = await inv_client.post("/api/v1/invocations", json={
        "target": "fake:abc",
        "input": "hi",
    })
    inv_id = create.json()["invocation_id"]

    # Allow the Hand task to run
    await asyncio.sleep(0.05)

    resp = await inv_client.get(f"/api/v1/invocations/{inv_id}/events")
    assert resp.status_code == 200
    body = resp.json()
    events = body["events"]
    kinds = [e["kind"] for e in events]
    assert "started" in kinds
    assert "done" in kinds
    # Monotonic
    ids = [e["id"] for e in events]
    assert ids == sorted(ids)


@pytest.mark.asyncio
async def test_events_after_filter_returns_only_newer(inv_client):
    create = await inv_client.post("/api/v1/invocations", json={
        "target": "fake:abc",
        "input": "hi",
    })
    inv_id = create.json()["invocation_id"]
    await asyncio.sleep(0.05)

    full = await inv_client.get(f"/api/v1/invocations/{inv_id}/events")
    full_events = full.json()["events"]
    if len(full_events) < 2:
        pytest.skip("need at least 2 events to test the after filter")

    after = full_events[0]["id"]
    resp = await inv_client.get(
        f"/api/v1/invocations/{inv_id}/events?after={after}",
    )
    body = resp.json()
    for e in body["events"]:
        assert e["id"] > after


# ---------------------------------------------------------------------------
# POST /api/v1/invocations/:id/result  (manual close, used by HumanHand)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_result_closes_running_invocation(inv_client, _install_fake_hand):
    """When the Hand blocks waiting for an external signal (HumanHand
    pattern), POST /:id/result delivers the operator's envelope and
    causes execute() to return that envelope."""
    import asyncio as _asyncio
    from tests.test_invocation_service import FakeHand
    from krewhub.services.invocation_service import InvocationService
    from krewhub.db.connection import get_db
    from krewhub.watch.globals import get_watch_service

    app, _default_hand = _install_fake_hand

    # Replace the service's hands with a blocking Hand whose only exit
    # path is `submit_result()` plumbed through the service.
    block = _asyncio.Event()  # never set
    blocking = FakeHand(target_type="human", block_until_event=block)
    db = await get_db()
    app.state.invocations = InvocationService(
        db, hands={"human": blocking}, watch=get_watch_service(),
    )

    create = await inv_client.post("/api/v1/invocations", json={
        "target": "human",
        "input": "what should I do?",
    })
    inv_id = create.json()["invocation_id"]

    submit = await inv_client.post(
        f"/api/v1/invocations/{inv_id}/result",
        json={
            "action": "accept",
            "content": "do the thing",
        },
    )
    assert submit.status_code == 200

    # Within ~1s the invocation should be terminal
    for _ in range(50):
        resp = await inv_client.get(f"/api/v1/invocations/{inv_id}")
        inv = resp.json().get("invocation", resp.json())
        if inv.get("status") in ("completed", "cancelled", "errored"):
            break
        await _asyncio.sleep(0.02)

    assert inv["status"] == "completed"


@pytest.mark.asyncio
async def test_post_result_on_terminal_returns_409(inv_client):
    create = await inv_client.post("/api/v1/invocations", json={
        "target": "fake:abc",
        "input": "x",
    })
    inv_id = create.json()["invocation_id"]
    # Wait for terminal
    await asyncio.sleep(0.1)

    resp = await inv_client.post(
        f"/api/v1/invocations/{inv_id}/result",
        json={"action": "accept", "content": "late"},
    )
    assert resp.status_code in (409, 400)


# ---------------------------------------------------------------------------
# POST /api/v1/invocations/:id/cancel
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_cancel_terminates_running(inv_client, _install_fake_hand):
    import asyncio as _asyncio
    from tests.test_invocation_service import FakeHand
    from krewhub.services.invocation_service import InvocationService
    from krewhub.db.connection import get_db
    from krewhub.watch.globals import get_watch_service

    app, _ = _install_fake_hand
    block = _asyncio.Event()
    blocking = FakeHand(target_type="fake", block_until_event=block)
    db = await get_db()
    app.state.invocations = InvocationService(
        db, hands={"fake": blocking}, watch=get_watch_service(),
    )

    create = await inv_client.post("/api/v1/invocations", json={
        "target": "fake:abc",
        "input": "long-running",
    })
    inv_id = create.json()["invocation_id"]
    await _asyncio.sleep(0.02)  # let Hand start

    resp = await inv_client.post(f"/api/v1/invocations/{inv_id}/cancel")
    assert resp.status_code == 200

    for _ in range(50):
        get_resp = await inv_client.get(f"/api/v1/invocations/{inv_id}")
        inv = get_resp.json().get("invocation", get_resp.json())
        if inv.get("status") in ("completed", "cancelled", "errored"):
            break
        await _asyncio.sleep(0.02)
    assert inv["status"] == "cancelled"


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_invocation_requires_auth(_install_fake_hand):
    from httpx import ASGITransport, AsyncClient

    app, _ = _install_fake_hand
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post("/api/v1/invocations", json={
            "target": "fake:abc",
            "input": "x",
        })
    assert resp.status_code in (401, 403)


@pytest.mark.asyncio
async def test_post_invocation_accepts_cookie_auth(_install_fake_hand):
    """Cookie auth (browser path) must work alongside X-API-Key (BFF path)."""
    import jwt
    from httpx import ASGITransport, AsyncClient

    from krewhub.config import get_settings

    app, _ = _install_fake_hand
    settings = get_settings()
    token = jwt.encode(
        {"sub": "acc_test_cookie", "username": "cookie_tester", "method": "passkey"},
        settings.jwt_secret,
        algorithm="HS256",
    )
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={"krew_session": token},
    ) as ac:
        resp = await ac.post("/api/v1/invocations", json={
            "target": "fake:abc",
            "input": "x",
        })
    assert resp.status_code == 200
