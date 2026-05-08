"""Slice 2 — full POST → SSE → terminal flow with SandboxHand.

Wires SandboxHand against a stubbed E2bClient and verifies the round-trip
through the real HTTP routes.

Status: RED.
"""
from __future__ import annotations

import asyncio
import json

import pytest_asyncio
import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def sandbox_app(_setup_db):
    """App wired with SandboxHand backed by a FakeE2bClient."""
    from httpx import ASGITransport, AsyncClient
    from tests.test_sandbox_hand import FakeE2bClient
    from krewhub.app import create_app
    from krewhub.workers.sandbox_hand import SandboxHand
    from krewhub.services.invocation_service import InvocationService
    from krewhub.db.connection import get_db
    from krewhub.watch.globals import get_watch_service

    app = create_app()
    db = await get_db()
    fake_e2b = FakeE2bClient()
    hand = SandboxHand(fake_e2b)
    app.state.invocations = InvocationService(
        db, hands={"sandbox": hand}, watch=get_watch_service(),
    )
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-API-Key": "test-key"},
    ) as ac:
        yield ac, fake_e2b


# ---------------------------------------------------------------------------
# E2E flow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_sandbox_invocation_runs_command(sandbox_app):
    ac, fake_e2b = sandbox_app
    fake_e2b.scripts["sbx_42"] = [
        {"stream": "stdout", "data": "hello world\n"},
        {"exit_code": 0},
    ]

    create = await ac.post("/api/v1/invocations", json={
        "target": "sandbox:sbx_42",
        "input": "echo 'hello world'",
    })
    assert create.status_code == 200
    inv_id = create.json()["invocation_id"]

    # Poll for terminal
    for _ in range(50):
        resp = await ac.get(f"/api/v1/invocations/{inv_id}")
        body = resp.json()
        if body["status"] in ("completed", "errored", "cancelled"):
            break
        await asyncio.sleep(0.02)
    assert body["status"] == "completed"

    inv = body["invocation"]
    assert inv["result"]["action"] == "accept"
    assert inv["result"]["content"]["exit_code"] == 0
    assert "hello world" in inv["result"]["content"]["stdout_tail"]

    # FakeE2bClient was called with the right args
    assert fake_e2b.calls[0]["sandbox_id"] == "sbx_42"
    assert fake_e2b.calls[0]["command"] == "echo 'hello world'"


@pytest.mark.asyncio
async def test_sse_stream_emits_output_and_done(sandbox_app):
    ac, fake_e2b = sandbox_app
    fake_e2b.scripts["sbx_42"] = [
        {"stream": "stdout", "data": "line1\n"},
        {"stream": "stderr", "data": "warn1\n"},
        {"stream": "stdout", "data": "line2\n"},
        {"exit_code": 0},
    ]

    create = await ac.post("/api/v1/invocations", json={
        "target": "sandbox:sbx_42",
        "input": "cmd",
    })
    inv_id = create.json()["invocation_id"]
    # Give the Hand a moment to finish
    await asyncio.sleep(0.1)

    events_resp = await ac.get(f"/api/v1/invocations/{inv_id}/events")
    events = events_resp.json()["events"]
    kinds = [e["kind"] for e in events]
    assert kinds[0] == "started"
    assert kinds[-1] == "done"
    output_events = [e for e in events if e["kind"] == "output"]
    assert len(output_events) == 3
    streams = [e["payload"]["stream"] for e in output_events]
    assert streams == ["stdout", "stderr", "stdout"]


@pytest.mark.asyncio
async def test_dict_input_with_cwd_and_env(sandbox_app):
    ac, fake_e2b = sandbox_app
    fake_e2b.scripts["sbx_42"] = [{"exit_code": 0}]

    resp = await ac.post("/api/v1/invocations", json={
        "target": "sandbox:sbx_42",
        "input": {
            "command": "pwd",
            "cwd": "/workdir",
            "env": {"FOO": "bar"},
        },
    })
    assert resp.status_code == 200
    inv_id = resp.json()["invocation_id"]
    await asyncio.sleep(0.1)

    assert fake_e2b.calls[0]["command"] == "pwd"
    assert fake_e2b.calls[0]["cwd"] == "/workdir"
    assert fake_e2b.calls[0]["env"] == {"FOO": "bar"}


@pytest.mark.asyncio
async def test_e2b_failure_marks_invocation_errored(sandbox_app):
    ac, fake_e2b = sandbox_app
    fake_e2b.scripts["sbx_42"] = [
        {"__raise": ConnectionError("e2b unreachable")},
    ]

    create = await ac.post("/api/v1/invocations", json={
        "target": "sandbox:sbx_42",
        "input": "cmd",
    })
    inv_id = create.json()["invocation_id"]

    for _ in range(50):
        resp = await ac.get(f"/api/v1/invocations/{inv_id}")
        body = resp.json()
        if body["status"] in ("completed", "errored", "cancelled"):
            break
        await asyncio.sleep(0.02)
    assert body["status"] == "errored"
    assert body["invocation"]["result"]["action"] == "error"


@pytest.mark.asyncio
async def test_non_zero_exit_is_completed(sandbox_app):
    """A program returning non-zero is data, not failure."""
    ac, fake_e2b = sandbox_app
    fake_e2b.scripts["sbx_42"] = [
        {"stream": "stderr", "data": "test failed\n"},
        {"exit_code": 1},
    ]

    create = await ac.post("/api/v1/invocations", json={
        "target": "sandbox:sbx_42",
        "input": "false",
    })
    inv_id = create.json()["invocation_id"]
    await asyncio.sleep(0.1)
    resp = await ac.get(f"/api/v1/invocations/{inv_id}")
    body = resp.json()
    assert body["status"] == "completed"
    assert body["invocation"]["result"]["action"] == "accept"
    assert body["invocation"]["result"]["content"]["exit_code"] == 1


# ---------------------------------------------------------------------------
# Phase 5 — agent-driven file ops through the full HTTP → InvocationService
# → SandboxHand path. Plan:
# docs/superpowers/plans/2026-05-08-sandbox-hand-vocabulary.md
# ---------------------------------------------------------------------------


async def _wait_terminal(ac, inv_id: str) -> dict:
    """Poll until the invocation reaches a terminal status (cap ~1s)."""
    for _ in range(50):
        resp = await ac.get(f"/api/v1/invocations/{inv_id}")
        body = resp.json()
        if body["status"] in ("completed", "errored", "cancelled"):
            return body
        await asyncio.sleep(0.02)
    return body


@pytest.mark.asyncio
async def test_op_write_round_trip_through_http(sandbox_app):
    ac, fake_e2b = sandbox_app

    create = await ac.post("/api/v1/invocations", json={
        "target": "sandbox:sbx_42",
        "input": {"op": "write", "path": "/tmp/hi.txt", "data": "hello"},
    })
    assert create.status_code == 200
    inv_id = create.json()["invocation_id"]

    body = await _wait_terminal(ac, inv_id)
    assert body["status"] == "completed"
    result = body["invocation"]["result"]
    assert result["action"] == "accept"
    assert result["content"] == {"path": "/tmp/hi.txt", "bytes_written": 5}

    # FakeE2bClient saw the decoded bytes.
    assert fake_e2b.write_calls == [("sbx_42", "/tmp/hi.txt", b"hello")]


@pytest.mark.asyncio
async def test_op_read_round_trip_through_http(sandbox_app):
    ac, fake_e2b = sandbox_app
    fake_e2b.read_files["/tmp/hi.txt"] = b"hello"

    create = await ac.post("/api/v1/invocations", json={
        "target": "sandbox:sbx_42",
        "input": {"op": "read", "path": "/tmp/hi.txt"},
    })
    assert create.status_code == 200
    inv_id = create.json()["invocation_id"]

    body = await _wait_terminal(ac, inv_id)
    assert body["status"] == "completed"
    result = body["invocation"]["result"]
    assert result["action"] == "accept"
    assert result["content"] == {
        "path": "/tmp/hi.txt",
        "data": "hello",
        "encoding": "utf-8",
    }


@pytest.mark.asyncio
async def test_op_list_round_trip_through_http(sandbox_app):
    ac, fake_e2b = sandbox_app
    entries = [
        {"name": "hi.txt", "type": "FILE_TYPE_FILE", "path": "/tmp/hi.txt", "size": "5"},
        {"name": "sub", "type": "FILE_TYPE_DIRECTORY", "path": "/tmp/sub", "size": "4096"},
    ]
    fake_e2b.list_entries["/tmp"] = entries

    create = await ac.post("/api/v1/invocations", json={
        "target": "sandbox:sbx_42",
        "input": {"op": "list", "path": "/tmp"},
    })
    assert create.status_code == 200
    inv_id = create.json()["invocation_id"]

    body = await _wait_terminal(ac, inv_id)
    assert body["status"] == "completed"
    result = body["invocation"]["result"]
    assert result["action"] == "accept"
    assert result["content"] == {"path": "/tmp", "entries": entries}


@pytest.mark.asyncio
async def test_op_read_path_not_found_marks_errored(sandbox_app):
    """Distinct from exec: a missing-path read is `errored`, not `accept`
    with a non-zero exit. Stable reason code lets the agent branch on it."""
    ac, _fake_e2b = sandbox_app

    create = await ac.post("/api/v1/invocations", json={
        "target": "sandbox:sbx_42",
        "input": {"op": "read", "path": "/tmp/nope"},
    })
    inv_id = create.json()["invocation_id"]

    body = await _wait_terminal(ac, inv_id)
    assert body["status"] == "errored"
    result = body["invocation"]["result"]
    assert result["action"] == "error"
    assert "path_not_found" in (result.get("reason") or "")
