"""Slice 6 — AgentHand unit + service integration tests.

AgentHand bridges `delegate(to="agent:<id>", ...)` to the existing
A2A invocation queue. The krewcli daemon owns the dispatch + sub-Brain
execution side; AgentHand only:
  1. Parses target_id into (agent_name, owner) per the @-convention.
  2. Creates a row in `a2a_invocations` with `method="delegate"` and
     params={input, schema?}.
  3. Polls the row until it reaches a terminal status, or until cancel /
     deadline fires.
  4. Maps the A2A status into a ResultEnvelope:
        completed → accept, content = result text
        failed    → error,  reason = error text
        timeout   → cancel, reason = "a2a_timeout"

Tests use the real A2AInvocationRepo against the in-memory DB, with a
background task that "completes" the row to simulate the daemon.

Status: RED.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import pytest


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _CapturingTape:
    def __init__(self, tape_id: str = "tape_test") -> None:
        self.tape_id = tape_id
        self.events: list[dict] = []
        self._next_id = 0

    async def append(
        self, kind, *, body="", payload=None,
        actor_type="system", actor_id="", parent_id=None,
    ):
        from krewhub.models.invocation import Event
        ev = Event(
            tape_id=self.tape_id,
            id=self._next_id,
            actor_type=actor_type,  # type: ignore[arg-type]
            actor_id=actor_id,
            kind=kind,
            body=body,
            payload=payload or {},
            ts=datetime.now(timezone.utc),
        )
        self._next_id += 1
        self.events.append({
            "kind": kind, "body": body, "payload": payload or {},
            "actor_type": actor_type, "actor_id": actor_id,
        })
        return ev


class _StubCancel:
    def __init__(self) -> None:
        self.cancelled = False
        self.reason: str | None = None
        self._event = asyncio.Event()

    async def wait(self):
        await self._event.wait()

    def raise_if_cancelled(self):
        if self.cancelled:
            raise asyncio.CancelledError(self.reason)

    def fire(self, reason):
        self.cancelled = True
        self.reason = reason
        self._event.set()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _complete_a2a_after(
    delay: float, agent_name: str, owner: str,
    *, status: str = "completed", result: str | None = "ok",
    error: str | None = None,
) -> None:
    """Background helper: after `delay`, find the most-recent pending
    A2A invocation matching (owner, agent_name) and mark it terminal."""
    from krewhub.db.connection import get_db
    from krewhub.repositories.a2a_invocation_repo import A2AInvocationRepo

    await asyncio.sleep(delay)
    db = await get_db()
    repo = A2AInvocationRepo(db)
    pending = await repo.list_pending(owner, agent_name)
    if not pending:
        return
    inv_id = pending[-1]["invocation_id"]
    await repo.update_status(inv_id, status, result=result, error=error)


async def _make_service(hands: dict | None = None):
    from krewhub.db.connection import get_db
    from krewhub.services.invocation_service import InvocationService
    from krewhub.watch.globals import get_watch_service

    db = await get_db()
    return InvocationService(db, hands=hands or {}, watch=get_watch_service())


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_target_type_attribute():
    from krewhub.workers.agent_hand import AgentHand
    from krewhub.db.connection import get_db
    db = await get_db()
    assert AgentHand(db).target_type == "agent"


@pytest.mark.asyncio
async def test_creates_a2a_invocation_with_target_split_on_at_sign():
    """target_id 'claude@krew' → agent_name='claude', owner='krew'."""
    from krewhub.db.connection import get_db
    from krewhub.repositories.a2a_invocation_repo import A2AInvocationRepo
    from krewhub.workers.agent_hand import AgentHand

    db = await get_db()
    hand = AgentHand(db, poll_interval_s=0.05)
    tape = _CapturingTape()
    cancel = _StubCancel()

    asyncio.create_task(_complete_a2a_after(0.1, "claude", "krew", result="hi"))

    result = await hand.execute(
        target_id="claude@krew",
        input="say hi",
        schema=None,
        deadline_s=10,
        tape=tape,  # type: ignore[arg-type]
        cancel=cancel,  # type: ignore[arg-type]
    )

    assert result.action == "accept"
    assert result.content == "hi"

    # Validate the row went into a2a_invocations with the right shape.
    pending = await A2AInvocationRepo(db).list_pending("krew", "claude")
    # Should be empty now (we marked it completed)
    assert pending == []


@pytest.mark.asyncio
async def test_target_id_without_at_sign_uses_default_owner():
    """`target_id='claude'` (no @owner) is allowed — agent_name='claude',
    owner falls back to the invocation's caller account_id."""
    from krewhub.db.connection import get_db
    from krewhub.repositories.a2a_invocation_repo import A2AInvocationRepo
    from krewhub.workers.agent_hand import AgentHand

    db = await get_db()
    hand = AgentHand(db, poll_interval_s=0.05, default_owner="acc_123")
    tape = _CapturingTape()
    cancel = _StubCancel()

    asyncio.create_task(_complete_a2a_after(0.1, "claude", "acc_123", result="ok"))

    result = await hand.execute(
        target_id="claude",
        input="hi",
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )
    assert result.action == "accept"
    assert result.content == "ok"


@pytest.mark.asyncio
async def test_no_target_id_returns_error():
    from krewhub.db.connection import get_db
    from krewhub.workers.agent_hand import AgentHand

    db = await get_db()
    hand = AgentHand(db)
    tape = _CapturingTape()
    cancel = _StubCancel()

    result = await hand.execute(
        target_id=None, input="hi",
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )
    assert result.action == "error"
    assert "target_id" in (result.reason or "")


@pytest.mark.asyncio
async def test_returns_accept_with_string_input():
    from krewhub.db.connection import get_db
    from krewhub.workers.agent_hand import AgentHand

    db = await get_db()
    hand = AgentHand(db, poll_interval_s=0.05)
    tape = _CapturingTape()
    cancel = _StubCancel()

    asyncio.create_task(_complete_a2a_after(0.1, "echo", "krew", result="echoed back"))
    result = await hand.execute(
        target_id="echo@krew", input="hello",
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )
    assert result.action == "accept"
    assert result.content == "echoed back"


@pytest.mark.asyncio
async def test_a2a_failed_returns_error_envelope():
    from krewhub.db.connection import get_db
    from krewhub.workers.agent_hand import AgentHand

    db = await get_db()
    hand = AgentHand(db, poll_interval_s=0.05)
    tape = _CapturingTape()
    cancel = _StubCancel()

    asyncio.create_task(
        _complete_a2a_after(0.1, "claude", "krew", status="failed", result=None, error="boom"),
    )
    result = await hand.execute(
        target_id="claude@krew", input="run the thing",
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )
    assert result.action == "error"
    assert "boom" in (result.reason or "")


@pytest.mark.asyncio
async def test_cancel_marks_a2a_timeout_and_returns_cancel():
    from krewhub.db.connection import get_db
    from krewhub.workers.agent_hand import AgentHand

    db = await get_db()
    hand = AgentHand(db, poll_interval_s=0.05)
    tape = _CapturingTape()
    cancel = _StubCancel()

    async def _cancel_soon():
        await asyncio.sleep(0.1)
        cancel.fire("operator_cancelled")

    asyncio.create_task(_cancel_soon())
    result = await hand.execute(
        target_id="claude@krew", input="x",
        schema=None, deadline_s=30,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )
    assert result.action == "cancel"
    assert result.reason == "operator_cancelled"


@pytest.mark.asyncio
async def test_input_dict_is_encoded_into_a2a_params():
    """When input is a dict, the whole dict goes to a2a_invocations.params."""
    from krewhub.db.connection import get_db
    from krewhub.repositories.a2a_invocation_repo import A2AInvocationRepo
    from krewhub.workers.agent_hand import AgentHand

    db = await get_db()
    hand = AgentHand(db, poll_interval_s=0.05)
    tape = _CapturingTape()
    cancel = _StubCancel()

    # Snoop the row before it's completed.
    rows: list[dict] = []

    async def _snoop_then_complete():
        await asyncio.sleep(0.05)
        repo = A2AInvocationRepo(db)
        # repo signature: list_pending(owner, agent_name)
        pending = await repo.list_pending("krew", "claude")
        if pending:
            rows.append(pending[-1])
            await repo.update_status(pending[-1]["invocation_id"], "completed", result="ok")

    asyncio.create_task(_snoop_then_complete())

    await hand.execute(
        target_id="claude@krew",
        input={"message": "hi", "tone": "casual"},
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )
    assert rows
    assert rows[0]["params"]["input"]["message"] == "hi"
    assert rows[0]["params"]["input"]["tone"] == "casual"


# ---------------------------------------------------------------------------
# Service integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_dispatches_to_agent_hand():
    """End-to-end: POST /invocations target=agent:foo@bar → A2A row →
    background completion → ResultEnvelope on the invocation row."""
    from krewhub.db.connection import get_db
    from krewhub.models.invocation import InvocationRequest
    from krewhub.workers.agent_hand import AgentHand

    db = await get_db()
    hand = AgentHand(db, poll_interval_s=0.05)
    svc = await _make_service({"agent": hand})

    asyncio.create_task(_complete_a2a_after(0.15, "claude", "krew", result="42"))

    inv = await svc.create(
        InvocationRequest(target="agent:claude@krew", input="what?"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=3.0)
    refreshed = await svc.get(inv.id)
    assert refreshed.status == "completed"
    assert refreshed.result.action == "accept"
    assert refreshed.result.content == "42"
