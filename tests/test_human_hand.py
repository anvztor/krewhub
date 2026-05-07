"""Slice 5 — HumanHand unit tests (Invocation Contract §10.2).

The HumanHand bridges the agent → operator gap: it emits an `elicit`
event (carrying message + optional schema), then awaits the operator's
submission via `POST /invocations/:id/result`. The InvocationService
translates the operator's `ResultEnvelope` into a `decision` event on
the tape and signals the blocked Hand to return.

Two layers:
1. Unit tests: HumanHand.execute() against a stub TapeWriter + CancelToken.
2. Service integration: a real InvocationService run with HumanHand
   registered, exercising submit_result() and verifying the `decision`
   event lands before `done`.

Status: RED.
"""
from __future__ import annotations

import asyncio
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
    """Plain CancelToken — fires only on .fire()."""

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


class _ExternalAwareCancel:
    """Mimics the service-side _ExternalAwareCancel: wait() resolves
    when EITHER the inner cancel fires OR external_result is set.
    Kept here so tests don't depend on service internals."""

    def __init__(self, inner: _StubCancel, external):
        self._inner = inner
        self._external = external

    @property
    def cancelled(self):
        return self._inner.cancelled

    @property
    def reason(self):
        return self._inner.reason

    async def wait(self):
        async def _ext():
            try:
                await asyncio.shield(self._external)
            except asyncio.CancelledError:
                pass

        cancel_task = asyncio.create_task(self._inner.wait())
        external_task = asyncio.create_task(_ext())
        try:
            await asyncio.wait(
                {cancel_task, external_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for t in (cancel_task, external_task):
                if not t.done():
                    t.cancel()

    def raise_if_cancelled(self):
        if self._inner.cancelled:
            raise asyncio.CancelledError(self._inner.reason)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_target_type_attribute():
    from krewhub.workers.human_hand import HumanHand
    assert HumanHand().target_type == "human"


@pytest.mark.asyncio
async def test_emits_elicit_event_with_message_and_schema():
    from krewhub.workers.human_hand import HumanHand

    hand = HumanHand()
    tape = _CapturingTape()
    cancel = _StubCancel()

    # Fire cancel right after starting so execute() returns
    async def _cancel_soon():
        await asyncio.sleep(0.02)
        cancel.fire("test_done")

    asyncio.create_task(_cancel_soon())

    schema = {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
    }
    await hand.execute(
        target_id=None,
        input="What's your name?",
        schema=schema,
        deadline_s=10,
        tape=tape,  # type: ignore[arg-type]
        cancel=cancel,  # type: ignore[arg-type]
    )
    elicit_events = [e for e in tape.events if e["kind"] == "elicit"]
    assert len(elicit_events) == 1
    p = elicit_events[0]["payload"]
    assert p["message"] == "What's your name?"
    assert p["schema"] == schema
    assert "deadline_ts" in p
    assert elicit_events[0]["actor_type"] == "human"


@pytest.mark.asyncio
async def test_returns_cancel_envelope_when_cancel_fired():
    from krewhub.workers.human_hand import HumanHand
    from krewhub.models.invocation import ResultEnvelope

    hand = HumanHand()
    tape = _CapturingTape()
    cancel = _StubCancel()

    async def _cancel_soon():
        await asyncio.sleep(0.02)
        cancel.fire("operator_cancelled")

    asyncio.create_task(_cancel_soon())

    result = await hand.execute(
        target_id=None,
        input="Q?",
        schema=None,
        deadline_s=10,
        tape=tape,  # type: ignore[arg-type]
        cancel=cancel,  # type: ignore[arg-type]
    )
    assert isinstance(result, ResultEnvelope)
    assert result.action == "cancel"
    assert result.reason == "operator_cancelled"


@pytest.mark.asyncio
async def test_returns_error_envelope_when_target_id_provided():
    """human target accepts NO id (contract §8)."""
    from krewhub.workers.human_hand import HumanHand

    hand = HumanHand()
    tape = _CapturingTape()
    cancel = _StubCancel()

    result = await hand.execute(
        target_id="should_not_have_id",
        input="Q?",
        schema=None,
        deadline_s=10,
        tape=tape,  # type: ignore[arg-type]
        cancel=cancel,  # type: ignore[arg-type]
    )
    assert result.action == "error"
    assert "human" in (result.reason or "").lower()


@pytest.mark.asyncio
async def test_returns_error_envelope_when_input_empty():
    from krewhub.workers.human_hand import HumanHand

    hand = HumanHand()
    tape = _CapturingTape()
    cancel = _StubCancel()

    result = await hand.execute(
        target_id=None,
        input="",
        schema=None,
        deadline_s=10,
        tape=tape,  # type: ignore[arg-type]
        cancel=cancel,  # type: ignore[arg-type]
    )
    assert result.action == "error"
    assert "empty" in (result.reason or "").lower() or "message" in (result.reason or "").lower()


@pytest.mark.asyncio
async def test_dict_input_with_message_field():
    """Input may be a dict with a 'message' key (matches HITL popout shape)."""
    from krewhub.workers.human_hand import HumanHand

    hand = HumanHand()
    tape = _CapturingTape()
    cancel = _StubCancel()

    async def _cancel_soon():
        await asyncio.sleep(0.02)
        cancel.fire("done")

    asyncio.create_task(_cancel_soon())

    await hand.execute(
        target_id=None,
        input={"message": "Pick a color", "label": "color picker"},
        schema={
            "type": "object",
            "properties": {
                "color": {"type": "string", "enum": ["red", "green"]},
            },
        },
        deadline_s=10,
        tape=tape,  # type: ignore[arg-type]
        cancel=cancel,  # type: ignore[arg-type]
    )
    elicit_events = [e for e in tape.events if e["kind"] == "elicit"]
    assert elicit_events[0]["payload"]["message"] == "Pick a color"


# ---------------------------------------------------------------------------
# Service integration: submit_result writes `decision` and signals Hand
# ---------------------------------------------------------------------------


async def _make_service(hands: dict | None = None):
    from krewhub.db.connection import get_db
    from krewhub.services.invocation_service import InvocationService
    from krewhub.watch.globals import get_watch_service

    db = await get_db()
    return InvocationService(db, hands=hands or {}, watch=get_watch_service())


@pytest.mark.asyncio
async def test_submit_result_writes_decision_event_then_done():
    """End-to-end: post invocation, submit_result, verify event order:
    started → elicit → decision → done."""
    from krewhub.workers.human_hand import HumanHand
    from krewhub.models.invocation import InvocationRequest, ResultEnvelope

    svc = await _make_service({"human": HumanHand()})

    inv = await svc.create(
        InvocationRequest(target="human", input="Should I retry?"),
        caller_account_id="acc_test",
    )

    # Give the Hand a moment to emit `elicit`.
    await asyncio.sleep(0.05)

    answer = ResultEnvelope(action="accept", content="yes — retry once more")
    await svc.submit_result(inv.id, answer)

    events = await svc.list_events(inv.tape_id)
    kinds = [e.kind for e in events]
    assert kinds[0] == "started"
    assert "elicit" in kinds
    assert "decision" in kinds
    assert kinds[-1] == "done"
    assert kinds.index("decision") < kinds.index("done")

    # The decision event payload IS the operator's envelope
    decision = next(e for e in events if e.kind == "decision")
    assert decision.payload["action"] == "accept"
    assert decision.payload["content"] == "yes — retry once more"
    assert decision.actor_type == "human"


@pytest.mark.asyncio
async def test_submit_result_propagates_to_invocation_status():
    """Operator answer flips invocation row to status='completed' with
    the submitted envelope as result."""
    from krewhub.workers.human_hand import HumanHand
    from krewhub.models.invocation import InvocationRequest, ResultEnvelope

    svc = await _make_service({"human": HumanHand()})

    inv = await svc.create(
        InvocationRequest(target="human", input="Q?"),
        caller_account_id="acc_test",
    )
    await asyncio.sleep(0.02)
    await svc.submit_result(
        inv.id,
        ResultEnvelope(action="decline", reason="not_now"),
    )
    refreshed = await svc.get(inv.id)
    assert refreshed.status == "completed"
    assert refreshed.result.action == "decline"
    assert refreshed.result.reason == "not_now"


@pytest.mark.asyncio
async def test_human_invocation_deadline_returns_cancel():
    """If no operator answer arrives before deadline_s, the service
    times out and writes done(action=cancel, reason=deadline_exceeded)."""
    from krewhub.workers.human_hand import HumanHand
    from krewhub.models.invocation import InvocationRequest

    svc = await _make_service({"human": HumanHand()})

    inv = await svc.create(
        InvocationRequest(
            target="human",
            input="Quick question",
            deadline_s=1,
        ),
        caller_account_id="acc_test",
    )
    # Wait past the deadline
    await svc.wait_for_terminal(inv.id, timeout=3.0)
    refreshed = await svc.get(inv.id)
    assert refreshed.status == "cancelled"
    assert refreshed.result.action == "cancel"
    assert "deadline" in (refreshed.result.reason or "")


@pytest.mark.asyncio
async def test_operator_cancel_returns_cancel_envelope():
    """POST /invocations/:id/cancel mid-elicit terminates as cancel."""
    from krewhub.workers.human_hand import HumanHand
    from krewhub.models.invocation import InvocationRequest

    svc = await _make_service({"human": HumanHand()})

    inv = await svc.create(
        InvocationRequest(target="human", input="Q?"),
        caller_account_id="acc_test",
    )
    await asyncio.sleep(0.02)  # let Hand emit elicit
    await svc.cancel(inv.id, reason="operator_dismissed")
    await svc.wait_for_terminal(inv.id, timeout=2.0)

    refreshed = await svc.get(inv.id)
    assert refreshed.status == "cancelled"
    assert refreshed.result.action == "cancel"
    assert refreshed.result.reason == "operator_dismissed"


@pytest.mark.asyncio
async def test_submit_result_after_terminal_raises_conflict():
    """Once the invocation is terminal, submit_result is rejected."""
    from krewhub.workers.human_hand import HumanHand
    from krewhub.models.invocation import InvocationRequest, ResultEnvelope
    from krewhub.services.invocation_service import _ConflictError

    svc = await _make_service({"human": HumanHand()})
    inv = await svc.create(
        InvocationRequest(target="human", input="Q?"),
        caller_account_id="acc_test",
    )
    await asyncio.sleep(0.02)
    await svc.submit_result(inv.id, ResultEnvelope(action="accept", content="x"))

    # Second submission should be rejected
    with pytest.raises(_ConflictError):
        await svc.submit_result(
            inv.id, ResultEnvelope(action="accept", content="late"),
        )


@pytest.mark.asyncio
async def test_schema_validation_on_submitted_envelope():
    """If the request supplied a schema and the operator submits content
    that doesn't match, the service rewrites accept→error."""
    from krewhub.workers.human_hand import HumanHand
    from krewhub.models.invocation import InvocationRequest, ResultEnvelope

    svc = await _make_service({"human": HumanHand()})
    inv = await svc.create(
        InvocationRequest(
            target="human",
            input="Pick a color",
            schema={
                "type": "object",
                "properties": {"color": {"type": "string", "enum": ["red", "green"]}},
                "required": ["color"],
            },
        ),
        caller_account_id="acc_test",
    )
    await asyncio.sleep(0.02)
    # Operator submits something not matching schema (color="purple")
    await svc.submit_result(
        inv.id,
        ResultEnvelope(action="accept", content={"color": "purple"}),
    )
    refreshed = await svc.get(inv.id)
    assert refreshed.status == "errored"
    assert refreshed.result.action == "error"
    assert "schema_mismatch" in (refreshed.result.reason or "")
