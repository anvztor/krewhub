"""Slice 1 — InvocationService + repos (contract §4, §5).

These tests pin down the orchestration shell that wraps Hand.execute():
- create() writes a `started` event at id=0
- create() dispatches to the Hand registry by target_type
- Hand.execute()'s returned ResultEnvelope becomes the terminal `done` event
- TapeWriter ids are monotonic per tape
- CancelToken plumbing → cancel envelope from Hand
- Hand-raised exceptions become error envelopes (failures are values)
- Idempotency: duplicate (parent_tape_id, idempotency_key) returns same row
- Schema validation: accept content matching schema → OK; mismatch → error
- Parent tape handoff event written when invocation has parent_tape_id

No HTTP layer here. Pure service-on-db tests.

Status: RED. The service does not exist yet.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest


# ---------------------------------------------------------------------------
# FakeHand — test double for the Hand Protocol
# ---------------------------------------------------------------------------


class FakeHand:
    """Configurable test Hand. Writes scripted events, returns a configured
    envelope, optionally raises, optionally honors cancel."""

    def __init__(
        self,
        target_type: str = "fake",
        result=None,
        events=None,             # list[(kind, payload)]
        delay_s: float = 0.0,
        raise_exc: Exception | None = None,
        block_until_event: asyncio.Event | None = None,
    ):
        from krewhub.models.invocation import ResultEnvelope

        self.target_type = target_type
        self._result = result or ResultEnvelope(action="accept", content="ok")
        self._events = events or []
        self._delay_s = delay_s
        self._raise_exc = raise_exc
        self._block = block_until_event
        self.calls: list[dict] = []

    async def execute(
        self,
        *,
        target_id,
        input,
        schema,
        deadline_s,
        tape,
        cancel,
    ):
        from krewhub.models.invocation import ResultEnvelope

        self.calls.append({
            "target_id": target_id,
            "input": input,
            "schema": schema,
            "deadline_s": deadline_s,
        })

        for kind, payload in self._events:
            await tape.append(
                kind,
                payload=payload,
                actor_type="sandbox" if self.target_type == "sandbox" else "system",
                actor_id=target_id or self.target_type,
            )

        if self._block is not None:
            # Honor cancel by awaiting both the block event and cancel.wait()
            done = asyncio.create_task(self._block.wait())
            cancelled = asyncio.create_task(cancel.wait())
            try:
                _, pending = await asyncio.wait(
                    {done, cancelled}, return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()
            except asyncio.CancelledError:
                pass
            if cancel.cancelled:
                return ResultEnvelope(
                    action="cancel", reason="operator_cancelled",
                )

        if self._delay_s:
            await asyncio.sleep(self._delay_s)

        if self._raise_exc:
            raise self._raise_exc

        return self._result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _make_service(hands: dict | None = None):
    """Build an InvocationService against the in-memory db with given hands."""
    from krewhub.db.connection import get_db
    from krewhub.services.invocation_service import InvocationService
    from krewhub.watch.globals import get_watch_service

    db = await get_db()
    return InvocationService(db, hands=hands or {}, watch=get_watch_service())


async def _list_events(svc, tape_id: str) -> list:
    """Read all events from a tape in id order."""
    return await svc.list_events(tape_id)


# ---------------------------------------------------------------------------
# create() and started event
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_writes_started_event_at_id_zero():
    from krewhub.models.invocation import InvocationRequest, ResultEnvelope

    hand = FakeHand(result=ResultEnvelope(action="accept", content="hi"))
    svc = await _make_service({"fake": hand})

    inv = await svc.create(
        InvocationRequest(target="fake:abc", input="hello"),
        caller_account_id="acc_test",
    )
    assert inv.id.startswith("inv_")
    assert inv.tape_id.startswith("tape_")
    assert inv.status in ("pending", "running")  # may already be running

    # Wait for the Hand to finish
    await svc.wait_for_terminal(inv.id, timeout=2.0)

    events = await _list_events(svc, inv.tape_id)
    assert len(events) >= 2
    assert events[0].id == 0
    assert events[0].kind == "started"
    assert events[-1].kind == "done"


@pytest.mark.asyncio
async def test_create_dispatches_to_hand_by_target_type():
    from krewhub.models.invocation import InvocationRequest

    sandbox_hand = FakeHand(target_type="sandbox")
    human_hand = FakeHand(target_type="human")
    svc = await _make_service({"sandbox": sandbox_hand, "human": human_hand})

    inv = await svc.create(
        InvocationRequest(target="sandbox:sbx_1", input="ls"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=2.0)
    assert len(sandbox_hand.calls) == 1
    assert sandbox_hand.calls[0]["target_id"] == "sbx_1"
    assert sandbox_hand.calls[0]["input"] == "ls"
    assert len(human_hand.calls) == 0


@pytest.mark.asyncio
async def test_create_unknown_target_returns_error_envelope():
    from krewhub.models.invocation import InvocationRequest

    svc = await _make_service({})  # no Hands registered

    inv = await svc.create(
        InvocationRequest(target="sandbox:sbx_1", input="ls"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=2.0)

    refreshed = await svc.get(inv.id)
    assert refreshed.status == "errored"
    assert refreshed.result is not None
    assert refreshed.result.action == "error"
    assert "no_hand_registered" in (refreshed.result.reason or "")


# ---------------------------------------------------------------------------
# Hand return values → terminal state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hand_returns_accept_status_completed():
    from krewhub.models.invocation import InvocationRequest, ResultEnvelope

    hand = FakeHand(result=ResultEnvelope(action="accept", content={"exit": 0}))
    svc = await _make_service({"fake": hand})

    inv = await svc.create(
        InvocationRequest(target="fake:abc", input="x"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=2.0)
    refreshed = await svc.get(inv.id)
    assert refreshed.status == "completed"
    assert refreshed.result.action == "accept"
    assert refreshed.result.content == {"exit": 0}


@pytest.mark.asyncio
async def test_hand_returns_decline_status_completed():
    from krewhub.models.invocation import InvocationRequest, ResultEnvelope

    hand = FakeHand(result=ResultEnvelope(action="decline", reason="user_said_no"))
    svc = await _make_service({"fake": hand})

    inv = await svc.create(
        InvocationRequest(target="fake:abc", input="x"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=2.0)
    refreshed = await svc.get(inv.id)
    assert refreshed.status == "completed"
    assert refreshed.result.action == "decline"


@pytest.mark.asyncio
async def test_hand_returns_error_status_errored():
    from krewhub.models.invocation import InvocationRequest, ResultEnvelope

    hand = FakeHand(result=ResultEnvelope(action="error", reason="sandbox_dead"))
    svc = await _make_service({"fake": hand})

    inv = await svc.create(
        InvocationRequest(target="fake:abc", input="x"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=2.0)
    refreshed = await svc.get(inv.id)
    assert refreshed.status == "errored"


@pytest.mark.asyncio
async def test_hand_raises_exception_becomes_error_envelope():
    """Failures are values, not exceptions. The service catches Hand
    exceptions and writes an `error` envelope."""
    from krewhub.models.invocation import InvocationRequest

    hand = FakeHand(raise_exc=RuntimeError("e2b connection refused"))
    svc = await _make_service({"fake": hand})

    inv = await svc.create(
        InvocationRequest(target="fake:abc", input="x"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=2.0)
    refreshed = await svc.get(inv.id)
    assert refreshed.status == "errored"
    assert refreshed.result.action == "error"
    assert "e2b connection refused" in (refreshed.result.reason or "")


# ---------------------------------------------------------------------------
# TapeWriter monotonic ids
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tape_writer_monotonic_ids():
    from krewhub.models.invocation import InvocationRequest

    hand = FakeHand(events=[
        ("output", {"stream": "stdout", "chunk": "hello"}),
        ("output", {"stream": "stdout", "chunk": " world"}),
        ("milestone", {"label": "mid", "detail": ""}),
    ])
    svc = await _make_service({"fake": hand})

    inv = await svc.create(
        InvocationRequest(target="fake:abc", input="x"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=2.0)
    events = await _list_events(svc, inv.tape_id)
    ids = [e.id for e in events]
    assert ids == list(range(len(ids)))     # 0, 1, 2, ...
    assert events[0].kind == "started"
    assert events[-1].kind == "done"


@pytest.mark.asyncio
async def test_two_tapes_have_independent_id_sequences():
    from krewhub.models.invocation import InvocationRequest

    hand = FakeHand()
    svc = await _make_service({"fake": hand})

    inv1 = await svc.create(
        InvocationRequest(target="fake:a", input="x"),
        caller_account_id="acc_test",
    )
    inv2 = await svc.create(
        InvocationRequest(target="fake:b", input="x"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv1.id, timeout=2.0)
    await svc.wait_for_terminal(inv2.id, timeout=2.0)

    e1 = await _list_events(svc, inv1.tape_id)
    e2 = await _list_events(svc, inv2.tape_id)
    assert e1[0].id == 0
    assert e2[0].id == 0     # each tape restarts at 0


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_fires_token_and_hand_returns_cancel_envelope():
    from krewhub.models.invocation import InvocationRequest

    block = asyncio.Event()  # never set; Hand only exits via cancel
    hand = FakeHand(block_until_event=block)
    svc = await _make_service({"fake": hand})

    inv = await svc.create(
        InvocationRequest(target="fake:abc", input="x"),
        caller_account_id="acc_test",
    )
    # Yield once so the Hand has started
    await asyncio.sleep(0.01)

    await svc.cancel(inv.id, reason="operator_cancelled")
    await svc.wait_for_terminal(inv.id, timeout=2.0)

    refreshed = await svc.get(inv.id)
    assert refreshed.status == "cancelled"
    assert refreshed.result.action == "cancel"


@pytest.mark.asyncio
async def test_cancel_after_terminal_is_noop():
    from krewhub.models.invocation import InvocationRequest

    hand = FakeHand()
    svc = await _make_service({"fake": hand})

    inv = await svc.create(
        InvocationRequest(target="fake:abc", input="x"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=2.0)

    # Should not raise, should not change state
    await svc.cancel(inv.id, reason="too_late")
    refreshed = await svc.get(inv.id)
    assert refreshed.status == "completed"


# ---------------------------------------------------------------------------
# Idempotency (contract §13.3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_idempotency_key_returns_existing_invocation():
    from krewhub.models.invocation import InvocationRequest

    hand = FakeHand()
    svc = await _make_service({"fake": hand})

    req = InvocationRequest(
        target="fake:abc",
        input="x",
        parent_tape_id="tape_parent",
        idempotency_key="key_xyz",
    )
    inv1 = await svc.create(req, caller_account_id="acc_test")
    inv2 = await svc.create(req, caller_account_id="acc_test")

    assert inv1.id == inv2.id          # same row
    assert inv1.tape_id == inv2.tape_id


@pytest.mark.asyncio
async def test_idempotency_only_within_same_parent_tape():
    """Same key under a different parent_tape_id is a different invocation."""
    from krewhub.models.invocation import InvocationRequest

    hand = FakeHand()
    svc = await _make_service({"fake": hand})

    inv1 = await svc.create(
        InvocationRequest(
            target="fake:abc", input="x",
            parent_tape_id="tape_a", idempotency_key="key_xyz",
        ),
        caller_account_id="acc_test",
    )
    inv2 = await svc.create(
        InvocationRequest(
            target="fake:abc", input="x",
            parent_tape_id="tape_b", idempotency_key="key_xyz",
        ),
        caller_account_id="acc_test",
    )
    assert inv1.id != inv2.id


# ---------------------------------------------------------------------------
# Schema validation against returned content (contract §7)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schema_match_passes_through():
    from krewhub.models.invocation import InvocationRequest, ResultEnvelope

    hand = FakeHand(result=ResultEnvelope(action="accept", content={"name": "alice"}))
    svc = await _make_service({"fake": hand})

    inv = await svc.create(
        InvocationRequest(
            target="fake:abc", input="x",
            schema={
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
            },
        ),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=2.0)
    refreshed = await svc.get(inv.id)
    assert refreshed.status == "completed"
    assert refreshed.result.action == "accept"


@pytest.mark.asyncio
async def test_schema_mismatch_becomes_error_envelope():
    from krewhub.models.invocation import InvocationRequest, ResultEnvelope

    # Hand "accepts" but content doesn't satisfy schema → service rewrites to error.
    hand = FakeHand(result=ResultEnvelope(action="accept", content={"name": 42}))
    svc = await _make_service({"fake": hand})

    inv = await svc.create(
        InvocationRequest(
            target="fake:abc", input="x",
            schema={
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
            },
        ),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=2.0)
    refreshed = await svc.get(inv.id)
    assert refreshed.status == "errored"
    assert refreshed.result.action == "error"
    assert (refreshed.result.reason or "").startswith("schema_mismatch")


# ---------------------------------------------------------------------------
# Parent tape handoff (contract §3, §6)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handoff_event_written_when_parent_tape_exists():
    from krewhub.models.invocation import InvocationRequest, ResultEnvelope

    hand = FakeHand(result=ResultEnvelope(action="accept", content="done"))
    svc = await _make_service({"fake": hand})

    # Allocate a parent tape by creating a parent invocation first.
    parent = await svc.create(
        InvocationRequest(target="fake:p", input="parent"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(parent.id, timeout=2.0)

    # Child invocation referencing the parent tape.
    child = await svc.create(
        InvocationRequest(
            target="fake:c", input="child",
            parent_tape_id=parent.tape_id,
            parent_fork_point=0,
        ),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(child.id, timeout=2.0)

    parent_events = await _list_events(svc, parent.tape_id)
    handoffs = [e for e in parent_events if e.kind == "handoff"]
    assert len(handoffs) == 1
    assert handoffs[0].payload.get("from_fork") == child.tape_id
    # The handoff payload carries the child's ResultEnvelope
    result_payload = handoffs[0].payload.get("result")
    assert result_payload is not None
    assert result_payload["action"] == "accept"


# ---------------------------------------------------------------------------
# Constraint: exactly one done per tape
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tape_has_exactly_one_done_event():
    from krewhub.models.invocation import InvocationRequest

    hand = FakeHand()
    svc = await _make_service({"fake": hand})

    inv = await svc.create(
        InvocationRequest(target="fake:abc", input="x"),
        caller_account_id="acc_test",
    )
    await svc.wait_for_terminal(inv.id, timeout=2.0)
    events = await _list_events(svc, inv.tape_id)
    done = [e for e in events if e.kind == "done"]
    assert len(done) == 1
