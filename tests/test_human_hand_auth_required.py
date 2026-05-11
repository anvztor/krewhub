"""HumanHand structured `op: "auth_required"` payload.

When the brain delegates {to:"human", input:{op:"auth_required",host,reason,...}}
HumanHand must emit an `elicit` tape event whose payload exposes those
fields so cookrew-web can render the AuthRequiredCard (rather than the
generic schema form).
"""
from __future__ import annotations

import asyncio

import pytest

from krewhub.workers.human_hand import HumanHand


class _RecordingTape:
    def __init__(self) -> None:
        self.events: list[dict] = []

    async def append(self, kind: str, *, body: str, payload: dict,
                     actor_type: str, actor_id: str) -> None:
        self.events.append({
            "kind": kind, "body": body, "payload": payload,
            "actor_type": actor_type, "actor_id": actor_id,
        })


class _CancelImmediately:
    def __init__(self) -> None:
        self.cancelled = True
        self.reason = "test_cancel"
        self._event = asyncio.Event()
        self._event.set()

    async def wait(self):
        await self._event.wait()


@pytest.mark.asyncio
async def test_auth_required_emits_structured_payload():
    hand = HumanHand()
    tape = _RecordingTape()
    res = await hand.execute(
        target_id=None,
        input={
            "op": "auth_required",
            "host": "api.github.com",
            "env_var_name": "GITHUB_TOKEN",
            "reason": "git push needs auth",
        },
        schema=None,
        deadline_s=60,
        tape=tape,
        cancel=_CancelImmediately(),
    )
    assert res.action == "cancel"  # cancel was fired in the fixture
    assert len(tape.events) == 1
    ev = tape.events[0]
    assert ev["kind"] == "elicit"
    payload = ev["payload"]
    assert payload["op"] == "auth_required"
    assert payload["host"] == "api.github.com"
    assert payload["env_var_name"] == "GITHUB_TOKEN"
    assert payload["reason"] == "git push needs auth"
    # A human-readable message is synthesized so the tape body is sensible
    assert "api.github.com" in payload["message"]


@pytest.mark.asyncio
async def test_free_form_message_still_works():
    """Backwards compat: brain may still delegate a plain string."""
    hand = HumanHand()
    tape = _RecordingTape()
    await hand.execute(
        target_id=None,
        input="Should we deploy now?",
        schema=None,
        deadline_s=60,
        tape=tape,
        cancel=_CancelImmediately(),
    )
    assert len(tape.events) == 1
    payload = tape.events[0]["payload"]
    assert payload["message"] == "Should we deploy now?"
    assert "op" not in payload, "free-form input should not synthesize an op"


@pytest.mark.asyncio
async def test_unknown_op_falls_back_to_free_form():
    """Unrecognized op shape is treated as free-form, requires a message."""
    hand = HumanHand()
    tape = _RecordingTape()
    res = await hand.execute(
        target_id=None,
        input={"op": "fly_to_mars", "destination": "olympus_mons"},
        schema=None,
        deadline_s=60,
        tape=tape,
        cancel=_CancelImmediately(),
    )
    # No message + no known op → error envelope
    assert res.action == "error"
    assert "empty_message" in (res.reason or "")
    assert len(tape.events) == 0
