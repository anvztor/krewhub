"""Slice 2 — SandboxHand unit tests (Invocation Contract §10.1).

Pins down the SandboxHand `execute()` behavior against a fake e2b client:
- streams output chunks as `output` events
- returns accept envelope with exit_code, stdout_tail, stderr_tail
- non-zero exit code is still `accept` (program ran, captured result)
- infra failure (sandbox crash) → `error` envelope
- cancel → `cancel` envelope
- 16KB cap on stdout_tail / stderr_tail

These tests use a FakeE2bClient (test double) — no httpx, no e2b. The
e2b_client.exec_command() HTTP contract is validated separately.

Status: RED. SandboxHand and its package don't exist yet.
"""
from __future__ import annotations

import asyncio
from typing import AsyncIterator

import pytest

from krewhub.models.invocation import Event, EventKind, ResultEnvelope


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class FakeE2bClient:
    """Test double for E2bClient. Returns scripted exec_command chunks.

    Each chunk is one of:
      - {"stream": "stdout", "data": "..."}
      - {"stream": "stderr", "data": "..."}
      - {"exit_code": int}
      - {"error": "..."}
    """

    def __init__(self, scripts: dict[str, list[dict]] | None = None) -> None:
        self.scripts = scripts or {}
        self.calls: list[dict] = []
        self.kill_calls: list[str] = []

    async def exec_command(
        self,
        sandbox_id: str,
        command: str | dict,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float = 300.0,
    ) -> AsyncIterator[dict]:
        self.calls.append({
            "sandbox_id": sandbox_id,
            "command": command,
            "cwd": cwd,
            "env": env,
            "timeout": timeout,
        })
        chunks = self.scripts.get(sandbox_id, [{"exit_code": 0}])

        async def _gen():
            for chunk in chunks:
                if chunk.get("__delay") is not None:
                    await asyncio.sleep(chunk["__delay"])
                    continue
                if chunk.get("__raise") is not None:
                    raise chunk["__raise"]
                yield chunk
        return _gen()

    async def kill_process(self, sandbox_id: str, *, process_id: str | None = None) -> None:
        self.kill_calls.append(sandbox_id)


class _CapturingTape:
    """Minimal TapeWriter test double — records appends, doesn't persist."""

    def __init__(self, tape_id: str = "tape_test") -> None:
        self.tape_id = tape_id
        self.events: list[dict] = []
        self._next_id = 0

    async def append(
        self,
        kind: EventKind,
        *,
        body: str = "",
        payload: dict | None = None,
        actor_type: str = "system",
        actor_id: str = "",
        parent_id: int | None = None,
    ) -> Event:
        from datetime import datetime, timezone
        ev = Event(
            tape_id=self.tape_id,
            id=self._next_id,
            parent_id=parent_id,
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

    async def wait(self) -> None:
        await self._event.wait()

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise asyncio.CancelledError(self.reason)

    def fire(self, reason: str) -> None:
        self.cancelled = True
        self.reason = reason
        self._event.set()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_executes_command_returns_accept_with_exit_zero():
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient(scripts={"sbx_1": [
        {"stream": "stdout", "data": "hello\n"},
        {"exit_code": 0},
    ]})
    hand = SandboxHand(e2b)
    tape = _CapturingTape()
    cancel = _StubCancel()

    result = await hand.execute(
        target_id="sbx_1", input="echo hello",
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )

    assert isinstance(result, ResultEnvelope)
    assert result.action == "accept"
    assert isinstance(result.content, dict)
    assert result.content["exit_code"] == 0
    assert "hello" in result.content["stdout_tail"]


@pytest.mark.asyncio
async def test_non_zero_exit_is_still_accept():
    """Per contract §7: a process that ran but returned non-zero is
    still `accept`. The exit_code lives in content; `error` is reserved
    for infrastructure failures."""
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient(scripts={"sbx_1": [
        {"stream": "stderr", "data": "thing failed\n"},
        {"exit_code": 1},
    ]})
    hand = SandboxHand(e2b)
    tape = _CapturingTape()
    cancel = _StubCancel()

    result = await hand.execute(
        target_id="sbx_1", input="false",
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )
    assert result.action == "accept"
    assert result.content["exit_code"] == 1
    assert "thing failed" in result.content["stderr_tail"]


@pytest.mark.asyncio
async def test_emits_output_events_with_stream_type():
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient(scripts={"sbx_1": [
        {"stream": "stdout", "data": "first\n"},
        {"stream": "stderr", "data": "warning\n"},
        {"stream": "stdout", "data": "second\n"},
        {"exit_code": 0},
    ]})
    hand = SandboxHand(e2b)
    tape = _CapturingTape()
    cancel = _StubCancel()

    await hand.execute(
        target_id="sbx_1", input="cmd",
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )

    output_events = [e for e in tape.events if e["kind"] == "output"]
    assert len(output_events) == 3
    assert output_events[0]["payload"] == {"stream": "stdout", "chunk": "first\n"}
    assert output_events[1]["payload"] == {"stream": "stderr", "chunk": "warning\n"}
    assert output_events[2]["payload"] == {"stream": "stdout", "chunk": "second\n"}
    # actor_type for sandbox stdout is "sandbox"
    assert all(e["actor_type"] == "sandbox" for e in output_events)


@pytest.mark.asyncio
async def test_does_not_emit_started_or_done_events():
    """The Hand never writes started/done — those belong to the service."""
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient(scripts={"sbx_1": [{"exit_code": 0}]})
    hand = SandboxHand(e2b)
    tape = _CapturingTape()
    cancel = _StubCancel()

    await hand.execute(
        target_id="sbx_1", input="cmd",
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )

    kinds = [e["kind"] for e in tape.events]
    assert "started" not in kinds
    assert "done" not in kinds


@pytest.mark.asyncio
async def test_e2b_exception_returns_error_envelope():
    """Infrastructure failure → error envelope (not raise)."""
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient(scripts={"sbx_1": [
        {"__raise": ConnectionError("e2b unreachable")},
    ]})
    hand = SandboxHand(e2b)
    tape = _CapturingTape()
    cancel = _StubCancel()

    result = await hand.execute(
        target_id="sbx_1", input="cmd",
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )
    assert result.action == "error"
    assert "e2b unreachable" in (result.reason or "")


@pytest.mark.asyncio
async def test_no_target_id_returns_error():
    """SandboxHand requires a target_id (the sandbox to attach to)."""
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient()
    hand = SandboxHand(e2b)
    tape = _CapturingTape()
    cancel = _StubCancel()

    result = await hand.execute(
        target_id=None, input="cmd",
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )
    assert result.action == "error"
    assert "target_id" in (result.reason or "") or "sandbox_id" in (result.reason or "")


@pytest.mark.asyncio
async def test_dict_input_carries_command_field():
    """Per contract §10.1, input may be a dict with a 'command' key."""
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient(scripts={"sbx_1": [{"exit_code": 0}]})
    hand = SandboxHand(e2b)
    tape = _CapturingTape()
    cancel = _StubCancel()

    await hand.execute(
        target_id="sbx_1",
        input={"command": "ls -la", "cwd": "/tmp"},
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )
    assert e2b.calls[0]["command"] == "ls -la"
    assert e2b.calls[0]["cwd"] == "/tmp"


@pytest.mark.asyncio
async def test_cancel_returns_cancel_envelope():
    """When CancelToken fires, execute() returns a cancel envelope and
    asks e2b to kill the process."""
    from krewhub.workers.sandbox_hand import SandboxHand

    # Long-running script: fires events forever via __delay
    e2b = FakeE2bClient(scripts={"sbx_1": [
        {"stream": "stdout", "data": "starting...\n"},
        {"__delay": 5.0},
        {"exit_code": 0},
    ]})
    hand = SandboxHand(e2b)
    tape = _CapturingTape()
    cancel = _StubCancel()

    async def _cancel_after(t: float):
        await asyncio.sleep(t)
        cancel.fire("operator_cancelled")

    asyncio.create_task(_cancel_after(0.05))
    result = await hand.execute(
        target_id="sbx_1", input="long",
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )

    assert result.action == "cancel"
    assert result.reason == "operator_cancelled"
    assert e2b.kill_calls == ["sbx_1"]


@pytest.mark.asyncio
async def test_stdout_tail_capped_at_16kb():
    """contract §10.1: stdout_tail / stderr_tail capped at 16KB."""
    from krewhub.workers.sandbox_hand import SandboxHand

    big = "A" * (32 * 1024)  # 32KB > cap
    e2b = FakeE2bClient(scripts={"sbx_1": [
        {"stream": "stdout", "data": big},
        {"exit_code": 0},
    ]})
    hand = SandboxHand(e2b)
    tape = _CapturingTape()
    cancel = _StubCancel()

    result = await hand.execute(
        target_id="sbx_1", input="cmd",
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )
    assert result.action == "accept"
    tail = result.content["stdout_tail"]
    assert len(tail.encode()) <= 16 * 1024, f"stdout_tail too big: {len(tail)} chars"
    # The TAIL of the output (last bytes) is preserved
    assert tail.endswith("A")


@pytest.mark.asyncio
async def test_target_type_attribute():
    """SandboxHand declares target_type='sandbox' for the registry."""
    from krewhub.workers.sandbox_hand import SandboxHand

    hand = SandboxHand(FakeE2bClient())
    assert hand.target_type == "sandbox"
