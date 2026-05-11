"""Exec-path dead-sandbox recovery (Slice [A] of vault MVP follow-up).

Parity with the existing file-op recovery: if `op:exec` against the
attached sandbox surfaces a 502 / "sandbox was not found" envd error,
SandboxHand calls SandboxService.reprovision_for_bundle and retries
exactly once on the fresh e2b id. The brain receives a clean envelope;
the tape sees a `sandbox_reprovisioned` milestone between the two
attempts for operator audit.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import AsyncIterator

import pytest

from krewhub.workers import sandbox_hand as sh


class _FakeE2bClient:
    """Returns scripted exec streams. First call to a given eid → dead.
    Subsequent calls succeed."""

    def __init__(self) -> None:
        self.exec_calls: list[tuple[str, str]] = []

    async def exec_command(self, sandbox_id, command, *, cwd=None, env=None, timeout=300.0):
        self.exec_calls.append((sandbox_id, command))
        is_dead = (sandbox_id == "e2b_dead")

        async def _gen() -> AsyncIterator[dict]:
            if is_dead:
                yield {
                    "error": (
                        f'envd Start returned 502: {{"sandboxId":"{sandbox_id}",'
                        '"message":"The sandbox was not found","code":502}'
                    ),
                }
                return
            yield {"stream": "stdout", "data": "hello after reprovision\n"}
            yield {"exit_code": 0}
        return _gen()

    async def set_timeout(self, sandbox_id, *, timeout_s=3600):
        return None


class _FakeSandboxService:
    """Reprovision returns a fresh row that exec_command will accept."""

    def __init__(self) -> None:
        self.reprovision_calls: list[tuple[str, str]] = []

    async def reprovision_for_bundle(self, bundle_id: str, *, dead_sandbox_id: str):
        self.reprovision_calls.append((bundle_id, dead_sandbox_id))
        return SimpleNamespace(id="sbx_fresh", e2b_sandbox_id="e2b_fresh", bundle_id=bundle_id)


class _CapturingTape:
    def __init__(self) -> None:
        self.events: list[dict] = []

    async def append(self, kind: str, *, body: str, payload: dict | None = None,
                     actor_type: str = "system", actor_id: str = "") -> None:
        self.events.append({"kind": kind, "body": body, "payload": payload or {}})


class _NoopCancel:
    def __init__(self) -> None:
        self.cancelled = False
        self.reason: str | None = None

    async def wait(self):
        await asyncio.Event().wait()  # never fires in these tests


def _async_return(value):
    async def _g():
        return value
    return _g()


@pytest.mark.asyncio
async def test_dead_sandbox_on_exec_triggers_reprovision_and_retries_clean(monkeypatch):
    """First exec attempt hits dead-sandbox 502; recovery reprovisions
    and the second attempt on the fresh e2b id succeeds — brain sees
    `accept` with the fresh-stream output."""
    e2b = _FakeE2bClient()
    sandbox_row = SimpleNamespace(
        id="sbx_dead", e2b_sandbox_id="e2b_dead",
        bundle_id="bun_1", owner_account_id=None,
    )
    monkeypatch.setattr(
        sh, "SandboxRepo",
        lambda _db: SimpleNamespace(get=lambda sid: _async_return(sandbox_row)),
        raising=False,
    )

    svc = _FakeSandboxService()
    hand = sh.SandboxHand(e2b, db=object(), sandbox_service=svc)
    tape = _CapturingTape()

    result = await hand.execute(
        target_id="sbx_dead",
        input={"op": "exec", "command": "echo hi"},
        schema=None,
        deadline_s=10,
        tape=tape,
        cancel=_NoopCancel(),
    )

    # Brain sees a clean accept envelope from the second attempt
    assert result.action == "accept", result
    assert result.content is not None
    assert result.content["exit_code"] == 0
    assert "hello after reprovision" in result.content["stdout_tail"]

    # exec was attempted twice — first dead, then fresh
    eids = [eid for eid, _ in e2b.exec_calls]
    assert eids == ["e2b_dead", "e2b_fresh"], eids

    # reprovision was called once, with the dead id as idempotency key
    assert svc.reprovision_calls == [("bun_1", "sbx_dead")]

    # Tape has the milestone so the operator can audit the recovery
    milestones = [e for e in tape.events if e["kind"] == "milestone"]
    assert any(
        m["payload"].get("event") == "sandbox_reprovisioned"
        for m in milestones
    ), milestones


@pytest.mark.asyncio
async def test_dead_sandbox_on_exec_without_service_surfaces_to_brain(monkeypatch):
    """If SandboxService isn't wired, the dead-sandbox error reaches the
    brain unmodified — same fall-through as file ops."""
    e2b = _FakeE2bClient()
    sandbox_row = SimpleNamespace(
        id="sbx_dead", e2b_sandbox_id="e2b_dead",
        bundle_id="bun_1", owner_account_id=None,
    )
    monkeypatch.setattr(
        sh, "SandboxRepo",
        lambda _db: SimpleNamespace(get=lambda sid: _async_return(sandbox_row)),
        raising=False,
    )

    hand = sh.SandboxHand(e2b, db=object(), sandbox_service=None)
    result = await hand.execute(
        target_id="sbx_dead",
        input={"op": "exec", "command": "echo hi"},
        schema=None,
        deadline_s=10,
        tape=_CapturingTape(),
        cancel=_NoopCancel(),
    )

    assert result.action == "error"
    assert "sandbox was not found" in (result.reason or "").lower()
    # Only ONE exec attempt — no recovery, no retry
    assert len(e2b.exec_calls) == 1


@pytest.mark.asyncio
async def test_non_dead_error_is_NOT_retried(monkeypatch):
    """A non-recoverable failure (e.g. timeout) is surfaced once, no retry."""
    e2b = _FakeE2bClient()

    async def _exec_timeout(sandbox_id, command, *, cwd=None, env=None, timeout=300.0):
        e2b.exec_calls.append((sandbox_id, command))
        async def _gen():
            yield {"error": "envd request failed: timeout after 5s"}
        return _gen()

    e2b.exec_command = _exec_timeout  # type: ignore[assignment]
    sandbox_row = SimpleNamespace(
        id="sbx_x", e2b_sandbox_id="e2b_x",
        bundle_id="bun_1", owner_account_id=None,
    )
    monkeypatch.setattr(
        sh, "SandboxRepo",
        lambda _db: SimpleNamespace(get=lambda sid: _async_return(sandbox_row)),
        raising=False,
    )

    hand = sh.SandboxHand(e2b, db=object(), sandbox_service=_FakeSandboxService())
    result = await hand.execute(
        target_id="sbx_x",
        input={"op": "exec", "command": "echo hi"},
        schema=None,
        deadline_s=10,
        tape=_CapturingTape(),
        cancel=_NoopCancel(),
    )

    assert result.action == "error"
    assert "timeout" in (result.reason or "")
    # No retry since the error wasn't dead-sandbox shaped
    assert len(e2b.exec_calls) == 1
