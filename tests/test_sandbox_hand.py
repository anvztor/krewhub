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
        # Heartbeat tracker — SandboxHand calls set_timeout after success.
        self.heartbeat_calls: list[tuple[str, int]] = []
        # Filesystem RPC scripting (Phase 3).
        self.write_calls: list[tuple[str, str, bytes]] = []
        self.read_files: dict[str, bytes] = {}
        self.list_entries: dict[str, list[dict]] = {}
        self.stat_entries: dict[str, dict] = {}
        self.fs_errors: dict[str, Exception] = {}  # method name → exception

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

    async def set_timeout(self, sandbox_id: str, *, timeout_s: int = 3600) -> None:
        if "set_timeout" in self.fs_errors:
            raise self.fs_errors["set_timeout"]
        self.heartbeat_calls.append((sandbox_id, timeout_s))

    async def write_file(self, sandbox_id: str, path: str, data: bytes) -> None:
        if "write_file" in self.fs_errors:
            raise self.fs_errors["write_file"]
        self.write_calls.append((sandbox_id, path, data))

    async def read_file(self, sandbox_id: str, path: str) -> bytes:
        if "read_file" in self.fs_errors:
            raise self.fs_errors["read_file"]
        if path not in self.read_files:
            raise FileNotFoundError(path)
        return self.read_files[path]

    async def list_dir(
        self, sandbox_id: str, path: str, *, depth: int = 1,
    ) -> list[dict]:
        if "list_dir" in self.fs_errors:
            raise self.fs_errors["list_dir"]
        if path not in self.list_entries:
            raise FileNotFoundError(path)
        return self.list_entries[path]

    async def stat(self, sandbox_id: str, path: str) -> dict:
        if "stat" in self.fs_errors:
            raise self.fs_errors["stat"]
        if path not in self.stat_entries:
            raise FileNotFoundError(path)
        return self.stat_entries[path]


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


# ---------------------------------------------------------------------------
# Phase 3 — agent-driven `op` vocabulary (write / read / list)
# Plan: docs/superpowers/plans/2026-05-08-sandbox-hand-vocabulary.md
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_op_exec_explicit_dispatches_to_exec_command():
    """Agent passes {op:'exec', command:'foo'} → goes through exec_command path."""
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient(scripts={"sbx_1": [{"exit_code": 0}]})
    hand = SandboxHand(e2b)
    tape = _CapturingTape()
    cancel = _StubCancel()

    result = await hand.execute(
        target_id="sbx_1",
        input={"op": "exec", "command": "echo hi", "cwd": "/tmp"},
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )

    assert result.action == "accept"
    assert e2b.calls[0]["command"] == "echo hi"
    assert e2b.calls[0]["cwd"] == "/tmp"


@pytest.mark.asyncio
async def test_op_write_returns_accept_with_bytes_written():
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient()
    hand = SandboxHand(e2b)
    tape = _CapturingTape()
    cancel = _StubCancel()

    result = await hand.execute(
        target_id="sbx_1",
        input={"op": "write", "path": "/tmp/hi.txt", "data": "hello"},
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )

    assert result.action == "accept"
    assert result.content == {"path": "/tmp/hi.txt", "bytes_written": 5}
    assert e2b.write_calls == [("sbx_1", "/tmp/hi.txt", b"hello")]


@pytest.mark.asyncio
async def test_op_write_base64_decodes_data_before_write():
    from krewhub.workers.sandbox_hand import SandboxHand
    import base64 as _b64

    raw = bytes([0, 1, 2, 0xff])
    e2b = FakeE2bClient()
    hand = SandboxHand(e2b)
    tape = _CapturingTape()
    cancel = _StubCancel()

    result = await hand.execute(
        target_id="sbx_1",
        input={
            "op": "write", "path": "/tmp/blob.bin",
            "data": _b64.b64encode(raw).decode("ascii"),
            "encoding": "base64",
        },
        schema=None, deadline_s=10,
        tape=tape, cancel=cancel,  # type: ignore[arg-type]
    )

    assert result.action == "accept"
    assert result.content["bytes_written"] == 4
    assert e2b.write_calls == [("sbx_1", "/tmp/blob.bin", raw)]


@pytest.mark.asyncio
async def test_op_write_missing_path_returns_error():
    from krewhub.workers.sandbox_hand import SandboxHand

    hand = SandboxHand(FakeE2bClient())
    result = await hand.execute(
        target_id="sbx_1",
        input={"op": "write", "data": "hello"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    assert result.action == "error"
    assert "path" in (result.reason or "")


@pytest.mark.asyncio
async def test_op_write_missing_data_returns_error():
    from krewhub.workers.sandbox_hand import SandboxHand

    hand = SandboxHand(FakeE2bClient())
    result = await hand.execute(
        target_id="sbx_1",
        input={"op": "write", "path": "/tmp/x"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    assert result.action == "error"
    assert "data" in (result.reason or "")


@pytest.mark.asyncio
async def test_op_write_path_too_large_surfaces_as_error():
    """E2bClient raises ValueError('path_too_large') above the size cap.
    SandboxHand maps that to an `error` envelope with stable reason."""
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient()
    e2b.fs_errors["write_file"] = ValueError("path_too_large: 2000000 bytes exceeds cap")
    hand = SandboxHand(e2b)

    result = await hand.execute(
        target_id="sbx_1",
        input={"op": "write", "path": "/tmp/big", "data": "x"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    assert result.action == "error"
    assert "path_too_large" in (result.reason or "")


@pytest.mark.asyncio
async def test_op_read_text_returns_utf8_encoding():
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient()
    e2b.read_files["/tmp/hi.txt"] = b"hello world"
    hand = SandboxHand(e2b)

    result = await hand.execute(
        target_id="sbx_1",
        input={"op": "read", "path": "/tmp/hi.txt"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    assert result.action == "accept"
    assert result.content == {
        "path": "/tmp/hi.txt",
        "data": "hello world",
        "encoding": "utf-8",
    }


@pytest.mark.asyncio
async def test_op_read_binary_returns_base64_encoding():
    from krewhub.workers.sandbox_hand import SandboxHand
    import base64 as _b64

    raw = bytes([0, 1, 2, 0xff, 0xfe])  # has NUL → must base64
    e2b = FakeE2bClient()
    e2b.read_files["/tmp/blob.bin"] = raw
    hand = SandboxHand(e2b)

    result = await hand.execute(
        target_id="sbx_1",
        input={"op": "read", "path": "/tmp/blob.bin"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    assert result.action == "accept"
    assert result.content["encoding"] == "base64"
    assert _b64.b64decode(result.content["data"]) == raw


@pytest.mark.asyncio
async def test_op_read_missing_path_returns_error_path_not_found():
    from krewhub.workers.sandbox_hand import SandboxHand

    hand = SandboxHand(FakeE2bClient())  # no preloaded files
    result = await hand.execute(
        target_id="sbx_1",
        input={"op": "read", "path": "/tmp/nope"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    assert result.action == "error"
    assert "path_not_found" in (result.reason or "")


@pytest.mark.asyncio
async def test_op_list_returns_accept_with_entries():
    from krewhub.workers.sandbox_hand import SandboxHand

    entries = [
        {"name": "a.txt", "type": "FILE_TYPE_FILE", "path": "/tmp/a.txt", "size": "5"},
        {"name": "sub", "type": "FILE_TYPE_DIRECTORY", "path": "/tmp/sub", "size": "4096"},
    ]
    e2b = FakeE2bClient()
    e2b.list_entries["/tmp"] = entries
    hand = SandboxHand(e2b)

    result = await hand.execute(
        target_id="sbx_1",
        input={"op": "list", "path": "/tmp"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    assert result.action == "accept"
    assert result.content == {"path": "/tmp", "entries": entries}


@pytest.mark.asyncio
async def test_op_list_missing_path_returns_error():
    from krewhub.workers.sandbox_hand import SandboxHand

    hand = SandboxHand(FakeE2bClient())  # no preloaded list_entries
    result = await hand.execute(
        target_id="sbx_1",
        input={"op": "list", "path": "/nope"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    assert result.action == "error"
    assert "path_not_found" in (result.reason or "")


@pytest.mark.asyncio
async def test_op_unknown_returns_error():
    from krewhub.workers.sandbox_hand import SandboxHand

    hand = SandboxHand(FakeE2bClient())
    result = await hand.execute(
        target_id="sbx_1",
        input={"op": "wat", "path": "/tmp"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    assert result.action == "error"
    assert "unknown_op" in (result.reason or "")


# ---------------------------------------------------------------------------
# Phase 5 — heartbeat: SandboxHand calls e2b.set_timeout(...) after every
# successful op so actively-used sandboxes never get reaped mid-bundle.
# Cookrew-beta task on 2026-05-09 surfaced the failure mode (502 "sandbox
# not found" mid-task) that this fix prevents.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_heartbeat_fires_after_successful_exec():
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient(scripts={"sbx_1": [{"exit_code": 0}]})
    hand = SandboxHand(e2b)
    await hand.execute(
        target_id="sbx_1", input="ls",
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    assert e2b.heartbeat_calls == [("sbx_1", 3600)]


@pytest.mark.asyncio
async def test_heartbeat_fires_after_successful_write_read_list():
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient()
    e2b.read_files["/tmp/x"] = b"hi"
    e2b.list_entries["/tmp"] = []
    hand = SandboxHand(e2b)

    await hand.execute(
        target_id="sbx_1",
        input={"op": "write", "path": "/tmp/x", "data": "hi"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    await hand.execute(
        target_id="sbx_1",
        input={"op": "read", "path": "/tmp/x"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    await hand.execute(
        target_id="sbx_1",
        input={"op": "list", "path": "/tmp"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    # One heartbeat per successful op — write, read, list = 3
    assert e2b.heartbeat_calls == [
        ("sbx_1", 3600),
        ("sbx_1", 3600),
        ("sbx_1", 3600),
    ]


@pytest.mark.asyncio
async def test_heartbeat_does_not_fire_on_failure():
    """Failed ops should not waste a heartbeat call. Both keep
    set_timeout best-effort so a heartbeat failure never becomes the
    reason a task fails."""
    from krewhub.workers.sandbox_hand import SandboxHand

    hand = SandboxHand(FakeE2bClient())  # no preloaded read_files
    result = await hand.execute(
        target_id="sbx_1",
        input={"op": "read", "path": "/tmp/missing"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    assert result.action == "error"
    # The fake's heartbeat_calls list stays empty.
    assert hand._e2b.heartbeat_calls == []  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_heartbeat_failure_does_not_break_op():
    """If the orchestrator rejects set_timeout, the op result must still
    return successfully. The user's task should not fail because of a
    missed heartbeat."""
    from krewhub.workers.sandbox_hand import SandboxHand

    e2b = FakeE2bClient()
    e2b.fs_errors["set_timeout"] = RuntimeError("orchestrator rejected")
    e2b.read_files["/tmp/x"] = b"hi"
    hand = SandboxHand(e2b)

    result = await hand.execute(
        target_id="sbx_1",
        input={"op": "read", "path": "/tmp/x"},
        schema=None, deadline_s=10,
        tape=_CapturingTape(), cancel=_StubCancel(),  # type: ignore[arg-type]
    )
    assert result.action == "accept"
    assert result.content == {"path": "/tmp/x", "data": "hi", "encoding": "utf-8"}
