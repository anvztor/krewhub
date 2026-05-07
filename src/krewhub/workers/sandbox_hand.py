"""SandboxHand — runs a shell command in an e2b sandbox.

Implements the `Hand` Protocol from `krewhub.invocations.protocol`.
The Hand author writes only `execute()`. The InvocationService
takes care of `started`/`done`/`handoff`/cancellation propagation.

Behavior (contract §10.1, §13.12):
- Streams stdout/stderr as `output` events on the tape.
- Returns ResultEnvelope(action="accept", content={...}) for ANY process
  exit (zero or non-zero). The exit_code lives in content.
- ResultEnvelope(action="error", reason=...) is reserved for execution
  *infrastructure* failures: sandbox didn't start, network died,
  deadline kill, etc. — not for non-zero process exits.
- stdout_tail / stderr_tail are capped at 16KB each; longer logs remain
  on the tape via preceding `output` events.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Literal

from krewhub.invocations.protocol import CancelToken, Hand, TapeWriter
from krewhub.models.invocation import ResultEnvelope


logger = logging.getLogger(__name__)


_TAIL_BYTES = 16 * 1024  # 16KB per stream


class SandboxHand:
    """Hand impl for `target_type="sandbox"`."""

    target_type: Literal["sandbox", "agent", "human"] = "sandbox"

    def __init__(self, e2b: Any) -> None:
        # Duck-typed: anything with `exec_command(sandbox_id, command, ...)`
        # returning an async-iterable of NDJSON chunks. Real prod is
        # `krewhub.services.e2b_client.E2bClient`.
        self._e2b = e2b

    async def execute(
        self,
        *,
        target_id: str | None,
        input: str | dict,
        schema: dict | None,
        deadline_s: int,
        tape: TapeWriter,
        cancel: CancelToken,
    ) -> ResultEnvelope:
        if not target_id:
            return ResultEnvelope(
                action="error",
                reason="sandbox_id_missing: SandboxHand requires a target_id",
            )

        command, cwd, env = _parse_input(input)
        if not command:
            return ResultEnvelope(
                action="error",
                reason="empty_command: SandboxHand input must contain a non-empty command",
            )

        stdout_buf = _Tail(_TAIL_BYTES)
        stderr_buf = _Tail(_TAIL_BYTES)
        exit_code: int | None = None
        infra_error: str | None = None

        # Spawn the e2b stream and a cancel watcher in parallel.
        # The e2b client returns an async generator (via async function).
        try:
            stream = await self._e2b.exec_command(
                target_id, command,
                cwd=cwd, env=env,
                timeout=float(deadline_s),
            )
        except Exception as exc:  # pragma: no cover — covered by tests
            return ResultEnvelope(
                action="error",
                reason=f"e2b_exec_failed: {exc}",
            )

        async def _consume() -> None:
            nonlocal exit_code, infra_error
            try:
                async for chunk in stream:
                    if cancel.cancelled:
                        return
                    if "exit_code" in chunk:
                        try:
                            exit_code = int(chunk["exit_code"])
                        except (TypeError, ValueError):
                            exit_code = None
                            infra_error = (
                                f"bad_exit_code: {chunk.get('exit_code')!r}"
                            )
                        return
                    if "error" in chunk:
                        infra_error = str(chunk["error"])
                        return
                    stream_name = chunk.get("stream")
                    data = chunk.get("data") or ""
                    if stream_name in ("stdout", "stderr") and data:
                        if stream_name == "stdout":
                            stdout_buf.append(data)
                        else:
                            stderr_buf.append(data)
                        await tape.append(
                            "output",
                            body="",
                            payload={"stream": stream_name, "chunk": data},
                            actor_type="sandbox",
                            actor_id=target_id,
                        )
            except Exception as exc:
                infra_error = f"e2b_stream_error: {exc}"

        consume_task = asyncio.create_task(_consume(), name="sandbox-consume")
        cancel_task = asyncio.create_task(cancel.wait(), name="sandbox-cancel-watch")

        try:
            done, pending = await asyncio.wait(
                {consume_task, cancel_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for t in (consume_task, cancel_task):
                if not t.done():
                    t.cancel()
            # Surface any exception from the consume task that completed.
            if consume_task.done() and not consume_task.cancelled():
                exc = consume_task.exception()
                if exc is not None and infra_error is None:
                    infra_error = f"e2b_consume_crashed: {exc}"

        if cancel.cancelled:
            # Best-effort: ask e2b to kill the process. Tolerate no kill_process.
            kill = getattr(self._e2b, "kill_process", None)
            if kill is not None:
                try:
                    await kill(target_id)
                except Exception as exc:  # pragma: no cover
                    logger.warning("sandbox kill failed: %s", exc)
            return ResultEnvelope(
                action="cancel",
                reason=cancel.reason or "operator_cancelled",
            )

        if infra_error is not None:
            # Detect the self-hosted e2b deployment's "exec endpoint not
            # wired" failure mode and surface a clearer reason. The
            # orchestrator returns 404 "no matching operation was found"
            # because exec lives on envd (per-sandbox), not on the
            # orchestrator. krewhub's e2b client needs to be taught to
            # route through the client-proxy with the right Host header
            # to reach the sandbox's envd; until then SandboxHand is
            # contract-shaped but operationally inert.
            if "no matching operation" in infra_error or "404" in infra_error:
                return ResultEnvelope(
                    action="error",
                    reason=(
                        "e2b_exec_not_wired: the krewhub→envd exec path "
                        "is not configured in this deployment. "
                        f"Underlying: {infra_error}"
                    ),
                )
            return ResultEnvelope(action="error", reason=infra_error)

        # If the stream ended without an exit_code event AND no error event,
        # treat as infra failure (program didn't terminate cleanly).
        if exit_code is None:
            return ResultEnvelope(
                action="error",
                reason="no_exit_code: e2b stream ended without exit code",
            )

        return ResultEnvelope(
            action="accept",
            content={
                "exit_code": exit_code,
                "stdout_tail": stdout_buf.value(),
                "stderr_tail": stderr_buf.value(),
                "artifacts": [],          # populated by slice 3
                "diff_summary": None,     # populated by slice 3
            },
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_input(
    input: str | dict,
) -> tuple[str, str | None, dict[str, str] | None]:
    """Extract (command, cwd, env) from either string or dict input."""
    if isinstance(input, str):
        return input, None, None
    command = str(input.get("command") or "")
    cwd = input.get("cwd")
    env = input.get("env")
    if cwd is not None and not isinstance(cwd, str):
        cwd = str(cwd)
    if env is not None and not isinstance(env, dict):
        env = None
    return command, cwd, env


class _Tail:
    """Bounded byte buffer that keeps the *last* N bytes of appended text.

    Stores raw text (str). Tail is computed as the last N bytes of
    UTF-8 encoding to satisfy the contract's byte cap; we then decode
    leniently so partial multi-byte sequences don't crash the response.
    """

    def __init__(self, max_bytes: int) -> None:
        self._max = max_bytes
        self._chunks: list[str] = []
        self._total_bytes: int = 0

    def append(self, text: str) -> None:
        if not text:
            return
        self._chunks.append(text)
        self._total_bytes += len(text.encode("utf-8", errors="replace"))
        # Trim if we've accumulated way past the cap (keep ~2× headroom).
        if self._total_bytes > self._max * 4:
            joined = "".join(self._chunks).encode("utf-8", errors="replace")
            tail = joined[-self._max:]
            self._chunks = [tail.decode("utf-8", errors="replace")]
            self._total_bytes = len(tail)

    def value(self) -> str:
        joined = "".join(self._chunks).encode("utf-8", errors="replace")
        if len(joined) <= self._max:
            return joined.decode("utf-8", errors="replace")
        return joined[-self._max:].decode("utf-8", errors="replace")
