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
import base64
import binascii
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from krewhub.invocations.protocol import CancelToken, Hand, TapeWriter
from krewhub.models.invocation import ResultEnvelope
from krewhub.repositories.sandbox_repo import SandboxRepo


logger = logging.getLogger(__name__)


_TAIL_BYTES = 16 * 1024  # 16KB per stream


class SandboxHand:
    """Hand impl for `target_type="sandbox"`."""

    target_type: Literal["sandbox", "agent", "human"] = "sandbox"

    def __init__(
        self,
        e2b: Any,
        db: Any | None = None,
        *,
        sandbox_service: Any | None = None,
        credentials_service: Any | None = None,
    ) -> None:
        # Duck-typed: anything with `exec_command(sandbox_id, command, ...)`
        # returning an async-iterable of NDJSON chunks. Real prod is
        # `krewhub.services.e2b_client.E2bClient`.
        self._e2b = e2b
        # When set, target_id is treated as a krewhub-side `sbx_*` id
        # and resolved to the actual e2b sandbox id via `SandboxRepo`
        # before calling `exec_command`. When None, target_id is passed
        # through verbatim (test path).
        self._db = db
        # When set, dead-sandbox failures trigger transparent re-provision
        # via SandboxService.reprovision_for_bundle (Anthropic Managed
        # Agents `provision({resources})`). When None, the brain sees the
        # raw error envelope (legacy behavior — kept for tests that
        # don't construct a SandboxService).
        self._sandbox_service = sandbox_service
        # When set, op:exec calls are augmented with the sandbox owner's
        # stored credentials (encrypted-at-rest, decrypted just-in-time).
        # This is the Path-B env-injection MVP for the credential vault.
        # See services/credentials_service.py.
        self._credentials_service = credentials_service

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

        # Resolve the krewhub-side `sbx_*` id to e2b's actual sandbox id
        # (long alphanumeric the orchestrator/proxy expects). Tests skip
        # this by constructing SandboxHand without `db`.
        e2b_sandbox_id = target_id
        owner_account_id: str | None = None
        if self._db is not None and target_id.startswith("sbx_"):
            try:
                row = await SandboxRepo(self._db).get(target_id)
                if row is None:
                    return ResultEnvelope(
                        action="error",
                        reason=f"sandbox_not_found: {target_id}",
                    )
                e2b_sandbox_id = row.e2b_sandbox_id
                # `owner_account_id` is optional on legacy rows / test
                # fixtures — fall back to None and skip env injection.
                owner_account_id = getattr(row, "owner_account_id", None)
            except Exception as exc:
                return ResultEnvelope(
                    action="error",
                    reason=f"sandbox_lookup_failed: {exc}",
                )

        op, parse_err = _parse_op(input)
        if op is None:
            return ResultEnvelope(action="error", reason=parse_err or "bad_input")

        if isinstance(op, _OpWrite):
            return await self._execute_write(
                target_id, e2b_sandbox_id, op.path, op.data, tape,
            )
        if isinstance(op, _OpRead):
            return await self._execute_read(
                target_id, e2b_sandbox_id, op.path, tape,
            )
        if isinstance(op, _OpList):
            return await self._execute_list(
                target_id, e2b_sandbox_id, op.path, op.depth, tape,
            )
        # Default + explicit op="exec" fall through to the streaming path.
        # Merge operator-stored credentials (env-var form) into the brain's
        # env. Brain-supplied env vars win on conflict — the brain may
        # legitimately want to override e.g. GITHUB_TOKEN with a per-op
        # value. Credentials are decrypted just-in-time and never logged.
        merged_env = await self._merge_credentials_into_env(
            owner_account_id, op.env,
        )

        return await self._execute_exec(
            target_id=target_id,
            e2b_sandbox_id=e2b_sandbox_id,
            command=op.command, cwd=op.cwd, env=merged_env,
            deadline_s=deadline_s,
            tape=tape, cancel=cancel,
        )

    async def _merge_credentials_into_env(
        self,
        owner_account_id: str | None,
        brain_env: dict[str, str] | None,
    ) -> dict[str, str] | None:
        """Return env with operator's credentials merged in.

        Brain's env wins on conflict. Returns None iff both sources are
        empty, to preserve the existing test-path behavior of "no env" =
        envd uses sandbox defaults.
        """
        if self._credentials_service is None or owner_account_id is None:
            return brain_env
        try:
            stored = await self._credentials_service.get_envs(owner_account_id)
        except Exception as exc:
            # A storage failure shouldn't block the op — the brain may
            # be running something that doesn't need a credential at all.
            logger.warning(
                "credentials.get_envs failed for account=%s: %s",
                owner_account_id, exc,
            )
            return brain_env
        if not stored and not brain_env:
            return None
        merged: dict[str, str] = dict(stored)
        if brain_env:
            merged.update(brain_env)  # brain wins on key conflict
        return merged

    async def _execute_exec(
        self,
        *,
        target_id: str,
        e2b_sandbox_id: str,
        command: str,
        cwd: str | None,
        env: dict[str, str] | None,
        deadline_s: int,
        tape: TapeWriter,
        cancel: CancelToken,
    ) -> ResultEnvelope:
        stdout_buf = _Tail(_TAIL_BYTES)
        stderr_buf = _Tail(_TAIL_BYTES)
        exit_code: int | None = None
        infra_error: str | None = None

        # Spawn the e2b stream and a cancel watcher in parallel.
        # The e2b client returns an async generator (via async function).
        try:
            stream = await self._e2b.exec_command(
                e2b_sandbox_id, command,
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

        await self._heartbeat(e2b_sandbox_id)
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

    # ---- File-op dispatchers (Phase 3) ------------------------------------

    async def _with_recovery(
        self,
        target_id: str,
        e2b_sandbox_id: str,
        op: Callable[[str], Awaitable[Any]],
        tape: TapeWriter | None = None,
    ) -> Any:
        """Run `op(e2b_id)` once; if it dies with a `sandbox not found`
        signal, reprovision via SandboxService and retry exactly once
        with the new e2b id.

        Returns op's return value on success; re-raises the original
        exception if recovery isn't applicable (no service wired,
        non-dead error, no bundle_id, reprovision itself fails).

        The brain receives a clean ResultEnvelope; recovery is a system-
        side concern surfaced only via the tape (`sandbox.reprovisioned`
        event) for operator audit.
        """
        try:
            return await op(e2b_sandbox_id)
        except Exception as exc:
            if not _looks_like_dead_sandbox(exc):
                raise
            if self._sandbox_service is None or self._db is None:
                raise
            sandbox_row = await self._lookup_sandbox(target_id)
            if sandbox_row is None or not sandbox_row.bundle_id:
                # Per-task sandboxes (no bundle_id) can't be safely
                # re-provisioned — there's no recipe to recreate from.
                raise
            try:
                # Pass the id we just saw die as the idempotency token.
                # If a parallel recovery already swapped bundle.sandbox_id,
                # the service returns that replacement instead of
                # provisioning yet another.
                fresh = await self._sandbox_service.reprovision_for_bundle(
                    sandbox_row.bundle_id,
                    dead_sandbox_id=sandbox_row.id,
                )
            except Exception as reprov_exc:
                logger.warning(
                    "recovery: reprovision_for_bundle(%s) failed: %s",
                    sandbox_row.bundle_id, reprov_exc,
                )
                raise exc
            if tape is not None:
                try:
                    await tape.append(
                        "milestone",
                        body=f"sandbox reprovisioned: {sandbox_row.id} → {fresh.id}",
                        payload={
                            "event": "sandbox_reprovisioned",
                            "old_sandbox_id": sandbox_row.id,
                            "new_sandbox_id": fresh.id,
                            "new_e2b_id": fresh.e2b_sandbox_id,
                            "reason": "dead_sandbox_recovery",
                        },
                        actor_type="system",
                    )
                except Exception:  # pragma: no cover — tape best-effort
                    logger.exception("recovery: tape append failed (continuing)")
            # Retry exactly once on the fresh sandbox. If this also
            # fails, the caller surfaces the failure to the brain.
            return await op(fresh.e2b_sandbox_id)

    async def _lookup_sandbox(self, target_id: str) -> Any | None:
        """Return the SandboxRow for `target_id` (krewhub `sbx_*` id),
        or None if not resolvable. Bridges to SandboxRepo via the same
        db handle SandboxHand was constructed with."""
        if self._db is None or not target_id.startswith("sbx_"):
            return None
        try:
            return await SandboxRepo(self._db).get(target_id)
        except Exception:
            return None

    async def _heartbeat(self, e2b_sandbox_id: str) -> None:
        """Bump the e2b sandbox's `endAt` to `now + 1h` after a successful
        op. Best-effort: a missed heartbeat shouldn't fail the actual op.

        Without this, a sandbox provisioned for a long bundle gets reaped
        by the orchestrator at `created_at + timeout_s` (max 1h) regardless
        of how active it is. Heartbeating on every successful Hand call
        keeps actively-used sandboxes alive indefinitely.
        """
        set_timeout = getattr(self._e2b, "set_timeout", None)
        if set_timeout is None:
            return
        try:
            await set_timeout(e2b_sandbox_id, timeout_s=3600)
        except Exception as exc:  # pragma: no cover — best-effort
            logger.debug(
                "heartbeat: set_timeout(%s) failed: %s",
                e2b_sandbox_id, exc,
            )

    async def _execute_write(
        self,
        target_id: str,
        e2b_sandbox_id: str,
        path: str,
        data: bytes,
        tape: TapeWriter,
    ) -> ResultEnvelope:
        async def _op(eid: str) -> str:
            await self._e2b.write_file(eid, path, data)
            return eid  # propagate the e2b id heartbeat should refresh
        try:
            used_eid = await self._with_recovery(
                target_id, e2b_sandbox_id, _op, tape,
            )
        except ValueError as exc:
            msg = str(exc)
            # Preserve the stable `path_too_large:` reason code from the
            # client so the agent can branch on it; map any other ValueError
            # to a generic filesystem-failure envelope.
            if "path_too_large" in msg:
                return ResultEnvelope(action="error", reason=msg)
            return ResultEnvelope(
                action="error", reason=f"e2b_filesystem_failed: {exc}",
            )
        except FileNotFoundError as exc:
            return ResultEnvelope(
                action="error", reason=f"path_not_found: {exc}",
            )
        except Exception as exc:
            return ResultEnvelope(
                action="error", reason=f"e2b_filesystem_failed: {exc}",
            )
        await self._heartbeat(used_eid)
        return ResultEnvelope(
            action="accept",
            content={"path": path, "bytes_written": len(data)},
        )

    async def _execute_read(
        self,
        target_id: str,
        e2b_sandbox_id: str,
        path: str,
        tape: TapeWriter,
    ) -> ResultEnvelope:
        captured: dict[str, Any] = {}
        async def _op(eid: str) -> bytes:
            data = await self._e2b.read_file(eid, path)
            captured["e2b_id"] = eid
            return data
        try:
            data = await self._with_recovery(
                target_id, e2b_sandbox_id, _op, tape,
            )
        except FileNotFoundError as exc:
            return ResultEnvelope(
                action="error", reason=f"path_not_found: {exc}",
            )
        except Exception as exc:
            return ResultEnvelope(
                action="error", reason=f"e2b_filesystem_failed: {exc}",
            )
        await self._heartbeat(captured.get("e2b_id", e2b_sandbox_id))
        text, encoding = _classify_bytes(data)
        return ResultEnvelope(
            action="accept",
            content={"path": path, "data": text, "encoding": encoding},
        )

    async def _execute_list(
        self,
        target_id: str,
        e2b_sandbox_id: str,
        path: str,
        depth: int,
        tape: TapeWriter,
    ) -> ResultEnvelope:
        captured: dict[str, Any] = {}
        async def _op(eid: str) -> list[dict]:
            entries = await self._e2b.list_dir(eid, path, depth=depth)
            captured["e2b_id"] = eid
            return entries
        try:
            entries = await self._with_recovery(
                target_id, e2b_sandbox_id, _op, tape,
            )
        except FileNotFoundError as exc:
            return ResultEnvelope(
                action="error", reason=f"path_not_found: {exc}",
            )
        except Exception as exc:
            return ResultEnvelope(
                action="error", reason=f"e2b_filesystem_failed: {exc}",
            )
        await self._heartbeat(captured.get("e2b_id", e2b_sandbox_id))
        return ResultEnvelope(
            action="accept",
            content={"path": path, "entries": entries},
        )


# ---------------------------------------------------------------------------
# Op vocabulary (Phase 3 — agent-driven file ops)
#
# Discriminated union over the input shapes accepted by `delegate(sandbox,
# input)`. Backwards compat: a bare string OR a dict with no `op` key is
# treated as `op:"exec"` so legacy callers continue to work.
# ---------------------------------------------------------------------------


@dataclass
class _OpExec:
    command: str
    cwd: str | None = None
    env: dict[str, str] | None = None


@dataclass
class _OpWrite:
    path: str
    data: bytes


@dataclass
class _OpRead:
    path: str


@dataclass
class _OpList:
    path: str
    depth: int = 1


_Op = _OpExec | _OpWrite | _OpRead | _OpList


def _parse_op(input: str | dict) -> tuple[_Op | None, str | None]:
    """Parse delegate input into a typed op. Returns `(op, None)` on success
    or `(None, reason)` on a parse error so the caller can return a stable
    error envelope."""
    if isinstance(input, str):
        return _OpExec(command=input), None
    if not isinstance(input, dict):
        return None, "bad_input: input must be a string or object"

    op = str(input.get("op") or "exec").lower()

    if op == "exec":
        command = str(input.get("command") or "")
        if not command:
            return None, (
                "empty_command: SandboxHand exec requires a non-empty command"
            )
        cwd = input.get("cwd")
        env = input.get("env")
        if cwd is not None and not isinstance(cwd, str):
            cwd = str(cwd)
        if env is not None and not isinstance(env, dict):
            env = None
        return _OpExec(command=command, cwd=cwd, env=env), None

    if op == "write":
        path = input.get("path")
        if not isinstance(path, str) or not path:
            return None, "missing_path: write op requires 'path' string"
        if "data" not in input:
            return None, "missing_data: write op requires 'data' field"
        encoding = str(input.get("encoding") or "utf-8").lower()
        raw = input.get("data")
        try:
            if encoding == "base64":
                if not isinstance(raw, (str, bytes)):
                    return None, "bad_data: base64 'data' must be string"
                data_bytes = base64.b64decode(raw, validate=True)
            elif encoding == "utf-8":
                if isinstance(raw, bytes):
                    data_bytes = raw
                else:
                    data_bytes = str(raw).encode("utf-8")
            else:
                return None, (
                    f"bad_encoding: '{encoding}' "
                    f"(expected 'utf-8' or 'base64')"
                )
        except (binascii.Error, ValueError) as exc:
            return None, f"bad_data: {exc}"
        return _OpWrite(path=path, data=data_bytes), None

    if op == "read":
        path = input.get("path")
        if not isinstance(path, str) or not path:
            return None, "missing_path: read op requires 'path' string"
        return _OpRead(path=path), None

    if op == "list":
        path = input.get("path")
        if not isinstance(path, str) or not path:
            return None, "missing_path: list op requires 'path' string"
        depth = input.get("depth", 1)
        try:
            depth = max(1, int(depth))
        except (TypeError, ValueError):
            depth = 1
        return _OpList(path=path, depth=depth), None

    return None, (
        f"unknown_op: '{op}' "
        "(expected 'exec', 'write', 'read', or 'list')"
    )


def _classify_bytes(data: bytes) -> tuple[str, str]:
    """Heuristic for read responses: prefer `(text, 'utf-8')` when the
    bytes are clean UTF-8 with no NULs, else `(base64, 'base64')`."""
    if b"\x00" in data:
        return base64.b64encode(data).decode("ascii"), "base64"
    try:
        return data.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        return base64.b64encode(data).decode("ascii"), "base64"


# Dead-sandbox detection — conservative pattern match. Only patterns
# that uniquely identify a reaped/missing sandbox trigger reprovision.
# Generic transient failures (connection refused, network timeout) are
# NOT covered, since reprovisioning helps nothing if the path itself
# is broken.
#
# A reaped sandbox surfaces three different ways depending on the path:
#   1. Exec (Connect-streaming via :3002):
#        envd Start returned 502 with body
#        `{"sandboxId":"...","message":"The sandbox was not found","code":502}`
#      → chunk `{"error": "envd Start returned 502: ..."}` in the stream
#   2. Filesystem unary (Connect-unary via :3002):
#        client-proxy responds 502 Bad Gateway when it can't route to
#        a missing envd. httpx raises HTTPStatusError("Server error
#        '502 Bad Gateway' for url '...'"). No body, just the status.
#   3. REST file ops (/files endpoint via :3002): same as #2.
#
# Pattern (1) is detectable by the verbose message; (2) and (3) only
# carry "502" in the httpx wrapper, so we accept "502 bad gateway" as
# the dead-sandbox signal too. Real 502s from a healthy sandbox would
# be a service bug we'd want to know about anyway.
_DEAD_SANDBOX_MARKERS = (
    "sandbox was not found",
    "sandbox_not_found",
    '"code":502',
    "code:502",
    "502 bad gateway",
)


def _looks_like_dead_sandbox(error: BaseException | str) -> bool:
    """True iff the error message uniquely identifies a reaped sandbox.
    Used by SandboxHand to decide whether `_with_recovery` should call
    SandboxService.reprovision_for_bundle and retry the op."""
    msg = (str(error) if not isinstance(error, str) else error).lower()
    return any(marker.lower() in msg for marker in _DEAD_SANDBOX_MARKERS)


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
