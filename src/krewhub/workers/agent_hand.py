"""AgentHand — bridges `delegate(to="agent:<id>", ...)` to the A2A queue.

Implements the `Hand` Protocol from `krewhub.invocations.protocol`.

Flow (contract §10.3):
1. Parse `target_id` into `(agent_name, owner)` per the `@`-convention
   (`claude@krew` → agent_name="claude", owner="krew"). Bare ids (no `@`)
   use a default owner.
2. Insert a row into `a2a_invocations` with `method="delegate"` and
   `params={input, schema?}`. The existing krewcli daemon polls
   `list_pending(owner, agent_name)` and claims the row.
3. Poll the row at `poll_interval_s` until it reaches a terminal status
   or `cancel.cancelled` flips. Honor `deadline_s` on top of the row's
   own `expires_at`.
4. Map A2A status → ResultEnvelope:
   - `completed` → `accept` with `content = result text`
   - `failed`    → `error`  with `reason = error text`
   - `timeout`   → `cancel` with `reason = "a2a_timeout"`
5. On operator cancel, mark the A2A row as `timeout` (best-effort,
   tolerates already-claimed/completed) and return cancel envelope.

The daemon-side handling of `method="delegate"` (running a sub-Brain
against `params.input` and posting the reply via /a2a/respond) is the
counterpart in krewcli. Without it, AgentHand invocations sit pending
until the deadline and return a cancel envelope — which is the correct
contract behavior, just not very useful operationally.
"""
from __future__ import annotations

import asyncio
import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import Literal

import aiosqlite

from krewhub.invocations.protocol import CancelToken, TapeWriter
from krewhub.models.invocation import ResultEnvelope
from krewhub.repositories.a2a_invocation_repo import A2AInvocationRepo


_DEFAULT_POLL_INTERVAL_S = 0.5


class AgentHand:
    target_type: Literal["sandbox", "agent", "human"] = "agent"

    def __init__(
        self,
        db: aiosqlite.Connection,
        *,
        poll_interval_s: float = _DEFAULT_POLL_INTERVAL_S,
        default_owner: str = "krew",
    ) -> None:
        self._db = db
        self._poll_interval_s = poll_interval_s
        self._default_owner = default_owner

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
                reason="target_id_missing: AgentHand requires a target_id (e.g. 'claude@krew')",
            )

        agent_name, owner = _parse_target_id(target_id, self._default_owner)
        params = {"input": input}
        if schema is not None:
            params["schema"] = schema  # type: ignore[assignment]

        repo = A2AInvocationRepo(self._db)
        invocation_id = _new_a2a_id()
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=deadline_s)
        try:
            await repo.create(
                id=invocation_id,
                owner=owner,
                agent_name=agent_name,
                method="delegate",
                params_json=json.dumps(params),
                caller_id=None,
                expires_at=expires_at,
            )
        except Exception as exc:
            return ResultEnvelope(
                action="error",
                reason=f"a2a_create_failed: {exc}",
            )

        # Poll loop with cancel-aware sleep.
        deadline_t = asyncio.get_event_loop().time() + float(deadline_s)
        while True:
            row = await repo.get(invocation_id)
            if row is None:
                return ResultEnvelope(
                    action="error",
                    reason=f"a2a_row_lost: {invocation_id}",
                )
            status = row.get("status")
            if status == "completed":
                return ResultEnvelope(
                    action="accept",
                    content=row.get("result") or "",
                )
            if status == "failed":
                return ResultEnvelope(
                    action="error",
                    reason=row.get("error") or "a2a_failed",
                )
            if status == "timeout":
                return ResultEnvelope(
                    action="cancel",
                    reason="a2a_timeout",
                )

            if cancel.cancelled:
                # Best-effort — tolerates the row being completed in-flight.
                try:
                    await repo.mark_timeout(invocation_id)
                except Exception:
                    pass
                return ResultEnvelope(
                    action="cancel",
                    reason=cancel.reason or "operator_cancelled",
                )

            if asyncio.get_event_loop().time() > deadline_t:
                try:
                    await repo.mark_timeout(invocation_id)
                except Exception:
                    pass
                return ResultEnvelope(
                    action="cancel",
                    reason="a2a_deadline_exceeded",
                )

            # Race the poll interval against cancel.wait() so we exit fast.
            try:
                await asyncio.wait_for(cancel.wait(), timeout=self._poll_interval_s)
            except asyncio.TimeoutError:
                pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_target_id(target_id: str, default_owner: str) -> tuple[str, str]:
    """Return (agent_name, owner) from `<agent>@<owner>` or `<agent>`."""
    if "@" in target_id:
        agent_name, _, owner = target_id.partition("@")
        return agent_name or target_id, owner or default_owner
    return target_id, default_owner


def _new_a2a_id() -> str:
    return f"a2a_{secrets.token_hex(8)}"
