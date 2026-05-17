"""HumanHand — bridges the agent to the human operator.

Implements the `Hand` Protocol from `krewhub.invocations.protocol`.

Flow (contract §10.2):
1. `execute()` emits ONE `elicit` event carrying message + schema +
   deadline_ts.
2. `execute()` blocks on `cancel.wait()`. The InvocationService wraps
   `cancel` with `_ExternalAwareCancel`, so wait() resolves on EITHER:
     - operator pressed Cancel       → `cancel.cancelled = True`
     - operator submitted a result   → service's `external_result` set
3. If `cancel.cancelled`, the Hand returns a cancel envelope.
   Otherwise the service overrides the Hand's return value with the
   operator-submitted envelope.
4. The InvocationService writes the `decision` event (in `submit_result`)
   and the terminal `done` event.

The Hand never writes `decision` itself — that's the service's job,
written atomically with the operator's submission so the tape order is
always: started → elicit → decision → done.

This is the *human-as-MCP-tool* implementation that fills the gap left
by Anthropic Managed Agents' Session/Harness/Sandbox triple. From the
brain's POV it's just a `delegate(to="human", ...)` tool call.

Auth Phase 0: when `db` is provided at construction, HumanHand will
also write a durable `elicits` row for every `op:auth_required` elicit
so the credential-relay endpoint can atomically reserve and forward it.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from krewhub.invocations.protocol import CancelToken, TapeWriter
from krewhub.models.invocation import ResultEnvelope

logger = logging.getLogger(__name__)


class HumanHand:
    target_type: Literal["sandbox", "agent", "human"] = "human"

    def __init__(self, db: Any | None = None) -> None:
        # Optional db connection. When provided, auth_required ops also
        # write a durable elicit row for credential-relay reservation.
        self._db = db

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
        # Contract §8: human accepts no id.
        if target_id is not None:
            return ResultEnvelope(
                action="error",
                reason="bad_target: human accepts no id",
            )

        # Two delegate shapes land here:
        #  - Free-form: input is a string OR {message: "..."}.
        #  - Structured op: input is {op: "auth_required", host: "...", ...}.
        # The structured form lets cookrew-web render a typed card (e.g.
        # AuthRequiredCard for paste-PAT or OAuth-launch) instead of a
        # generic textbox. Brain's DELEGATE_SYSTEM_NOTE tells it to use
        # the structured form on auth failures.
        structured = _parse_structured_op(input)
        message = _parse_human_input(input)
        if structured is None and not message:
            return ResultEnvelope(
                action="error",
                reason="empty_message: HumanHand input must contain a non-empty message or structured op",
            )

        # For structured ops with no explicit message, synthesize one so
        # the tape body remains human-readable.
        if structured is not None and not message:
            message = _synthesize_message(structured)

        deadline_ts = (
            datetime.now(timezone.utc) + timedelta(seconds=deadline_s)
        ).isoformat()

        payload: dict = {
            "message": message,
            "schema": schema,
            "deadline_ts": deadline_ts,
        }
        if structured is not None:
            payload["op"] = structured.get("op")
            for k in ("host", "reason", "env_var_name", "hint"):
                if k in structured:
                    payload[k] = structured[k]

        await tape.append(
            "elicit",
            body=message[:200],
            payload=payload,
            actor_type="human",
            actor_id="operator",
        )

        # Auth Phase 0: for op:auth_required elicits, also write a durable
        # elicit row so the credential-relay endpoint can atomically reserve
        # and forward it. The row must be written AFTER the tape event so
        # that the SPA's /invocations/{id}/task endpoint always finds a row
        # if it sees an elicit event. Best-effort: a failure here does NOT
        # block the operator flow — the existing HITL still works.
        if structured is not None and structured.get("op") == "auth_required" and self._db is not None:
            try:
                await self._write_elicit_row(tape, structured)
            except Exception:
                logger.exception(
                    "HumanHand: failed to write elicit row for tape %s; "
                    "credential-relay will be unavailable for this elicit",
                    tape.tape_id,
                )

        # Wait for one of:
        # - cancel.fire()          → operator dismissed / service deadline
        # - external_result set    → operator submitted via /result endpoint
        await cancel.wait()

        if cancel.cancelled:
            return ResultEnvelope(
                action="cancel",
                reason=cancel.reason or "operator_cancelled",
            )

        # external_result was set; the InvocationService overrides our
        # return value with that envelope. The placeholder below would
        # only ship if the override silently fails.
        return ResultEnvelope(
            action="cancel",
            reason="operator_submission_pending_capture",
        )

    async def _write_elicit_row(self, tape: TapeWriter, structured: dict) -> None:
        """Write a durable elicit row for credential-relay reservation.

        Looks up the invocation_id from the invocations table using the
        tape_id, then inserts an elicits row. Idempotent: ON CONFLICT DO
        NOTHING prevents duplicate rows on retry.
        """
        from uuid import uuid4

        from krewhub.repositories.elicit_repo import ElicitRepo, ElicitRow

        # Look up the invocation_id for this tape.
        cur = await self._db.execute(
            "SELECT id FROM invocations WHERE tape_id = ?",
            (tape.tape_id,),
        )
        row = await cur.fetchone()
        if row is None:
            logger.warning(
                "HumanHand._write_elicit_row: no invocation for tape_id=%s",
                tape.tape_id,
            )
            return

        invocation_id = row[0]
        elicit_id = f"el_{uuid4().hex[:16]}"

        # Build payload from the structured op dict (drop the op key itself
        # since it's stored in the `op` column).
        payload_data = {k: v for k, v in structured.items() if k != "op"}

        await ElicitRepo(self._db).put(ElicitRow(
            id=elicit_id,
            invocation_id=invocation_id,
            op="auth_required",
            payload_json=json.dumps(payload_data),
            status="pending",
        ))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_human_input(input: str | dict) -> str:
    if isinstance(input, str):
        return input.strip()
    if isinstance(input, dict):
        msg = input.get("message")
        if isinstance(msg, str):
            return msg.strip()
    return ""


# Allow-list of structured ops the brain can issue via delegate(to: "human").
# Adding a new op MUST be paired with cookrew-web rendering logic — otherwise
# the operator gets a textbox that can't actually resolve the credential
# need.
_KNOWN_HUMAN_OPS: frozenset[str] = frozenset({
    "auth_required",   # paste a credential / launch OAuth ceremony
})


def _parse_structured_op(input: str | dict) -> dict | None:
    """Return the structured op dict iff input has a recognized `op`."""
    if not isinstance(input, dict):
        return None
    op = input.get("op")
    if not isinstance(op, str) or op not in _KNOWN_HUMAN_OPS:
        return None
    return input


def _synthesize_message(structured: dict) -> str:
    """Default tape-body text for a structured op when no message is set."""
    op = structured.get("op")
    host = structured.get("host", "")
    reason = structured.get("reason", "")
    if op == "auth_required":
        base = f"Authentication needed for {host}".strip()
        if reason:
            return f"{base} — {reason}"
        return base or "Authentication needed"
    return "Operator action requested"
