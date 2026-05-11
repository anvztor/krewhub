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
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal

from krewhub.invocations.protocol import CancelToken, TapeWriter
from krewhub.models.invocation import ResultEnvelope


class HumanHand:
    target_type: Literal["sandbox", "agent", "human"] = "human"

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
