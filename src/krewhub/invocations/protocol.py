"""Invocation Contract — Hand / TapeWriter / CancelToken Protocols.

The Hand surface is intentionally minimal: one async method,
`execute(target_id, input, schema, deadline_s, tape, cancel) -> ResultEnvelope`.

All orchestration concerns (event ordering, fork parentage, cancellation
propagation, SSE fan-out) belong to `InvocationService`, not to individual
Hand implementations.

Aligned with Anthropic Managed Agents' `execute(name, input) -> string`
primitive (https://www.anthropic.com/engineering/managed-agents).
The return widens to `ResultEnvelope` so accept/decline/cancel/error +
structured `content` are first-class.
"""
from __future__ import annotations

from typing import Literal, Protocol, runtime_checkable

from krewhub.models.invocation import ActorType, Event, EventKind, ResultEnvelope


@runtime_checkable
class TapeWriter(Protocol):
    """Provided to `Hand.execute()`. The Hand writes events here; the
    service handles persistence, monotonic ids, and watch fan-out."""

    tape_id: str

    async def append(
        self,
        kind: EventKind,
        *,
        body: str = "",
        payload: dict | None = None,
        actor_type: ActorType = "system",
        actor_id: str = "",
        parent_id: int | None = None,
    ) -> Event: ...


@runtime_checkable
class CancelToken(Protocol):
    """Cooperative cancellation. Hands MUST honor this either by polling
    `cancelled` between long ops, or by `await cancel.wait()` in a parallel
    task. On cancellation, return `ResultEnvelope(action="cancel", ...)` —
    do NOT raise."""

    cancelled: bool
    reason: str | None

    async def wait(self) -> None:
        """Resolves when cancel fires; never resolves otherwise."""
        ...

    def raise_if_cancelled(self) -> None:
        """Raise asyncio.CancelledError if cancellation has fired."""
        ...


@runtime_checkable
class Hand(Protocol):
    """The single execute() primitive shared by sandbox / agent / human.

    Three rules for implementers:
    1. Return the terminal `ResultEnvelope`. The service writes the
       `done` event for you using the returned envelope; do NOT append
       `done` yourself.
    2. Stream progress via `tape.append(...)`. Each Hand emits its own
       subset of `EventKind` (see contract §6).
    3. Honor `cancel`. Polling or `await cancel.wait()` is fine.
       On cancellation, return `ResultEnvelope(action="cancel",
       reason="operator_cancelled")`; don't raise.
    """

    target_type: Literal["sandbox", "agent", "human"]

    async def execute(
        self,
        *,
        target_id: str | None,
        input: str | dict,
        schema: dict | None,
        deadline_s: int,
        tape: TapeWriter,
        cancel: CancelToken,
    ) -> ResultEnvelope: ...
