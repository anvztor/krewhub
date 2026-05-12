"""InvocationService — orchestration shell around `Hand.execute()`.

Owns:
- Hand registry (target_type → Hand impl)
- Tape allocation + monotonic event ids (via InvocationEventRepo)
- Status transitions on the invocations row
- CancelToken plumbing
- HumanHand result-submission bridge (POST /invocations/:id/result)
- Schema validation of returned content (rewrite to error envelope on mismatch)
- Handoff event onto the parent tape on terminal

Hand authors implement only `Hand.execute()`. The service does the rest.
"""
from __future__ import annotations

import asyncio
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
import aiosqlite

from krewhub.invocations.protocol import Hand
from krewhub.models.invocation import (
    ActorType,
    Event,
    EventKind,
    Invocation,
    InvocationRequest,
    InvocationStatus,
    ResultEnvelope,
    parse_target,
    validate_content,
    validate_request_schema,
)
from krewhub.repositories.invocation_event_repo import InvocationEventRepo
from krewhub.repositories.invocation_repo import InvocationRepo
from krewhub.watch.service import WatchService
from krewhub.models import WatchEventType


# ---------------------------------------------------------------------------
# Cancel token + tape writer impls
# ---------------------------------------------------------------------------


class _CancelToken:
    def __init__(self) -> None:
        self.cancelled: bool = False
        self.reason: str | None = None
        self._event = asyncio.Event()

    async def wait(self) -> None:
        await self._event.wait()

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise asyncio.CancelledError(self.reason or "cancelled")

    def fire(self, reason: str) -> None:
        if self.cancelled:
            return
        self.cancelled = True
        self.reason = reason
        self._event.set()


class _TapeWriter:
    """Writes events to the invocation_events table and fans out via
    WatchService so SSE subscribers see them in real time."""

    def __init__(
        self,
        events: InvocationEventRepo,
        watch: WatchService,
        tape_id: str,
        *,
        recipe_id: str | None = None,
        invocation_id: str | None = None,
    ) -> None:
        self._events = events
        self._watch = watch
        self.tape_id = tape_id
        self._recipe_id = recipe_id
        self._invocation_id = invocation_id

    async def append(
        self,
        kind: EventKind,
        *,
        body: str = "",
        payload: dict | None = None,
        actor_type: ActorType = "system",
        actor_id: str = "",
        parent_id: int | None = None,
    ) -> Event:
        ev = await self._events.append(
            self.tape_id, kind,
            body=body, payload=payload, actor_type=actor_type,
            actor_id=actor_id, parent_id=parent_id,
        )
        # Fan out via WatchService so SSE subscribers see it. We tag the
        # watch envelope with `invocation_id` so frontend listeners can
        # resolve the tape → invocation without an extra round-trip.
        await self._watch.record(
            resource_type="invocation",
            resource_id=ev.tape_id,
            event_type=WatchEventType.MODIFIED,
            resource_version=ev.id,
            payload={
                "kind": ev.kind,
                "tape_id": ev.tape_id,
                "invocation_id": self._invocation_id,
                "id": ev.id,
                "actor_type": ev.actor_type,
                "actor_id": ev.actor_id,
                "body": ev.body,
                "payload": ev.payload,
                "ts": ev.ts.isoformat(),
            },
        )
        return ev


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


@dataclass
class _Run:
    cancel: _CancelToken
    task: asyncio.Task
    # For HumanHand: external submit_result delivers an envelope that the
    # blocking Hand picks up via this future.
    external_result: asyncio.Future[ResultEnvelope]


class _ConflictError(Exception):
    """Raised when submit_result is called on a terminal invocation."""


class InvocationService:
    def __init__(
        self,
        db: aiosqlite.Connection,
        hands: dict[str, Hand],
        watch: WatchService,
    ) -> None:
        self._db = db
        self._hands = dict(hands)
        self._watch = watch
        self._invocations = InvocationRepo(db)
        self._events = InvocationEventRepo(db)
        self._runs: dict[str, _Run] = {}

    # -------- public API --------

    async def create(
        self,
        req: InvocationRequest,
        *,
        caller_account_id: str,
    ) -> Invocation:
        # 1. Parse target — widen the allowed set to whatever Hands the
        #    service actually has registered. The route layer should
        #    have already enforced the contract's closed set; the service
        #    is permissive about test doubles.
        target_type, target_id = parse_target(
            req.target,
            allowed_types=frozenset(self._hands.keys()) | {"human", "sandbox", "agent"},
        )

        # 2. Validate schema dialect (raises ValueError on bad shape)
        if req.schema is not None:
            validate_request_schema(req.schema)

        # 3. Idempotency check
        if req.parent_tape_id and req.idempotency_key:
            existing = await self._invocations.find_by_idempotency_key(
                req.parent_tape_id, req.idempotency_key,
            )
            if existing is not None:
                return existing

        # 4. Allocate ids + persist row
        now = datetime.now(timezone.utc)
        invocation = Invocation(
            id=_new_id("inv_"),
            target_type=target_type,
            target_id=target_id,
            input=req.input,
            schema=req.schema,
            deadline_s=req.deadline_s,
            label=req.label,
            parent_tape_id=req.parent_tape_id,
            parent_fork_point=req.parent_fork_point,
            idempotency_key=req.idempotency_key,
            tape_id=_new_id("tape_"),
            status="pending",
            result=None,
            created_at=now,
            started_at=None,
            completed_at=None,
            created_by=caller_account_id,
        )
        await self._invocations.create(invocation)

        # 5. Register the fork (if this is a child invocation)
        if req.parent_tape_id is not None and req.parent_fork_point is not None:
            await self._events.register_fork(
                child_tape_id=invocation.tape_id,
                parent_tape_id=req.parent_tape_id,
                fork_point_event_id=req.parent_fork_point,
            )

        # 6. Mark running + write `started` synchronously so the
        # response carries status='running' and the SSE stream's
        # backlog includes the opening event.
        await self._invocations.set_running(invocation.id, now)
        invocation = invocation.model_copy(update={"status": "running", "started_at": now})

        # 7. Schedule the Hand work
        cancel = _CancelToken()
        external_result: asyncio.Future[ResultEnvelope] = asyncio.get_event_loop().create_future()
        task = asyncio.create_task(
            self._run(invocation, cancel, external_result, req.recipe_id),
            name=f"invocation:{invocation.id}",
        )
        self._runs[invocation.id] = _Run(
            cancel=cancel, task=task, external_result=external_result,
        )

        return invocation

    async def get(self, invocation_id: str) -> Invocation | None:
        return await self._invocations.get(invocation_id)

    async def list_events(
        self,
        tape_id: str,
        *,
        after: int | None = None,
        limit: int | None = None,
    ) -> list[Event]:
        return await self._events.list_for_tape(tape_id, after=after, limit=limit)

    async def submit_result(
        self,
        invocation_id: str,
        result: ResultEnvelope,
    ) -> Event:
        """Deliver an externally-submitted ResultEnvelope to a running
        HumanHand. Raises _ConflictError if the invocation is terminal.

        The Hand is blocked on `cancel.wait()` and `external_result`; we
        set the future, the Hand returns the envelope, and the service
        completes the tape normally.
        """
        inv = await self._invocations.get(invocation_id)
        if inv is None:
            raise KeyError(invocation_id)
        if inv.status in ("completed", "cancelled", "errored"):
            raise _ConflictError(
                f"invocation {invocation_id} already terminal ({inv.status})",
            )
        run = self._runs.get(invocation_id)
        if run is None:
            raise _ConflictError(
                f"invocation {invocation_id} has no active runner",
            )

        # On a human invocation, write the `decision` event onto the tape
        # BEFORE signaling the Hand. This guarantees the canonical event
        # order: started → elicit → decision → done (contract §6).
        if inv.target_type == "human":
            tape_writer = _TapeWriter(
                self._events, self._watch, inv.tape_id,
                invocation_id=inv.id,
            )
            await tape_writer.append(
                "decision",
                body=f"decision · {result.action}",
                payload=result.model_dump(mode="json"),
                actor_type="human",
                actor_id=inv.created_by,
            )

        if not run.external_result.done():
            run.external_result.set_result(result)
        # Wait briefly for the Hand to pick it up and the run to finish
        await self.wait_for_terminal(invocation_id, timeout=2.0)
        # Return the terminal `done` event for the caller's convenience.
        events = await self._events.list_for_tape(inv.tape_id)
        return events[-1]

    async def cancel(
        self,
        invocation_id: str,
        reason: str = "operator_cancelled",
    ) -> None:
        inv = await self._invocations.get(invocation_id)
        if inv is None:
            return
        if inv.status in ("completed", "cancelled", "errored"):
            return
        run = self._runs.get(invocation_id)
        if run is None:
            return
        run.cancel.fire(reason)

    async def wait_for_terminal(
        self,
        invocation_id: str,
        *,
        timeout: float = 5.0,
    ) -> Invocation:
        run = self._runs.get(invocation_id)
        if run is not None:
            try:
                await asyncio.wait_for(asyncio.shield(run.task), timeout=timeout)
            except asyncio.TimeoutError:
                pass
        inv = await self._invocations.get(invocation_id)
        assert inv is not None
        return inv

    # -------- internals --------

    async def _run(
        self,
        invocation: Invocation,
        cancel: _CancelToken,
        external_result: asyncio.Future[ResultEnvelope],
        recipe_id: str | None = None,
    ) -> None:
        tape = _TapeWriter(
            self._events, self._watch, invocation.tape_id,
            invocation_id=invocation.id,
        )

        # Write `started` (status='running' was set by create()).
        await tape.append(
            "started",
            body=f"started → {invocation.target_type}",
            payload={
                "target": invocation.target_type
                + (f":{invocation.target_id}" if invocation.target_id else ""),
                "deadline_ts": _deadline_ts(invocation.deadline_s),
            },
            actor_type="system",
            actor_id="krewhub",
        )

        # Resolve hand
        hand = self._hands.get(invocation.target_type)
        if hand is None:
            await self._finalize(
                invocation,
                ResultEnvelope(
                    action="error",
                    reason=f"no_hand_registered: target_type={invocation.target_type!r}",
                ),
                tape,
            )
            return

        # For HumanHand-style Hands, expose the external_result via a
        # wrapped CancelToken so the Hand can await both "operator
        # submitted" and "operator cancelled" together.
        bridged = _ExternalAwareCancel(cancel, external_result)

        try:
            result = await asyncio.wait_for(
                hand.execute(
                    target_id=invocation.target_id,
                    input=invocation.input,
                    schema=invocation.schema,
                    deadline_s=invocation.deadline_s,
                    tape=tape,
                    cancel=bridged,
                ),
                timeout=float(invocation.deadline_s),
            )
        except asyncio.TimeoutError:
            result = ResultEnvelope(
                action="cancel", reason="deadline_exceeded",
            )
        except asyncio.CancelledError:
            result = ResultEnvelope(
                action="cancel",
                reason=cancel.reason or "operator_cancelled",
            )
        except Exception as exc:
            result = ResultEnvelope(
                action="error", reason=f"hand_crash: {exc}",
            )

        # If the bridged cancel saw an external_result first, that
        # envelope wins over the Hand's natural return.
        if external_result.done() and not external_result.cancelled():
            try:
                submitted = external_result.result()
                result = submitted
            except Exception:
                pass

        # Validate against schema if accept + schema present
        if (
            result.action == "accept"
            and invocation.schema is not None
            and result.content is not None
            and not isinstance(result.content, str)
        ):
            try:
                validate_content(invocation.schema, result.content)
            except ValueError as exc:
                result = ResultEnvelope(action="error", reason=str(exc))
        elif (
            result.action == "accept"
            and invocation.schema is not None
            and isinstance(result.content, str)
        ):
            # String content with a schema requested → schema_mismatch
            result = ResultEnvelope(
                action="error",
                reason="schema_mismatch: expected object content",
            )

        await self._finalize(invocation, result, tape)

    async def _finalize(
        self,
        invocation: Invocation,
        result: ResultEnvelope,
        tape: _TapeWriter,
    ) -> None:
        # Append `done`
        await tape.append(
            "done",
            body=f"done · {result.action}",
            payload={"result": result.model_dump(mode="json")},
            actor_type="system",
            actor_id="krewhub",
        )

        # Status row
        status = _action_to_status(result.action)
        await self._invocations.set_terminal(
            invocation.id, status, result, datetime.now(timezone.utc),
        )

        # Handoff onto parent tape
        if invocation.parent_tape_id is not None:
            parent_writer = _TapeWriter(
                self._events, self._watch, invocation.parent_tape_id,
            )
            await parent_writer.append(
                "handoff",
                body=f"handoff ← {invocation.target_type}",
                payload={
                    "from_fork": invocation.tape_id,
                    "result": result.model_dump(mode="json"),
                },
                actor_type="system",
                actor_id="krewhub",
            )

        # Drop runner
        self._runs.pop(invocation.id, None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _ExternalAwareCancel:
    """A CancelToken adapter that resolves either when cancel fires or
    when an external result has been submitted (HumanHand pattern)."""

    def __init__(
        self,
        inner: _CancelToken,
        external: asyncio.Future[ResultEnvelope],
    ) -> None:
        self._inner = inner
        self._external = external

    @property
    def cancelled(self) -> bool:
        return self._inner.cancelled

    @property
    def reason(self) -> str | None:
        return self._inner.reason

    async def wait(self) -> None:
        async def _await_external() -> None:
            try:
                await asyncio.shield(self._external)
            except asyncio.CancelledError:
                pass

        cancel_task = asyncio.create_task(self._inner.wait())
        external_task = asyncio.create_task(_await_external())
        try:
            await asyncio.wait(
                {cancel_task, external_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for t in (cancel_task, external_task):
                if not t.done():
                    t.cancel()

    def raise_if_cancelled(self) -> None:
        if self._inner.cancelled:
            raise asyncio.CancelledError(self._inner.reason or "cancelled")


def _action_to_status(action: str) -> InvocationStatus:
    if action in ("accept", "decline"):
        return "completed"
    if action == "cancel":
        return "cancelled"
    return "errored"


def _new_id(prefix: str) -> str:
    return f"{prefix}{secrets.token_hex(6)}"


def _deadline_ts(deadline_s: int) -> str:
    from datetime import timedelta
    return (
        datetime.now(timezone.utc) + timedelta(seconds=deadline_s)
    ).isoformat()
