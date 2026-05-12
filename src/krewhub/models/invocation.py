"""Invocation Contract — pydantic models + helpers (slice 1).

Implements the normative shapes from `docs/INVOCATION-CONTRACT.md` §6,
§7, §8, §9: `Event`, `ResultEnvelope`, `InvocationRequest`, `Invocation`,
plus `parse_target()` and the MCP-elicitation-subset schema dialect
helpers `validate_request_schema()` and `validate_content()`.

Kept in its own module so it does not collide with the existing
`krewhub.models.domain.Event` (which models a different, legacy
event-stream concept). Import paths are explicit on the consumer side.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


# ---------------------------------------------------------------------------
# Closed enums (Literals — surfaced through pydantic for ValidationError)
# ---------------------------------------------------------------------------


ActorType = Literal["brain", "sandbox", "human", "system"]
"""Whom an event came from. `agent` is intentionally absent (contract §6
constraint): brain covers all model cognition events, including sub-agent
runs; the runtime identity lives in `actor_id`. `agent` is a target type,
not an actor type."""


EventKind = Literal[
    # Lifecycle
    "started",
    "done",
    # Brain (cognition)
    "thinking",
    "tool_call",
    "tool_result",
    "reply",
    # Sandbox
    "output",
    "diff",
    "artifact",
    # Human
    "elicit",
    "decision",
    # System / cross-fork
    "milestone",
    "fork",
    "handoff",
]


Action = Literal["accept", "decline", "cancel", "error", "pending"]
"""ResultEnvelope.action — borrowed from MCP elicitation, extended with
`error` for execution failures (contract §7) and `pending` for non-
blocking delegate semantics: the bridge returns `pending` to the brain
when the operator hasn't answered within the short poll window so the
brain can end its turn cleanly; the answer arrives on the task tape
and is threaded into the next prompt by `_build_prompt_with_context`."""


TargetType = Literal["sandbox", "agent", "human"]


# ---------------------------------------------------------------------------
# ResultEnvelope (contract §7)
# ---------------------------------------------------------------------------


class ResultEnvelope(BaseModel):
    """The terminal value of an Invocation.

    Reason rules:
    - accept  → content required (string or schema-shaped dict); reason n/a
    - decline → content typically null; reason recommended
    - cancel  → content typically null; reason REQUIRED
    - error   → content typically null; reason REQUIRED
    - pending → content typically `{invocation_id: str}`; reason recommended
                (non-terminal — the brain should end its turn; the
                operator's answer arrives later on the task tape)
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    action: Action
    content: str | dict | None = None
    reason: str | None = None

    @model_validator(mode="after")
    def _check_reason_for_terminal_failures(self) -> "ResultEnvelope":
        if self.action in ("cancel", "error") and not self.reason:
            raise ValueError(f"reason is required when action='{self.action}'")
        return self


# ---------------------------------------------------------------------------
# Event (contract §6)
# ---------------------------------------------------------------------------


class Event(BaseModel):
    """One immutable entry on an invocation tape."""

    model_config = ConfigDict(frozen=True)

    tape_id: str
    id: int
    parent_id: int | None = None
    fork_id: str | None = None
    actor_type: ActorType
    actor_id: str
    kind: EventKind
    body: str = ""
    payload: dict = Field(default_factory=dict)
    ts: datetime


# ---------------------------------------------------------------------------
# InvocationRequest (contract §9)
# ---------------------------------------------------------------------------


class InvocationRequest(BaseModel):
    """Body of POST /api/v1/invocations.

    `target` is the colon-prefixed string `<type>` or `<type>:<id>` —
    parse with :func:`parse_target` to split. Validation of the format
    happens at the service layer so a malformed `target` becomes a 400
    rather than a 422 (kept out of pydantic on purpose).
    """

    model_config = ConfigDict(frozen=True)

    target: str
    input: str | dict
    schema: dict | None = None
    deadline_s: int = Field(default=300, ge=1, le=86_400)
    label: str | None = Field(default=None, max_length=60)
    parent_tape_id: str | None = None
    parent_fork_point: int | None = None
    idempotency_key: str | None = None
    # Optional bundle scoping — required when `target == "sandbox"`
    # (bare). The route resolves bare sandbox to the bundle's current
    # ready sandbox via SandboxService.ensure_sandbox_for_bundle,
    # provisioning if missing or terminated. Eliminates the
    # `no_sandbox_attached` failure path; the brain never sees
    # substrate state.
    bundle_id: str | None = None
    # Optional task scoping — set when the brain invokes delegate(human)
    # while running a task. The result projection (POST /result) uses
    # this to append a `delegate_answer` event onto the task's events
    # tape so the next prompt-build can thread the operator's answer as
    # a HUMAN turn for the brain's re-entry. Not enforced for non-human
    # targets, but harmless to pass through for any invocation.
    task_id: str | None = None


# ---------------------------------------------------------------------------
# Invocation (full server-side state)
# ---------------------------------------------------------------------------


InvocationStatus = Literal[
    "pending", "running", "completed", "cancelled", "errored",
]


class Invocation(BaseModel):
    """Server-side row representation. Returned by GET /:id.

    `target_type` is a free string at the service layer so test doubles
    and future Hand types can be registered without a schema bump. The
    HTTP route layer enforces the contract's closed set via
    `parse_target()` (sandbox / agent / human).
    """

    model_config = ConfigDict(frozen=True)

    id: str
    target_type: str
    target_id: str | None
    input: str | dict
    schema: dict | None = None
    deadline_s: int
    label: str | None = None
    parent_tape_id: str | None = None
    parent_fork_point: int | None = None
    idempotency_key: str | None = None
    tape_id: str
    task_id: str | None = None
    status: InvocationStatus
    result: ResultEnvelope | None = None
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    created_by: str


# ---------------------------------------------------------------------------
# Target parsing (contract §8)
# ---------------------------------------------------------------------------


_VALID_TARGET_TYPES: frozenset[str] = frozenset({"sandbox", "agent", "human"})


def parse_target(
    target: str,
    *,
    allowed_types: frozenset[str] | None = None,
) -> tuple[TargetType, str | None]:
    """Parse a target string into (type, id).

    Format: `<type>` or `<type>:<id>`.
    - `human` accepts no id.
    - All other types require one.

    By default, only the contract types `sandbox`, `agent`, `human` are
    allowed. Callers (e.g. the InvocationService, which carries its own
    Hand registry, or tests using a fake target type) may pass
    `allowed_types` to widen the set.

    Raises ValueError with a stable prefix the route layer can grep:
    - "unknown target type: ..."
    - "human accepts no id"
    - "sandbox requires an id"
    - "agent requires an id"
    """
    if not isinstance(target, str) or not target:
        raise ValueError("target must be a non-empty string")

    if ":" in target:
        type_part, _, id_part = target.partition(":")
        target_id: str | None = id_part or None
    else:
        type_part = target
        target_id = None

    valid = allowed_types if allowed_types is not None else _VALID_TARGET_TYPES
    if type_part not in valid:
        raise ValueError(f"unknown target type: {type_part!r}")

    if type_part == "human":
        if target_id is not None:
            raise ValueError("human accepts no id")
        return "human", None  # type: ignore[return-value]

    # All non-human types require an id (per contract §8).
    if not target_id:
        raise ValueError(f"{type_part} requires an id")
    return type_part, target_id  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Schema dialect — MCP elicitation subset (contract §13.5)
# ---------------------------------------------------------------------------
#
# Only the subset documented at
# https://modelcontextprotocol.io/specification/draft/client/elicitation
# is allowed:
#   - root: object with primitive properties
#   - properties: string | number | integer | boolean | enum (single)
#                 | array of (primitive enum) for multi-select
#   - NO nested objects, NO arrays of objects
#
# This keeps the cookrew popout's auto-form simple. Lift the restriction
# only when there's a concrete UI need.

_PRIMITIVE_TYPES: frozenset[str] = frozenset({
    "string", "number", "integer", "boolean",
})


def validate_request_schema(schema: dict) -> None:
    """Reject schemas outside the MCP elicitation subset.

    Raises ValueError with a descriptive message. The route layer
    catches this and returns 400.
    """
    if not isinstance(schema, dict):
        raise ValueError("schema must be a JSON object")

    if schema.get("type") != "object":
        raise ValueError("schema root must be an object")

    properties = schema.get("properties")
    if properties is not None and not isinstance(properties, dict):
        raise ValueError("schema.properties must be an object")

    for prop_name, prop_schema in (properties or {}).items():
        _validate_property_schema(prop_name, prop_schema)


def _validate_property_schema(name: str, prop: Any) -> None:
    if not isinstance(prop, dict):
        raise ValueError(f"property {name!r} must be an object schema")

    # enum-with-titles via oneOf is acceptable as long as each branch is
    # primitive (`{const: ..., title: ...}` shape).
    if "oneOf" in prop:
        for branch in prop["oneOf"]:
            if not isinstance(branch, dict) or "const" not in branch:
                raise ValueError(
                    f"property {name!r}: oneOf branches must be {{const, title}}",
                )
        return

    t = prop.get("type")
    if t in _PRIMITIVE_TYPES:
        return

    if t == "array":
        items = prop.get("items")
        if not isinstance(items, dict):
            raise ValueError(f"property {name!r}: array items must be an object schema")
        # Multi-select: items is primitive (with optional enum) OR an
        # anyOf of {const, title} branches.
        if "anyOf" in items:
            for branch in items["anyOf"]:
                if not isinstance(branch, dict) or "const" not in branch:
                    raise ValueError(
                        f"property {name!r}: array items.anyOf branches must be {{const, title}}",
                    )
            return
        items_type = items.get("type")
        if items_type == "object":
            raise ValueError(
                f"property {name!r}: array of objects is not supported",
            )
        if items_type not in _PRIMITIVE_TYPES:
            raise ValueError(
                f"property {name!r}: array items must be a primitive type",
            )
        return

    if t == "object":
        raise ValueError(
            f"property {name!r}: nested objects are not supported in this dialect",
        )

    raise ValueError(f"property {name!r}: unsupported type {t!r}")


def validate_content(schema: dict, content: Any) -> None:
    """Validate `content` against the request `schema`.

    On mismatch raises ValueError with message prefixed `schema_mismatch:`
    so the InvocationService can rewrite the envelope to
    `action="error", reason=<message>`.
    """
    if schema.get("type") != "object":
        raise ValueError(
            "schema_mismatch: schema root must be 'object' (rejected upstream)",
        )
    if not isinstance(content, dict):
        raise ValueError(
            f"schema_mismatch: content must be an object, got {type(content).__name__}",
        )

    properties: dict = schema.get("properties") or {}
    required: list = list(schema.get("required") or [])

    for req in required:
        if req not in content:
            raise ValueError(f"schema_mismatch: missing required field {req!r}")

    for key, value in content.items():
        if key not in properties:
            # Be lenient on extra keys — MCP doesn't forbid them.
            continue
        _check_value(properties[key], key, value)


def _check_value(prop: dict, key: str, value: Any) -> None:
    # oneOf with const branches → membership check
    if "oneOf" in prop:
        consts = [b.get("const") for b in prop["oneOf"]]
        if value not in consts:
            raise ValueError(
                f"schema_mismatch: {key!r}={value!r} not in oneOf {consts}",
            )
        return

    t = prop.get("type")

    if t == "string":
        if not isinstance(value, str):
            raise ValueError(
                f"schema_mismatch: {key!r} expected string, got {type(value).__name__}",
            )
        if "enum" in prop and value not in prop["enum"]:
            raise ValueError(
                f"schema_mismatch: {key!r}={value!r} not in enum {prop['enum']}",
            )
        return

    if t == "number":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValueError(
                f"schema_mismatch: {key!r} expected number, got {type(value).__name__}",
            )
        return

    if t == "integer":
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(
                f"schema_mismatch: {key!r} expected integer, got {type(value).__name__}",
            )
        return

    if t == "boolean":
        if not isinstance(value, bool):
            raise ValueError(
                f"schema_mismatch: {key!r} expected boolean, got {type(value).__name__}",
            )
        return

    if t == "array":
        if not isinstance(value, list):
            raise ValueError(
                f"schema_mismatch: {key!r} expected array, got {type(value).__name__}",
            )
        items = prop.get("items") or {}
        if "anyOf" in items:
            consts = [b.get("const") for b in items["anyOf"]]
            for v in value:
                if v not in consts:
                    raise ValueError(
                        f"schema_mismatch: {key!r} item {v!r} not in oneOf {consts}",
                    )
        else:
            item_type = items.get("type")
            for v in value:
                _check_value({"type": item_type, **(items or {})}, f"{key}[]", v)
        return

    raise ValueError(
        f"schema_mismatch: {key!r} unsupported schema type {t!r}",
    )
