"""Slice 1 — pydantic models for the Invocation Contract (§4, §6, §7, §9).

These tests pin down the public shape of `Event`, `ResultEnvelope`, and
`InvocationRequest`, plus the MCP-elicitation-subset schema dialect.

No DB, no service, no HTTP — pure model tests. Fast.

Status: RED. The models do not exist yet. Implement
`krewhub/models/invocation.py` to make these pass.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError


# ---------------------------------------------------------------------------
# ResultEnvelope (contract §7)
# ---------------------------------------------------------------------------


def test_result_envelope_accept_requires_content():
    from krewhub.models.invocation import ResultEnvelope

    env = ResultEnvelope(action="accept", content="hi")
    assert env.action == "accept"
    assert env.content == "hi"
    assert env.reason is None


def test_result_envelope_accept_accepts_dict_content():
    from krewhub.models.invocation import ResultEnvelope

    env = ResultEnvelope(action="accept", content={"exit_code": 0, "tail": "ok"})
    assert env.content == {"exit_code": 0, "tail": "ok"}


def test_result_envelope_decline_with_reason():
    from krewhub.models.invocation import ResultEnvelope

    env = ResultEnvelope(action="decline", reason="operator_refused")
    assert env.action == "decline"
    assert env.content is None
    assert env.reason == "operator_refused"


def test_result_envelope_cancel_requires_reason():
    from krewhub.models.invocation import ResultEnvelope

    with pytest.raises(ValidationError):
        ResultEnvelope(action="cancel")  # reason required for cancel


def test_result_envelope_error_requires_reason():
    from krewhub.models.invocation import ResultEnvelope

    with pytest.raises(ValidationError):
        ResultEnvelope(action="error")  # reason required for error


def test_result_envelope_unknown_action_rejected():
    from krewhub.models.invocation import ResultEnvelope

    with pytest.raises(ValidationError):
        ResultEnvelope(action="success")  # not in {accept,decline,cancel,error}


def test_result_envelope_is_frozen():
    from krewhub.models.invocation import ResultEnvelope

    env = ResultEnvelope(action="accept", content="x")
    with pytest.raises(ValidationError):
        env.action = "decline"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Event schema (contract §6)
# ---------------------------------------------------------------------------


def test_event_minimal_construct():
    from krewhub.models.invocation import Event

    ev = Event(
        tape_id="tape_1",
        id=0,
        actor_type="system",
        actor_id="krewhub",
        kind="started",
        body="started",
        payload={"target": "fake", "deadline_ts": "2026-01-01T00:00:00Z"},
        ts=datetime.now(timezone.utc),
    )
    assert ev.id == 0
    assert ev.kind == "started"
    assert ev.parent_id is None
    assert ev.fork_id is None


def test_event_actor_type_enum():
    from krewhub.models.invocation import Event

    for actor in ("brain", "sandbox", "human", "system"):
        Event(
            tape_id="t", id=0, actor_type=actor, actor_id="x",
            kind="started", body="", payload={},
            ts=datetime.now(timezone.utc),
        )

    with pytest.raises(ValidationError):
        Event(
            tape_id="t", id=0, actor_type="agent", actor_id="x",
            kind="started", body="", payload={},
            ts=datetime.now(timezone.utc),
        )


def test_event_kind_closed_set():
    from krewhub.models.invocation import Event

    valid = {
        "started", "thinking", "tool_call", "tool_result", "reply",
        "output", "milestone", "elicit", "decision", "diff", "artifact",
        "fork", "handoff", "done",
    }
    for k in valid:
        Event(
            tape_id="t", id=0, actor_type="system", actor_id="x",
            kind=k, body="", payload={},
            ts=datetime.now(timezone.utc),
        )

    with pytest.raises(ValidationError):
        Event(
            tape_id="t", id=0, actor_type="system", actor_id="x",
            kind="invented_kind", body="", payload={},
            ts=datetime.now(timezone.utc),
        )


def test_event_is_frozen():
    from krewhub.models.invocation import Event

    ev = Event(
        tape_id="t", id=0, actor_type="system", actor_id="x",
        kind="started", body="", payload={},
        ts=datetime.now(timezone.utc),
    )
    with pytest.raises(ValidationError):
        ev.id = 1  # type: ignore[misc]


# ---------------------------------------------------------------------------
# InvocationRequest (contract §9)
# ---------------------------------------------------------------------------


def test_invocation_request_minimal():
    from krewhub.models.invocation import InvocationRequest

    req = InvocationRequest(target="fake:abc", input="hi")
    assert req.target == "fake:abc"
    assert req.input == "hi"
    assert req.deadline_s == 300  # contract default
    assert req.schema is None
    assert req.parent_tape_id is None


def test_invocation_request_dict_input():
    from krewhub.models.invocation import InvocationRequest

    req = InvocationRequest(target="fake:abc", input={"command": "ls", "args": ["-l"]})
    assert req.input == {"command": "ls", "args": ["-l"]}


def test_invocation_request_deadline_bounds():
    from krewhub.models.invocation import InvocationRequest

    InvocationRequest(target="fake", input="x", deadline_s=1)        # min
    InvocationRequest(target="fake", input="x", deadline_s=86_400)   # max (24h)
    with pytest.raises(ValidationError):
        InvocationRequest(target="fake", input="x", deadline_s=0)
    with pytest.raises(ValidationError):
        InvocationRequest(target="fake", input="x", deadline_s=86_401)


def test_invocation_request_no_parent_session_id():
    """Codex review removed `parent_session_id`; tape_id+fork_point is the
    complete parent address. Verify the field is gone."""
    from krewhub.models.invocation import InvocationRequest

    fields = InvocationRequest.model_fields
    assert "parent_session_id" not in fields
    assert "parent_tape_id" in fields
    assert "parent_fork_point" in fields


# ---------------------------------------------------------------------------
# Target parsing (contract §8)
# ---------------------------------------------------------------------------


def test_parse_target_human_no_id():
    from krewhub.models.invocation import parse_target

    target_type, target_id = parse_target("human")
    assert target_type == "human"
    assert target_id is None


def test_parse_target_sandbox_with_id():
    from krewhub.models.invocation import parse_target

    target_type, target_id = parse_target("sandbox:sbx_abc123")
    assert target_type == "sandbox"
    assert target_id == "sbx_abc123"


def test_parse_target_agent_with_id():
    from krewhub.models.invocation import parse_target

    target_type, target_id = parse_target("agent:claude@krew")
    assert target_type == "agent"
    assert target_id == "claude@krew"


def test_parse_target_human_with_id_rejected():
    from krewhub.models.invocation import parse_target

    with pytest.raises(ValueError, match="human accepts no id"):
        parse_target("human:somebody")


def test_parse_target_sandbox_without_id_rejected():
    from krewhub.models.invocation import parse_target

    with pytest.raises(ValueError, match="sandbox requires"):
        parse_target("sandbox")


def test_parse_target_agent_without_id_rejected():
    from krewhub.models.invocation import parse_target

    with pytest.raises(ValueError, match="agent requires"):
        parse_target("agent")


def test_parse_target_unknown_type_rejected():
    from krewhub.models.invocation import parse_target

    with pytest.raises(ValueError, match="unknown target type"):
        parse_target("workflow:abc")


# ---------------------------------------------------------------------------
# Schema dialect (MCP elicitation subset, contract §13.5)
# ---------------------------------------------------------------------------


def test_schema_dialect_accepts_string():
    from krewhub.models.invocation import validate_request_schema

    validate_request_schema({
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
    })


def test_schema_dialect_accepts_enum():
    from krewhub.models.invocation import validate_request_schema

    validate_request_schema({
        "type": "object",
        "properties": {
            "color": {"type": "string", "enum": ["red", "green", "blue"]},
        },
    })


def test_schema_dialect_accepts_number_with_bounds():
    from krewhub.models.invocation import validate_request_schema

    validate_request_schema({
        "type": "object",
        "properties": {
            "age": {"type": "number", "minimum": 0, "maximum": 150},
        },
    })


def test_schema_dialect_accepts_multi_select_enum():
    from krewhub.models.invocation import validate_request_schema

    validate_request_schema({
        "type": "object",
        "properties": {
            "colors": {
                "type": "array",
                "items": {"type": "string", "enum": ["red", "green", "blue"]},
                "minItems": 1,
            },
        },
    })


def test_schema_dialect_rejects_nested_object():
    from krewhub.models.invocation import validate_request_schema

    with pytest.raises(ValueError, match="nested"):
        validate_request_schema({
            "type": "object",
            "properties": {
                "address": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                },
            },
        })


def test_schema_dialect_rejects_array_of_objects():
    from krewhub.models.invocation import validate_request_schema

    with pytest.raises(ValueError):
        validate_request_schema({
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"k": {"type": "string"}},
                    },
                },
            },
        })


def test_schema_dialect_root_must_be_object():
    from krewhub.models.invocation import validate_request_schema

    with pytest.raises(ValueError):
        validate_request_schema({"type": "string"})  # root must be object


# ---------------------------------------------------------------------------
# Schema-against-content validation (contract §7)
# ---------------------------------------------------------------------------


def test_validate_content_against_schema_match():
    from krewhub.models.invocation import validate_content

    schema = {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
    }
    validate_content(schema, {"name": "octocat"})  # no raise


def test_validate_content_against_schema_missing_required():
    from krewhub.models.invocation import validate_content

    schema = {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
    }
    with pytest.raises(ValueError, match="schema_mismatch"):
        validate_content(schema, {})


def test_validate_content_against_schema_wrong_type():
    from krewhub.models.invocation import validate_content

    schema = {
        "type": "object",
        "properties": {"age": {"type": "number"}},
    }
    with pytest.raises(ValueError, match="schema_mismatch"):
        validate_content(schema, {"age": "not a number"})


def test_validate_content_enum_match():
    from krewhub.models.invocation import validate_content

    schema = {
        "type": "object",
        "properties": {"color": {"type": "string", "enum": ["red", "green"]}},
    }
    validate_content(schema, {"color": "red"})

    with pytest.raises(ValueError, match="schema_mismatch"):
        validate_content(schema, {"color": "purple"})
