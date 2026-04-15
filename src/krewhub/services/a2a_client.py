"""Unified A2A JSON-RPC client for hub gateway dispatch.

All A2A message/send calls to the hub gateway should go through this
module. It centralises URL resolution, JSON-RPC envelope building, and
response parsing so changes to the protocol only touch one place.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from krewhub.models import AgentPresence, Task

logger = logging.getLogger(__name__)

_HUB_BASE = "http://127.0.0.1:8420"
_ACCEPTED_STATES: frozenset[str] = frozenset({"submitted", "working", "completed"})


@dataclass(frozen=True)
class A2AResult:
    """Parsed result of an A2A JSON-RPC call."""

    accepted: bool
    task_id: str | None = None
    state: str = ""
    error: str | None = None


# ---------------------------------------------------------------------------
# URL resolution
# ---------------------------------------------------------------------------


def resolve_hub_url(agent: "AgentPresence") -> str:
    """Build the hub gateway URL for an agent.

    The pattern ``{owner}/{agent_short_name}`` is shared across all
    dispatch paths (task_dispatch, planner_dispatch, graph_runtime).
    """
    owner = agent.owner_username or agent.agent_id.split("@")[-1]
    agent_short = agent.agent_id.split("@")[0]
    return f"{_HUB_BASE}/a2a/{owner}/{agent_short}"


# ---------------------------------------------------------------------------
# Payload building
# ---------------------------------------------------------------------------


def build_a2a_payload(
    req_id: str,
    prompt: str,
    metadata: dict[str, object],
    *,
    return_immediately: bool = True,
) -> dict:
    """Build a JSON-RPC 2.0 message/send envelope."""
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "message/send",
        "params": {
            "message": {
                "messageId": uuid.uuid4().hex,
                "role": "user",
                "parts": [{"kind": "text", "text": prompt}],
                "metadata": metadata,
            },
            "configuration": {"returnImmediately": return_immediately},
        },
    }


# ---------------------------------------------------------------------------
# Send + parse
# ---------------------------------------------------------------------------


async def send_a2a_message(
    http: httpx.AsyncClient,
    url: str,
    payload: dict,
) -> A2AResult:
    """POST a JSON-RPC payload to the hub and parse the response.

    Never raises on network/transport errors — returns A2AResult with
    accepted=False instead.
    """
    try:
        resp = await http.post(url, json=payload)
    except (httpx.RequestError, httpx.TimeoutException) as exc:
        return A2AResult(accepted=False, error=str(exc))

    if resp.status_code != 200:
        return A2AResult(
            accepted=False,
            error=f"HTTP {resp.status_code}: {resp.text[:500]}",
        )

    try:
        body = resp.json()
    except ValueError:
        return A2AResult(accepted=False, error="non-JSON response")

    result = body.get("result", {}) if isinstance(body, dict) else {}
    state = ""
    task_id = None
    if isinstance(result, dict):
        task_id = result.get("id")
        status = result.get("status", {})
        if isinstance(status, dict):
            state = status.get("state", "")

    accepted = state in _ACCEPTED_STATES or bool(task_id)
    return A2AResult(accepted=accepted, task_id=task_id, state=state)


# ---------------------------------------------------------------------------
# High-level dispatch (used by dispatch_cycle / graph_runtime)
# ---------------------------------------------------------------------------


async def dispatch_to_gateway(
    http: httpx.AsyncClient,
    *,
    agent: "AgentPresence",
    task: "Task",
    prompt: str,
    attempt: int,
    recipe_meta: dict[str, str] | None = None,
) -> bool:
    """POST a task to a gateway via A2A JSON-RPC message/send.

    Args:
        http: shared httpx client.
        agent: target gateway (must have endpoint_url).
        task: the krewhub task row being dispatched.
        prompt: the instruction string the agent should act on.
        attempt: 1-indexed retry counter; passed in metadata for idempotency.
        recipe_meta: optional repo/branch context forwarded to the gateway.

    Returns:
        True if the gateway accepted the task. False on rejection or error.
    """
    url = resolve_hub_url(agent)

    metadata: dict[str, object] = {
        "task_id": task.id,
        "bundle_id": task.bundle_id,
        "attempt": attempt,
    }
    for key in ("recipe_id", "recipe_name", "repo_url", "branch"):
        if recipe_meta and key in recipe_meta:
            metadata[key] = recipe_meta[key]

    payload = build_a2a_payload(
        req_id=f"{task.id}:{attempt}",
        prompt=prompt,
        metadata=metadata,
    )

    logger.info(
        "dispatch_to_gateway: POST %s task=%s attempt=%d",
        url, task.id, attempt,
    )

    result = await send_a2a_message(http, url, payload)
    if not result.accepted:
        logger.info(
            "dispatch_to_gateway: gateway %s did not accept task %s attempt %d: %s",
            agent.agent_id, task.id, attempt, result.error or result.state,
        )
    return result.accepted
