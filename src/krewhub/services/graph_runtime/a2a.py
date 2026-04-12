"""A2A JSON-RPC dispatch helper.

Mirrors TaskDispatchController._send_to_gateway but is decoupled from the
controller so dispatch_cycle can call it directly. The two should be unified
in a follow-up; for now this is the canonical implementation for the
graph_runtime path.

Idempotency: every dispatch carries an `attempt` integer in the message
metadata. Gateways that receive the same (task_id, attempt) twice — for
example after a retry whose response was lost — should dedupe by that pair.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from krewhub.models import AgentPresence, Task

logger = logging.getLogger(__name__)

_ACCEPTED_STATES: frozenset[str] = frozenset({"submitted", "working", "completed"})


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
        prompt: the instruction string the agent should act on. The cycle
            builds this with progressive failure context across retries.
        attempt: 1-indexed retry counter; passed in metadata for idempotency.
        recipe_meta: optional repo/branch context forwarded to the gateway.

    Returns:
        True if the gateway accepted the task (state in submitted/working/
        completed, or response carries a result.id). False on rejection,
        4xx/5xx, or network failure. Never raises.
    """
    # Route through the A2A hub gateway (same krewhub process) instead of
    # hitting the agent's local endpoint_url which is behind NAT.
    owner = agent.owner_username or agent.agent_id.split("@")[-1]
    agent_short = agent.agent_id.split("@")[0]
    url = f"http://127.0.0.1:8420/a2a/{owner}/{agent_short}"

    metadata: dict[str, object] = {
        "task_id": task.id,
        "bundle_id": task.bundle_id,
        "attempt": attempt,
    }
    for key in ("recipe_id", "recipe_name", "repo_url", "branch"):
        if recipe_meta and key in recipe_meta:
            metadata[key] = recipe_meta[key]

    payload = {
        "jsonrpc": "2.0",
        "id": f"{task.id}:{attempt}",
        "method": "message/send",
        "params": {
            "message": {
                "messageId": uuid.uuid4().hex,
                "role": "user",
                "parts": [{"kind": "text", "text": prompt}],
                "metadata": metadata,
            },
            "configuration": {"returnImmediately": True},
        },
    }

    try:
        logger.info(
            "dispatch_to_gateway: POST %s task=%s attempt=%d",
            url, task.id, attempt,
        )
        resp = await http.post(url, json=payload)
    except (httpx.RequestError, httpx.TimeoutException) as exc:
        logger.info(
            "dispatch_to_gateway: gateway %s unreachable for task %s attempt %d: %s",
            agent.agent_id, task.id, attempt, exc,
        )
        return False

    if resp.status_code != 200:
        logger.info(
            "dispatch_to_gateway: gateway %s returned %d for task %s: %s",
            agent.agent_id, resp.status_code, task.id, resp.text[:500],
        )
        return False

    try:
        body = resp.json()
    except ValueError:
        logger.info(
            "dispatch_to_gateway: gateway %s returned non-JSON for task %s",
            agent.agent_id, task.id,
        )
        return False

    result = body.get("result", {}) if isinstance(body, dict) else {}
    state = ""
    if isinstance(result, dict):
        status = result.get("status", {})
        if isinstance(status, dict):
            state = status.get("state", "")

    if state in _ACCEPTED_STATES:
        return True
    if isinstance(result, dict) and result.get("id"):
        return True

    logger.info(
        "dispatch_to_gateway: gateway %s did not accept task %s (state=%r)",
        agent.agent_id, task.id, state,
    )
    return False
