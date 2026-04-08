"""Pick the best A2A gateway for a given task kind.

Used by dispatch_cycle to choose which agent receives each step's work.
The picker is deterministic — given the same agent pool and exclude set,
it always returns the same agent — so retries can be reproduced and
unit-tested without flake.

Ranking:
    1. Agent must be ONLINE or BUSY (offline agents are skipped).
    2. Agent must have a non-empty endpoint_url (only gateways, not workers).
    3. Agent must not be in `exclude` (used by retry logic to try a fresh agent).
    4. Prefer agents whose `capabilities` contain the requested task_kind.
    5. Tiebreak: ONLINE > BUSY (least loaded first).
    6. Final tiebreak: lexicographic agent_id (deterministic).

If no capability-matching agent is available, falls back to the best-ranked
non-matching agent rather than failing — better to send work to *some* agent
than block the bundle. The cycle records this fallback in the AttemptRecord
summary so callers can detect mismatch.
"""

from __future__ import annotations

from collections.abc import Iterable

from krewhub.models import AgentPresence, AgentStatus


def pick_agent_for_kind(
    agents: Iterable[AgentPresence],
    task_kind: str,
    *,
    exclude: set[str] | None = None,
) -> AgentPresence | None:
    """Return the best gateway for `task_kind`, or None if none are eligible.

    Args:
        agents: candidate AgentPresence rows from agent_repo.list_by_cookbook.
        task_kind: a free-form skill name like "planner", "coder", "reviewer",
            "tester". Matched against AgentPresence.capabilities (case-insensitive).
        exclude: agent_ids to skip — typically agents that already failed
            this cycle, so the retry tries something fresh.

    Returns:
        The chosen AgentPresence, or None if no eligible agent exists.
    """
    excluded = exclude or set()
    eligible = [
        agent
        for agent in agents
        if agent.status in (AgentStatus.ONLINE, AgentStatus.BUSY)
        and agent.endpoint_url
        and agent.agent_id not in excluded
    ]
    if not eligible:
        return None

    kind_lower = task_kind.lower()

    def rank_key(agent: AgentPresence) -> tuple[int, int, str]:
        # Lower tuple sorts earlier → better candidate.
        capability_match = 0 if any(
            cap.lower() == kind_lower for cap in agent.capabilities
        ) else 1
        status_rank = 0 if agent.status == AgentStatus.ONLINE else 1
        return (capability_match, status_rank, agent.agent_id)

    eligible.sort(key=rank_key)
    return eligible[0]
