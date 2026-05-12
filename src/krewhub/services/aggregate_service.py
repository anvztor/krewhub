"""Aggregate data service.

Step (e): recipes are gone. Aggregates are cookbook-direct now.
Returns plain dicts with snake_case keys for the frontend.
"""

from __future__ import annotations

from typing import Any

import aiosqlite

from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.cookbook_repo import CookbookRepo
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.tape.manager import TapeManager
from krewhub.tape.store import entry_to_dict

# Active bundle = open. Step (d.1) collapsed everything else.
_NON_TERMINAL_STATUSES = frozenset({"open"})


def _model_to_dict(obj: Any) -> dict:
    """Convert a frozen pydantic model to a JSON-safe dict."""
    return obj.model_dump(mode="json")


def _select_bundle_id(
    bundles: list[dict],
    requested_bundle_id: str | None = None,
) -> str | None:
    """Pick the best bundle to display."""
    if requested_bundle_id:
        if any(b["id"] == requested_bundle_id for b in bundles):
            return requested_bundle_id

    for b in bundles:
        if b["status"] in _NON_TERMINAL_STATUSES:
            return b["id"]

    return bundles[0]["id"] if bundles else None


def _build_cookbook_summary(
    cookbook: dict,
    agents: list[dict],
    bundles: list[dict],
) -> dict:
    """Cookbook-level summary."""
    active_bundle = next(
        (b for b in bundles if b["status"] in _NON_TERMINAL_STATUSES),
        None,
    )

    return {
        "cookbook": cookbook,
        "agent_count": len(agents),
        "online_agent_count": sum(
            1 for a in agents if a["status"] != "offline"
        ),
        "active_bundle_count": sum(
            1 for b in bundles if b["status"] in _NON_TERMINAL_STATUSES
        ),
        "active_bundle": active_bundle,
    }


# ---------------------------------------------------------------------------
# Public aggregate functions
# ---------------------------------------------------------------------------


async def list_cookbook_data(db: aiosqlite.Connection) -> dict:
    """Aggregate all cookbooks with their bundle summaries."""
    cookbook_repo = CookbookRepo(db)
    agent_repo = AgentRepo(db)
    bundle_repo = BundleRepo(db)

    cookbooks = await cookbook_repo.list_all()

    cookbook_groups: list[dict] = []
    first_cookbook_id: str | None = None

    for cookbook in cookbooks:
        if first_cookbook_id is None:
            first_cookbook_id = cookbook.id

        agents = await agent_repo.list_by_cookbook(cookbook.id)
        agents_dicts = [_model_to_dict(a) for a in agents]

        bundles = await bundle_repo.list_by_cookbook(cookbook.id)
        bundles_dicts = [_model_to_dict(b) for b in bundles]

        cookbook_groups.append({
            "cookbook": _model_to_dict(cookbook),
            "agents": agents_dicts,
            "summary": _build_cookbook_summary(
                cookbook=_model_to_dict(cookbook),
                agents=agents_dicts,
                bundles=bundles_dicts,
            ),
        })

    return {
        "cookbooks": cookbook_groups,
        "selected_cookbook_id": first_cookbook_id,
    }


async def get_workspace_data(
    db: aiosqlite.Connection,
    cookbook_id: str,
    bundle_id: str | None = None,
) -> dict | None:
    """Aggregate workspace data for a cookbook."""
    cookbook_repo = CookbookRepo(db)
    bundle_repo = BundleRepo(db)
    agent_repo = AgentRepo(db)
    task_repo = TaskRepo(db)
    event_repo = EventRepo(db)

    cookbook = await cookbook_repo.get(cookbook_id)
    if cookbook is None:
        return None

    agents = await agent_repo.list_by_cookbook(cookbook_id)
    bundles = await bundle_repo.list_by_cookbook(cookbook_id)

    bundles_dicts = [_model_to_dict(b) for b in bundles]
    selected_bundle_id = _select_bundle_id(bundles_dicts, bundle_id)

    selected_bundle_detail: dict | None = None
    if selected_bundle_id:
        sel_bundle = await bundle_repo.get(selected_bundle_id)
        if sel_bundle is not None:
            tasks = await task_repo.list_by_bundle(selected_bundle_id)
            events = await event_repo.list_by_bundle(selected_bundle_id)
            tape_mgr = TapeManager(db, f"cookbook:{cookbook_id}")
            fork_entries = await tape_mgr.get_bundle_fork_entries(selected_bundle_id)
            fork_anchors = [
                entry_to_dict(e) for e in fork_entries if e.kind == "anchor"
            ]

            selected_bundle_detail = {
                "bundle": _model_to_dict(sel_bundle),
                "tasks": [_model_to_dict(t) for t in tasks],
                "events": [_model_to_dict(e) for e in events],
                "fork_anchors": fork_anchors,
            }

    return {
        "cookbook": _model_to_dict(cookbook),
        "agents": [_model_to_dict(a) for a in agents],
        "bundles": bundles_dicts,
        "selected_bundle_id": selected_bundle_id,
        "selected_bundle": selected_bundle_detail,
    }


async def get_cookbook_detail_data(
    db: aiosqlite.Connection,
    cookbook_id: str,
) -> dict | None:
    """Aggregate cookbook detail with agents."""
    cookbook_repo = CookbookRepo(db)
    agent_repo = AgentRepo(db)
    bundle_repo = BundleRepo(db)

    cookbook = await cookbook_repo.get(cookbook_id)
    if cookbook is None:
        return None

    agents = await agent_repo.list_by_cookbook(cookbook_id)
    bundles = await bundle_repo.list_by_cookbook(cookbook_id)

    return {
        "cookbook": _model_to_dict(cookbook),
        "agents": [_model_to_dict(a) for a in agents],
        "bundles": [_model_to_dict(b) for b in bundles],
    }
