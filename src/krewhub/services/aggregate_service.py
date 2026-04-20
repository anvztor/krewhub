"""Aggregate data service for BFF elimination.

Replaces the BFF's cookrew-queries.ts + cookrew-helpers.ts by
querying krewhub repositories directly (no HTTP round-trips).

All functions return plain dicts with snake_case keys.
"""

from __future__ import annotations

from typing import Any

import aiosqlite

from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.cookbook_repo import CookbookRepo
from krewhub.repositories.digest_repo import DigestRepo
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.tape.manager import TapeManager
from krewhub.tape.store import entry_to_dict

_NON_TERMINAL_STATUSES = frozenset({"open", "claimed", "cooked", "blocked"})


def _model_to_dict(obj: Any) -> dict:
    """Convert a frozen pydantic model to a JSON-safe dict."""
    return obj.model_dump(mode="json")


def _calculate_median(values: list[int | float]) -> float:
    """Calculate median of a list of numbers."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0
    return float(sorted_vals[mid])


def _select_bundle_id(
    bundles: list[dict],
    requested_bundle_id: str | None = None,
) -> str | None:
    """Pick the best bundle to display (mirrors BFF selectBundleId)."""
    if requested_bundle_id:
        if any(b["id"] == requested_bundle_id for b in bundles):
            return requested_bundle_id

    for b in bundles:
        if b["status"] in _NON_TERMINAL_STATUSES:
            return b["id"]

    return bundles[0]["id"] if bundles else None


def _build_recipe_summary(
    recipe: dict,
    members: list[dict],
    agents: list[dict],
    bundles: list[dict],
    digests: list[dict],
) -> dict:
    """Build a recipe summary (mirrors BFF buildSummary)."""
    owners = [m["actor_id"] for m in members if m["role"] == "owner"]

    active_bundle = next(
        (b for b in bundles if b["status"] in _NON_TERMINAL_STATUSES),
        None,
    )

    return {
        "recipe": recipe,
        "member_count": len(members),
        "agent_count": len(agents),
        "online_agent_count": sum(
            1 for a in agents if a["status"] != "offline"
        ),
        "active_bundle_count": sum(
            1 for b in bundles if b["status"] in _NON_TERMINAL_STATUSES
        ),
        "owners": owners,
        "active_bundle": active_bundle,
        "latest_digest": digests[0] if digests else None,
    }


def _build_history_metrics(digests: list[dict]) -> dict:
    """Build history metrics (mirrors BFF buildHistoryMetrics)."""
    review_durations: list[int] = []
    for d in digests:
        if d.get("decided_at"):
            from datetime import datetime
            submitted = datetime.fromisoformat(d["submitted_at"])
            decided = datetime.fromisoformat(d["decided_at"])
            minutes = max(1, round((decided - submitted).total_seconds() / 60))
            review_durations.append(minutes)

    return {
        "approved_count": len(digests),
        "median_review_minutes": _calculate_median(review_durations),
        "most_recent_approval_at": digests[0].get("decided_at") if digests else None,
    }


# ---------------------------------------------------------------------------
# Public aggregate functions
# ---------------------------------------------------------------------------


async def list_cookbook_data(db: aiosqlite.Connection) -> dict:
    """Aggregate all cookbooks with recipe summaries."""
    cookbook_repo = CookbookRepo(db)
    recipe_repo = RecipeRepo(db)
    agent_repo = AgentRepo(db)
    bundle_repo = BundleRepo(db)
    digest_repo = DigestRepo(db)

    cookbooks = await cookbook_repo.list_all()
    all_recipes = await recipe_repo.list_all()

    # Group recipes by cookbook_id
    recipes_by_cookbook: dict[str, list] = {}
    for recipe in all_recipes:
        cb_id = recipe.cookbook_id or ""
        recipes_by_cookbook.setdefault(cb_id, []).append(recipe)

    cookbook_groups: list[dict] = []
    first_recipe_id: str | None = None

    for cookbook in cookbooks:
        cb_recipes = recipes_by_cookbook.get(cookbook.id, [])
        agents = await agent_repo.list_by_cookbook(cookbook.id)
        agents_dicts = [_model_to_dict(a) for a in agents]

        summaries: list[dict] = []
        for recipe in cb_recipes:
            if first_recipe_id is None:
                first_recipe_id = recipe.id

            members = await recipe_repo.list_members(recipe.id)
            bundles = await bundle_repo.list_by_recipe(recipe.id)
            digests = await digest_repo.list_approved_by_recipe(recipe.id)

            summaries.append(_build_recipe_summary(
                recipe=_model_to_dict(recipe),
                members=[_model_to_dict(m) for m in members],
                agents=agents_dicts,
                bundles=[_model_to_dict(b) for b in bundles],
                digests=[_model_to_dict(d) for d in digests],
            ))

        cookbook_groups.append({
            "cookbook": _model_to_dict(cookbook),
            "recipes": summaries,
            "agents": agents_dicts,
        })

    return {
        "cookbooks": cookbook_groups,
        "selected_recipe_id": first_recipe_id,
    }


async def get_workspace_data(
    db: aiosqlite.Connection,
    recipe_id: str,
    bundle_id: str | None = None,
) -> dict | None:
    """Aggregate workspace data for a recipe."""
    recipe_repo = RecipeRepo(db)
    bundle_repo = BundleRepo(db)
    digest_repo = DigestRepo(db)
    agent_repo = AgentRepo(db)
    task_repo = TaskRepo(db)
    event_repo = EventRepo(db)

    recipe = await recipe_repo.get(recipe_id)
    if recipe is None:
        return None

    members = await recipe_repo.list_members(recipe_id)
    agents = await agent_repo.list_by_recipe(recipe_id)
    bundles = await bundle_repo.list_by_recipe(recipe_id)
    recent_digests = await digest_repo.list_approved_by_recipe(recipe_id)

    bundles_dicts = [_model_to_dict(b) for b in bundles]
    selected_bundle_id = _select_bundle_id(bundles_dicts, bundle_id)

    selected_bundle_detail: dict | None = None
    if selected_bundle_id:
        sel_bundle = await bundle_repo.get(selected_bundle_id)
        if sel_bundle is not None:
            tasks = await task_repo.list_by_bundle(selected_bundle_id)
            events = await event_repo.list_by_bundle(selected_bundle_id)
            digest = await digest_repo.get_by_bundle(selected_bundle_id)
            selected_bundle_detail = {
                "bundle": _model_to_dict(sel_bundle),
                "tasks": [_model_to_dict(t) for t in tasks],
                "events": [_model_to_dict(e) for e in events],
                "digest": _model_to_dict(digest) if digest else None,
            }

    return {
        "recipe": _model_to_dict(recipe),
        "members": [_model_to_dict(m) for m in members],
        "agents": [_model_to_dict(a) for a in agents],
        "bundles": bundles_dicts,
        "recent_digests": [_model_to_dict(d) for d in recent_digests[:4]],
        "selected_bundle_id": selected_bundle_id,
        "selected_bundle": selected_bundle_detail,
    }


async def get_cookbook_detail_data(
    db: aiosqlite.Connection,
    cookbook_id: str,
) -> dict | None:
    """Aggregate cookbook detail with recipes, agents, deduplicated members."""
    cookbook_repo = CookbookRepo(db)
    recipe_repo = RecipeRepo(db)
    agent_repo = AgentRepo(db)
    bundle_repo = BundleRepo(db)
    digest_repo = DigestRepo(db)

    cookbook = await cookbook_repo.get(cookbook_id)
    if cookbook is None:
        return None

    recipes = await recipe_repo.list_by_cookbook(cookbook_id)
    agents = await agent_repo.list_by_cookbook(cookbook_id)
    agents_dicts = [_model_to_dict(a) for a in agents]

    summaries: list[dict] = []
    all_members: list[dict] = []

    for recipe in recipes:
        members = await recipe_repo.list_members(recipe.id)
        bundles = await bundle_repo.list_by_recipe(recipe.id)
        digests = await digest_repo.list_approved_by_recipe(recipe.id)
        members_dicts = [_model_to_dict(m) for m in members]

        all_members.extend(members_dicts)
        summaries.append(_build_recipe_summary(
            recipe=_model_to_dict(recipe),
            members=members_dicts,
            agents=agents_dicts,
            bundles=[_model_to_dict(b) for b in bundles],
            digests=[_model_to_dict(d) for d in digests],
        ))

    # Deduplicate members by actor_id
    seen_actors: set[str] = set()
    unique_members: list[dict] = []
    for member in all_members:
        actor_id = member["actor_id"]
        if actor_id not in seen_actors:
            seen_actors.add(actor_id)
            unique_members.append(member)

    return {
        "cookbook": _model_to_dict(cookbook),
        "recipes": summaries,
        "agents": agents_dicts,
        "members": unique_members,
    }


async def get_digest_review_data(
    db: aiosqlite.Connection,
    recipe_id: str,
    bundle_id: str,
) -> dict | None:
    """Get recipe + bundle with full details for digest review."""
    recipe_repo = RecipeRepo(db)
    bundle_repo = BundleRepo(db)
    task_repo = TaskRepo(db)
    event_repo = EventRepo(db)
    digest_repo = DigestRepo(db)

    recipe = await recipe_repo.get(recipe_id)
    bundle = await bundle_repo.get(bundle_id)
    if recipe is None or bundle is None:
        return None

    tasks = await task_repo.list_by_bundle(bundle_id)
    events = await event_repo.list_by_bundle(bundle_id)
    digest = await digest_repo.get_by_bundle(bundle_id)

    # Assemble fork tape anchors for anchor-to-anchor review navigation.
    tape_mgr = TapeManager(db, recipe_id)
    fork_entries = await tape_mgr.get_bundle_fork_entries(bundle_id)
    fork_anchors = [
        entry_to_dict(e) for e in fork_entries if e.kind == "anchor"
    ]

    return {
        "recipe": _model_to_dict(recipe),
        "selected_bundle": {
            "bundle": _model_to_dict(bundle),
            "tasks": [_model_to_dict(t) for t in tasks],
            "events": [_model_to_dict(e) for e in events],
            "digest": _model_to_dict(digest) if digest else None,
            "fork_anchors": fork_anchors,
        },
    }


async def get_history_data(
    db: aiosqlite.Connection,
    recipe_id: str,
) -> dict | None:
    """Get recipe history with approved digests and metrics."""
    recipe_repo = RecipeRepo(db)
    bundle_repo = BundleRepo(db)
    digest_repo = DigestRepo(db)

    recipe = await recipe_repo.get(recipe_id)
    if recipe is None:
        return None

    digests = await digest_repo.list_approved_by_recipe(recipe_id)
    bundles = await bundle_repo.list_by_recipe(recipe_id)

    digests_dicts = [_model_to_dict(d) for d in digests]
    bundles_dicts = [_model_to_dict(b) for b in bundles]

    # Build bundle lookup for fast matching
    bundle_by_id = {b["id"]: b for b in bundles_dicts}

    records = [
        {
            "digest": d,
            "bundle": bundle_by_id.get(d["bundle_id"]),
        }
        for d in digests_dicts
    ]

    return {
        "recipe": _model_to_dict(recipe),
        "metrics": _build_history_metrics(digests_dicts),
        "records": records,
    }
