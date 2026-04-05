"""Post-receive indexer — parse git repo and upsert DB index."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

from krewhub.git.parser import (
    ResolvedRecipe,
    parse_gitmodules,
    parse_tree_gitlinks,
    resolve_recipes,
)
from krewhub.models import Cookbook, WatchEventType
from krewhub.watch.globals import get_watch_service

logger = logging.getLogger(__name__)


async def index_cookbook(
    repo_path: Path,
    owner_id: str,
    db: aiosqlite.Connection,
) -> Cookbook | None:
    """Parse git state and upsert cookbook + recipes in DB.

    Called after every successful git push (post-receive).
    Returns the upserted cookbook, or None if the repo has no HEAD yet.
    """
    modules = await parse_gitmodules(repo_path)
    gitlinks = await parse_tree_gitlinks(repo_path)
    recipes = resolve_recipes(modules, gitlinks)

    cookbook_name = repo_path.name.removesuffix(".git")
    repo_path_str = str(repo_path)

    return await _upsert_cookbook(db, cookbook_name, owner_id, repo_path_str, recipes)


async def _upsert_cookbook(
    db: aiosqlite.Connection,
    name: str,
    owner_id: str,
    repo_path: str,
    recipes: list[ResolvedRecipe],
) -> Cookbook:
    """Idempotent upsert of cookbook + recipes from git state."""
    now = datetime.now(timezone.utc)

    # Upsert cookbook by repo_path (the stable identifier)
    cursor = await db.execute(
        "SELECT * FROM cookbooks WHERE repo_path = ?", (repo_path,),
    )
    row = await cursor.fetchone()

    if row is not None:
        cookbook_id = row["id"]
        await db.execute(
            "UPDATE cookbooks SET name = ?, owner_id = ? WHERE id = ?",
            (name, owner_id, cookbook_id),
        )
    else:
        cookbook_id = f"cb_{uuid.uuid4().hex[:8]}"
        await db.execute(
            "INSERT INTO cookbooks (id, name, owner_id, created_at, repo_path) VALUES (?, ?, ?, ?, ?)",
            (cookbook_id, name, owner_id, now.isoformat(), repo_path),
        )

    # Sync recipes: delete removed, upsert current
    cursor = await db.execute(
        "SELECT id, name FROM recipes WHERE cookbook_id = ?", (cookbook_id,),
    )
    existing_recipes = {row["name"]: row["id"] for row in await cursor.fetchall()}

    incoming_names = {r.name for r in recipes}

    # Delete recipes no longer in git
    removed = set(existing_recipes.keys()) - incoming_names
    for removed_name in removed:
        rid = existing_recipes[removed_name]
        await db.execute("DELETE FROM recipes WHERE id = ?", (rid,))
        logger.info("Removed recipe %s from cookbook %s", removed_name, cookbook_id)

    # Upsert recipes from git
    for recipe in recipes:
        if recipe.name in existing_recipes:
            rid = existing_recipes[recipe.name]
            await db.execute(
                """UPDATE recipes SET repo_url = ?, default_branch = ?, commit_sha = ?
                   WHERE id = ?""",
                (recipe.url, recipe.branch, recipe.commit_sha, rid),
            )
        else:
            rid = f"rec_{uuid.uuid4().hex[:8]}"
            await db.execute(
                """INSERT INTO recipes (id, name, repo_url, default_branch, created_by, created_at, cookbook_id, commit_sha)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (rid, recipe.name, recipe.url, recipe.branch, owner_id, now.isoformat(), cookbook_id, recipe.commit_sha),
            )
            logger.info("Added recipe %s to cookbook %s", recipe.name, cookbook_id)

    await db.commit()

    cookbook = Cookbook(
        id=cookbook_id,
        name=name,
        owner_id=owner_id,
        created_at=datetime.fromisoformat(row["created_at"]) if row else now,
    )

    try:
        watch = get_watch_service()
        await watch.record_resource(
            "cookbook", cookbook_id, WatchEventType.MODIFIED, cookbook,
        )
    except Exception:
        pass  # watch service may not be available during indexing

    return cookbook
