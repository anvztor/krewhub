from __future__ import annotations

from datetime import datetime

import aiosqlite

from krewhub.models import Recipe, RecipeMember


class RecipeRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(self, recipe: Recipe) -> Recipe:
        await self._db.execute(
            """INSERT INTO recipes (id, name, repo_url, default_branch, created_by, created_at, cookbook_id)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (recipe.id, recipe.name, recipe.repo_url, recipe.default_branch,
             recipe.created_by, recipe.created_at.isoformat(), recipe.cookbook_id),
        )
        await self._db.commit()
        return recipe

    async def get(self, recipe_id: str) -> Recipe | None:
        cursor = await self._db.execute(
            "SELECT * FROM recipes WHERE id = ?", (recipe_id,)
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_recipe(row)

    async def list_all(self) -> list[Recipe]:
        cursor = await self._db.execute("SELECT * FROM recipes ORDER BY created_at DESC")
        rows = await cursor.fetchall()
        return [_row_to_recipe(r) for r in rows]

    async def add_member(self, member: RecipeMember) -> RecipeMember:
        await self._db.execute(
            """INSERT INTO recipe_members (id, recipe_id, actor_id, actor_type, role, joined_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (member.id, member.recipe_id, member.actor_id, member.actor_type,
             member.role, member.joined_at.isoformat()),
        )
        await self._db.commit()
        return member

    async def list_by_cookbook(self, cookbook_id: str) -> list[Recipe]:
        cursor = await self._db.execute(
            "SELECT * FROM recipes WHERE cookbook_id = ? ORDER BY created_at DESC",
            (cookbook_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_recipe(r) for r in rows]

    async def list_members(self, recipe_id: str) -> list[RecipeMember]:
        cursor = await self._db.execute(
            "SELECT * FROM recipe_members WHERE recipe_id = ? ORDER BY joined_at",
            (recipe_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_member(r) for r in rows]


def _row_to_recipe(row: aiosqlite.Row) -> Recipe:
    return Recipe(
        id=row["id"],
        name=row["name"],
        repo_url=row["repo_url"],
        default_branch=row["default_branch"],
        created_by=row["created_by"],
        created_at=datetime.fromisoformat(row["created_at"]),
        cookbook_id=row["cookbook_id"] if "cookbook_id" in row.keys() else None,
    )


def _row_to_member(row: aiosqlite.Row) -> RecipeMember:
    return RecipeMember(
        id=row["id"],
        recipe_id=row["recipe_id"],
        actor_id=row["actor_id"],
        actor_type=row["actor_type"],
        role=row["role"],
        joined_at=datetime.fromisoformat(row["joined_at"]),
    )
