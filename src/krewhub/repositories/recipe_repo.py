"""DEPRECATED stub — recipes are gone in step (e).

This module remains only so legacy import chains don't break before
all consumers migrate to cookbook-direct lookups. Every method
returns empty / None to make the rest of the system behave as if no
recipes exist (which is the truth — the table is dropped by
migration). Remove this file once all callers are gone.
"""
from __future__ import annotations

from typing import Any

import aiosqlite


class RecipeRepo:
    """Deprecated stub. Every method is a no-op returning empty data."""

    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(self, recipe: Any) -> Any:
        raise RuntimeError(
            "RecipeRepo is deprecated; recipes table no longer exists",
        )

    async def get(self, recipe_id: str) -> None:
        return None

    async def list_all(self) -> list:
        return []

    async def add_member(self, member: Any) -> Any:
        raise RuntimeError(
            "RecipeRepo is deprecated; recipe_members table no longer exists",
        )

    async def list_by_cookbook(self, cookbook_id: str) -> list:
        return []

    async def list_members(self, recipe_id: str) -> list:
        return []
