"""Bundles table has Track A1 ownership columns."""
from __future__ import annotations

import aiosqlite
import pytest

from krewhub.db.schema import SCHEMA_SQL


@pytest.mark.asyncio
async def test_bundles_has_owner_and_default_runtime():
    async with aiosqlite.connect(":memory:") as db:
        db.row_factory = aiosqlite.Row
        await db.executescript(SCHEMA_SQL)
        cursor = await db.execute("PRAGMA table_info(bundles)")
        cols = {row[1] for row in await cursor.fetchall()}
    assert "owner_account_id" in cols
    assert "default_agent_runtime_id" in cols
