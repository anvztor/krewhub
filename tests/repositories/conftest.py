from __future__ import annotations

import pytest_asyncio


@pytest_asyncio.fixture
async def seeded_db(test_db):
    await test_db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) "
        "VALUES (?, ?, ?, ?)",
        ("cb-seed", "seed", "u1", "2026-05-01T00:00:00+00:00"),
    )
    await test_db.execute(
        "INSERT INTO bundles (id, cookbook_id, prompt, status, "
        "created_by, created_at, resource_version, generation) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "b-seed",
            "cb-seed",
            "",
            "open",
            "u1",
            "2026-05-01T00:00:00+00:00",
            1,
            1,
        ),
    )
    await test_db.commit()
    yield test_db
