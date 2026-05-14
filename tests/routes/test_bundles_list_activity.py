"""GET /api/v1/cookbooks/{id}/bundles includes latest_task_activity_at on every bundle."""
from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_list_bundles_returns_activity_timestamp(cookie_client: AsyncClient):
    """Each bundle in the list response has a non-empty latest_task_activity_at field."""
    from krewhub.db.connection import get_db

    init = await cookie_client.post("/api/v1/me/init-workspace")
    assert init.status_code == 200, init.text
    cookbook_id = init.json()["cookbook"]["id"]

    db = await get_db()
    await db.execute(
        "INSERT INTO bundles (id, cookbook_id, prompt, status, "
        "created_by, created_at, owner_account_id) "
        "VALUES (?,?,?,?,?,?,?)",
        (
            "BUN_ACTIVITY",
            cookbook_id,
            "prompt",
            "open",
            "acc_test_cookie",
            "2026-01-01T00:00:00+00:00",
            "acc_test_cookie",
        ),
    )
    await db.commit()

    resp = await cookie_client.get(f"/api/v1/cookbooks/{cookbook_id}/bundles")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["bundles"], "expected at least one bundle in fixture"
    for bundle in body["bundles"]:
        assert "latest_task_activity_at" in bundle
        assert bundle["latest_task_activity_at"]  # non-empty ISO string
