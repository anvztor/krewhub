"""Tests for the bundles.autoplan_enabled opt-in flag.

Bug history: PlannerDispatchController auto-dispatched a planner agent
for every empty bundle on the next 2s reconcile tick. cookrew-beta's
"+ NEW" tab creates an empty bundle on purpose (the operator wants a
blank board, not an LLM-generated graph), so the auto-plan was a UX
regression. Default is now off; orchestrator-mode flows flip it on.
"""
from __future__ import annotations

import pytest
from httpx import AsyncClient


async def _seed_recipe(client: AsyncClient) -> str:
    """Use init-workspace to create the user's cookbook. Returns cookbook_id."""
    r = await client.post("/api/v1/me/init-workspace")
    assert r.status_code == 200, r.text
    return r.json()["cookbook"]["id"]


@pytest.mark.asyncio
async def test_default_bundle_creation_is_inert(cookie_client: AsyncClient):
    cookbook_id = await _seed_recipe(cookie_client)
    r = await cookie_client.post(
        f"/api/v1/cookbooks/{cookbook_id}/bundles",
        json={"prompt": "", "requested_by": "tester", "tasks": []},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    bundle_id = body["bundle"]["id"]
    assert body["tasks"] == []

    # Direct DB check — autoplan_enabled must be 0 by default.
    from krewhub.db.connection import get_db
    db = await get_db()
    cur = await db.execute(
        "SELECT autoplan_enabled FROM bundles WHERE id = ?", (bundle_id,),
    )
    row = await cur.fetchone()
    assert row is not None
    assert row["autoplan_enabled"] == 0

    cur = await db.execute(
        "SELECT body FROM events WHERE bundle_id = ? AND type = 'plan'",
        (bundle_id,),
    )
    event = await cur.fetchone()
    assert event is not None
    assert event["body"] == "Created empty bundle; add tasks when ready."


@pytest.mark.asyncio
async def test_autoplan_true_persists_flag(cookie_client: AsyncClient):
    cookbook_id = await _seed_recipe(cookie_client)
    r = await cookie_client.post(
        f"/api/v1/cookbooks/{cookbook_id}/bundles",
        json={
            "prompt": "build a hello-world endpoint",
            "requested_by": "tester",
            "tasks": [],
            "autoplan": True,
        },
    )
    assert r.status_code == 200, r.text
    bundle_id = r.json()["bundle"]["id"]

    from krewhub.db.connection import get_db
    db = await get_db()
    cur = await db.execute(
        "SELECT autoplan_enabled FROM bundles WHERE id = ?", (bundle_id,),
    )
    assert (await cur.fetchone())["autoplan_enabled"] == 1


@pytest.mark.asyncio
async def test_planner_controller_skips_inert_bundles(cookie_client: AsyncClient):
    """The PlannerDispatchController._find_empty_bundles query is the
    canonical filter — assert it returns nothing for a default bundle
    and exactly the autoplan-enabled one when both exist."""
    from krewhub.controllers.planner_dispatch import PlannerDispatchController
    from krewhub.db.connection import get_db
    from krewhub.watch.service import WatchService

    cookbook_id = await _seed_recipe(cookie_client)
    inert = await cookie_client.post(
        f"/api/v1/cookbooks/{cookbook_id}/bundles",
        json={"prompt": "", "tasks": []},
    )
    enabled = await cookie_client.post(
        f"/api/v1/cookbooks/{cookbook_id}/bundles",
        json={"prompt": "p", "tasks": [], "autoplan": True},
    )
    inert_id = inert.json()["bundle"]["id"]
    enabled_id = enabled.json()["bundle"]["id"]

    db = await get_db()
    ctrl = PlannerDispatchController(db, WatchService(db))
    found = await ctrl._find_empty_bundles()
    found_ids = {b.id for b in found}
    assert enabled_id in found_ids
    assert inert_id not in found_ids
