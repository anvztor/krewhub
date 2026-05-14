"""init_workspace seeds one starter bundle on first call, idempotent thereafter.

Bundle Lifecycle Task 5: a brand-new operator landing on cookrew-beta
should see a populated mission board (one starter bundle visible),
not the bare "EMPTY MISSION BOARD" placeholder. The seed is idempotent
across re-inits AND across deliberate close — never reseeds once any
bundle (open OR closed) exists in the cookbook.
"""
from __future__ import annotations

import asyncio

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_first_init_seeds_starter_bundle(cookie_client: AsyncClient):
    """A brand-new account gets one bundle inside its newly-bootstrapped cookbook."""
    r1 = await cookie_client.post("/api/v1/me/init-workspace")
    assert r1.status_code == 200, r1.text
    cookbook_id = r1.json()["cookbook"]["id"]

    bundles = await cookie_client.get(f"/api/v1/cookbooks/{cookbook_id}/bundles")
    assert bundles.status_code == 200, bundles.text
    body = bundles.json()
    assert len(body["bundles"]) == 1, "starter bundle should be seeded once"
    assert body["bundles"][0]["status"] == "open"


@pytest.mark.asyncio
async def test_second_init_is_idempotent(cookie_client: AsyncClient):
    """Second init_workspace call does NOT seed a second bundle."""
    r1 = await cookie_client.post("/api/v1/me/init-workspace")
    assert r1.status_code == 200, r1.text
    cookbook_id = r1.json()["cookbook"]["id"]

    r2 = await cookie_client.post("/api/v1/me/init-workspace")
    assert r2.status_code == 200, r2.text
    assert r2.json()["cookbook"]["id"] == cookbook_id

    bundles = await cookie_client.get(f"/api/v1/cookbooks/{cookbook_id}/bundles")
    assert bundles.status_code == 200, bundles.text
    assert len(bundles.json()["bundles"]) == 1


@pytest.mark.asyncio
async def test_init_after_close_does_not_reseed(cookie_client: AsyncClient):
    """If operator closes the starter bundle then calls init again, no new bundle."""
    r1 = await cookie_client.post("/api/v1/me/init-workspace")
    assert r1.status_code == 200, r1.text
    cookbook_id = r1.json()["cookbook"]["id"]
    bundles = await cookie_client.get(f"/api/v1/cookbooks/{cookbook_id}/bundles")
    assert bundles.status_code == 200, bundles.text
    bundle_id = bundles.json()["bundles"][0]["id"]

    close = await cookie_client.patch(
        f"/api/v1/cookbooks/{cookbook_id}/bundles/{bundle_id}",
        json={"status": "closed"},
    )
    assert close.status_code == 200, close.text

    await cookie_client.post("/api/v1/me/init-workspace")
    bundles_after = await cookie_client.get(
        f"/api/v1/cookbooks/{cookbook_id}/bundles"
    )
    # Still one bundle (the closed one); init does not reseed.
    assert len(bundles_after.json()["bundles"]) == 1
    assert bundles_after.json()["bundles"][0]["status"] == "closed"


@pytest.mark.asyncio
async def test_concurrent_init_seeds_at_most_one(cookie_client: AsyncClient):
    """Parallel init calls after the cookbook exists must not seed two starters.

    The race targeted by the fix is the check-then-create on the
    starter bundle: two callers pass has_any_for_cookbook == False
    before either inserts. We bootstrap the cookbook first (so both
    parallel callers reuse it), then immediately delete the seeded
    bundle so the next two parallel inits exercise the seed race.

    A per-cookbook asyncio.Lock + re-check inside the critical section
    in init_workspace serializes the seed; without it both callers
    would insert.

    Note: this test does not exercise the separate (pre-existing)
    cookbook-creation race — only the seed race covered by this fix.
    """
    # 1. Establish the cookbook (and its initial seed) sequentially.
    r0 = await cookie_client.post("/api/v1/me/init-workspace")
    assert r0.status_code == 200, r0.text
    cookbook_id = r0.json()["cookbook"]["id"]

    # 2. Drop the seeded bundle so has_any_for_cookbook flips back to
    # False. There is no DELETE route, so wipe at the DB layer (events
    # first to satisfy the FK constraint).
    bundles = await cookie_client.get(f"/api/v1/cookbooks/{cookbook_id}/bundles")
    assert bundles.status_code == 200, bundles.text
    bundle_id = bundles.json()["bundles"][0]["id"]
    from krewhub.db.connection import get_db
    db = await get_db()
    await db.execute("DELETE FROM events WHERE bundle_id = ?", (bundle_id,))
    await db.execute("DELETE FROM bundles WHERE id = ?", (bundle_id,))
    await db.commit()

    # 3. Race two parallel inits — both should re-seed at most one bundle.
    r1, r2 = await asyncio.gather(
        cookie_client.post("/api/v1/me/init-workspace"),
        cookie_client.post("/api/v1/me/init-workspace"),
    )
    assert r1.status_code == 200, r1.text
    assert r2.status_code == 200, r2.text
    assert r1.json()["cookbook"]["id"] == cookbook_id
    assert r2.json()["cookbook"]["id"] == cookbook_id

    bundles_after = await cookie_client.get(
        f"/api/v1/cookbooks/{cookbook_id}/bundles"
    )
    assert bundles_after.status_code == 200, bundles_after.text
    assert len(bundles_after.json()["bundles"]) == 1, (
        "concurrent init should seed at most one starter bundle"
    )


@pytest.mark.asyncio
async def test_init_returns_200_when_seed_fails(
    cookie_client: AsyncClient, monkeypatch
):
    """If BundleService.create_bundle raises, init still returns 200 with the cookbook.

    Cookbook ensure-or-create is the primary contract; the starter
    bundle is a UX nicety. Seed failures (e2b unreachable, watch blip,
    etc.) must not break the bootstrap path.
    """
    from krewhub.services import bundle_service

    async def boom(*args, **kwargs):
        raise RuntimeError("seed simulated failure")

    monkeypatch.setattr(bundle_service.BundleService, "create_bundle", boom)

    r = await cookie_client.post("/api/v1/me/init-workspace")
    assert r.status_code == 200, r.text
    cookbook_id = r.json()["cookbook"]["id"]

    # Cookbook landed, just no starter bundle.
    bundles = await cookie_client.get(f"/api/v1/cookbooks/{cookbook_id}/bundles")
    assert bundles.status_code == 200, bundles.text
    assert len(bundles.json()["bundles"]) == 0
