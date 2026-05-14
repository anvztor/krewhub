"""init_workspace seeds one starter bundle on first call, idempotent thereafter.

Bundle Lifecycle Task 5: a brand-new operator landing on cookrew-beta
should see a populated mission board (one starter bundle visible),
not the bare "EMPTY MISSION BOARD" placeholder. The seed is idempotent
across re-inits AND across deliberate close — never reseeds once any
bundle (open OR closed) exists in the cookbook.
"""
from __future__ import annotations

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
