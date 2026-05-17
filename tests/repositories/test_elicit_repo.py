"""ElicitRepo unit tests — durable elicit tracking with reservation semantics."""
from __future__ import annotations

import asyncio

import pytest

from krewhub.repositories.elicit_repo import ElicitRepo, ElicitRow


@pytest.mark.asyncio
async def test_put_then_get_pending(test_db):
    repo = ElicitRepo(test_db)
    await repo.put(ElicitRow(
        id="el_1", invocation_id="inv_1", op="auth_required",
        payload_json='{"provider":"github"}', status="pending",
    ))
    got = await repo.get_pending(invocation_id="inv_1", elicit_id="el_1")
    assert got is not None
    assert got.op == "auth_required"
    assert got.invocation_id == "inv_1"


@pytest.mark.asyncio
async def test_put_is_idempotent(test_db):
    """Re-emitting the same elicit_id is a no-op."""
    repo = ElicitRepo(test_db)
    row = ElicitRow(
        id="el_1", invocation_id="inv_1", op="auth_required",
        payload_json='{}', status="pending",
    )
    await repo.put(row)
    await repo.put(row)  # should not raise / duplicate.
    cur = await test_db.execute("SELECT COUNT(*) FROM elicits WHERE id='el_1'")
    assert (await cur.fetchone())[0] == 1


@pytest.mark.asyncio
async def test_get_pending_returns_none_for_wrong_invocation(test_db):
    repo = ElicitRepo(test_db)
    await repo.put(ElicitRow(
        id="el_1", invocation_id="inv_1", op="auth_required",
        payload_json='{}', status="pending",
    ))
    got = await repo.get_pending(invocation_id="inv_2", elicit_id="el_1")
    assert got is None


@pytest.mark.asyncio
async def test_get_pending_returns_none_after_reserve(test_db):
    repo = ElicitRepo(test_db)
    await repo.put(ElicitRow(
        id="el_1", invocation_id="inv_1", op="auth_required",
        payload_json='{}', status="pending",
    ))
    await repo.reserve(invocation_id="inv_1", elicit_id="el_1", lease_s=30)
    got = await repo.get_pending(invocation_id="inv_1", elicit_id="el_1")
    assert got is None  # now injecting, not pending


@pytest.mark.asyncio
async def test_reserve_atomic_under_concurrency(test_db):
    repo = ElicitRepo(test_db)
    await repo.put(ElicitRow(
        id="el_1", invocation_id="inv_1", op="auth_required",
        payload_json='{}', status="pending",
    ))
    results = await asyncio.gather(
        repo.reserve(invocation_id="inv_1", elicit_id="el_1", lease_s=30),
        repo.reserve(invocation_id="inv_1", elicit_id="el_1", lease_s=30),
    )
    # Exactly one wins.
    assert sorted(results) == [False, True]


@pytest.mark.asyncio
async def test_finalize_only_from_injecting(test_db):
    repo = ElicitRepo(test_db)
    await repo.put(ElicitRow(
        id="el_1", invocation_id="inv_1", op="auth_required",
        payload_json='{}', status="pending",
    ))
    # Can't finalize from 'pending'.
    assert (await repo.finalize(invocation_id="inv_1", elicit_id="el_1")) is False
    await repo.reserve(invocation_id="inv_1", elicit_id="el_1", lease_s=30)
    assert (await repo.finalize(invocation_id="inv_1", elicit_id="el_1")) is True


@pytest.mark.asyncio
async def test_finalize_clears_injecting_until(test_db):
    repo = ElicitRepo(test_db)
    await repo.put(ElicitRow(
        id="el_1", invocation_id="inv_1", op="auth_required",
        payload_json='{}', status="pending",
    ))
    await repo.reserve(invocation_id="inv_1", elicit_id="el_1", lease_s=30)
    await repo.finalize(invocation_id="inv_1", elicit_id="el_1")
    cur = await test_db.execute(
        "SELECT status, injecting_until FROM elicits WHERE id='el_1'",
    )
    row = await cur.fetchone()
    assert row[0] == "resolved"
    assert row[1] is None


@pytest.mark.asyncio
async def test_sweep_expired_leases_reverts(test_db):
    repo = ElicitRepo(test_db)
    await repo.put(ElicitRow(
        id="el_1", invocation_id="inv_1", op="auth_required",
        payload_json='{}', status="pending",
    ))
    # Use a 1-second lease — wait 1.5s for it to expire naturally.
    await repo.reserve(invocation_id="inv_1", elicit_id="el_1", lease_s=1)
    await asyncio.sleep(1.5)
    swept = await repo.sweep_expired_leases()
    assert swept == 1
    # Now back to pending → re-reservable.
    again = await repo.get_pending(invocation_id="inv_1", elicit_id="el_1")
    assert again is not None
    assert again.status == "pending"


@pytest.mark.asyncio
async def test_sweep_does_not_revert_active_lease(test_db):
    """An unexpired injecting row should NOT be swept."""
    repo = ElicitRepo(test_db)
    await repo.put(ElicitRow(
        id="el_1", invocation_id="inv_1", op="auth_required",
        payload_json='{}', status="pending",
    ))
    await repo.reserve(invocation_id="inv_1", elicit_id="el_1", lease_s=300)
    swept = await repo.sweep_expired_leases()
    assert swept == 0


@pytest.mark.asyncio
async def test_latest_pending_auth_required(test_db):
    repo = ElicitRepo(test_db)
    await repo.put(ElicitRow(
        id="el_old", invocation_id="inv_1", op="other",
        payload_json='{}', status="pending",
    ))
    await repo.put(ElicitRow(
        id="el_new", invocation_id="inv_1", op="auth_required",
        payload_json='{"provider":"github"}', status="pending",
    ))
    latest = await repo.latest_pending_auth_required(invocation_id="inv_1")
    assert latest is not None
    assert latest.id == "el_new"


@pytest.mark.asyncio
async def test_latest_pending_none_when_all_resolved(test_db):
    repo = ElicitRepo(test_db)
    await repo.put(ElicitRow(
        id="el_1", invocation_id="inv_1", op="auth_required",
        payload_json='{}', status="pending",
    ))
    await repo.reserve(invocation_id="inv_1", elicit_id="el_1", lease_s=30)
    await repo.finalize(invocation_id="inv_1", elicit_id="el_1")
    latest = await repo.latest_pending_auth_required(invocation_id="inv_1")
    assert latest is None


@pytest.mark.asyncio
async def test_double_finalize_returns_false(test_db):
    """Second finalize on a resolved row returns False."""
    repo = ElicitRepo(test_db)
    await repo.put(ElicitRow(
        id="el_1", invocation_id="inv_1", op="auth_required",
        payload_json='{}', status="pending",
    ))
    await repo.reserve(invocation_id="inv_1", elicit_id="el_1", lease_s=30)
    assert (await repo.finalize(invocation_id="inv_1", elicit_id="el_1")) is True
    assert (await repo.finalize(invocation_id="inv_1", elicit_id="el_1")) is False
