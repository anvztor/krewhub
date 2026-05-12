"""Tests for require_bundle_owner ABAC predicate."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

from krewhub.auth import CallerContext, require_bundle_owner
from krewhub.db.connection import get_db


async def _seed_bundle(bundle_id: str, owner_account_id: str | None) -> None:
    db = await get_db()
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT OR IGNORE INTO cookbooks (id, name, owner_id, created_at) "
        "VALUES (?,?,?,?)",
        ("cb-test", "cb", "owner-x", now),
    )
    await db.execute(
        "INSERT INTO bundles "
        "(id, cookbook_id, prompt, status, created_by, created_at, "
        "owner_account_id) VALUES (?,?,?,?,?,?,?)",
        (bundle_id, "cb-test", "p", "open", "owner-x", now, owner_account_id),
    )
    await db.commit()


@pytest.mark.asyncio
async def test_require_bundle_owner_denies_cross_account():
    await _seed_bundle("b-cross", "alice")
    db = await get_db()
    caller = CallerContext(account_id="bob", auth_method="passkey")
    with pytest.raises(HTTPException) as exc:
        await require_bundle_owner("b-cross", caller, db)
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_require_bundle_owner_allows_owner():
    await _seed_bundle("b-allow", "alice")
    db = await get_db()
    caller = CallerContext(account_id="alice", auth_method="passkey")
    bundle = await require_bundle_owner("b-allow", caller, db)
    assert bundle.id == "b-allow"


@pytest.mark.asyncio
async def test_require_bundle_owner_404_for_unknown():
    db = await get_db()
    caller = CallerContext(account_id="alice", auth_method="passkey")
    with pytest.raises(HTTPException) as exc:
        await require_bundle_owner("does-not-exist", caller, db)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_require_bundle_owner_403_when_no_owner_set():
    """Bundles without an owner are not implicitly owned by the caller."""
    await _seed_bundle("b-noowner", None)
    db = await get_db()
    caller = CallerContext(account_id="alice", auth_method="passkey")
    with pytest.raises(HTTPException) as exc:
        await require_bundle_owner("b-noowner", caller, db)
    assert exc.value.status_code == 403
