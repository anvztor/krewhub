"""Aggregate endpoints for BFF elimination.

These endpoints replace the BFF's multi-request aggregation by
querying krewhub repositories directly in a single round-trip.
"""

from __future__ import annotations

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException

from krewhub.auth import CallerContext, optional_cookie_caller
from krewhub.db.connection import get_db
from krewhub.services import aggregate_service

router = APIRouter(tags=["aggregate"])


@router.get("/cookbooks-data")
async def cookbooks_data(
    db: aiosqlite.Connection = Depends(get_db),
    _caller: CallerContext | None = Depends(optional_cookie_caller),
) -> dict:
    """List all cookbooks with recipe summaries."""
    return await aggregate_service.list_cookbook_data(db)


@router.get("/cookbooks/{cookbook_id}/detail")
async def cookbook_detail(
    cookbook_id: str,
    db: aiosqlite.Connection = Depends(get_db),
    _caller: CallerContext | None = Depends(optional_cookie_caller),
) -> dict:
    """Get cookbook detail with recipes, agents, and members."""
    result = await aggregate_service.get_cookbook_detail_data(db, cookbook_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Cookbook not found")
    return result


@router.get("/cookbooks/{cookbook_id}/workspace")
async def cookbook_workspace(
    cookbook_id: str,
    bundle_id: str | None = None,
    db: aiosqlite.Connection = Depends(get_db),
    _caller: CallerContext | None = Depends(optional_cookie_caller),
) -> dict:
    """Get workspace data for a cookbook."""
    result = await aggregate_service.get_workspace_data(
        db, cookbook_id, bundle_id,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Cookbook not found")
    return result


# Digest review + history endpoints removed in step (d) along with
# the rest of the digest layer. Bundles now collapse to OPEN|CLOSED
# without an approve/reject artifact; close events live in the
# event log if a historical view is needed later.
