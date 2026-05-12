"""Cookbook sharing + repo grants — the new RBAC + permission surface
that replaces RecipeMember and the implicit recipe-as-repo-binding.

These endpoints live alongside cookbooks because both shares and
grants are cookbook-scoped:
  - shares  → other accounts can act inside this cookbook
  - grants  → which repos this cookbook (and everyone it's shared with)
              is authorized to materialize at JIT clone time
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException

from krewhub.auth import CallerContext, resolve_caller_or_cookie
from krewhub.db.connection import get_db
from krewhub.models import (
    CookbookShare,
    RepoGrant,
    RepoProvider,
    ShareRole,
)
from krewhub.repositories.cookbook_repo import CookbookRepo
from krewhub.repositories.cookbook_share_repo import CookbookShareRepo
from krewhub.repositories.repo_grant_repo import RepoGrantRepo
from krewhub.routes.schemas import (
    CreateCookbookShareRequest,
    CreateRepoGrantRequest,
    UpdateCookbookShareRequest,
)

router = APIRouter(
    tags=["cookbook-sharing"],
    dependencies=[Depends(resolve_caller_or_cookie)],
)


# --------------------------------------------------------------------------
# RBAC helpers
# --------------------------------------------------------------------------


# Role ranks for "at least this much" checks.
# OWNER > MEMBER > VIEWER. Higher = more authority.
_RANK = {
    ShareRole.OWNER: 3,
    ShareRole.MEMBER: 2,
    ShareRole.VIEWER: 1,
}


async def _resolve_caller_role(
    cookbook_id: str, caller: CallerContext, db: aiosqlite.Connection,
) -> ShareRole | None:
    """Return the caller's effective role on this cookbook, or None.

    Order:
      1. caller is the cookbook owner (owner_id) → ShareRole.OWNER
      2. caller has an active cookbook_share → that share's role
      3. neither → None (no access)
    """
    cookbook = await CookbookRepo(db).get(cookbook_id)
    if cookbook is None:
        return None
    if cookbook.owner_id == caller.account_id:
        return ShareRole.OWNER
    share = await CookbookShareRepo(db).get_active_for(
        cookbook_id, caller.account_id,
    )
    return share.role if share else None


async def _require_role(
    cookbook_id: str,
    caller: CallerContext,
    db: aiosqlite.Connection,
    minimum: ShareRole,
) -> ShareRole:
    """Raise 403 unless the caller has at least `minimum` on this cookbook.

    Returns the caller's actual role on success — handy for handlers
    that branch on it (e.g. members can read but only owners can
    mutate).
    """
    role = await _resolve_caller_role(cookbook_id, caller, db)
    if role is None or _RANK[role] < _RANK[minimum]:
        raise HTTPException(
            status_code=403,
            detail=f"Cookbook {cookbook_id}: requires at least {minimum.value}",
        )
    return role


# --------------------------------------------------------------------------
# Cookbook shares
# --------------------------------------------------------------------------


@router.post("/cookbooks/{cookbook_id}/shares")
async def create_share(
    cookbook_id: str,
    req: CreateCookbookShareRequest,
    db: aiosqlite.Connection = Depends(get_db),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    """Share this cookbook with another account.

    Only the cookbook OWNER may invite others. The cookbook creator's
    OWNER role is implicit (owner_id column); shares can grant OWNER
    too but the original owner cannot be removed.
    """
    await _require_role(cookbook_id, caller, db, ShareRole.OWNER)

    try:
        role = ShareRole(req.role)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role {req.role!r}; expected owner|member|viewer",
        )

    # Reject self-share (would just duplicate the implicit owner row).
    cookbook = await CookbookRepo(db).get(cookbook_id)
    if cookbook is None:
        raise HTTPException(status_code=404, detail="Cookbook not found")
    if req.shared_with_account_id == cookbook.owner_id:
        raise HTTPException(
            status_code=400,
            detail="Cookbook owner already has OWNER access implicitly",
        )

    share_repo = CookbookShareRepo(db)

    # If a share already exists (active or revoked), surface a conflict
    # rather than silently double-inserting (UNIQUE constraint would
    # raise an opaque IntegrityError otherwise).
    existing = await share_repo.get_active_for(
        cookbook_id, req.shared_with_account_id,
    )
    if existing is not None:
        raise HTTPException(
            status_code=409,
            detail=f"Account {req.shared_with_account_id} already has access",
        )

    share = CookbookShare(
        id=f"shr_{uuid.uuid4().hex[:8]}",
        cookbook_id=cookbook_id,
        shared_with_account_id=req.shared_with_account_id,
        role=role,
        shared_by_account_id=caller.account_id,
        shared_at=datetime.now(timezone.utc),
    )
    try:
        share = await share_repo.create(share)
    except aiosqlite.IntegrityError:
        # Race: a revoked row with the same (cookbook, account) pair
        # blocks INSERT under UNIQUE. Surface as conflict instead of 500.
        raise HTTPException(
            status_code=409,
            detail="Share already exists (possibly revoked) — un-revoke instead",
        )

    return {"share": share.model_dump(mode="json")}


@router.get("/cookbooks/{cookbook_id}/shares")
async def list_shares(
    cookbook_id: str,
    db: aiosqlite.Connection = Depends(get_db),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    """List active shares on this cookbook. Any member can read."""
    await _require_role(cookbook_id, caller, db, ShareRole.VIEWER)
    shares = await CookbookShareRepo(db).list_by_cookbook(cookbook_id)
    return {"shares": [s.model_dump(mode="json") for s in shares]}


@router.patch("/cookbooks/{cookbook_id}/shares/{share_id}")
async def update_share(
    cookbook_id: str,
    share_id: str,
    req: UpdateCookbookShareRequest,
    db: aiosqlite.Connection = Depends(get_db),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    """Change a share's role. OWNER only."""
    await _require_role(cookbook_id, caller, db, ShareRole.OWNER)

    try:
        role = ShareRole(req.role)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role {req.role!r}; expected owner|member|viewer",
        )

    share_repo = CookbookShareRepo(db)
    existing = await share_repo.get(share_id)
    if existing is None or existing.cookbook_id != cookbook_id:
        raise HTTPException(status_code=404, detail="Share not found")
    if existing.revoked_at is not None:
        raise HTTPException(
            status_code=409, detail="Cannot modify a revoked share",
        )

    updated = await share_repo.update_role(share_id, role)
    if updated is None:
        # Race — got revoked between the get + update. Treat as 404.
        raise HTTPException(status_code=404, detail="Share not found")
    return {"share": updated.model_dump(mode="json")}


@router.delete("/cookbooks/{cookbook_id}/shares/{share_id}")
async def revoke_share(
    cookbook_id: str,
    share_id: str,
    db: aiosqlite.Connection = Depends(get_db),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    """Revoke a share (soft-delete). OWNER only.

    Existing bundles/working trees stay alive — revocation only blocks
    new access. That's intentional: we don't kill mid-flight work.
    """
    await _require_role(cookbook_id, caller, db, ShareRole.OWNER)

    share_repo = CookbookShareRepo(db)
    existing = await share_repo.get(share_id)
    if existing is None or existing.cookbook_id != cookbook_id:
        raise HTTPException(status_code=404, detail="Share not found")
    if existing.revoked_at is not None:
        return {"share": existing.model_dump(mode="json"), "already_revoked": True}

    revoked = await share_repo.revoke(share_id, at=datetime.now(timezone.utc))
    return {"share": revoked.model_dump(mode="json") if revoked else None}


# --------------------------------------------------------------------------
# Repo grants
# --------------------------------------------------------------------------


@router.post("/cookbooks/{cookbook_id}/repo-grants")
async def create_grant(
    cookbook_id: str,
    req: CreateRepoGrantRequest,
    db: aiosqlite.Connection = Depends(get_db),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    """Authorize a repo scope on this cookbook. OWNER only.

    token_ref must point at an already-stored secret (vault key, KMS
    arn, etc). krewhub does not accept raw tokens here; the OAuth /
    PAT exchange flow runs separately and writes the secret out-of-band
    before calling this endpoint with the resulting reference.
    """
    await _require_role(cookbook_id, caller, db, ShareRole.OWNER)

    try:
        provider = RepoProvider(req.provider)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid provider {req.provider!r}; "
                   "expected github|gitlab|bitbucket",
        )

    if not req.scope.strip():
        raise HTTPException(status_code=400, detail="scope cannot be empty")
    if not req.token_ref.strip():
        raise HTTPException(
            status_code=400,
            detail="token_ref cannot be empty (must reference a stored secret)",
        )

    grant = RepoGrant(
        id=f"grt_{uuid.uuid4().hex[:8]}",
        cookbook_id=cookbook_id,
        provider=provider,
        scope=req.scope.strip(),
        token_ref=req.token_ref.strip(),
        granted_by_account_id=caller.account_id,
        granted_at=datetime.now(timezone.utc),
    )
    grant = await RepoGrantRepo(db).create(grant)
    return {"grant": grant.model_dump(mode="json")}


@router.get("/cookbooks/{cookbook_id}/repo-grants")
async def list_grants(
    cookbook_id: str,
    include_revoked: bool = False,
    db: aiosqlite.Connection = Depends(get_db),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    """List repo grants on this cookbook. MEMBER+ may read (so agents
    inside the cookbook can discover what's available); VIEWER cannot
    — viewing the grant list would reveal token_refs."""
    await _require_role(cookbook_id, caller, db, ShareRole.MEMBER)
    grants = await RepoGrantRepo(db).list_by_cookbook(
        cookbook_id, include_revoked=include_revoked,
    )
    return {"grants": [g.model_dump(mode="json") for g in grants]}


@router.delete("/cookbooks/{cookbook_id}/repo-grants/{grant_id}")
async def revoke_grant(
    cookbook_id: str,
    grant_id: str,
    db: aiosqlite.Connection = Depends(get_db),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
):
    """Revoke a repo grant. OWNER only.

    Mid-flight working trees are NOT torn down — revocation only blocks
    future JIT materializations. The sandbox reaper handles cleanup
    when bundles eventually close.
    """
    await _require_role(cookbook_id, caller, db, ShareRole.OWNER)

    grant_repo = RepoGrantRepo(db)
    existing = await grant_repo.get(grant_id)
    if existing is None or existing.cookbook_id != cookbook_id:
        raise HTTPException(status_code=404, detail="Grant not found")
    if existing.revoked_at is not None:
        return {"grant": existing.model_dump(mode="json"), "already_revoked": True}

    revoked = await grant_repo.revoke(grant_id, at=datetime.now(timezone.utc))
    return {"grant": revoked.model_dump(mode="json") if revoked else None}
