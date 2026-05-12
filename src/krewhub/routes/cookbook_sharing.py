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


async def _canonical_owner_account_id(
    cookbook,
    db: aiosqlite.Connection,
) -> str | None:
    """Resolve a cookbook's owner to a canonical account_id, walking
    the wallet→account graph when owner_id is wallet-flavored. Returns
    None if no resolution is possible.

    Used for checks that need the account-id form of the owner (e.g.
    self-share rejection), independent of who's calling.
    """
    owner_id = cookbook.owner_id
    if owner_id.startswith("acc_"):
        return owner_id
    if owner_id.startswith("0x"):
        cursor = await db.execute(
            "SELECT account_id FROM wallet_links WHERE wallet_address = ?",
            (owner_id,),
        )
        row = await cursor.fetchone()
        if row is not None:
            return row["account_id"]
    return None


async def _is_caller_cookbook_owner(
    cookbook,
    caller: CallerContext,
    db: aiosqlite.Connection,
) -> bool:
    """Decide whether the caller owns this cookbook.

    Cookbook.owner_id is historically not a canonical type — it can be:
      * an account_id ("acc_..."), the new convention
      * a wallet address ("0x..."), legacy from the wallet-only era
      * a free-form username, even earlier legacy

    To stay correct across that history, we accept ownership if any of
    the following match:
      1. owner_id == caller.account_id                  — new flow
      2. owner_id == caller.wallet_address              — wallet legacy,
                                                         caller has wallet
                                                         on the JWT
      3. owner_id == caller.username                    — username legacy
      4. wallet_links[owner_id].account_id == caller    — wallet legacy,
                                                         caller arrived
                                                         via a different
                                                         auth path
    Step (c) will backfill cookbook.owner_id to a canonical account_id
    so most of this fallback chain becomes dead code; until then,
    keep it tolerant.
    """
    owner_id = cookbook.owner_id
    if owner_id == caller.account_id:
        return True
    if caller.wallet_address and owner_id == caller.wallet_address:
        return True
    if caller.username and owner_id == caller.username:
        return True
    # Wallet → account graph: a cookbook owned by 0xABC belongs to
    # whichever account has that wallet linked.
    if owner_id.startswith("0x"):
        cursor = await db.execute(
            "SELECT account_id FROM wallet_links WHERE wallet_address = ?",
            (owner_id,),
        )
        row = await cursor.fetchone()
        if row is not None and row["account_id"] == caller.account_id:
            return True
    return False


async def _role_for_existing_cookbook(
    cookbook,
    caller: CallerContext,
    db: aiosqlite.Connection,
) -> ShareRole | None:
    """Compute caller's role on an already-loaded cookbook row.

    Order:
      1. caller is the cookbook owner (legacy-tolerant) → ShareRole.OWNER
      2. caller has an active cookbook_share → that share's role
      3. neither → None (no access)
    """
    if await _is_caller_cookbook_owner(cookbook, caller, db):
        return ShareRole.OWNER
    share = await CookbookShareRepo(db).get_active_for(
        cookbook.id, caller.account_id,
    )
    return share.role if share else None


async def _require_role(
    cookbook_id: str,
    caller: CallerContext,
    db: aiosqlite.Connection,
    minimum: ShareRole,
) -> ShareRole:
    """Enforce: caller has at least `minimum` on this cookbook.

    Raises:
      404 — cookbook doesn't exist (we don't existence-hide cookbook
            IDs; they're not sensitive)
      403 — cookbook exists but caller lacks the required role

    Returns the caller's actual role on success — handy for handlers
    that branch on it (e.g. members can read but only owners can
    mutate).
    """
    cookbook = await CookbookRepo(db).get(cookbook_id)
    if cookbook is None:
        raise HTTPException(status_code=404, detail="Cookbook not found")

    role = await _role_for_existing_cookbook(cookbook, caller, db)
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
    # _require_role above already verified the cookbook exists.
    cookbook = await CookbookRepo(db).get(cookbook_id)
    assert cookbook is not None  # _require_role would have 404'd

    # Resolve owner_id to a canonical account_id via the same graph
    # walk used in _is_caller_cookbook_owner, so a wallet-flavored
    # owner_id doesn't trick the self-share check.
    owner_account_id = await _canonical_owner_account_id(cookbook, db)
    if req.shared_with_account_id == owner_account_id:
        raise HTTPException(
            status_code=400,
            detail="Cookbook owner already has OWNER access implicitly",
        )

    share_repo = CookbookShareRepo(db)

    # An ACTIVE share (revoked_at IS NULL) blocks reshare with 409.
    # A REVOKED share is fine to shadow with a fresh row — the partial
    # unique index only covers active rows. Revoked entries stay for
    # audit history.
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
    except aiosqlite.IntegrityError as exc:
        # Race: another request inserted an active share for the same
        # (cookbook, account) between our get_active_for + create. The
        # partial unique index catches it; surface as 409.
        raise HTTPException(
            status_code=409,
            detail=f"Account {req.shared_with_account_id} already has access",
        ) from exc

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
