"""Browser-facing auth routes for BFF elimination.

These endpoints handle OAuth callback, session cookies, and
user profile — replacing the BFF's auth proxy layer.
"""

from __future__ import annotations

import base64
import json
import logging
import uuid
from datetime import datetime, timezone

import aiosqlite
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel

from krewhub.auth import (
    CallerContext,
    require_bundle_owner,
    resolve_caller_or_cookie,
)
from krewhub.config import Settings, get_settings
from krewhub.db.connection import get_db
from krewhub.git.transport import ensure_bare_repo, resolve_repo_path
from krewhub.models import Cookbook, Recipe, RecipeMember
from krewhub.repositories.cookbook_repo import CookbookRepo
from krewhub.repositories.recipe_repo import RecipeRepo

logger = logging.getLogger(__name__)

router = APIRouter(tags=["auth_web"])


def _cookie_kwargs(settings: Settings) -> dict:
    """Build cookie kwargs from settings (immutable dict)."""
    result = {
        "httponly": True,
        "samesite": "lax",
        "secure": settings.cookie_secure,
        "path": "/",
        "max_age": 86400,
    }
    if settings.cookie_domain:
        return {**result, "domain": settings.cookie_domain}
    return result


@router.get("/auth/callback")
async def auth_callback(code: str, state: str = "") -> Response:  # noqa: ARG001
    """Exchange authorization code for token, set session cookie, redirect."""
    settings = get_settings()

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"{settings.krew_auth_url}/auth/token",
                json={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": settings.auth_redirect_uri,
                    "client_id": "cookrew",
                },
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("Token exchange failed: %s", exc.response.text)
            raise HTTPException(status_code=502, detail="Token exchange failed")
        except httpx.RequestError as exc:
            logger.error("Token exchange request error: %s", exc)
            raise HTTPException(status_code=502, detail="Auth service unreachable")

    token_data = resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise HTTPException(status_code=502, detail="No access_token in response")

    redirect_url = settings.app_origin
    response = RedirectResponse(url=redirect_url, status_code=302)
    response.set_cookie(key="krew_session", value=access_token, **_cookie_kwargs(settings))
    return response


@router.get("/me")
async def me(
    caller: CallerContext = Depends(resolve_caller_or_cookie),
) -> dict:
    """Track A1 contract: returns the resolved caller (cookie or bearer)."""
    return {
        "account_id": caller.account_id,
        "auth_method": caller.auth_method,
        "principal_type": caller.principal_type,
        "username": caller.username,
        "wallet_address": caller.wallet_address,
    }


@router.get("/auth/me")
async def auth_me(request: Request) -> JSONResponse:
    """Read session cookie and return decoded claims (no verification)."""
    token = request.cookies.get("krew_session")
    if not token:
        return JSONResponse(
            content={"authenticated": False},
            status_code=401,
        )

    try:
        # Decode JWT payload without verification (just reading claims)
        parts = token.split(".")
        if len(parts) < 2:
            raise ValueError("malformed JWT")
        # Add padding for base64
        payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
    except Exception:
        return JSONResponse(
            content={"authenticated": False},
            status_code=401,
        )

    return JSONResponse(content={
        "authenticated": True,
        "account_id": payload.get("sub"),
        "wallet_address": payload.get("wallet"),
        "auth_method": payload.get("method"),
        "username": payload.get("username"),
    })


@router.delete("/auth/me")
async def auth_logout() -> JSONResponse:
    """Clear session cookie."""
    settings = get_settings()
    response = JSONResponse(content={"ok": True})
    response.delete_cookie(
        key="krew_session",
        path="/",
        domain=settings.cookie_domain or None,
    )
    response.delete_cookie(
        key="krewauth_session",
        path="/",
        domain=settings.cookie_domain or None,
    )
    return response


@router.post("/auth/logout")
async def post_auth_logout(request: Request) -> JSONResponse:
    """Track A1 contract: log out via krewauth + clear local cookie."""
    settings = get_settings()
    cookie_token = (
        request.cookies.get("krewauth_session")
        or request.cookies.get("krew_session")
    )
    if cookie_token:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"{settings.krewauth_url}/oauth/logout",
                    cookies={"krewauth_session": cookie_token},
                )
        except Exception as exc:  # pragma: no cover - best-effort
            logger.warning("krewauth logout relay failed: %s", exc)
    response = JSONResponse(content={"detail": "logged_out"})
    response.delete_cookie(
        key="krewauth_session",
        path="/",
        domain=settings.cookie_domain or None,
    )
    response.delete_cookie(
        key="krew_session",
        path="/",
        domain=settings.cookie_domain or None,
    )
    return response


class SetUsernameRequest(BaseModel, frozen=True):
    username: str


@router.post("/auth/username")
async def set_username(body: SetUsernameRequest, request: Request) -> JSONResponse:
    """Proxy username-set to krewauth with cookie token."""
    token = request.cookies.get("krew_session")
    if not token:
        raise HTTPException(status_code=401, detail="No session cookie")

    settings = get_settings()

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"{settings.krew_auth_url}/auth/username/set",
                json={"username": body.username, "token": token},
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=exc.response.text,
            )
        except httpx.RequestError as exc:
            logger.error("Username set request error: %s", exc)
            raise HTTPException(status_code=502, detail="Auth service unreachable")

    data = resp.json()
    response = JSONResponse(content=data)
    new_token = data.get("token")
    if new_token:
        response.set_cookie(key="krew_session", value=new_token, **_cookie_kwargs(settings))
    return response


# ---------------------------------------------------------------------------
# Track A1: machine-code pairing ("Hire Agent")
# ---------------------------------------------------------------------------


class PairAgentRequest(BaseModel, frozen=True):
    user_code: str


@router.post("/bundles/{bundle_id}/pair-agent")
async def pair_agent(
    bundle_id: str,
    req: PairAgentRequest,
    caller: CallerContext = Depends(resolve_caller_or_cookie),
    db: aiosqlite.Connection = Depends(get_db),
) -> dict:
    """Inverted RFC 8628: relay user_code approval to krewauth on behalf of caller.

    Flow:
      1. ABAC: caller must own the bundle.
      2. Call krewauth /auth/device/approve-on-behalf with the service token
         and X-On-Behalf-Of: <caller.account_id>.
      3. Insert an agent_runtimes row, set bundles.default_agent_runtime_id.
    """
    bundle = await require_bundle_owner(bundle_id, caller, db)
    settings = get_settings()
    if not settings.krewauth_service_token:
        raise HTTPException(
            status_code=500, detail="krewauth_service_token_not_configured",
        )

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{settings.krewauth_url}/auth/device/approve-on-behalf",
                json={"user_code": req.user_code},
                headers={
                    "X-Service-Token": settings.krewauth_service_token,
                    "X-On-Behalf-Of": caller.account_id,
                },
            )
    except httpx.RequestError as exc:
        logger.error("krewauth pair relay failed: %s", exc)
        raise HTTPException(status_code=502, detail="auth_service_unreachable")

    if resp.status_code != 200:
        try:
            detail = resp.json().get("detail", "pair_failed")
        except Exception:
            detail = "pair_failed"
        raise HTTPException(status_code=resp.status_code, detail=detail)

    runtime_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    # agent_id semantically = bundle_id for v1 (one default agent per bundle)
    await db.execute(
        "INSERT INTO agent_runtimes "
        "(id, agent_id, account_id, daemon_version, provider, host_info, "
        "status, last_seen_at, started_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            runtime_id,
            bundle_id,
            caller.account_id,
            None,
            None,
            "{}",
            "online",
            now,
            now,
        ),
    )
    await db.execute(
        "UPDATE bundles SET default_agent_runtime_id = ?, "
        "resource_version = resource_version + 1 WHERE id = ?",
        (runtime_id, bundle_id),
    )
    await db.commit()
    return {
        "detail": "paired",
        "runtime_id": runtime_id,
        "bundle_id": bundle.id,
    }


@router.post("/agents/pair")
async def pair_agent_account(
    req: PairAgentRequest,
    caller: CallerContext = Depends(resolve_caller_or_cookie),
    db: aiosqlite.Connection = Depends(get_db),
) -> dict:
    """Account-scoped variant of /bundles/{id}/pair-agent.

    Same inverted RFC 8628 flow — relay user_code approval to krewauth
    on behalf of the caller, then create an agent_runtimes row owned
    by the caller's account. No bundle is required; the runtime is
    global to the user and can be assigned to any bundle later.

    Used by cookrew-beta's "Hire Agent" UI when no scratch bundle
    exists yet.
    """
    settings = get_settings()
    if not settings.krewauth_service_token:
        raise HTTPException(
            status_code=500, detail="krewauth_service_token_not_configured",
        )

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{settings.krewauth_url}/auth/device/approve-on-behalf",
                json={"user_code": req.user_code},
                headers={
                    "X-Service-Token": settings.krewauth_service_token,
                    "X-On-Behalf-Of": caller.account_id,
                },
            )
    except httpx.RequestError as exc:
        logger.error("krewauth pair relay failed: %s", exc)
        raise HTTPException(status_code=502, detail="auth_service_unreachable")

    if resp.status_code != 200:
        try:
            detail = resp.json().get("detail", "pair_failed")
        except Exception:
            detail = "pair_failed"
        raise HTTPException(status_code=resp.status_code, detail=detail)

    runtime_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    # No bundle binding — agent_id is a stable label tied to the account.
    await db.execute(
        "INSERT INTO agent_runtimes "
        "(id, agent_id, account_id, daemon_version, provider, host_info, "
        "status, last_seen_at, started_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            runtime_id,
            f"acct_{caller.account_id}",
            caller.account_id,
            None,
            None,
            "{}",
            "online",
            now,
            now,
        ),
    )
    await db.commit()
    return {
        "detail": "paired",
        "runtime_id": runtime_id,
    }


# ---------------------------------------------------------------------------
# Workspace bootstrap — first-time web users
# ---------------------------------------------------------------------------


@router.post("/api/v1/me/init-workspace")
async def init_workspace(
    caller: CallerContext = Depends(resolve_caller_or_cookie),
    db: aiosqlite.Connection = Depends(get_db),
) -> dict:
    """Idempotent: ensure the caller has at least one owned cookbook
    and one recipe inside it.

    First-time web users (those who land on beta.cookrew.dev BEFORE
    running ``krewcli login`` on their laptop) used to get stuck on
    "recipe still loading" because no cookbook existed for their
    account, and the SPA wouldn't show the empty-state CTA without a
    bundle. krewcli's auto-bootstrap (``_ensure_cookbook`` /
    ``_ensure_recipe``) already does the same work — this endpoint is
    the server-side equivalent, callable from the SPA.

    Reuses the existing entities (no duplicate creation) so calling it
    on every load is safe.
    """
    cookbook_repo = CookbookRepo(db)
    recipe_repo = RecipeRepo(db)

    # 1. Find or create the user's "my-cookbook".
    cookbook = await cookbook_repo.find_by_name_and_owner(
        "my-cookbook", caller.account_id,
    )
    if cookbook is None:
        # Init bare repo on disk so future git pushes have a target.
        # ensure_bare_repo is idempotent.
        repo_path = resolve_repo_path(caller.account_id, "my-cookbook")
        await ensure_bare_repo(repo_path)
        now = datetime.now(timezone.utc)
        cookbook = Cookbook(
            id=f"cb_{uuid.uuid4().hex[:8]}",
            name="my-cookbook",
            owner_id=caller.account_id,
            created_at=now,
        )
        cookbook = await cookbook_repo.create(cookbook, repo_path=str(repo_path))

    # 2. Find or create "my-recipe" inside it.
    existing = await recipe_repo.list_by_cookbook(cookbook.id)
    recipe = next((r for r in existing if r.name == "my-recipe"), None)
    if recipe is None:
        now = datetime.now(timezone.utc)
        recipe = Recipe(
            id=f"rec_{uuid.uuid4().hex[:8]}",
            name="my-recipe",
            repo_url="",
            default_branch="main",
            created_by=caller.account_id,
            created_at=now,
            cookbook_id=cookbook.id,
        )
        recipe = await recipe_repo.create(recipe)
        await recipe_repo.add_member(RecipeMember(
            id=f"mem_{uuid.uuid4().hex[:8]}",
            recipe_id=recipe.id,
            actor_id=caller.account_id,
            actor_type="human",
            role="owner",
            joined_at=now,
        ))

    return {
        "cookbook": cookbook.model_dump(mode="json"),
        "recipe": recipe.model_dump(mode="json"),
    }
