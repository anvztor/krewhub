"""KrewHub auth — relying party for krewauth.

krewhub no longer issues tokens. It verifies ES256 JWTs from krewauth
via JWKS, and falls back to legacy HS256 + API key during migration.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

import jwt
from jwt import PyJWKClient
from fastapi import Depends, HTTPException, Request
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from krewhub.config import Settings, get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------


class CallerContext(BaseModel, frozen=True):
    """Per-request identity resolved by middleware."""

    account_id: str
    username: str | None = None
    principal_type: Literal["human", "agent"] = "human"

    wallet_address: str | None = None
    session_id: str | None = None

    acting_as: Literal["human", "agent"] = "human"
    acting_agent_id: int | None = None

    auth_method: str = "api_key"
    machine_key_thumbprint: str | None = None


# ---------------------------------------------------------------------------
# JWKS client (fetches public key from krewauth)
# ---------------------------------------------------------------------------

_jwk_client: PyJWKClient | None = None


def init_jwk_client(jwks_url: str) -> None:
    global _jwk_client
    _jwk_client = PyJWKClient(jwks_url, cache_keys=True, lifespan=3600)
    logger.info("JWKS client initialized: %s", jwks_url)


def _decode_es256(token: str) -> dict:
    """Decode an ES256 JWT using the JWKS public key."""
    if _jwk_client is None:
        raise RuntimeError("JWKS client not initialized")
    signing_key = _jwk_client.get_signing_key_from_jwt(token)
    return jwt.decode(
        token,
        signing_key.key,
        algorithms=["ES256"],
        issuer="krewauth",
    )


def _decode_hs256(token: str, secret: str) -> dict:
    """Decode a legacy HS256 JWT (transitional)."""
    return jwt.decode(token, secret, algorithms=["HS256"])


# ---------------------------------------------------------------------------
# FastAPI dependencies
# ---------------------------------------------------------------------------

_bearer_scheme = HTTPBearer(auto_error=False)
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

_LEGACY_ACCOUNT_ID = "acc_legacy_apikey"


async def resolve_caller(
    request: Request,
    bearer: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
    api_key: str | None = Depends(_api_key_header),
    settings: Settings = Depends(get_settings),
) -> CallerContext:
    """Resolve caller identity from request.

    Priority: Bearer JWT (ES256 from krewauth, or legacy HS256) > X-API-Key.
    """

    if bearer is not None:
        token = bearer.credentials
        payload: dict[str, Any] | None = None

        # Try ES256 (krewauth) first
        if _jwk_client is not None:
            try:
                payload = _decode_es256(token)
            except Exception:
                pass  # fall through to HS256

        # Fall back to legacy HS256
        if payload is None and settings.jwt_secret:
            try:
                payload = _decode_hs256(token, settings.jwt_secret)
            except jwt.ExpiredSignatureError:
                raise HTTPException(status_code=401, detail="Session expired")
            except jwt.InvalidTokenError:
                pass

        if payload is None:
            raise HTTPException(status_code=401, detail="Invalid session token")

        sub = payload["sub"]
        method = payload.get("method", "siwe")
        wallet = payload.get("wallet")

        # Backward compat: old tokens have sub=wallet_address (0x...)
        if sub.startswith("0x"):
            wallet = sub
            account_id = sub  # best effort, no DB lookup in relying party mode
        else:
            account_id = sub

        # Agent mode: act claim (RFC 8693) or X-Acting-As header
        acting_as: Literal["human", "agent"] = "human"
        acting_agent_id: int | None = None

        act = payload.get("act")
        if act and isinstance(act, dict):
            acting_as = "agent"
            acting_agent_id = act.get("erc8004_id")
        else:
            acting_header = request.headers.get("X-Acting-As")
            if acting_header and acting_header.startswith("agent:"):
                try:
                    acting_agent_id = int(acting_header.split(":", 1)[1])
                    acting_as = "agent"
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail="X-Acting-As must be 'agent:<erc8004_agent_id>'",
                    )

        return CallerContext(
            account_id=account_id,
            username=payload.get("username"),
            principal_type="agent" if acting_as == "agent" else "human",
            wallet_address=wallet,
            session_id=payload.get("sid"),
            acting_as=acting_as,
            acting_agent_id=acting_agent_id,
            auth_method=method,
            machine_key_thumbprint=(payload.get("cnf") or {}).get("jkt"),
        )

    # Legacy API key
    if api_key and api_key == settings.api_key:
        return CallerContext(
            account_id=_LEGACY_ACCOUNT_ID,
            auth_method="api_key",
        )

    raise HTTPException(status_code=401, detail="Missing or invalid credentials")


# ---------------------------------------------------------------------------
# Cookie-based auth (BFF elimination)
# ---------------------------------------------------------------------------


def _decode_jwt_token(token: str, settings: Settings) -> dict[str, Any] | None:
    """Try ES256 then HS256 to decode a JWT. Return payload or None."""
    payload: dict[str, Any] | None = None

    if _jwk_client is not None:
        try:
            payload = _decode_es256(token)
        except Exception:
            pass

    if payload is None and settings.jwt_secret:
        try:
            payload = _decode_hs256(token, settings.jwt_secret)
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Session expired")
        except jwt.InvalidTokenError:
            pass

    return payload


def _caller_from_payload(payload: dict[str, Any], request: Request) -> CallerContext:
    """Build a CallerContext from a decoded JWT payload."""
    sub = payload["sub"]
    method = payload.get("method", "siwe")
    wallet = payload.get("wallet")

    if sub.startswith("0x"):
        wallet = sub
        account_id = sub
    else:
        account_id = sub

    acting_as: Literal["human", "agent"] = "human"
    acting_agent_id: int | None = None

    act = payload.get("act")
    if act and isinstance(act, dict):
        acting_as = "agent"
        acting_agent_id = act.get("erc8004_id")
    else:
        acting_header = request.headers.get("X-Acting-As")
        if acting_header and acting_header.startswith("agent:"):
            try:
                acting_agent_id = int(acting_header.split(":", 1)[1])
                acting_as = "agent"
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="X-Acting-As must be 'agent:<erc8004_agent_id>'",
                )

    return CallerContext(
        account_id=account_id,
        username=payload.get("username"),
        principal_type="agent" if acting_as == "agent" else "human",
        wallet_address=wallet,
        session_id=payload.get("sid"),
        acting_as=acting_as,
        acting_agent_id=acting_agent_id,
        auth_method=method,
        machine_key_thumbprint=(payload.get("cnf") or {}).get("jkt"),
    )


async def resolve_caller_or_cookie(
    request: Request,
    bearer: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
    api_key: str | None = Depends(_api_key_header),
    settings: Settings = Depends(get_settings),
) -> CallerContext:
    """Resolve caller from Bearer JWT, API key, or krew_session cookie.

    Priority: Bearer JWT > X-API-Key > Cookie.
    """
    # Auth track A2 dev escape hatch — when KREWHUB_KREW_DEV_FAKE_AUTH=1,
    # bypass all credential checks and resolve every request as
    # `dev-user-1`. Used while Auth track A1 (passkey + cookie) is still
    # in flight so A2 can be exercised end-to-end without krewauth.
    if settings.krew_dev_fake_auth:
        return CallerContext(
            account_id="dev-user-1",
            username="dev-user-1",
            principal_type="human",
            auth_method="dev_stub",
        )

    # Try Bearer JWT
    if bearer is not None:
        payload = _decode_jwt_token(bearer.credentials, settings)
        if payload is None:
            raise HTTPException(status_code=401, detail="Invalid session token")
        return _caller_from_payload(payload, request)

    # Try API key
    if api_key and api_key == settings.api_key:
        return CallerContext(
            account_id=_LEGACY_ACCOUNT_ID,
            auth_method="api_key",
        )

    # Try cookie (Track A1: krewauth_session is canonical; krew_session legacy)
    cookie_token = (
        request.cookies.get("krewauth_session")
        or request.cookies.get("krew_session")
    )
    if cookie_token:
        payload = _decode_jwt_token(cookie_token, settings)
        if payload is None:
            raise HTTPException(status_code=401, detail="Invalid session cookie")
        return _caller_from_payload(payload, request)

    raise HTTPException(status_code=401, detail="Missing or invalid credentials")


async def optional_cookie_caller(
    request: Request,
    bearer: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
    api_key: str | None = Depends(_api_key_header),
    settings: Settings = Depends(get_settings),
) -> CallerContext | None:
    """Like resolve_caller_or_cookie but returns None instead of 401."""
    try:
        return await resolve_caller_or_cookie(request, bearer, api_key, settings)
    except HTTPException:
        return None


# ---------------------------------------------------------------------------
# ABAC predicates
# ---------------------------------------------------------------------------


async def require_bundle_owner(
    bundle_id: str, caller: CallerContext, db,
):
    """Allow only the bundle owner. Returns the Bundle.

    Auth track ABAC predicate. Imported by both A1 and A2 routes.

    Resolution order:
      1. owner_account_id matches caller.account_id → allow
      2. owner_account_id is NULL (legacy bundle, backfill missed) →
         fall back to created_by match
      3. Legacy api-key sentinel (`acc_legacy_apikey`) is allowed for
         backward compatibility with existing service integrations
      4. Otherwise → 403
    """
    from krewhub.repositories.bundle_repo import BundleRepo

    bundle = await BundleRepo(db).get(bundle_id)
    if bundle is None:
        raise HTTPException(status_code=404, detail="Bundle not found")

    if caller.account_id == _LEGACY_ACCOUNT_ID:
        return bundle

    owner = bundle.owner_account_id
    if owner is None:
        if bundle.created_by == caller.account_id:
            return bundle
        raise HTTPException(status_code=403, detail="Not your bundle")

    if owner != caller.account_id:
        raise HTTPException(status_code=403, detail="Not your bundle")
    return bundle


# Backward-compat alias
async def verify_api_key(
    api_key: str | None = Depends(_api_key_header),
    settings: Settings = Depends(get_settings),
) -> str:
    if not api_key or api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return api_key


async def is_assigned_runtime(
    caller: "CallerContext",
    task,
    db,
) -> bool:
    """Return True iff the caller owns the runtime assigned to a task."""
    runtime_id = getattr(task, "assigned_runtime_id", None)
    if runtime_id is None:
        return False
    cursor = await db.execute(
        "SELECT account_id FROM agent_runtimes WHERE id = ?",
        (runtime_id,),
    )
    row = await cursor.fetchone()
    return bool(row and row["account_id"] == caller.account_id)


async def authorize_task_mutation(
    caller: "CallerContext",
    task,
    db,
) -> None:
    """Authorize a write against a task, or raise 403.

    A task may be mutated by exactly three classes of caller:
      1. legacy API-key integrations — they predate the auth journey and
         are trusted service-to-service callers (mirrors require_bundle_owner)
      2. the agent runtime assigned to the task — the agent doing the work
         posts events / status / completion for its own task
      3. the owner of the bundle the task belongs to — the operator who
         created the bundle (cancel / edit / follow-up / HITL answer)

    Anyone else authenticated gets 403 (not 401 — they ARE authenticated,
    they just don't own this task). Legacy-tolerant like
    require_bundle_owner: a bundle whose owner_account_id was never
    backfilled falls back to created_by.
    """
    # (1) legacy API-key service integrations
    if caller.account_id == _LEGACY_ACCOUNT_ID or caller.auth_method == "api_key":
        return

    # (2) the runtime assigned to this task
    if await is_assigned_runtime(caller, task, db):
        return

    # (3) the bundle owner
    from krewhub.repositories.bundle_repo import BundleRepo

    bundle_id = getattr(task, "bundle_id", None)
    bundle = await BundleRepo(db).get(bundle_id) if bundle_id else None
    if bundle is None:
        raise HTTPException(status_code=403, detail="Not authorized for this task")

    owner = bundle.owner_account_id
    if owner is None:
        # Legacy bundle, owner backfill missed → fall back to created_by.
        if bundle.created_by == caller.account_id:
            return
        raise HTTPException(status_code=403, detail="Not authorized for this task")
    if owner != caller.account_id:
        raise HTTPException(status_code=403, detail="Not authorized for this task")


def _agent_owner_identity(caller: "CallerContext") -> str:
    """The identity an agent presence row is owned by.

    register_agent stamps owner_username = caller.username or
    caller.account_id; this mirrors that so the ownership comparison is
    symmetric.
    """
    return caller.username or caller.account_id


async def authorize_agent_mutation(caller: "CallerContext", presence) -> None:
    """Anti-hijack guard for agent-presence writes, or raise 403.

    Only the agent's owner (or a legacy API-key integration) may mutate an
    existing presence. Two cases are deliberately permitted so live flows
    keep working:
      * presence is None        — brand-new registration (no owner yet)
      * owner_username is None   — unclaimed legacy row (pre-ownership)
    """
    if caller.account_id == _LEGACY_ACCOUNT_ID or caller.auth_method == "api_key":
        return
    if presence is None:
        return
    owner = getattr(presence, "owner_username", None)
    if owner is None or owner == _agent_owner_identity(caller):
        return
    raise HTTPException(status_code=403, detail="Not your agent")
