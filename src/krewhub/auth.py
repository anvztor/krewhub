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

    # Try cookie
    cookie_token = request.cookies.get("krew_session")
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


# Backward-compat alias
async def verify_api_key(
    api_key: str | None = Depends(_api_key_header),
    settings: Settings = Depends(get_settings),
) -> str:
    if not api_key or api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return api_key


# ---------------------------------------------------------------------------
# ABAC predicates (Auth track A2)
# ---------------------------------------------------------------------------
#
# require_bundle_owner is owned by Auth track A1; we ship a defensive
# stub here so A2 can be developed in parallel. When A1 merges, this
# stub should be replaced by A1's canonical implementation.
# REMOVE ON A1 MERGE.

async def require_bundle_owner(
    bundle_id: str,
    caller: "CallerContext",
    db,
):
    """Resolve a bundle and assert the caller owns it.

    A1 will replace this with the canonical predicate. For now we accept
    legacy bundles (owner_account_id NULL) when KREWHUB_KREW_DEV_FAKE_AUTH
    is set, and we accept the hardcoded `acc_legacy_apikey` to keep
    existing API-key flows working.
    """
    from krewhub.repositories.bundle_repo import BundleRepo
    bundle = await BundleRepo(db).get(bundle_id)
    if bundle is None:
        raise HTTPException(status_code=404, detail="Bundle not found")

    # Legacy bundles may not yet have owner_account_id populated.
    owner = getattr(bundle, "owner_account_id", None)
    if owner is None:
        # Fall back to created_by string for legacy ownership.
        if bundle.created_by != caller.account_id and caller.account_id != _LEGACY_ACCOUNT_ID:
            settings = get_settings()
            if not settings.krew_dev_fake_auth:
                raise HTTPException(status_code=403, detail="Not your bundle")
        return bundle

    if owner != caller.account_id:
        settings = get_settings()
        if not settings.krew_dev_fake_auth:
            raise HTTPException(status_code=403, detail="Not your bundle")
    return bundle


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
