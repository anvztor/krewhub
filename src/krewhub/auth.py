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


# Backward-compat alias
async def verify_api_key(
    api_key: str | None = Depends(_api_key_header),
    settings: Settings = Depends(get_settings),
) -> str:
    if not api_key or api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return api_key
