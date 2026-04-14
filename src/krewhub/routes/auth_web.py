"""Browser-facing auth routes for BFF elimination.

These endpoints handle OAuth callback, session cookies, and
user profile — replacing the BFF's auth proxy layer.
"""

from __future__ import annotations

import base64
import json
import logging

import httpx
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel

from krewhub.config import Settings, get_settings

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
async def auth_callback(code: str, state: str = "") -> Response:
    """Exchange authorization code for token, set session cookie, redirect."""
    settings = get_settings()

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"{settings.krew_auth_url}/auth/token",
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": settings.auth_redirect_uri,
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

    redirect_url = state if state else settings.app_origin
    response = RedirectResponse(url=redirect_url, status_code=302)
    response.set_cookie(key="krew_session", value=access_token, **_cookie_kwargs(settings))
    return response


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
                headers={"Authorization": f"Bearer {token}"},
                json={"username": body.username},
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

    return JSONResponse(content=resp.json())
