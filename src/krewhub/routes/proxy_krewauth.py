"""Proxy routes for krewauth session-keys and mint-ops.

These endpoints read the krew_session cookie and forward requests
to the krewauth service, eliminating the BFF proxy layer.
"""

from __future__ import annotations

import logging

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from krewhub.config import get_settings

logger = logging.getLogger(__name__)

router = APIRouter(tags=["proxy_krewauth"])


def _require_cookie(request: Request) -> str:
    """Extract krew_session cookie or raise 401."""
    token = request.cookies.get("krew_session")
    if not token:
        raise HTTPException(status_code=401, detail="No session cookie")
    return token


# ---------------------------------------------------------------------------
# Session keys
# ---------------------------------------------------------------------------


@router.get("/session-keys")
async def list_pending_session_keys(request: Request) -> JSONResponse:
    """Proxy pending session-key requests from krewauth."""
    token = _require_cookie(request)
    settings = get_settings()

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                f"{settings.krew_auth_url}/auth/session-keys/pending",
                params={"token": token},
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=exc.response.text,
            )
        except httpx.RequestError as exc:
            logger.error("Session-keys proxy error: %s", exc)
            raise HTTPException(status_code=502, detail="Auth service unreachable")

    return JSONResponse(content=resp.json())


class SessionKeyAction(BaseModel, frozen=True):
    action: str
    request_id: str


@router.post("/session-keys")
async def act_on_session_key(body: SessionKeyAction, request: Request) -> JSONResponse:
    """Proxy session-key approve/reject to krewauth."""
    token = _require_cookie(request)
    settings = get_settings()

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"{settings.krew_auth_url}/auth/session-keys/{body.action}",
                params={"request_id": body.request_id, "token": token},
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=exc.response.text,
            )
        except httpx.RequestError as exc:
            logger.error("Session-key action proxy error: %s", exc)
            raise HTTPException(status_code=502, detail="Auth service unreachable")

    return JSONResponse(content=resp.json())


# ---------------------------------------------------------------------------
# Mint ops
# ---------------------------------------------------------------------------


@router.get("/mint-ops")
async def list_pending_mint_ops(request: Request) -> JSONResponse:
    """Proxy pending mint operations from krewauth."""
    token = _require_cookie(request)
    settings = get_settings()

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                f"{settings.krew_auth_url}/auth/mint-ops/pending",
                params={"token": token},
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=exc.response.text,
            )
        except httpx.RequestError as exc:
            logger.error("Mint-ops proxy error: %s", exc)
            raise HTTPException(status_code=502, detail="Auth service unreachable")

    return JSONResponse(content=resp.json())


class MintOpConfirm(BaseModel, frozen=True):
    mint_id: str
    tx_hash: str


@router.post("/mint-ops")
async def confirm_mint_op(body: MintOpConfirm, request: Request) -> JSONResponse:
    """Proxy mint-op confirmation to krewauth."""
    token = _require_cookie(request)
    settings = get_settings()

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"{settings.krew_auth_url}/auth/mint-ops/confirm",
                params={
                    "mint_id": body.mint_id,
                    "token": token,
                    "tx_hash": body.tx_hash,
                },
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=exc.response.text,
            )
        except httpx.RequestError as exc:
            logger.error("Mint-op confirm proxy error: %s", exc)
            raise HTTPException(status_code=502, detail="Auth service unreachable")

    return JSONResponse(content=resp.json())
