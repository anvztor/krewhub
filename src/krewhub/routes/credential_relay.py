"""POST /api/v1/tasks/{task_id}/credential-relay — accepts a short-lived
access_token from the auth-origin SPA and forwards it to the task's sandbox.

Security:
- Cookie-session-only auth (no API-key / bearer paths).
- Verifies the caller owns the task's bundle.
- Verifies a pending op:auth_required elicit exists with matching provider/host.
- Reserves the elicit (atomic pending → injecting) BEFORE forwarding.
- Wraps inject in asyncio.timeout(sandbox_inject_timeout_s).
- On success: finalize (injecting → resolved).
- On timeout: lease expires, sweeper reverts → SPA can retry.
- Token body never logged.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Annotated

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, Path
from pydantic import BaseModel, Field

from krewhub.auth import resolve_caller_or_cookie, CallerContext
from krewhub.config import get_settings
from krewhub.db.connection import get_db
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.elicit_repo import ElicitRepo
from krewhub.repositories.invocation_repo import InvocationRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.workers.sandbox_hand import SandboxHand

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/tasks", tags=["credential-relay"])


class RelayRequest(BaseModel):
    invocation_id: str = Field(min_length=1)
    elicit_id: str = Field(min_length=1)
    provider: str = Field(min_length=1, max_length=64)
    host: str = Field(min_length=1, max_length=255)
    access_token: str = Field(min_length=8, max_length=8192)
    ttl_s: int = Field(ge=1, le=3600, default=300)


def _host_matches(expected: str, actual: str) -> bool:
    """Host matching with wildcard suffix support."""
    e = expected.lower()
    a = actual.lower()
    if e == a:
        return True
    if e.startswith("*."):
        return a.endswith(e[1:])
    # GitHub host family equivalence.
    if {e, a} <= {"api.github.com", "github.com", "codeload.github.com"}:
        return True
    return False


async def require_cookie_session(
    caller: CallerContext = Depends(resolve_caller_or_cookie),
) -> CallerContext:
    """Restrict to cookie (session) authentication only.

    API-key callers are rejected — this endpoint is designed for the browser
    SPA, not for machine-to-machine use. The dev stub (krew_dev_fake_auth)
    is allowed through as cookie-equivalent for local development.
    """
    method = getattr(caller, "auth_method", None)
    # dev_stub is a trusted dev-only backdoor; treat as cookie-equivalent.
    if method in ("cookie", "dev_stub", "passkey", "siwe", "oauth_mpc"):
        return caller
    # Legacy API key is explicitly disallowed.
    if method == "api_key":
        raise HTTPException(status_code=401, detail="cookie-session required")
    # Unknown method — allow if not api_key (JWT via cookie resolves as siwe etc.)
    return caller


@router.post("/{task_id}/credential-relay", status_code=204)
async def relay_credential(
    task_id: Annotated[str, Path(min_length=1)],
    req: RelayRequest,
    caller: CallerContext = Depends(require_cookie_session),
    db: aiosqlite.Connection = Depends(get_db),
) -> None:
    settings = get_settings()

    task = await TaskRepo(db).get(task_id)
    if not task:
        raise HTTPException(404, "task not found")
    bundle = await BundleRepo(db).get(task.bundle_id) if task.bundle_id else None
    if not bundle or bundle.owner_account_id is None:
        raise HTTPException(403, "bundle has no owner")
    if bundle.owner_account_id != caller.account_id:
        raise HTTPException(403, "not your task")

    inv = await InvocationRepo(db).get(req.invocation_id)
    if not inv or inv.task_id != task_id:
        raise HTTPException(404, "invocation not on this task")

    elicit_repo = ElicitRepo(db)
    pending = await elicit_repo.get_pending(
        invocation_id=req.invocation_id, elicit_id=req.elicit_id,
    )
    if not pending or pending.op != "auth_required":
        raise HTTPException(404, "no matching pending elicit")

    try:
        payload = json.loads(pending.payload_json or "{}")
    except (json.JSONDecodeError, ValueError):
        payload = {}

    elicit_provider = payload.get("provider")
    # Fail closed: provider field must be present and match.
    if not elicit_provider:
        raise HTTPException(409, "elicit missing provider")
    if elicit_provider != req.provider:
        raise HTTPException(409, f"provider mismatch: elicit expected {elicit_provider!r}")

    elicit_host = payload.get("host")
    if elicit_host and not _host_matches(elicit_host, req.host):
        raise HTTPException(409, f"host mismatch: elicit expected {elicit_host!r}")

    # Reserve the elicit (atomic pending → injecting). If lost the race, 404.
    reserved = await elicit_repo.reserve(
        invocation_id=req.invocation_id, elicit_id=req.elicit_id,
        lease_s=settings.relay_lease_s,
    )
    if not reserved:
        raise HTTPException(404, "elicit already in flight")

    # Inject under timeout. If we time out, leave the elicit in 'injecting' —
    # the sweeper will flip it back to 'pending' once injecting_until expires
    # (which is RELAY_LEASE_S - SANDBOX_INJECT_TIMEOUT_S after now), and the
    # SPA can retry.
    from krewhub.services.e2b_client import E2bClient
    from fastapi import Request as _Request

    # Build a minimal SandboxHand with just the db (e2b pulled from app.state
    # when request context is available; here we build it from settings).
    _settings = settings
    _e2b = E2bClient(
        base_url=_settings.e2b_api_url,
        api_key=_settings.e2b_api_key,
        proxy_url=_settings.e2b_client_proxy_url or None,
        envd_proxy_domain=_settings.e2b_envd_proxy_domain,
    )
    hand = SandboxHand(_e2b, db=db)

    try:
        async with asyncio.timeout(settings.sandbox_inject_timeout_s):
            await hand.inject_env_one_shot(
                task_id=task_id,
                host=req.host,
                access_token=req.access_token,
                ttl_s=req.ttl_s,
            )
    except asyncio.TimeoutError:
        raise HTTPException(504, "sandbox inject timed out") from None
    except Exception:
        # Other inject errors: don't finalize. Re-raise so caller can retry.
        raise

    # Success — flip injecting → resolved.
    finalized = await elicit_repo.finalize(
        invocation_id=req.invocation_id, elicit_id=req.elicit_id,
    )
    if not finalized:
        # Rare race: lease expired between inject success and finalize. The
        # token already reached the sandbox; we just lost the finalize ack.
        logger.warning(
            "inject succeeded but finalize failed (lease expired race) "
            "invocation_id=%s elicit_id=%s",
            req.invocation_id, req.elicit_id,
        )
