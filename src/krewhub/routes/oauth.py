"""OAuth ceremony routes for credential bootstrap.

The "Connect via GitHub" UX from the AuthRequiredCard popout. Flow:

  1. Brain hits Bad-credentials on a github operation; emits
     delegate({to:"human", input:{op:"auth_required", host:"api.github.com",
     env_var_name:"GITHUB_TOKEN", reason:...}}).
  2. cookrew-web renders AuthRequiredCard with two options. Operator
     clicks "Connect via GitHub".
  3. Card POSTs to /api/v1/oauth/github/start?invocation_id=X (this file).
     Returns {authorize_url} — a github.com URL with our client_id,
     requested scopes, and a signed state token.
  4. cookrew-web window.opens that URL. Operator's REAL browser handles
     the WebAuthn / 2FA / passkey ceremony in github.com's domain.
  5. github.com redirects operator's browser to /api/v1/oauth/github/callback
     with ?code&state (this file).
  6. Callback: verify state, exchange code → access_token via GitHub
     OAuth, store in vault, auto-resolve the invocation's elicit.
  7. Callback redirects browser back to cookrew-web success URL.

WebAuthn keys never leave the operator's device. Cookrew sees only
the derived OAuth access_token, which lands in the same vault the
paste path uses — so the brain's retry picks it up identically.
"""
from __future__ import annotations

import logging
import secrets
import time
import urllib.parse

import aiosqlite
import httpx
import jwt
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from krewhub.auth import CallerContext, resolve_caller_or_cookie
from krewhub.config import get_settings
from krewhub.db.connection import get_db
from krewhub.models.invocation import ResultEnvelope
from krewhub.services.credentials_service import (
    CredentialsService,
    resolve_encryption_key,
)

logger = logging.getLogger(__name__)


router = APIRouter(tags=["oauth"])


# State TTL: 10 min is plenty for the operator to consent + GitHub to
# redirect. Longer windows = bigger replay/CSRF risk.
_STATE_TTL_SECONDS = 10 * 60

# We sign state with the same passphrase we use for at-rest credential
# encryption. Both are server-side secrets with the same trust scope.
_STATE_ALGO = "HS256"


def _state_signing_key() -> str:
    settings = get_settings()
    key = resolve_encryption_key(settings.credentials_encryption_key)
    if not key:
        raise RuntimeError(
            "no credentials_encryption_key — cannot sign OAuth state"
        )
    # Append a domain-separator so a leaked encryption key doesn't
    # forge state tokens (and vice versa).
    return f"{key}::oauth_state_v1"


def _mint_state(*, invocation_id: str, account_id: str) -> str:
    return jwt.encode(
        {
            "iid": invocation_id,
            "acc": account_id,
            "nonce": secrets.token_hex(8),
            "iat": int(time.time()),
            "exp": int(time.time()) + _STATE_TTL_SECONDS,
        },
        _state_signing_key(),
        algorithm=_STATE_ALGO,
    )


def _verify_state(state: str) -> dict:
    try:
        return jwt.decode(state, _state_signing_key(), algorithms=[_STATE_ALGO])
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=400, detail=f"invalid_state: {exc}")


# ---------------------------------------------------------------------------
# /v1/oauth/github/start
# ---------------------------------------------------------------------------


class GitHubStartResponse(BaseModel):
    authorize_url: str
    state_ttl_seconds: int


@router.get("/oauth/github/start", response_model=GitHubStartResponse)
async def github_start(
    invocation_id: str = Query(..., min_length=1),
    caller: CallerContext = Depends(resolve_caller_or_cookie),
) -> GitHubStartResponse:
    settings = get_settings()
    if not settings.github_oauth_client_id:
        raise HTTPException(
            status_code=503,
            detail="github_oauth_not_configured: missing client_id",
        )
    state = _mint_state(
        invocation_id=invocation_id,
        account_id=caller.account_id,
    )
    callback_url = f"{settings.public_url.rstrip('/')}/api/v1/oauth/github/callback"
    params = {
        "client_id": settings.github_oauth_client_id,
        "redirect_uri": callback_url,
        # repo: read+write repo content (for git push). user: profile.
        "scope": "repo read:user",
        "state": state,
        "allow_signup": "false",
    }
    authorize_url = (
        "https://github.com/login/oauth/authorize?" + urllib.parse.urlencode(params)
    )
    return GitHubStartResponse(
        authorize_url=authorize_url,
        state_ttl_seconds=_STATE_TTL_SECONDS,
    )


# ---------------------------------------------------------------------------
# /v1/oauth/github/callback
# ---------------------------------------------------------------------------


@router.get("/oauth/github/callback")
async def github_callback(
    request: Request,
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
    error_description: str | None = Query(default=None),
    db: aiosqlite.Connection = Depends(get_db),
):
    settings = get_settings()

    # User declined consent → redirect back with an error param so the
    # cookrew-web popup can show "auth cancelled" and the operator can
    # fall back to paste.
    if error:
        target = (
            f"{settings.web_url.rstrip('/')}/oauth-result"
            f"?provider=github&status=denied&reason={urllib.parse.quote(error)}"
        )
        return RedirectResponse(target)

    if not code or not state:
        raise HTTPException(
            status_code=400, detail="missing code or state",
        )

    claims = _verify_state(state)
    invocation_id = claims["iid"]
    account_id = claims["acc"]

    # Exchange code → access_token. GitHub expects form-encoded body and
    # accepts Accept: application/json to get a JSON response.
    token_url = "https://github.com/login/oauth/access_token"
    try:
        async with httpx.AsyncClient(timeout=15.0) as cl:
            r = await cl.post(
                token_url,
                data={
                    "client_id": settings.github_oauth_client_id,
                    "client_secret": settings.github_oauth_client_secret,
                    "code": code,
                    "redirect_uri": (
                        f"{settings.public_url.rstrip('/')}/api/v1/oauth/github/callback"
                    ),
                },
                headers={"Accept": "application/json"},
            )
    except Exception as exc:
        logger.exception("github oauth token exchange failed: %s", exc)
        target = (
            f"{settings.web_url.rstrip('/')}/oauth-result"
            f"?provider=github&status=error&reason=token_exchange_network"
        )
        return RedirectResponse(target)

    if r.status_code != 200:
        logger.warning(
            "github oauth token exchange returned %s: %s",
            r.status_code, r.text[:200],
        )
        target = (
            f"{settings.web_url.rstrip('/')}/oauth-result"
            f"?provider=github&status=error&reason=token_exchange_{r.status_code}"
        )
        return RedirectResponse(target)

    body = r.json()
    access_token = body.get("access_token")
    if not access_token:
        # GitHub returns 200 with an error key when, e.g., the code is bad.
        err = body.get("error") or "no_access_token"
        logger.warning("github oauth response missing access_token: %s", body)
        target = (
            f"{settings.web_url.rstrip('/')}/oauth-result"
            f"?provider=github&status=error&reason={urllib.parse.quote(str(err))}"
        )
        return RedirectResponse(target)

    # Store in vault under the canonical env_var_name.
    cred_key = resolve_encryption_key(settings.credentials_encryption_key)
    svc = CredentialsService(db, cred_key)
    await svc.put(
        account_id=account_id,
        host="api.github.com",
        env_var_name="GITHUB_TOKEN",
        plaintext=access_token,
    )
    logger.info(
        "github oauth: stored credential account=%s scopes=%s",
        account_id, body.get("scope"),
    )

    # Auto-resolve the brain's elicit so the task resumes immediately.
    inv_svc = getattr(request.app.state, "invocations", None)
    if inv_svc is not None:
        try:
            await inv_svc.submit_result(
                invocation_id,
                ResultEnvelope(
                    action="accept",
                    content={"provisioned": True, "method": "github_oauth"},
                ),
            )
        except Exception as exc:
            logger.warning(
                "github oauth: failed to resolve invocation %s: %s",
                invocation_id, exc,
            )

    target = (
        f"{settings.web_url.rstrip('/')}/oauth-result"
        f"?provider=github&status=ok"
    )
    return RedirectResponse(target)
