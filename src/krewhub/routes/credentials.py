"""Operator credential management routes.

Operator-facing surface for the just-in-time auth UX:

  POST /v1/credentials/paste
      Body: {host, env_var_name, token, invocation_id?}
      Stores the credential encrypted-at-rest, scoped to the caller's
      cookrew account. When `invocation_id` is set (the typical case
      triggered by the AuthRequiredCard popout), ALSO posts a success
      `ResultEnvelope` to that invocation so HumanHand's elicit resolves
      and the brain resumes.

  GET /v1/credentials
      Returns non-secret metadata about every credential the caller
      owns. Plaintext is NEVER returned by any GET.

  DELETE /v1/credentials/{host}
      Soft-deletes (archives) a credential.

`CredentialsService.get_envs(account_id)` is the only consumer of
plaintext and is wired into SandboxHand to inject env vars on op:exec.
"""
from __future__ import annotations

import logging

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from krewhub.auth import CallerContext, resolve_caller_or_cookie
from krewhub.config import get_settings
from krewhub.db.connection import get_db
from krewhub.models.invocation import ResultEnvelope
from krewhub.services.credentials_service import (
    CredentialsService,
    resolve_encryption_key,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["credentials"],
    dependencies=[Depends(resolve_caller_or_cookie)],
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class PasteCredentialRequest(BaseModel):
    host: str = Field(..., min_length=1, max_length=253,
                      description="Upstream hostname, e.g. api.github.com")
    env_var_name: str = Field(..., min_length=1, max_length=64,
                              description="Sandbox env var to populate, "
                              "e.g. GITHUB_TOKEN")
    token: str = Field(..., min_length=1,
                       description="Secret. NEVER logged.")
    invocation_id: str | None = Field(
        default=None,
        description="If set, auto-resolve the matching HumanHand elicit by "
                    "posting a success ResultEnvelope. Triggered from the "
                    "AuthRequiredCard popout.",
    )


class CredentialView(BaseModel):
    id: str
    host: str
    env_var_name: str
    created_at: str
    updated_at: str


class CredentialListResponse(BaseModel):
    credentials: list[CredentialView]


class EnvsResponse(BaseModel):
    """Plaintext env-var view of the caller's stored credentials.

    Returned only to the krewcli daemon (or an equivalently trusted
    caller) for injection into a brain subprocess's spawn env. The
    daemon merges these into the env passed to `Popen(claude/codex/
    gemini, ...)` so MCP servers (mcp__github__*, etc.) inherit
    them as `GITHUB_TOKEN`, `OPENAI_API_KEY`, etc.

    The Path-B credential leak surface: plaintext briefly lives in the
    daemon's process memory and the brain's env. Accepted trade for
    not running an in-line egress broker.
    """
    envs: dict[str, str]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


def _service(db: aiosqlite.Connection) -> CredentialsService:
    settings = get_settings()
    key = resolve_encryption_key(settings.credentials_encryption_key)
    return CredentialsService(db, key)


@router.post("/credentials/paste", response_model=CredentialView)
async def paste_credential(
    request: Request,
    body: PasteCredentialRequest,
    caller: CallerContext = Depends(resolve_caller_or_cookie),
    db: aiosqlite.Connection = Depends(get_db),
) -> CredentialView:
    svc = _service(db)
    row = await svc.put(
        account_id=caller.account_id,
        host=body.host.strip().lower(),
        env_var_name=body.env_var_name.strip(),
        plaintext=body.token,
    )
    logger.info(
        "credentials.paste account=%s host=%s env=%s",
        caller.account_id, row.host, row.env_var_name,
    )

    # Auto-resolve the elicit, if the operator came from an
    # AuthRequiredCard popout. We accept the brain's task to continue.
    if body.invocation_id:
        svc_inv = getattr(request.app.state, "invocations", None)
        if svc_inv is not None:
            try:
                await svc_inv.submit_result(
                    body.invocation_id,
                    ResultEnvelope(action="accept", content={"provisioned": True}),
                )
            except Exception as exc:  # pragma: no cover
                # Don't fail the paste if elicit resolution falls through;
                # the credential is stored and the brain can be retried
                # via the bundle's normal retry path.
                logger.warning(
                    "credentials.paste: failed to resolve elicit %s: %s",
                    body.invocation_id, exc,
                )

    return CredentialView(
        id=row.id, host=row.host, env_var_name=row.env_var_name,
        created_at=row.created_at, updated_at=row.updated_at,
    )


@router.get("/credentials", response_model=CredentialListResponse)
async def list_credentials(
    caller: CallerContext = Depends(resolve_caller_or_cookie),
    db: aiosqlite.Connection = Depends(get_db),
) -> CredentialListResponse:
    svc = _service(db)
    rows = await svc.list_for_account(caller.account_id)
    return CredentialListResponse(
        credentials=[
            CredentialView(
                id=r.id, host=r.host, env_var_name=r.env_var_name,
                created_at=r.created_at, updated_at=r.updated_at,
            )
            for r in rows
        ]
    )


@router.get("/credentials/envs", response_model=EnvsResponse)
async def get_credentials_envs(
    caller: CallerContext = Depends(resolve_caller_or_cookie),
    db: aiosqlite.Connection = Depends(get_db),
) -> EnvsResponse:
    """Return plaintext env-vars for the caller's stored credentials.

    Called by the krewcli daemon at brain-spawn time so the brain
    subprocess inherits GITHUB_TOKEN, OPENAI_API_KEY, etc. Re-uses
    cookie/bearer auth — the daemon's pairing token resolves to the
    same account that owns the bundle the brain is about to work on.
    """
    svc = _service(db)
    envs = await svc.get_envs(caller.account_id)
    logger.info(
        "credentials.envs account=%s keys=%s",
        caller.account_id, sorted(envs.keys()),
    )
    return EnvsResponse(envs=envs)


@router.delete("/credentials/{host:path}")
async def delete_credential(
    host: str,
    caller: CallerContext = Depends(resolve_caller_or_cookie),
    db: aiosqlite.Connection = Depends(get_db),
) -> dict:
    svc = _service(db)
    ok = await svc.archive(account_id=caller.account_id, host=host.lower())
    if not ok:
        raise HTTPException(status_code=404, detail="credential_not_found")
    return {"archived": True, "host": host}
