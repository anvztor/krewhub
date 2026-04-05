"""Git smart HTTP protocol endpoints.

Implements the 3 endpoints needed for git push/pull:
  GET  /{owner}/{repo}/info/refs?service=...   (ref discovery)
  POST /{owner}/{repo}/git-receive-pack        (push)
  POST /{owner}/{repo}/git-upload-pack         (pull/clone)
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from krewhub.db.connection import get_db
from krewhub.git.transport import (
    VALID_SERVICES,
    ensure_bare_repo,
    resolve_repo_path,
    run_git_service,
)
from krewhub.git.indexer import index_cookbook

logger = logging.getLogger(__name__)

router = APIRouter(tags=["git"])


def _pkt_line(data: str) -> bytes:
    """Encode a git pkt-line."""
    encoded = data.encode()
    length = len(encoded) + 4
    return f"{length:04x}".encode() + encoded


def _flush_pkt() -> bytes:
    return b"0000"


@router.get("/{owner}/{repo}/info/refs")
async def info_refs(owner: str, repo: str, service: str = "") -> Response:
    """Ref advertisement — git client calls this first."""
    if service not in VALID_SERVICES:
        raise HTTPException(status_code=403, detail=f"Invalid service: {service}")

    repo_path = resolve_repo_path(owner, repo)
    await ensure_bare_repo(repo_path)

    output = await run_git_service(service, repo_path, advertise=True)

    # Smart HTTP ref advertisement format:
    # pkt-line with "# service=<service>\n", flush, then the refs
    body = _pkt_line(f"# service={service}\n") + _flush_pkt() + output

    return Response(
        content=body,
        media_type=f"application/x-{service}-advertisement",
        headers={"Cache-Control": "no-cache"},
    )


@router.post("/{owner}/{repo}/git-receive-pack")
async def receive_pack(owner: str, repo: str, request: Request) -> Response:
    """Receive pushed objects — the actual push."""
    repo_path = resolve_repo_path(owner, repo)
    if not repo_path.exists():
        raise HTTPException(status_code=404, detail="Repository not found")

    body = await request.body()
    output = await run_git_service("git-receive-pack", repo_path, input_data=body)

    # Post-receive: index the cookbook from git state
    try:
        db = await get_db()
        await index_cookbook(repo_path, owner, db)
        logger.info("Indexed cookbook after push: %s/%s", owner, repo)
    except Exception:
        logger.exception("Post-receive indexing failed for %s/%s", owner, repo)

    return Response(
        content=output,
        media_type="application/x-git-receive-pack-result",
        headers={"Cache-Control": "no-cache"},
    )


@router.post("/{owner}/{repo}/git-upload-pack")
async def upload_pack(owner: str, repo: str, request: Request) -> Response:
    """Serve objects for clone/fetch."""
    repo_path = resolve_repo_path(owner, repo)
    if not repo_path.exists():
        raise HTTPException(status_code=404, detail="Repository not found")

    body = await request.body()
    output = await run_git_service("git-upload-pack", repo_path, input_data=body)

    return Response(
        content=output,
        media_type="application/x-git-upload-pack-result",
        headers={"Cache-Control": "no-cache"},
    )
