"""FastAPI dependency injectors for shared service singletons."""
from __future__ import annotations

from fastapi import HTTPException, Request

from krewhub.services.e2b_client import E2bClient


def get_e2b(request: Request) -> E2bClient:
    """Resolve the E2bClient singleton from app state."""
    e2b: E2bClient | None = getattr(request.app.state, "e2b", None)
    if e2b is None:
        raise HTTPException(
            status_code=503,
            detail={"code": "e2b_not_configured", "message": "E2B client unavailable"},
        )
    return e2b
