from __future__ import annotations

from fastapi import Depends, HTTPException, Security
from fastapi.security import APIKeyHeader

from krewhub.config import Settings, get_settings

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(
    api_key: str | None = Security(_api_key_header),
    settings: Settings = Depends(get_settings),
) -> str:
    if not api_key or api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return api_key
