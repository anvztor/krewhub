"""E2B orchestrator HTTP client.

Thin wrapper over the self-hosted e2b orchestrator API. The reference
shape is captured in `infra/e2b/scripts/remote-api-create-base-sandbox.sh`:

    POST /sandboxes
        Headers: X-API-Key: <key>; Content-Type: application/json
        Body:    {"templateID": "base"}
        Returns: 201 {"sandboxID": "<id>", ...}

We accept both `sandboxID` and `id` keys defensively in case the API
shape changes.
"""
from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)


class E2bClient:
    def __init__(self, base_url: str, api_key: str, *, timeout: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        return {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json",
        }

    async def create_sandbox(self, *, template: str) -> str:
        """Create a new sandbox from a template; return the sandbox id.

        Raises httpx.HTTPStatusError on non-2xx responses; the caller is
        expected to translate this into a 503 ``sandbox_provision_timeout``
        error or similar at the API boundary.
        """
        url = f"{self.base_url}/sandboxes"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                url,
                headers=self._headers(),
                json={"templateID": template},
            )
            response.raise_for_status()
            body = response.json()
        sandbox_id = body.get("sandboxID") or body.get("id")
        if not sandbox_id:
            raise ValueError(
                f"e2b create_sandbox response missing sandboxID/id: {body!r}"
            )
        return sandbox_id

    async def terminate(self, sandbox_id: str) -> None:
        """Terminate a sandbox; tolerates already-gone (404)."""
        url = f"{self.base_url}/sandboxes/{sandbox_id}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.delete(url, headers=self._headers())
        if response.status_code in (200, 204, 404):
            return
        response.raise_for_status()
