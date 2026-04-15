"""Typed HTTP client for krewauth agent-wallet endpoints."""
from __future__ import annotations

import httpx
from pydantic import BaseModel


class DeployWalletResult(BaseModel, frozen=True):
    aa_wallet_address: str
    owner_address: str
    tx_hash: str | None
    already_deployed: bool


class AddSessionKeyResult(BaseModel, frozen=True):
    tx_hash: str
    session_key_active: bool


class KrewauthClient:
    def __init__(self, base_url: str, timeout: float = 60.0) -> None:
        self._http = httpx.AsyncClient(base_url=base_url, timeout=timeout)

    async def deploy_agent_wallet(
        self, token: str, agent_id: str, cookbook_id: str,
    ) -> DeployWalletResult:
        resp = await self._http.post("/auth/agent-wallet/deploy", json={
            "token": token,
            "agent_id": agent_id,
            "cookbook_id": cookbook_id,
        })
        resp.raise_for_status()
        return DeployWalletResult(**resp.json())

    async def add_session_key(
        self,
        token: str,
        agent_id: str,
        cookbook_id: str,
        session_pubkey: str,
        allowed_targets: list[str],
        allowed_selectors: list[str],
        valid_until: int,
        spend_limit: int,
    ) -> AddSessionKeyResult:
        resp = await self._http.post("/auth/agent-wallet/add-session-key", json={
            "token": token,
            "agent_id": agent_id,
            "cookbook_id": cookbook_id,
            "session_pubkey": session_pubkey,
            "allowed_targets": allowed_targets,
            "allowed_selectors": allowed_selectors,
            "valid_until": valid_until,
            "spend_limit": spend_limit,
        })
        resp.raise_for_status()
        return AddSessionKeyResult(**resp.json())

    async def close(self) -> None:
        await self._http.aclose()
