"""Orchestrates AA wallet provisioning for agents via krewauth."""
from __future__ import annotations

import logging
import time

from krewhub.clients.krewauth_client import KrewauthClient
from krewhub.config import Settings

logger = logging.getLogger(__name__)


class AgentWalletResult:
    """Immutable result of wallet provisioning."""

    __slots__ = ("aa_wallet_address", "deploy_tx_hash", "session_key_tx_hash")

    def __init__(
        self,
        aa_wallet_address: str,
        deploy_tx_hash: str | None,
        session_key_tx_hash: str | None,
    ) -> None:
        self.aa_wallet_address = aa_wallet_address
        self.deploy_tx_hash = deploy_tx_hash
        self.session_key_tx_hash = session_key_tx_hash


async def provision_agent_wallet(
    client: KrewauthClient,
    settings: Settings,
    caller_token: str,
    agent_id: str,
    cookbook_id: str,
) -> AgentWalletResult:
    """Deploy AA wallet and add session key. Idempotent and best-effort."""
    # 1. Deploy wallet (idempotent — returns existing if already deployed)
    deploy_result = await client.deploy_agent_wallet(
        token=caller_token,
        agent_id=agent_id,
        cookbook_id=cookbook_id,
    )

    # 2. Add session key if configured
    session_key_tx: str | None = None
    if settings.krewhub_session_pubkey and settings.aa_allowed_tokens:
        allowed_targets = [
            t.strip() for t in settings.aa_allowed_tokens.split(",") if t.strip()
        ]
        # ERC-20 transfer + approve selectors
        allowed_selectors = ["0xa9059cbb", "0x095ea7b3"]
        valid_until = int(time.time()) + (settings.aa_session_key_valid_hours * 3600)

        try:
            sk_result = await client.add_session_key(
                token=caller_token,
                agent_id=agent_id,
                cookbook_id=cookbook_id,
                session_pubkey=settings.krewhub_session_pubkey,
                allowed_targets=allowed_targets,
                allowed_selectors=allowed_selectors,
                valid_until=valid_until,
                spend_limit=int(settings.aa_session_key_spend_limit),
            )
            session_key_tx = sk_result.tx_hash
        except Exception as exc:
            # Wallet is deployed, session key can be added later
            logger.warning(
                "Session key setup failed for agent %s: %s", agent_id, exc,
            )

    return AgentWalletResult(
        aa_wallet_address=deploy_result.aa_wallet_address,
        deploy_tx_hash=deploy_result.tx_hash,
        session_key_tx_hash=session_key_tx,
    )
