"""Tests for agent wallet provisioning service."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from krewhub.clients.krewauth_client import (
    AddSessionKeyResult,
    DeployWalletResult,
    KrewauthClient,
)
from krewhub.services.agent_wallet_service import provision_agent_wallet


def _make_settings(**overrides):
    """Create a mock Settings object."""
    defaults = {
        "krewhub_session_pubkey": "0x" + "AA" * 20,
        "aa_allowed_tokens": "0x" + "BB" * 20 + ",0x" + "CC" * 20,
        "aa_session_key_valid_hours": 24,
        "aa_session_key_spend_limit": "1000000000000000000",
        "krewauth_base_url": "http://localhost:8421",
    }
    defaults.update(overrides)
    settings = MagicMock()
    for k, v in defaults.items():
        setattr(settings, k, v)
    return settings


@pytest.mark.asyncio
async def test_provision_deploy_and_session_key():
    """Happy path: deploy + session key both succeed."""
    client = AsyncMock(spec=KrewauthClient)
    client.deploy_agent_wallet.return_value = DeployWalletResult(
        aa_wallet_address="0x" + "DD" * 20,
        owner_address="0x" + "EE" * 20,
        tx_hash="0xdeploytx",
        already_deployed=False,
    )
    client.add_session_key.return_value = AddSessionKeyResult(
        tx_hash="0xsktx",
        session_key_active=True,
    )

    result = await provision_agent_wallet(
        client=client,
        settings=_make_settings(),
        caller_token="jwt_token",
        agent_id="claude@test",
        cookbook_id="cb_test",
    )

    assert result.aa_wallet_address == "0x" + "DD" * 20
    assert result.deploy_tx_hash == "0xdeploytx"
    assert result.session_key_tx_hash == "0xsktx"
    client.deploy_agent_wallet.assert_called_once()
    client.add_session_key.assert_called_once()


@pytest.mark.asyncio
async def test_provision_session_key_fails_gracefully():
    """Wallet deployed, but session key fails — returns wallet address anyway."""
    client = AsyncMock(spec=KrewauthClient)
    client.deploy_agent_wallet.return_value = DeployWalletResult(
        aa_wallet_address="0x" + "DD" * 20,
        owner_address="0x" + "EE" * 20,
        tx_hash="0xdeploytx",
        already_deployed=False,
    )
    client.add_session_key.side_effect = RuntimeError("gas too low")

    result = await provision_agent_wallet(
        client=client,
        settings=_make_settings(),
        caller_token="jwt_token",
        agent_id="claude@test",
        cookbook_id="cb_test",
    )

    assert result.aa_wallet_address == "0x" + "DD" * 20
    assert result.deploy_tx_hash == "0xdeploytx"
    assert result.session_key_tx_hash is None


@pytest.mark.asyncio
async def test_provision_skips_session_key_when_not_configured():
    """No session pubkey → only deploys, skips session key."""
    client = AsyncMock(spec=KrewauthClient)
    client.deploy_agent_wallet.return_value = DeployWalletResult(
        aa_wallet_address="0x" + "DD" * 20,
        owner_address="0x" + "EE" * 20,
        tx_hash="0xdeploytx",
        already_deployed=False,
    )

    result = await provision_agent_wallet(
        client=client,
        settings=_make_settings(krewhub_session_pubkey=""),
        caller_token="jwt_token",
        agent_id="claude@test",
        cookbook_id="cb_test",
    )

    assert result.aa_wallet_address == "0x" + "DD" * 20
    assert result.session_key_tx_hash is None
    client.add_session_key.assert_not_called()


@pytest.mark.asyncio
async def test_provision_deploy_fails_propagates():
    """Deploy failure propagates to caller."""
    client = AsyncMock(spec=KrewauthClient)
    client.deploy_agent_wallet.side_effect = RuntimeError("RPC down")

    with pytest.raises(RuntimeError, match="RPC down"):
        await provision_agent_wallet(
            client=client,
            settings=_make_settings(),
            caller_token="jwt_token",
            agent_id="claude@test",
            cookbook_id="cb_test",
        )
