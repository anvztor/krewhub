"""CredentialsService — at-rest encryption + envs lookup contract.

Pin down:
- put + get_envs round-trips through AES-GCM
- updating a credential rotates the nonce (ciphertext differs)
- archive removes from get_envs
- get_envs is per-account scoped (no cross-account leakage)
- empty plaintext / invalid env_var_name are rejected
- a corrupt row poisons one env, not the whole bundle
"""
from __future__ import annotations

import pytest

from krewhub.db.connection import get_db
from krewhub.services.credentials_service import CredentialsService


_TEST_KEY = "spike-test-key-not-for-prod"


@pytest.mark.asyncio
async def test_put_then_get_envs_returns_plaintext(_setup_db):
    db = await get_db()
    svc = CredentialsService(db, _TEST_KEY)
    await svc.put(
        account_id="acc_alice",
        host="api.github.com",
        env_var_name="GITHUB_TOKEN",
        plaintext="ghp_top_secret",
    )
    envs = await svc.get_envs("acc_alice")
    assert envs == {"GITHUB_TOKEN": "ghp_top_secret"}


@pytest.mark.asyncio
async def test_get_envs_is_account_scoped(_setup_db):
    db = await get_db()
    svc = CredentialsService(db, _TEST_KEY)
    await svc.put(
        account_id="acc_alice", host="api.github.com",
        env_var_name="GITHUB_TOKEN", plaintext="alice_token",
    )
    await svc.put(
        account_id="acc_bob", host="api.github.com",
        env_var_name="GITHUB_TOKEN", plaintext="bob_token",
    )
    assert (await svc.get_envs("acc_alice")) == {"GITHUB_TOKEN": "alice_token"}
    assert (await svc.get_envs("acc_bob")) == {"GITHUB_TOKEN": "bob_token"}
    assert (await svc.get_envs("acc_nobody")) == {}


@pytest.mark.asyncio
async def test_update_rotates_nonce_and_value(_setup_db):
    db = await get_db()
    svc = CredentialsService(db, _TEST_KEY)
    await svc.put(
        account_id="acc_alice", host="api.github.com",
        env_var_name="GITHUB_TOKEN", plaintext="old_token",
    )
    # Read raw ciphertext to confirm it changes on update.
    cur = await db.execute(
        "SELECT ciphertext, nonce FROM credentials WHERE account_id = ?",
        ("acc_alice",),
    )
    row1 = await cur.fetchone()
    await svc.put(
        account_id="acc_alice", host="api.github.com",
        env_var_name="GITHUB_TOKEN", plaintext="new_token",
    )
    cur = await db.execute(
        "SELECT ciphertext, nonce FROM credentials WHERE account_id = ?",
        ("acc_alice",),
    )
    row2 = await cur.fetchone()
    assert row1[0] != row2[0], "ciphertext should rotate"
    assert row1[1] != row2[1], "nonce should rotate"
    assert (await svc.get_envs("acc_alice")) == {"GITHUB_TOKEN": "new_token"}


@pytest.mark.asyncio
async def test_archive_removes_from_envs(_setup_db):
    db = await get_db()
    svc = CredentialsService(db, _TEST_KEY)
    await svc.put(
        account_id="acc_alice", host="api.github.com",
        env_var_name="GITHUB_TOKEN", plaintext="ghp_x",
    )
    ok = await svc.archive(account_id="acc_alice", host="api.github.com")
    assert ok is True
    assert (await svc.get_envs("acc_alice")) == {}
    # Idempotent: second archive returns False
    assert (await svc.archive(account_id="acc_alice", host="api.github.com")) is False


@pytest.mark.asyncio
async def test_list_for_account_strips_secrets(_setup_db):
    db = await get_db()
    svc = CredentialsService(db, _TEST_KEY)
    await svc.put(
        account_id="acc_alice", host="api.github.com",
        env_var_name="GITHUB_TOKEN", plaintext="ghp_x",
    )
    rows = await svc.list_for_account("acc_alice")
    assert len(rows) == 1
    # The CredentialRow dataclass intentionally has no secret field; if
    # someone tries to add one, this assertion forces them to think
    # about whether the public view should expose it.
    assert not hasattr(rows[0], "plaintext")
    assert not hasattr(rows[0], "ciphertext")
    assert rows[0].host == "api.github.com"
    assert rows[0].env_var_name == "GITHUB_TOKEN"


@pytest.mark.asyncio
async def test_empty_plaintext_rejected(_setup_db):
    db = await get_db()
    svc = CredentialsService(db, _TEST_KEY)
    with pytest.raises(ValueError, match="empty"):
        await svc.put(
            account_id="acc_alice", host="api.github.com",
            env_var_name="GITHUB_TOKEN", plaintext="",
        )


@pytest.mark.asyncio
async def test_invalid_env_var_name_rejected(_setup_db):
    db = await get_db()
    svc = CredentialsService(db, _TEST_KEY)
    with pytest.raises(ValueError, match="env_var_name"):
        await svc.put(
            account_id="acc_alice", host="api.github.com",
            env_var_name="bad name with spaces",
            plaintext="ghp_x",
        )


@pytest.mark.asyncio
async def test_corrupt_row_does_not_poison_other_envs(_setup_db):
    """Decryption failure on one row → that env is skipped, others returned."""
    db = await get_db()
    svc = CredentialsService(db, _TEST_KEY)
    await svc.put(
        account_id="acc_alice", host="api.github.com",
        env_var_name="GITHUB_TOKEN", plaintext="real_token",
    )
    await svc.put(
        account_id="acc_alice", host="api.openai.com",
        env_var_name="OPENAI_API_KEY", plaintext="sk_real",
    )
    # Corrupt one row's ciphertext.
    await db.execute(
        "UPDATE credentials SET ciphertext = ? WHERE host = ?",
        (b"\x00" * 64, "api.openai.com"),
    )
    await db.commit()
    envs = await svc.get_envs("acc_alice")
    assert envs == {"GITHUB_TOKEN": "real_token"}, (
        "valid env survives corrupt-sibling poisoning"
    )


@pytest.mark.asyncio
async def test_empty_encryption_key_rejected(_setup_db):
    db = await get_db()
    with pytest.raises(ValueError, match="encryption_key"):
        CredentialsService(db, "")
