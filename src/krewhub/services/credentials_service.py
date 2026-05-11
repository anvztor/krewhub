"""Operator credentials store (Path B: env-injection MVP).

Plaintext leaves the operator's browser once (paste form), lives in our
SQLite as AES-GCM ciphertext keyed by `settings.credentials_encryption_key`,
and is decrypted only at op:exec dispatch time when SandboxHand merges the
operator's credentials into the env passed to the sandbox.

This is intentionally NOT the broker pattern from the spike — that requires
the egress proxy + per-deployment CA + HTTPS_PROXY plumbing into the
sandbox. We accept "credential lives briefly in sandbox env" for the MVP
in exchange for ~5x less integration code.

Upgrade path: when threat model demands "brain never holds credentials,"
add an EgressBroker layer that swaps env-injection for HTTPS_PROXY +
MITM injection. This module's public surface (put / get_envs) doesn't
change; only the consumer (SandboxHand) does.
"""
from __future__ import annotations

import hashlib
import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone

import aiosqlite
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


@dataclass(frozen=True)
class CredentialRow:
    """Non-secret view of a credential. Plaintext is NOT included."""
    id: str
    account_id: str
    host: str
    env_var_name: str
    created_at: str
    updated_at: str


class CredentialsService:
    """Encrypted-at-rest credential storage scoped per cookrew account."""

    def __init__(self, db: aiosqlite.Connection, encryption_key: str) -> None:
        if not encryption_key:
            raise ValueError(
                "CredentialsService requires non-empty encryption_key; "
                "set KREWHUB_CREDENTIALS_ENCRYPTION_KEY"
            )
        self._db = db
        # AES-GCM wants exactly 32 bytes for AES-256. Derive from
        # the passphrase via sha256. Same pattern as
        # krewauth/crypto/managed_wallet.py.
        self._aes = AESGCM(hashlib.sha256(encryption_key.encode()).digest())

    async def put(
        self,
        *,
        account_id: str,
        host: str,
        env_var_name: str,
        plaintext: str,
    ) -> CredentialRow:
        """Insert or update the credential for (account_id, host).

        On update, generates a fresh nonce so the new ciphertext is
        distinguishable. Returns the public-view row.
        """
        if not plaintext:
            raise ValueError("plaintext credential cannot be empty")
        if not env_var_name or not env_var_name.replace("_", "").isalnum():
            raise ValueError(
                f"env_var_name must be alphanumeric/underscore: {env_var_name!r}"
            )

        nonce = secrets.token_bytes(12)
        ciphertext = self._aes.encrypt(nonce, plaintext.encode(), None)
        now = datetime.now(timezone.utc).isoformat()

        existing = await self._get_row(account_id, host)
        if existing is None:
            cred_id = f"cred_{secrets.token_hex(12)}"
            await self._db.execute(
                "INSERT INTO credentials (id, account_id, host, env_var_name, "
                "ciphertext, nonce, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (cred_id, account_id, host, env_var_name,
                 ciphertext, nonce, now, now),
            )
        else:
            cred_id = existing["id"]
            await self._db.execute(
                "UPDATE credentials SET env_var_name = ?, ciphertext = ?, "
                "nonce = ?, updated_at = ? WHERE id = ?",
                (env_var_name, ciphertext, nonce, now, cred_id),
            )
        await self._db.commit()

        return CredentialRow(
            id=cred_id,
            account_id=account_id,
            host=host,
            env_var_name=env_var_name,
            created_at=existing["created_at"] if existing else now,
            updated_at=now,
        )

    async def list_for_account(self, account_id: str) -> list[CredentialRow]:
        """Return non-secret metadata about every active credential.

        For listing in cookrew-web (operator-facing UI). Plaintext is
        NEVER returned by this method.
        """
        cursor = await self._db.execute(
            "SELECT id, host, env_var_name, created_at, updated_at "
            "FROM credentials WHERE account_id = ? AND archived_at IS NULL "
            "ORDER BY host",
            (account_id,),
        )
        rows = await cursor.fetchall()
        return [
            CredentialRow(
                id=r[0], account_id=account_id, host=r[1],
                env_var_name=r[2], created_at=r[3], updated_at=r[4],
            )
            for r in rows
        ]

    async def archive(self, *, account_id: str, host: str) -> bool:
        """Soft-delete; returns True if a row was archived."""
        now = datetime.now(timezone.utc).isoformat()
        cursor = await self._db.execute(
            "UPDATE credentials SET archived_at = ? "
            "WHERE account_id = ? AND host = ? AND archived_at IS NULL",
            (now, account_id, host),
        )
        await self._db.commit()
        return cursor.rowcount > 0

    async def get_envs(self, account_id: str) -> dict[str, str]:
        """Return {env_var_name: plaintext, ...} for SandboxHand to inject.

        The single seam where plaintext leaves this module. The caller
        MUST pass the result into a subprocess's env dict and NEVER log it.
        """
        cursor = await self._db.execute(
            "SELECT env_var_name, ciphertext, nonce FROM credentials "
            "WHERE account_id = ? AND archived_at IS NULL",
            (account_id,),
        )
        rows = await cursor.fetchall()
        envs: dict[str, str] = {}
        for env_var, ct, nonce in rows:
            try:
                plain = self._aes.decrypt(nonce, ct, None).decode()
            except Exception:
                # A corrupt or wrong-key row poisons one env, not the
                # whole bundle. Caller proceeds without that credential.
                continue
            envs[env_var] = plain
        return envs

    # ---- internals -----------------------------------------------------

    async def _get_row(self, account_id: str, host: str) -> dict | None:
        cursor = await self._db.execute(
            "SELECT id, created_at FROM credentials "
            "WHERE account_id = ? AND host = ? AND archived_at IS NULL",
            (account_id, host),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return {"id": row[0], "created_at": row[1]}


# ---------------------------------------------------------------------------
# Convenience: dev-mode key resolution
# ---------------------------------------------------------------------------


def resolve_encryption_key(settings_key: str) -> str:
    """Resolve the at-rest encryption key, falling back to a dev marker.

    In dev (no key set, no KREWHUB_CREDENTIALS_ENCRYPTION_KEY env), we
    use a fixed dev string so tests + local dev keep working. In prod
    the absence of a key MUST raise — the operator/admin has to set it
    via secret. CredentialsService.__init__ does the prod-side check;
    this just centralizes the dev fallback.
    """
    if settings_key:
        return settings_key
    if os.environ.get("KREWHUB_ENV", "dev") == "dev":
        return "dev-only-not-for-production"
    raise RuntimeError(
        "KREWHUB_CREDENTIALS_ENCRYPTION_KEY is required in non-dev env"
    )
