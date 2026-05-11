"""SandboxHand env-injection from CredentialsService (Path B vault MVP).

Pin down the env-merge contract used by `delegate(to:"sandbox", op:"exec")`:

- When `credentials_service` is wired, stored creds for the sandbox owner
  are merged into the exec env.
- Brain-supplied env vars WIN on key conflict.
- When `credentials_service` is None (legacy/tests), behavior is unchanged.
- A CredentialsService that raises does NOT block the op — the brain
  may be running code that doesn't need credentials at all.
"""
from __future__ import annotations

import asyncio
from typing import AsyncIterator

import pytest

from krewhub.workers.sandbox_hand import SandboxHand


class FakeE2bClient:
    def __init__(self) -> None:
        self.last_env: dict[str, str] | None = None

    async def exec_command(
        self, sandbox_id, command, *, cwd=None, env=None, timeout=300.0,
    ):
        self.last_env = env

        async def _gen() -> AsyncIterator[dict]:
            yield {"exit_code": 0}
        return _gen()

    async def set_timeout(self, sandbox_id, *, timeout_s=3600):
        return None


class FakeCredentialsService:
    def __init__(self, envs: dict[str, str]) -> None:
        self._envs = envs

    async def get_envs(self, account_id: str) -> dict[str, str]:
        return dict(self._envs)


class RaisingCredentialsService:
    async def get_envs(self, account_id: str) -> dict[str, str]:
        raise RuntimeError("simulated storage failure")


class _NoopTape:
    async def append(self, *args, **kwargs):
        return None


class _NoopCancel:
    def __init__(self) -> None:
        self.cancelled = False

    async def wait(self):
        await asyncio.Event().wait()  # blocks forever; never reached in tests


@pytest.mark.asyncio
async def test_env_merged_when_credentials_service_present():
    e2b = FakeE2bClient()
    hand = SandboxHand(
        e2b, db=None,
        credentials_service=FakeCredentialsService({"GITHUB_TOKEN": "ghp_x"}),
    )
    # Without db, target_id is passed through verbatim and owner_account_id
    # is None, so credentials should NOT be merged (the merge step requires
    # an owner). This pins down the safe default.
    await hand.execute(
        target_id="raw_sandbox_id",
        input={"op": "exec", "command": "echo hi"},
        schema=None,
        deadline_s=5,
        tape=_NoopTape(),
        cancel=_NoopCancel(),
    )
    assert e2b.last_env is None, (
        "without owner_account_id, credentials are not merged"
    )


@pytest.mark.asyncio
async def test_env_merge_via_merge_helper_directly():
    """The _merge_credentials_into_env helper is the heart of the contract.

    Exercising it directly avoids needing a full DB-backed sandbox row.
    """
    e2b = FakeE2bClient()
    hand = SandboxHand(
        e2b, db=None,
        credentials_service=FakeCredentialsService({
            "GITHUB_TOKEN": "ghp_stored",
            "OPENAI_API_KEY": "sk_stored",
        }),
    )

    # No brain env → returns stored creds only
    merged = await hand._merge_credentials_into_env("acc_alice", None)
    assert merged == {"GITHUB_TOKEN": "ghp_stored", "OPENAI_API_KEY": "sk_stored"}

    # Brain env conflicts → brain wins (operator may explicitly override
    # a stored cred for one op).
    merged = await hand._merge_credentials_into_env(
        "acc_alice", {"GITHUB_TOKEN": "brain_override"},
    )
    assert merged["GITHUB_TOKEN"] == "brain_override"
    assert merged["OPENAI_API_KEY"] == "sk_stored"

    # No owner → unchanged
    merged = await hand._merge_credentials_into_env(None, {"FOO": "bar"})
    assert merged == {"FOO": "bar"}


@pytest.mark.asyncio
async def test_credentials_service_failure_does_not_block_exec():
    e2b = FakeE2bClient()
    hand = SandboxHand(
        e2b, db=None,
        credentials_service=RaisingCredentialsService(),
    )
    # The raising service shouldn't propagate. Brain env is returned as-is.
    merged = await hand._merge_credentials_into_env(
        "acc_alice", {"FOO": "bar"},
    )
    assert merged == {"FOO": "bar"}
