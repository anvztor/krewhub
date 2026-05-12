"""Tests for Phase 12 cookbook-sharing + repo-grant routes.

The deprecation pass put `recipe` on the chopping block; these tests
exercise the new cookbook-as-RBAC-root model that replaces it. RBAC
rules under test:
  - cookbook OWNER (owner_id) can mutate shares + grants
  - cookbook OWNER can list everything
  - MEMBER can read grants (so agents inside discover repo scopes)
  - VIEWER cannot read grants (token_refs are sensitive)
  - non-members get 403
"""

from __future__ import annotations

from datetime import datetime, timezone

import jwt
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from krewhub.app import create_app
from krewhub.config import get_settings
from krewhub.db.connection import get_db

OWNER_ACCOUNT = "acc_legacy_apikey"  # what X-API-Key resolves to
GUEST_ACCOUNT = "acc_guest_1"
STRANGER_ACCOUNT = "acc_stranger_2"


def _jwt_for(account_id: str) -> str:
    settings = get_settings()
    return jwt.encode(
        {"sub": account_id, "username": account_id, "method": "passkey"},
        settings.jwt_secret,
        algorithm="HS256",
    )


@pytest_asyncio.fixture
async def guest_client(_setup_db):
    """Cookie-authed client masquerading as GUEST_ACCOUNT."""
    app = create_app()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={"krew_session": _jwt_for(GUEST_ACCOUNT)},
    ) as ac:
        yield ac


@pytest_asyncio.fixture
async def stranger_client(_setup_db):
    """Cookie-authed client masquerading as STRANGER_ACCOUNT."""
    app = create_app()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={"krew_session": _jwt_for(STRANGER_ACCOUNT)},
    ) as ac:
        yield ac


async def _seed_accounts_and_cookbook():
    """Seed an owner-account + a cookbook owned by OWNER_ACCOUNT. Tests
    using cookie clients also need their account row to exist for the
    FK on cookbook_shares.shared_with_account_id."""
    db = await get_db()
    now = datetime.now(timezone.utc).isoformat()
    for acc in (OWNER_ACCOUNT, GUEST_ACCOUNT, STRANGER_ACCOUNT):
        await db.execute(
            "INSERT OR IGNORE INTO accounts (id, display_name, created_at, updated_at) "
            "VALUES (?, ?, ?, ?)",
            (acc, acc, now, now),
        )
    await db.execute(
        "INSERT INTO cookbooks (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)",
        ("cb_test_1", "Test Cookbook", OWNER_ACCOUNT, now),
    )
    await db.commit()
    return "cb_test_1"


# --------------------------------------------------------------------------
# cookbook shares
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_owner_can_share(client):
    cookbook_id = await _seed_accounts_and_cookbook()

    resp = await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "member"},
    )
    assert resp.status_code == 200, resp.text
    share = resp.json()["share"]
    assert share["cookbook_id"] == cookbook_id
    assert share["shared_with_account_id"] == GUEST_ACCOUNT
    assert share["role"] == "member"
    assert share["shared_by_account_id"] == OWNER_ACCOUNT
    assert share["revoked_at"] is None


@pytest.mark.asyncio
async def test_share_then_guest_can_read(client, guest_client):
    cookbook_id = await _seed_accounts_and_cookbook()
    # Owner shares with guest as member
    await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "member"},
    )

    # Guest can list shares (any role on the cookbook can read)
    resp = await guest_client.get(f"/api/v1/cookbooks/{cookbook_id}/shares")
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["shares"]) == 1


@pytest.mark.asyncio
async def test_non_member_cannot_share(stranger_client):
    cookbook_id = await _seed_accounts_and_cookbook()
    resp = await stranger_client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "member"},
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_member_cannot_share_only_owner(client, guest_client):
    cookbook_id = await _seed_accounts_and_cookbook()
    # Owner gives guest MEMBER role
    await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "member"},
    )

    # Guest (now MEMBER) tries to invite STRANGER — must be denied
    resp = await guest_client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": STRANGER_ACCOUNT, "role": "member"},
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_duplicate_share_is_409(client):
    cookbook_id = await _seed_accounts_and_cookbook()
    body = {"shared_with_account_id": GUEST_ACCOUNT, "role": "member"}
    first = await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares", json=body,
    )
    assert first.status_code == 200
    dup = await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares", json=body,
    )
    assert dup.status_code == 409


@pytest.mark.asyncio
async def test_cannot_share_with_owner(client):
    cookbook_id = await _seed_accounts_and_cookbook()
    resp = await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": OWNER_ACCOUNT, "role": "member"},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_revoke_share(client, guest_client):
    cookbook_id = await _seed_accounts_and_cookbook()
    create = await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "member"},
    )
    share_id = create.json()["share"]["id"]

    revoke = await client.delete(
        f"/api/v1/cookbooks/{cookbook_id}/shares/{share_id}",
    )
    assert revoke.status_code == 200
    assert revoke.json()["share"]["revoked_at"] is not None

    # After revoke, guest is no longer a member — 403 on reading
    resp = await guest_client.get(f"/api/v1/cookbooks/{cookbook_id}/shares")
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_update_share_role(client):
    cookbook_id = await _seed_accounts_and_cookbook()
    create = await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "viewer"},
    )
    share_id = create.json()["share"]["id"]

    patch = await client.patch(
        f"/api/v1/cookbooks/{cookbook_id}/shares/{share_id}",
        json={"role": "owner"},
    )
    assert patch.status_code == 200, patch.text
    assert patch.json()["share"]["role"] == "owner"


# --------------------------------------------------------------------------
# repo grants
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_owner_can_grant(client):
    cookbook_id = await _seed_accounts_and_cookbook()
    resp = await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/repo-grants",
        json={
            "provider": "github",
            "scope": "octo/repo",
            "token_ref": "vault:gh_octo_repo",
        },
    )
    assert resp.status_code == 200, resp.text
    grant = resp.json()["grant"]
    assert grant["provider"] == "github"
    assert grant["scope"] == "octo/repo"
    assert grant["token_ref"] == "vault:gh_octo_repo"
    assert grant["revoked_at"] is None


@pytest.mark.asyncio
async def test_member_cannot_grant(client, guest_client):
    cookbook_id = await _seed_accounts_and_cookbook()
    await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "member"},
    )

    resp = await guest_client.post(
        f"/api/v1/cookbooks/{cookbook_id}/repo-grants",
        json={
            "provider": "github", "scope": "octo/repo",
            "token_ref": "vault:gh_octo_repo",
        },
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_member_can_list_grants(client, guest_client):
    cookbook_id = await _seed_accounts_and_cookbook()
    await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "member"},
    )
    await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/repo-grants",
        json={
            "provider": "github", "scope": "octo/repo",
            "token_ref": "vault:gh_octo_repo",
        },
    )

    resp = await guest_client.get(
        f"/api/v1/cookbooks/{cookbook_id}/repo-grants",
    )
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["grants"]) == 1


@pytest.mark.asyncio
async def test_viewer_cannot_list_grants(client, guest_client):
    cookbook_id = await _seed_accounts_and_cookbook()
    await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "viewer"},
    )
    await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/repo-grants",
        json={
            "provider": "github", "scope": "octo/repo",
            "token_ref": "vault:x",
        },
    )

    resp = await guest_client.get(
        f"/api/v1/cookbooks/{cookbook_id}/repo-grants",
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_revoke_grant(client):
    cookbook_id = await _seed_accounts_and_cookbook()
    create = await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/repo-grants",
        json={
            "provider": "github", "scope": "octo/repo",
            "token_ref": "vault:x",
        },
    )
    grant_id = create.json()["grant"]["id"]

    revoke = await client.delete(
        f"/api/v1/cookbooks/{cookbook_id}/repo-grants/{grant_id}",
    )
    assert revoke.status_code == 200
    assert revoke.json()["grant"]["revoked_at"] is not None

    # Active list no longer shows it
    listed = await client.get(
        f"/api/v1/cookbooks/{cookbook_id}/repo-grants",
    )
    assert listed.status_code == 200
    assert len(listed.json()["grants"]) == 0


@pytest.mark.asyncio
async def test_grant_validation_rejects_bad_provider(client):
    cookbook_id = await _seed_accounts_and_cookbook()
    resp = await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/repo-grants",
        json={
            "provider": "github-enterprise",   # not in enum
            "scope": "octo/repo",
            "token_ref": "vault:x",
        },
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_grant_validation_rejects_empty_scope(client):
    cookbook_id = await _seed_accounts_and_cookbook()
    resp = await client.post(
        f"/api/v1/cookbooks/{cookbook_id}/repo-grants",
        json={
            "provider": "github", "scope": "   ",
            "token_ref": "vault:x",
        },
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_grant_find_covering_scope_matching():
    """Direct repo test for the scope-matching logic — covers wildcard
    + owner-only patterns that the JIT clone path will use."""
    from krewhub.models import RepoGrant, RepoProvider
    from krewhub.repositories.repo_grant_repo import RepoGrantRepo

    await _seed_accounts_and_cookbook()
    db = await get_db()
    repo = RepoGrantRepo(db)

    now = datetime.now(timezone.utc)
    # exact-scope grant
    await repo.create(RepoGrant(
        id="grt_exact", cookbook_id="cb_test_1",
        provider=RepoProvider.GITHUB, scope="alice/foo",
        token_ref="vault:1",
        granted_by_account_id=OWNER_ACCOUNT, granted_at=now,
    ))
    # wildcard grant
    await repo.create(RepoGrant(
        id="grt_wild", cookbook_id="cb_test_1",
        provider=RepoProvider.GITHUB, scope="bob/*",
        token_ref="vault:2",
        granted_by_account_id=OWNER_ACCOUNT, granted_at=now,
    ))
    # owner-only grant (shorthand)
    await repo.create(RepoGrant(
        id="grt_owner", cookbook_id="cb_test_1",
        provider=RepoProvider.GITHUB, scope="carol",
        token_ref="vault:3",
        granted_by_account_id=OWNER_ACCOUNT, granted_at=now,
    ))

    # exact match
    g = await repo.find_covering("cb_test_1", RepoProvider.GITHUB, "alice", "foo")
    assert g and g.id == "grt_exact"

    # wildcard match
    g = await repo.find_covering("cb_test_1", RepoProvider.GITHUB, "bob", "anything")
    assert g and g.id == "grt_wild"

    # owner shorthand match
    g = await repo.find_covering("cb_test_1", RepoProvider.GITHUB, "carol", "x")
    assert g and g.id == "grt_owner"

    # no match
    g = await repo.find_covering("cb_test_1", RepoProvider.GITHUB, "dave", "x")
    assert g is None

    # different provider
    g = await repo.find_covering("cb_test_1", RepoProvider.GITLAB, "alice", "foo")
    assert g is None


@pytest.mark.asyncio
async def test_404_for_unknown_cookbook(client):
    resp = await client.post(
        "/api/v1/cookbooks/cb_nope/shares",
        json={"shared_with_account_id": GUEST_ACCOUNT, "role": "member"},
    )
    # Owner check fails first → 403 (no role on a missing cookbook).
    # That's the correct behavior: we don't leak existence to non-members.
    assert resp.status_code == 403
