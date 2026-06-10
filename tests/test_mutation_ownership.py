"""Ownership / anti-hijack ABAC for mutation routes.

Covers the security fix that closes the "any authenticated caller can
mutate any resource" gap. Each fixed route gets a positive case (the
owner — or the route's legitimate collaborator — is admitted) and a
negative case (a different authenticated principal is rejected with 403,
NOT 401: they ARE authenticated, they just don't own the resource).

Auth model under test:
  * task writes  → legacy api-key OR assigned-runtime OR bundle owner
  * bundle graph → bundle owner (require_bundle_owner)
  * agent writes → owner_username anti-hijack
"""
from __future__ import annotations

import jwt
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from krewhub.config import get_settings


# ---------------------------------------------------------------------------
# Fixtures: two distinct cookie-authenticated principals.
#   owner    = acc_test_cookie (matches conftest.cookie_client)
#   attacker = acc_attacker    (a different account, valid session)
# ---------------------------------------------------------------------------


def _cookie_client_for(account_id: str, username: str) -> AsyncClient:
    from krewhub.app import create_app

    settings = get_settings()
    token = jwt.encode(
        {"sub": account_id, "username": username, "method": "passkey"},
        settings.jwt_secret,
        algorithm="HS256",
    )
    app = create_app()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    return AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={"krew_session": token},
    )


@pytest_asyncio.fixture
async def owner_client(_setup_db):
    async with _cookie_client_for("acc_test_cookie", "owner_user") as ac:
        yield ac


@pytest_asyncio.fixture
async def attacker_client(_setup_db):
    async with _cookie_client_for("acc_attacker", "attacker_user") as ac:
        yield ac


async def _owned_bundle_with_task(owner) -> tuple[str, str, str]:
    """Create a cookbook + bundle + task all owned by acc_test_cookie."""
    r = await owner.post("/api/v1/cookbooks", json={
        "name": "owned-cookbook",
        "owner_id": "acc_test_cookie",
    })
    cookbook_id = r.json()["cookbook"]["id"]
    r = await owner.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
        "prompt": "owned bundle",
        "tasks": [{"title": "owned task"}],
    })
    bundle_id = r.json()["bundle"]["id"]
    task_id = r.json()["tasks"][0]["id"]
    return cookbook_id, bundle_id, task_id


# The full set of task mutation routes, with a minimal valid body each.
# (method, url_suffix, json_body)
def _task_mutation_calls(task_id: str):
    return [
        ("post", f"/api/v1/tasks/{task_id}/claim", {"agent_id": "agent_x"}),
        ("post", f"/api/v1/tasks/{task_id}/events", {
            "type": "agent_reply", "actor_id": "a", "actor_type": "agent",
            "body": "hi",
        }),
        ("post", f"/api/v1/tasks/{task_id}/events:batch", {"events": []}),
        ("patch", f"/api/v1/tasks/{task_id}/status", {"status": "done"}),
        ("post", f"/api/v1/tasks/{task_id}/completion", {"session_id": "s"}),
        ("post", f"/api/v1/tasks/{task_id}/usage", {
            "input_tokens": 1, "output_tokens": 1,
        }),
        ("post", f"/api/v1/tasks/{task_id}/cancel", None),
        ("post", f"/api/v1/tasks/{task_id}/progress", {"summary": "x"}),
        ("post", f"/api/v1/tasks/{task_id}/followup", {"prompt": "p"}),
        ("post", f"/api/v1/tasks/{task_id}/hitl/answer", {"answer": "a"}),
        ("patch", f"/api/v1/tasks/{task_id}", {"title": "new"}),
        ("delete", f"/api/v1/tasks/{task_id}", None),
    ]


async def _call(client, method: str, url: str, body):
    if method == "post":
        return await client.post(url, json=body) if body is not None else await client.post(url)
    if method == "patch":
        return await client.patch(url, json=body)
    if method == "delete":
        return await client.delete(url)
    raise AssertionError(method)


# ---------------------------------------------------------------------------
# Tasks — negative: every mutation route rejects a non-owner with 403.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_mutations_rejected_for_non_owner(owner_client, attacker_client):
    _, _, task_id = await _owned_bundle_with_task(owner_client)

    for method, url, body in _task_mutation_calls(task_id):
        resp = await _call(attacker_client, method, url, body)
        assert resp.status_code == 403, (
            f"{method.upper()} {url}: expected 403 for non-owner, "
            f"got {resp.status_code}: {resp.text}"
        )


# ---------------------------------------------------------------------------
# Tasks — positive: the owner is admitted by the authz layer (never 403).
# A non-403 means authorization passed; the concrete status depends on
# task state (e.g. hitl/answer 400 on a non-blocked task), which is fine.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_mutations_admit_owner(owner_client):
    _, _, task_id = await _owned_bundle_with_task(owner_client)

    for method, url, body in _task_mutation_calls(task_id):
        resp = await _call(owner_client, method, url, body)
        assert resp.status_code != 403, (
            f"{method.upper()} {url}: owner unexpectedly 403'd: {resp.text}"
        )
        assert resp.status_code != 401, (
            f"{method.upper()} {url}: owner unexpectedly 401'd: {resp.text}"
        )


@pytest.mark.asyncio
async def test_task_mutations_admit_legacy_api_key(client):
    """Legacy X-API-Key integrations keep their pre-ABAC access."""
    # Bundle owned by the legacy sentinel.
    r = await client.post("/api/v1/cookbooks", json={
        "name": "legacy-cb", "owner_id": "acc_legacy_apikey",
    })
    cookbook_id = r.json()["cookbook"]["id"]
    r = await client.post(f"/api/v1/cookbooks/{cookbook_id}/bundles", json={
        "prompt": "legacy", "tasks": [{"title": "t"}],
    })
    task_id = r.json()["tasks"][0]["id"]

    resp = await client.post(f"/api/v1/tasks/{task_id}/cancel")
    assert resp.status_code == 200, resp.text


# ---------------------------------------------------------------------------
# Bundle graph attach — owner OK, non-owner 403, missing bundle 404.
# ---------------------------------------------------------------------------

_VALID_GRAPH = (
    "from pydantic_graph import BaseNode, End, Graph, GraphRunContext\n"
    "from dataclasses import dataclass\n"
    "@dataclass\n"
    "class Start(BaseNode):\n"
    "    async def run(self, ctx: GraphRunContext) -> End:\n"
    "        return End(None)\n"
    "graph = Graph(nodes=[Start])\n"
)


@pytest.mark.asyncio
async def test_attach_graph_rejected_for_non_owner(owner_client, attacker_client):
    _, bundle_id, _ = await _owned_bundle_with_task(owner_client)
    resp = await attacker_client.post(
        f"/api/v1/bundles/{bundle_id}/graph",
        json={"code": _VALID_GRAPH, "created_by": "attacker"},
    )
    assert resp.status_code == 403, resp.text


@pytest.mark.asyncio
async def test_attach_graph_admits_owner(owner_client):
    _, bundle_id, _ = await _owned_bundle_with_task(owner_client)
    resp = await owner_client.post(
        f"/api/v1/bundles/{bundle_id}/graph",
        json={"code": _VALID_GRAPH, "created_by": "owner"},
    )
    # Owner passes authz; the concrete result depends on graph validation,
    # but it must not be an authorization failure.
    assert resp.status_code not in (401, 403), resp.text


# ---------------------------------------------------------------------------
# Agents — anti-hijack on register / heartbeat / mint.
# ---------------------------------------------------------------------------


async def _make_cookbook(client, owner_id: str) -> str:
    r = await client.post("/api/v1/cookbooks", json={
        "name": "agent-cb", "owner_id": owner_id,
    })
    return r.json()["cookbook"]["id"]


@pytest.mark.asyncio
async def test_agent_register_then_hijack_rejected(owner_client, attacker_client):
    cookbook_id = await _make_cookbook(owner_client, "acc_test_cookie")
    body = {
        "agent_id": "agent_owned",
        "cookbook_id": cookbook_id,
        "display_name": "Owned Agent",
    }
    # Owner registers — establishes owner_username.
    r = await owner_client.post("/api/v1/agents/register", json=body)
    assert r.status_code == 200, r.text

    # Attacker tries to re-register (re-own) the same presence → 403.
    r = await attacker_client.post("/api/v1/agents/register", json=body)
    assert r.status_code == 403, r.text

    # Attacker heartbeat on the same presence → 403.
    r = await attacker_client.post("/api/v1/agents/heartbeat", json=body)
    assert r.status_code == 403, r.text

    # Owner can heartbeat its own agent.
    r = await owner_client.post("/api/v1/agents/heartbeat", json=body)
    assert r.status_code == 200, r.text


@pytest.mark.asyncio
async def test_agent_mint_rejected_for_non_owner(owner_client, attacker_client):
    cookbook_id = await _make_cookbook(owner_client, "acc_test_cookie")
    body = {
        "agent_id": "agent_mint",
        "cookbook_id": cookbook_id,
        "display_name": "Mint Agent",
    }
    r = await owner_client.post("/api/v1/agents/register", json=body)
    assert r.status_code == 200, r.text

    # Non-owner records a mint on someone else's agent → 403.
    r = await attacker_client.patch(
        f"/api/v1/agents/{body['agent_id']}/mint",
        json={"cookbook_id": cookbook_id, "tx_hash": "0xdead", "token_id": 1},
    )
    assert r.status_code == 403, r.text

    # Owner is admitted by the authz guard (gets past it). NOTE: the mint
    # write itself currently 500s on a pre-existing schema bug ("no such
    # column: mint_tx_hash") unrelated to authorization — out of scope for
    # this security fix; asserting only that the owner is NOT rejected.
    r = await owner_client.patch(
        f"/api/v1/agents/{body['agent_id']}/mint",
        json={"cookbook_id": cookbook_id, "tx_hash": "0xbeef", "token_id": 2},
    )
    assert r.status_code not in (401, 403), r.text


# ---------------------------------------------------------------------------
# Regression: the residual cookie-401 bug (agents router was bearer-only).
# The web SPA reads the agent roster with a session cookie.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_agents_accepts_cookie_auth(owner_client):
    resp = await owner_client.get("/api/v1/agents")
    assert resp.status_code == 200, resp.text
    assert "agents" in resp.json()
