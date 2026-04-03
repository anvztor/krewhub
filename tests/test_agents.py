from __future__ import annotations

import pytest


async def _create_cookbook(client, name="test-cookbook"):
    resp = await client.post("/api/v1/cookbooks", json={
        "name": name,
        "owner_id": "human_1",
    })
    return resp.json()["cookbook"]["id"]


async def _create_recipe_in_cookbook(client, cookbook_id, name="test/agents"):
    resp = await client.post("/api/v1/recipes", json={
        "name": name,
        "repo_url": f"git@github.com:{name}.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    return resp.json()["recipe"]["id"]


@pytest.mark.asyncio
async def test_heartbeat(client):
    cookbook_id = await _create_cookbook(client)

    resp = await client.post("/api/v1/agents/heartbeat", json={
        "agent_id": "agent_alpha",
        "cookbook_id": cookbook_id,
        "display_name": "Agent Alpha",
        "capabilities": ["claim", "milestones", "facts", "code_refs"],
    })
    assert resp.status_code == 200
    presence = resp.json()["presence"]
    assert presence["status"] == "online"
    assert presence["display_name"] == "Agent Alpha"
    assert "claim" in presence["capabilities"]


@pytest.mark.asyncio
async def test_heartbeat_busy_when_working(client):
    cookbook_id = await _create_cookbook(client, "test-busy")

    resp = await client.post("/api/v1/agents/heartbeat", json={
        "agent_id": "agent_beta",
        "cookbook_id": cookbook_id,
        "display_name": "Agent Beta",
        "capabilities": ["claim"],
        "current_task_id": "task_123",
    })
    assert resp.status_code == 200
    assert resp.json()["presence"]["status"] == "busy"


@pytest.mark.asyncio
async def test_agents_visible_in_recipe(client):
    cookbook_id = await _create_cookbook(client, "test-visible")
    recipe_id = await _create_recipe_in_cookbook(client, cookbook_id, "test/visible")

    await client.post("/api/v1/agents/heartbeat", json={
        "agent_id": "agent_gamma",
        "cookbook_id": cookbook_id,
        "display_name": "Gamma",
        "capabilities": ["claim"],
    })

    resp = await client.get(f"/api/v1/recipes/{recipe_id}")
    agents = resp.json()["agents"]
    assert len(agents) == 1
    assert agents[0]["agent_id"] == "agent_gamma"


@pytest.mark.asyncio
async def test_heartbeat_requires_existing_cookbook(client):
    resp = await client.post("/api/v1/agents/heartbeat", json={
        "agent_id": "agent_missing",
        "cookbook_id": "cb_missing",
        "display_name": "Missing",
        "capabilities": ["claim"],
    })

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Cookbook not found"
