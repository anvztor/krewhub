from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_heartbeat(client):
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/agents",
        "repo_url": "git@github.com:test/agents.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post("/api/v1/agents/heartbeat", json={
        "agent_id": "agent_alpha",
        "recipe_id": recipe_id,
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
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/busy",
        "repo_url": "git@github.com:test/busy.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    resp = await client.post("/api/v1/agents/heartbeat", json={
        "agent_id": "agent_beta",
        "recipe_id": recipe_id,
        "display_name": "Agent Beta",
        "capabilities": ["claim"],
        "current_task_id": "task_123",
    })
    assert resp.status_code == 200
    assert resp.json()["presence"]["status"] == "busy"


@pytest.mark.asyncio
async def test_agents_visible_in_recipe(client):
    resp = await client.post("/api/v1/recipes", json={
        "name": "test/visible",
        "repo_url": "git@github.com:test/visible.git",
        "created_by": "human_1",
    })
    recipe_id = resp.json()["recipe"]["id"]

    await client.post("/api/v1/agents/heartbeat", json={
        "agent_id": "agent_gamma",
        "recipe_id": recipe_id,
        "display_name": "Gamma",
        "capabilities": ["claim"],
    })

    resp = await client.get(f"/api/v1/recipes/{recipe_id}")
    agents = resp.json()["agents"]
    assert len(agents) == 1
    assert agents[0]["agent_id"] == "agent_gamma"
