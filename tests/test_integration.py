from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from krewhub.db.connection import get_db
from krewhub.repositories.event_repo import EventRepo
from krewhub.services.retention_service import RetentionService
from krewhub.services.sse_service import sse_service


async def _expect_sse_event(
    recipe_id: str,
    queue: asyncio.Queue[dict[str, object]],
    expected_event: str,
) -> dict[str, object]:
    try:
        message = await asyncio.wait_for(queue.get(), timeout=1.0)
    except TimeoutError as exc:  # pragma: no cover - keeps failures readable
        raise AssertionError(
            f"Timed out waiting for SSE event {expected_event!r} on recipe {recipe_id}"
        ) from exc

    assert message["event"] == expected_event
    return message


@pytest.mark.asyncio
async def test_full_lifecycle_emits_sse_and_persists_history(client):
    recipe_response = await client.post(
        "/api/v1/recipes",
        json={
            "name": "test/phase4-lifecycle",
            "repo_url": "git@github.com:test/phase4-lifecycle.git",
            "default_branch": "release/integration",
            "created_by": "qa.lead",
        },
    )
    assert recipe_response.status_code == 200
    recipe_id = recipe_response.json()["recipe"]["id"]

    queue = sse_service.subscribe(recipe_id)
    try:
        bundle_response = await client.post(
            f"/api/v1/recipes/{recipe_id}/bundles",
            json={
                "prompt": "Verify the integrated Phase 4 loop.",
                "requested_by": "qa.lead",
                "tasks": [{"title": "Close the integration loop"}],
            },
        )
        assert bundle_response.status_code == 200
        bundle_id = bundle_response.json()["bundle"]["id"]
        task_id = bundle_response.json()["tasks"][0]["id"]

        bundle_created = await _expect_sse_event(recipe_id, queue, "bundle.created")
        assert bundle_created["data"]["bundle_id"] == bundle_id
        assert bundle_created["data"]["task_count"] == 1

        heartbeat_response = await client.post(
            "/api/v1/agents/heartbeat",
            json={
                "agent_id": "agent_phase4",
                "recipe_id": recipe_id,
                "display_name": "Phase 4 Agent",
                "capabilities": ["claim", "milestones", "digests"],
            },
        )
        assert heartbeat_response.status_code == 200

        agent_presence = await _expect_sse_event(recipe_id, queue, "agent.presence")
        assert agent_presence["data"]["agent_id"] == "agent_phase4"
        assert agent_presence["data"]["status"] == "online"

        claim_response = await client.post(
            f"/api/v1/tasks/{task_id}/claim",
            json={"agent_id": "agent_phase4"},
        )
        assert claim_response.status_code == 200

        claimed = await _expect_sse_event(recipe_id, queue, "task.claimed")
        assert claimed["data"]["task_id"] == task_id
        assert claimed["data"]["bundle_id"] == bundle_id

        milestone_response = await client.post(
            f"/api/v1/tasks/{task_id}/events",
            json={
                "type": "milestone",
                "actor_id": "agent_phase4",
                "body": "Milestone posted from the integration test.",
                "facts": [
                    {
                        "id": "fact_phase4",
                        "claim": "Cookrew can consume live milestone events.",
                        "captured_by": "agent_phase4",
                    }
                ],
                "code_refs": [
                    {
                        "repo_url": "git@github.com:test/phase4-lifecycle.git",
                        "branch": "release/integration",
                        "commit_sha": "abc1234",
                        "paths": ["cookrew/tests/e2e/krewhub.spec.ts"],
                    }
                ],
            },
        )
        assert milestone_response.status_code == 200

        milestone = await _expect_sse_event(recipe_id, queue, "task.updated")
        assert milestone["data"]["task_id"] == task_id
        assert milestone["data"]["event_type"] == "milestone"

        done_response = await client.patch(
            f"/api/v1/tasks/{task_id}/status",
            json={"status": "done"},
        )
        assert done_response.status_code == 200

        done = await _expect_sse_event(recipe_id, queue, "task.updated")
        assert done["data"]["task_id"] == task_id
        assert done["data"]["status"] == "done"

        bundle_detail_response = await client.get(f"/api/v1/bundles/{bundle_id}")
        assert bundle_detail_response.status_code == 200
        assert bundle_detail_response.json()["bundle"]["status"] == "cooked"

        digest_response = await client.post(
            f"/api/v1/bundles/{bundle_id}/digest",
            json={
                "submitted_by": "agent_phase4",
                "summary": "The integrated flow completed successfully.",
                "task_results": [
                    {
                        "task_id": task_id,
                        "outcome": "Task was completed after milestone reporting.",
                    }
                ],
                "facts": [
                    {
                        "id": "fact_digest_phase4",
                        "claim": "Digest submission works after the task is cooked.",
                        "captured_by": "agent_phase4",
                    }
                ],
                "code_refs": [
                    {
                        "repo_url": "git@github.com:test/phase4-lifecycle.git",
                        "branch": "release/integration",
                        "commit_sha": "abc1234",
                        "paths": ["krewhub/tests/test_integration.py"],
                    }
                ],
            },
        )
        assert digest_response.status_code == 200

        digest_submitted = await _expect_sse_event(
            recipe_id,
            queue,
            "bundle.digest_submitted",
        )
        assert digest_submitted["data"]["bundle_id"] == bundle_id

        decision_response = await client.post(
            f"/api/v1/bundles/{bundle_id}/decision",
            json={
                "decision": "approved",
                "decided_by": "qa.lead",
                "note": "Phase 4 flow looks good.",
            },
        )
        assert decision_response.status_code == 200

        decision = await _expect_sse_event(recipe_id, queue, "bundle.decision")
        assert decision["data"]["bundle_id"] == bundle_id
        assert decision["data"]["decision"] == "approved"

        recipe_detail_response = await client.get(f"/api/v1/recipes/{recipe_id}")
        assert recipe_detail_response.status_code == 200
        recipe_detail = recipe_detail_response.json()
        assert recipe_detail["agents"][0]["agent_id"] == "agent_phase4"
        assert recipe_detail["digests"][0]["bundle_id"] == bundle_id

        history_response = await client.get(f"/api/v1/recipes/{recipe_id}/digests")
        assert history_response.status_code == 200
        assert history_response.json()["digests"][0]["decision"] == "approved"

    finally:
        sse_service.unsubscribe(recipe_id, queue)


@pytest.mark.asyncio
async def test_rejected_bundle_events_expire_after_retention_cleanup(client):
    recipe_response = await client.post(
        "/api/v1/recipes",
        json={
            "name": "test/phase4-retention",
            "repo_url": "git@github.com:test/phase4-retention.git",
            "created_by": "qa.lead",
        },
    )
    recipe_id = recipe_response.json()["recipe"]["id"]

    bundle_response = await client.post(
        f"/api/v1/recipes/{recipe_id}/bundles",
        json={
            "prompt": "Create a rejected bundle so retention cleanup can run.",
            "requested_by": "qa.lead",
            "tasks": [{"title": "Create expiring events"}],
        },
    )
    bundle_id = bundle_response.json()["bundle"]["id"]
    task_id = bundle_response.json()["tasks"][0]["id"]

    await client.post(
        f"/api/v1/tasks/{task_id}/claim",
        json={"agent_id": "agent_cleanup"},
    )
    await client.patch(
        f"/api/v1/tasks/{task_id}/status",
        json={"status": "done"},
    )
    await client.post(
        f"/api/v1/bundles/{bundle_id}/digest",
        json={
            "submitted_by": "agent_cleanup",
            "summary": "This digest should be rejected and expire.",
            "task_results": [{"task_id": task_id, "outcome": "Done"}],
        },
    )
    reject_response = await client.post(
        f"/api/v1/bundles/{bundle_id}/decision",
        json={
            "decision": "rejected",
            "decided_by": "qa.lead",
            "note": "Use this bundle to verify retention cleanup.",
        },
    )
    assert reject_response.status_code == 200

    db = await get_db()
    event_repo = EventRepo(db)
    events_before_cleanup = await event_repo.list_by_bundle(bundle_id)
    assert events_before_cleanup
    assert all(event.expires_at is not None for event in events_before_cleanup)

    expired_at = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    await db.execute(
        "UPDATE events SET expires_at = ? WHERE bundle_id = ?",
        (expired_at, bundle_id),
    )
    await db.commit()

    deleted = await RetentionService(db).cleanup_expired()
    assert deleted == len(events_before_cleanup)

    events_after_cleanup = await event_repo.list_by_bundle(bundle_id)
    assert events_after_cleanup == []

    bundle_detail_response = await client.get(f"/api/v1/bundles/{bundle_id}")
    assert bundle_detail_response.status_code == 200
    assert bundle_detail_response.json()["bundle"]["status"] == "rejected"
