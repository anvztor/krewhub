"""GET /api/v1/bundles/{id} caps events via events_limit query param.

Bundle Lifecycle perf fix: cookrew-beta's refreshBundles used to N+1
getBundle on tab-strip render to compute task counts. We surfaced
task_count on the list aggregate, but the per-bundle detail call still
returned an unbounded events list — the fattest bundles have 65K+
milestone events from graph runners, so the SPA was downloading
megabytes per click. SSE drives real-time event updates, so historical
events on bundle load can be capped (or skipped entirely) safely.

events_limit=0  -> no events (use SSE)
events_limit=N  -> most-recent N events in chronological order
default         -> 100
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from httpx import AsyncClient


async def _seed_bundle_with_events(db, n_events: int) -> str:
    """Seed a cookbook + bundle + n events; returns the bundle id."""
    await db.execute(
        "INSERT OR IGNORE INTO cookbooks (id, name, owner_id, created_at) "
        "VALUES (?,?,?,?)",
        ("cb_evlim", "evlim", "alice", "2026-01-01"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO bundles (id, cookbook_id, prompt, status, "
        "created_by, created_at) VALUES (?,?,?,?,?,?)",
        ("b_evlim", "cb_evlim", "p", "open", "alice", "2026-01-01"),
    )
    for i in range(n_events):
        await db.execute(
            "INSERT INTO events (id, cookbook_id, bundle_id, task_id, type, "
            "actor_id, actor_type, body, payload, sequence, facts, code_refs, "
            "visibility, created_at) "
            "VALUES (?, ?, ?, NULL, 'milestone', 'sys', 'system', ?, '{}', ?, "
            "'[]', '[]', 'system', ?)",
            (
                f"e_evlim_{i}",
                "cb_evlim",
                "b_evlim",
                f"event {i}",
                i,
                datetime(2026, 5, 1, 0, 0, i, tzinfo=timezone.utc).isoformat(),
            ),
        )
    await db.commit()
    return "b_evlim"


@pytest.mark.asyncio
async def test_get_bundle_events_limit_zero_returns_no_events(client: AsyncClient):
    """events_limit=0 yields events=[] regardless of stored count."""
    from krewhub.db.connection import get_db
    db = await get_db()
    bundle_id = await _seed_bundle_with_events(db, n_events=5)

    r = await client.get(f"/api/v1/bundles/{bundle_id}?events_limit=0")
    assert r.status_code == 200, r.text
    assert r.json()["events"] == []


@pytest.mark.asyncio
async def test_get_bundle_events_limit_caps_count(client: AsyncClient):
    """events_limit=2 returns the 2 most-recent events in ascending order."""
    from krewhub.db.connection import get_db
    db = await get_db()
    bundle_id = await _seed_bundle_with_events(db, n_events=5)

    r = await client.get(f"/api/v1/bundles/{bundle_id}?events_limit=2")
    assert r.status_code == 200, r.text
    events = r.json()["events"]
    assert len(events) == 2
    # ASC contract: most-recent N returned in chronological order so the
    # last seeded (sequence=4) is at the END of the list.
    assert events[0]["sequence"] == 3
    assert events[1]["sequence"] == 4


@pytest.mark.asyncio
async def test_get_bundle_default_returns_events(client: AsyncClient):
    """No query param -> default 100 -> all 5 seeded events returned ASC."""
    from krewhub.db.connection import get_db
    db = await get_db()
    bundle_id = await _seed_bundle_with_events(db, n_events=5)

    r = await client.get(f"/api/v1/bundles/{bundle_id}")
    assert r.status_code == 200, r.text
    events = r.json()["events"]
    assert len(events) == 5
    assert [e["sequence"] for e in events] == [0, 1, 2, 3, 4]
