"""
Evals: verify republic tape integration works across the full
task and bundle lifecycle.

These tests exercise the complete cycle:
  fork → push → navigate → merge → anchor continuity

Each test validates a specific invariant of the tape.systems model
as implemented with republic primitives in krewhub.
"""

from __future__ import annotations

import pytest

from krewhub.db.connection import get_db


# ── helpers ───────────────────────────────────────────────────────


async def _setup_recipe(client) -> str:
    cb = await client.post("/api/v1/cookbooks", json={
        "name": "eval-tape-cookbook",
        "owner_id": "human_1",
    })
    cookbook_id = cb.json()["cookbook"]["id"]
    resp = await client.post("/api/v1/recipes", json={
        "name": "eval/tape",
        "repo_url": "git@github.com:eval/tape.git",
        "created_by": "human_1",
        "cookbook_id": cookbook_id,
    })
    return resp.json()["recipe"]["id"]


async def _setup_bundle(client, recipe_id: str, task_titles: list[str]):
    resp = await client.post(f"/api/v1/recipes/{recipe_id}/bundles", json={
        "prompt": "Eval tape lifecycle",
        "requested_by": "human_1",
        "tasks": [{"title": t} for t in task_titles],
    })
    bundle_id = resp.json()["bundle"]["id"]
    task_ids = [t["id"] for t in resp.json()["tasks"]]
    return bundle_id, task_ids


async def _claim_and_complete(client, task_ids: list[str]):
    for tid in task_ids:
        await client.post(f"/api/v1/tasks/{tid}/claim", json={"agent_id": "eval_agent"})
        await client.patch(f"/api/v1/tasks/{tid}/status", json={"status": "done"})


async def _push_fork(client, recipe_id: str, bundle_id: str, task_id: str, entries: list[dict]):
    resp = await client.post(f"/api/v1/tapes/{recipe_id}/fork-entries", json={
        "bundle_id": bundle_id,
        "task_id": task_id,
        "entries": entries,
    })
    assert resp.status_code == 200
    return resp.json()


async def _submit_and_approve(client, bundle_id: str, task_ids: list[str]):
    await client.post(f"/api/v1/bundles/{bundle_id}/digest", json={
        "submitted_by": "eval_agent",
        "summary": "Eval complete",
        "task_results": [{"task_id": tid, "outcome": "Done"} for tid in task_ids],
    })
    resp = await client.post(f"/api/v1/bundles/{bundle_id}/decision", json={
        "decision": "approved",
        "decided_by": "human_1",
    })
    assert resp.status_code == 200
    return resp.json()


# ── Eval 1: republic TapeEntry is a drop-in replacement ──────────


@pytest.mark.asyncio
async def test_eval_republic_entry_roundtrip(client):
    """TapeEntry from republic serializes/deserializes identically to
    the old custom TapeEntry — same fields, same JSON shape."""
    recipe_id = await _setup_recipe(client)

    resp = await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "milestone",
        "payload": {"body": "step 1", "score": 0.95},
        "meta": {"actor_id": "agent_1", "custom_key": "custom_val"},
    })
    assert resp.status_code == 200
    entry = resp.json()["entry"]

    # Verify all 5 republic TapeEntry fields present
    assert set(entry.keys()) == {"id", "kind", "payload", "meta", "date"}
    assert isinstance(entry["id"], int)
    assert entry["kind"] == "milestone"
    assert entry["payload"]["body"] == "step 1"
    assert entry["payload"]["score"] == 0.95
    assert entry["meta"]["actor_id"] == "agent_1"
    assert entry["meta"]["custom_key"] == "custom_val"
    assert isinstance(entry["date"], str)


# ── Eval 2: TapeQuery.last_anchor() works for context reads ──────


@pytest.mark.asyncio
async def test_eval_tapequery_last_anchor(client):
    """Context endpoint uses TapeQuery.last_anchor() correctly —
    only entries after the last anchor are returned."""
    recipe_id = await _setup_recipe(client)

    # Seed: 3 entries, then anchor, then 2 entries
    for i in range(3):
        await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
            "kind": "milestone", "payload": {"body": f"old-{i}"},
        })
    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "anchor",
        "payload": {"name": "phase-1", "phase": "digested", "summary": "Phase 1 done"},
    })
    for i in range(2):
        await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
            "kind": "milestone", "payload": {"body": f"new-{i}"},
        })

    # Context should return only the 2 post-anchor entries
    resp = await client.get(f"/api/v1/tapes/{recipe_id}/context")
    assert resp.status_code == 200
    entries = resp.json()["entries"]
    assert len(entries) == 2
    assert entries[0]["payload"]["body"] == "new-0"
    assert entries[1]["payload"]["body"] == "new-1"


# ── Eval 3: TapeQuery.kinds() filters correctly ──────────────────


@pytest.mark.asyncio
async def test_eval_tapequery_kinds_filter(client):
    """Anchors endpoint uses TapeQuery.kinds('anchor') — only anchor
    entries returned, other kinds excluded."""
    recipe_id = await _setup_recipe(client)

    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "milestone", "payload": {"body": "not an anchor"},
    })
    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "anchor", "payload": {"name": "a1", "summary": "first"},
    })
    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "event", "payload": {"body": "also not an anchor"},
    })
    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "anchor", "payload": {"name": "a2", "summary": "second"},
    })

    resp = await client.get(f"/api/v1/tapes/{recipe_id}/anchors")
    assert resp.status_code == 200
    anchors = resp.json()["anchors"]
    assert len(anchors) == 2
    assert anchors[0]["payload"]["name"] == "a1"
    assert anchors[1]["payload"]["name"] == "a2"


# ── Eval 4: fork tape isolation ──────────────────────────────────


@pytest.mark.asyncio
async def test_eval_fork_isolation(client):
    """Fork entries are isolated from the parent recipe tape.
    Parent tape reads should NOT include fork entries until merge."""
    recipe_id = await _setup_recipe(client)

    # Write to parent tape
    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "milestone", "payload": {"body": "parent work"},
    })

    # Write to fork tape
    await _push_fork(client, recipe_id, "bun_iso", "task_iso", [
        {"kind": "milestone", "payload": {"body": "forked work"}},
    ])

    # Parent tape should have 1 entry (not 2)
    resp = await client.get(f"/api/v1/tapes/{recipe_id}/history")
    parent_entries = resp.json()["entries"]
    assert len(parent_entries) == 1
    assert parent_entries[0]["payload"]["body"] == "parent work"

    # Fork tape should have its own entry
    resp = await client.get(
        f"/api/v1/tapes/{recipe_id}/fork-entries/bun_iso",
        params={"task_id": "task_iso"},
    )
    fork_entries = resp.json()["entries"]
    assert len(fork_entries) == 1
    assert fork_entries[0]["payload"]["body"] == "forked work"


# ── Eval 5: multi-task fork aggregation ──────────────────────────


@pytest.mark.asyncio
async def test_eval_multi_task_fork_aggregation(client):
    """Multiple tasks in a bundle each have their own fork tape.
    Bundle-level query returns entries from ALL task forks."""
    recipe_id = await _setup_recipe(client)
    bundle_id = "bun_agg"

    for i, tid in enumerate(["task_x", "task_y", "task_z"]):
        await _push_fork(client, recipe_id, bundle_id, tid, [
            {"kind": "milestone", "payload": {"body": f"work-{tid}"}},
            {"kind": "anchor", "payload": {
                "name": f"handoff:{bundle_id}/{tid}",
                "phase": "task_complete",
                "summary": f"Task {tid} done",
                "code_ref": {"sha": f"sha_{i}", "branch": f"task/{bundle_id}/{tid}"},
            }},
        ])

    # Bundle-level query returns all 6 entries (2 per task)
    resp = await client.get(f"/api/v1/tapes/{recipe_id}/fork-entries/{bundle_id}")
    assert resp.status_code == 200
    entries = resp.json()["entries"]
    assert len(entries) == 6

    # Each task's handoff anchor is present with its code_ref
    anchors = [e for e in entries if e["kind"] == "anchor"]
    assert len(anchors) == 3
    shas = {a["payload"]["code_ref"]["sha"] for a in anchors}
    assert shas == {"sha_0", "sha_1", "sha_2"}


# ── Eval 6: full lifecycle — fork → approve → merge ──────────────


@pytest.mark.asyncio
async def test_eval_full_lifecycle_fork_merge(client):
    """Complete cycle: create bundle → claim → push fork entries →
    complete tasks → submit digest → approve → verify fork entries
    merged into parent tape with provenance tracking."""
    recipe_id = await _setup_recipe(client)
    bundle_id, task_ids = await _setup_bundle(client, recipe_id, ["Build API", "Write tests"])

    # Claim and push fork entries for each task
    for i, tid in enumerate(task_ids):
        await client.post(f"/api/v1/tasks/{tid}/claim", json={"agent_id": "eval_agent"})
        await _push_fork(client, recipe_id, bundle_id, tid, [
            {"kind": "milestone", "payload": {"body": f"step-{i}a"}},
            {"kind": "tool_call", "payload": {"tool": "edit_file", "path": f"src/mod{i}.py"}},
            {"kind": "anchor", "payload": {
                "name": f"handoff:{bundle_id}/{tid}",
                "phase": "task_complete",
                "summary": f"Task {i} complete",
                "code_ref": {"sha": f"abc{i}", "branch": f"task/{bundle_id}/{tid}"},
            }},
        ])
        await client.patch(f"/api/v1/tasks/{tid}/status", json={"status": "done"})

    # Submit and approve digest
    await _submit_and_approve(client, bundle_id, task_ids)

    # Verify: parent tape now contains fork entries after the digest anchor
    resp = await client.get(f"/api/v1/tapes/{recipe_id}/history")
    all_entries = resp.json()["entries"]
    kinds = [e["kind"] for e in all_entries]

    # There must be an anchor from the digest
    assert "anchor" in kinds
    anchor_idx = max(i for i, k in enumerate(kinds) if k == "anchor"
                     and all_entries[i]["payload"].get("phase") == "digested")

    # Fork entries appear after the digest anchor
    post_anchor = all_entries[anchor_idx + 1:]
    merged_with_provenance = [
        e for e in post_anchor if e.get("meta", {}).get("fork_source")
    ]
    # 6 fork entries total (3 per task × 2 tasks)
    assert len(merged_with_provenance) == 6

    # Verify provenance tracks which fork each entry came from
    sources = {e["meta"]["fork_source"] for e in merged_with_provenance}
    for tid in task_ids:
        assert f"fork:{bundle_id}/{tid}" in sources

    # Verify handoff anchors preserved with code_ref
    merged_anchors = [e for e in merged_with_provenance if e["kind"] == "anchor"]
    assert len(merged_anchors) == 2
    assert all("code_ref" in a["payload"] for a in merged_anchors)


# ── Eval 7: anchor continuity across bundles ─────────────────────


@pytest.mark.asyncio
async def test_eval_anchor_continuity_across_bundles(client):
    """After bundle 1 is digested, bundle 2's context read starts
    from bundle 1's digest anchor — proving anchor continuity."""
    recipe_id = await _setup_recipe(client)

    # === Bundle 1 ===
    b1_id, b1_tasks = await _setup_bundle(client, recipe_id, ["Task 1A"])
    await _claim_and_complete(client, b1_tasks)
    await _push_fork(client, recipe_id, b1_id, b1_tasks[0], [
        {"kind": "milestone", "payload": {"body": "bundle-1-work"}},
    ])
    await _submit_and_approve(client, b1_id, b1_tasks)

    # === Bundle 2 ===
    b2_id, b2_tasks = await _setup_bundle(client, recipe_id, ["Task 2A"])

    # Context read for bundle 2 should NOT include bundle 1's pre-anchor entries
    resp = await client.get(f"/api/v1/tapes/{recipe_id}/context")
    context_entries = resp.json()["entries"]
    context_kinds = [e["kind"] for e in context_entries]

    # Pre-anchor entries (prompt, plan, task_claimed from bundle 1) should be excluded
    # Only post-anchor entries (digest_approved event + merged fork entries) should be present
    assert "prompt" not in context_kinds or all(
        e["payload"].get("bundle_id") != b1_id
        for e in context_entries
        if e["kind"] == "prompt"
    )


# ── Eval 8: empty fork is harmless ──────────────────────────────


@pytest.mark.asyncio
async def test_eval_empty_fork_merge_is_noop(client):
    """If no fork entries were pushed (legacy agents), digest approval
    still works — merge returns 0 and causes no errors."""
    recipe_id = await _setup_recipe(client)
    bundle_id, task_ids = await _setup_bundle(client, recipe_id, ["Legacy task"])
    await _claim_and_complete(client, task_ids)

    # Submit and approve WITHOUT pushing any fork entries
    result = await _submit_and_approve(client, bundle_id, task_ids)
    assert result["digest"]["decision"] == "approved"

    # Parent tape should have normal entries but no fork-sourced ones
    resp = await client.get(f"/api/v1/tapes/{recipe_id}/history")
    entries = resp.json()["entries"]
    fork_sourced = [e for e in entries if e.get("meta", {}).get("fork_source")]
    assert len(fork_sourced) == 0


# ── Eval 9: anchor payload carries structured handoff state ──────


@pytest.mark.asyncio
async def test_eval_anchor_handoff_payload(client):
    """Handoff anchors carry the full structured state contract:
    name, phase, summary, code_ref, facts, decisions, next_steps."""
    recipe_id = await _setup_recipe(client)
    bundle_id = "bun_handoff"
    task_id = "task_handoff"

    await _push_fork(client, recipe_id, bundle_id, task_id, [
        {"kind": "anchor", "payload": {
            "name": f"handoff:{bundle_id}/{task_id}",
            "phase": "task_complete",
            "summary": "Built REST API with JWT auth",
            "facts": [
                {"claim": "API rate limit is 100/min", "source_url": "https://docs.example.com"},
            ],
            "decisions": ["Chose RS256 for key rotation"],
            "code_ref": {
                "sha": "a1b2c3d",
                "branch": f"task/{bundle_id}/{task_id}",
                "paths": ["src/auth.py", "tests/test_auth.py"],
            },
            "next_steps": ["Write integration tests"],
        }},
    ])

    resp = await client.get(
        f"/api/v1/tapes/{recipe_id}/fork-entries/{bundle_id}",
        params={"task_id": task_id},
    )
    anchor = resp.json()["entries"][0]
    p = anchor["payload"]

    assert p["phase"] == "task_complete"
    assert p["summary"] == "Built REST API with JWT auth"
    assert len(p["facts"]) == 1
    assert p["facts"][0]["claim"] == "API rate limit is 100/min"
    assert p["decisions"] == ["Chose RS256 for key rotation"]
    assert p["code_ref"]["sha"] == "a1b2c3d"
    assert "src/auth.py" in p["code_ref"]["paths"]
    assert p["next_steps"] == ["Write integration tests"]


# ── Eval 10: rewind/forward via sinceAnchor int API ─────────────


@pytest.mark.asyncio
async def test_eval_rewind_forward_via_anchor_id(client):
    """sinceAnchor query param enables rewind/forward between anchors
    using integer anchor IDs — the existing API contract works with
    republic's SqliteTapeStore."""
    recipe_id = await _setup_recipe(client)

    # Build a timeline: entries, anchor-1, entries, anchor-2, entries
    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "milestone", "payload": {"body": "phase-0-work"},
    })
    resp = await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "anchor", "payload": {"name": "anchor-1", "summary": "Phase 0 done"},
    })
    anchor_1_id = resp.json()["entry"]["id"]

    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "milestone", "payload": {"body": "phase-1-work"},
    })
    resp = await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "anchor", "payload": {"name": "anchor-2", "summary": "Phase 1 done"},
    })
    anchor_2_id = resp.json()["entry"]["id"]

    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "milestone", "payload": {"body": "phase-2-work"},
    })

    # Forward from anchor-1: should see phase-1-work + anchor-2 + phase-2-work
    resp = await client.get(
        f"/api/v1/tapes/{recipe_id}/context",
        params={"sinceAnchor": anchor_1_id},
    )
    entries = resp.json()["entries"]
    bodies = [e["payload"].get("body") or e["payload"].get("summary") for e in entries]
    assert "phase-1-work" in bodies
    assert "phase-2-work" in bodies
    assert "phase-0-work" not in bodies

    # Forward from anchor-2: should see only phase-2-work
    resp = await client.get(
        f"/api/v1/tapes/{recipe_id}/context",
        params={"sinceAnchor": anchor_2_id},
    )
    entries = resp.json()["entries"]
    assert len(entries) == 1
    assert entries[0]["payload"]["body"] == "phase-2-work"


# ── Eval 11: tape_entries table unchanged (backward compat) ──────


@pytest.mark.asyncio
async def test_eval_sqlite_schema_backward_compat(client):
    """Republic integration did NOT change the tape_entries schema.
    Direct SQL queries still work identically to pre-republic code."""
    recipe_id = await _setup_recipe(client)

    await client.post(f"/api/v1/tapes/{recipe_id}/entries", json={
        "kind": "milestone", "payload": {"body": "direct SQL check"},
    })

    db = await get_db()
    cursor = await db.execute(
        "SELECT id, tape_name, kind, payload, meta, created_at "
        "FROM tape_entries WHERE tape_name = ? ORDER BY id",
        (f"recipe:{recipe_id}",),
    )
    rows = await cursor.fetchall()
    assert len(rows) == 1

    row = rows[0]
    assert row["tape_name"] == f"recipe:{recipe_id}"
    assert row["kind"] == "milestone"
    # payload and meta are JSON TEXT columns — parseable
    import json
    payload = json.loads(row["payload"])
    assert payload["body"] == "direct SQL check"
    meta = json.loads(row["meta"])
    assert isinstance(meta, dict)
    assert isinstance(row["created_at"], str)


# ── Eval 12: fork tape naming convention in DB ───────────────────


@pytest.mark.asyncio
async def test_eval_fork_tape_naming_in_db(client):
    """Fork tapes use 'fork:{bundle_id}/{task_id}' naming convention
    in the tape_entries table — verifiable via direct SQL."""
    recipe_id = await _setup_recipe(client)
    bundle_id = "bun_naming"
    task_id = "task_naming"

    await _push_fork(client, recipe_id, bundle_id, task_id, [
        {"kind": "milestone", "payload": {"body": "naming check"}},
    ])

    db = await get_db()
    cursor = await db.execute(
        "SELECT tape_name FROM tape_entries WHERE tape_name LIKE 'fork:%'",
    )
    rows = await cursor.fetchall()
    tape_names = [r["tape_name"] for r in rows]
    assert f"fork:{bundle_id}/{task_id}" in tape_names
