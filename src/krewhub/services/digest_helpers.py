"""DEPRECATED — digest aggregation helpers.

Bundles are moving to a two-state OPEN/CLOSED model with no
approve/reject step. This module exists to support the legacy
digest_service, which is itself deprecated. Do not extend.

Removal target: once GraphRunnerController stops calling these and
the digest flow is deleted.

Originally extracted from GraphRunnerController to aggregate facts +
code_refs collected by dispatch_cycle across graph nodes and submit a
digest.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from krewhub.services.digest_service import DigestService

if TYPE_CHECKING:
    import aiosqlite

    from krewhub.services.graph_runtime.state import OrchestratorState
    from krewhub.watch.service import WatchService

logger = logging.getLogger(__name__)


def dedupe_facts(facts: list[dict]) -> list[dict]:
    """Remove duplicate facts by (claim, source_url, captured_by) key."""
    seen: set[str] = set()
    unique: list[dict] = []
    for f in facts:
        key = f"{f.get('claim', '')}::{f.get('source_url', '')}::{f.get('captured_by', '')}"
        if key not in seen:
            seen.add(key)
            unique.append(f)
    return unique


def dedupe_code_refs(code_refs: list[dict]) -> list[dict]:
    """Remove duplicate code_refs by (repo_url, branch, commit_sha) key."""
    seen: set[str] = set()
    unique: list[dict] = []
    for c in code_refs:
        paths = "::".join(sorted(c.get("paths", [])))
        key = f"{c.get('repo_url', '')}::{c.get('branch', '')}::{c.get('commit_sha', '')}::{paths}"
        if key not in seen:
            seen.add(key)
            unique.append(c)
    return unique


async def aggregate_and_submit_digest(
    db: "aiosqlite.Connection",
    watch: "WatchService",
    bundle_id: str,
    state: "OrchestratorState",
) -> None:
    """Aggregate facts/code_refs from graph state and submit a digest."""
    all_facts: list[dict] = []
    all_code_refs: list[dict] = []
    task_results: list[dict] = []

    for result in state.task_results.values():
        task_results.append({
            "task_id": result.task_id,
            "outcome": result.summary,
        })
        all_facts.extend(result.facts)
        all_code_refs.extend(result.code_refs)

    all_facts = dedupe_facts(all_facts)
    all_code_refs = dedupe_code_refs(all_code_refs)

    node_count = len(state.task_results)
    fact_count = len(all_facts)
    code_ref_count = len(all_code_refs)
    summary = (
        f"Completed {node_count} task{'s' if node_count != 1 else ''}"
        f" — {fact_count} fact{'s' if fact_count != 1 else ''},"
        f" {code_ref_count} code ref{'s' if code_ref_count != 1 else ''}"
    )

    try:
        digest_svc = DigestService(db, watch)
        digest = await digest_svc.submit_digest(
            bundle_id=bundle_id,
            submitted_by="graph-runner",
            summary=summary,
            task_results=task_results,
            facts=all_facts,
            code_refs=all_code_refs,
        )
        if digest is not None:
            logger.info(
                "digest_helpers: auto-submitted digest %s for bundle %s",
                digest.id, bundle_id,
            )
        else:
            logger.warning(
                "digest_helpers: digest submission returned None for bundle %s "
                "(digest may already exist or bundle not in expected state)",
                bundle_id,
            )
    except Exception:
        logger.exception(
            "digest_helpers: failed to auto-submit digest for bundle %s",
            bundle_id,
        )
