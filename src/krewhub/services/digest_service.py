from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import aiosqlite

from krewhub.models import (
    ActorType,
    BundleStatus,
    CodeRef,
    Digest,
    DigestDecision,
    DigestTaskResult,
    Event,
    EventType,
    FactRef,
    TaskStatus,
    WatchEventType,
)
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.digest_repo import DigestRepo
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.tape.manager import TapeManager
from krewhub.watch.service import WatchService


class DigestService:
    def __init__(
        self, db: aiosqlite.Connection, watch: WatchService, retention_days: int = 7
    ) -> None:
        self._db = db
        self._bundles = BundleRepo(db)
        self._tasks = TaskRepo(db)
        self._digests = DigestRepo(db)
        self._events = EventRepo(db)
        self._watch = watch
        self._retention_days = retention_days

    async def submit_digest(
        self,
        bundle_id: str,
        submitted_by: str,
        summary: str,
        task_results: list[dict],
        facts: list[dict],
        code_refs: list[dict],
    ) -> Digest | None:
        bundle = await self._bundles.get(bundle_id)
        if bundle is None:
            return None
        if bundle.status not in (BundleStatus.COOKED, BundleStatus.BLOCKED):
            return None

        tasks = await self._tasks.list_by_bundle(bundle_id)
        all_terminal = all(
            t.status in (TaskStatus.DONE, TaskStatus.BLOCKED, TaskStatus.CANCELLED)
            for t in tasks
        )
        if not all_terminal:
            return None

        existing = await self._digests.get_by_bundle(bundle_id)
        if existing is not None:
            return None

        now = datetime.now(timezone.utc)
        digest = Digest(
            id=f"dig_{uuid.uuid4().hex[:8]}",
            recipe_id=bundle.recipe_id,
            bundle_id=bundle_id,
            summary=summary,
            task_results=[DigestTaskResult(**tr) for tr in task_results],
            facts=[FactRef(**f) for f in facts],
            code_refs=[CodeRef(**c) for c in code_refs],
            submitted_by=submitted_by,
            submitted_at=now,
        )
        digest = await self._digests.create(digest)

        submit_event = Event(
            id=f"evt_{uuid.uuid4().hex[:8]}",
            recipe_id=bundle.recipe_id,
            bundle_id=bundle_id,
            type=EventType.DIGEST_SUBMITTED,
            actor_id=submitted_by,
            actor_type=ActorType.AGENT,
            body=f"Digest submitted: {summary[:100]}",
            created_at=now,
        )
        await self._events.create(submit_event)

        await self._watch.record_resource(
            "digest", digest.id, WatchEventType.ADDED, digest,
            recipe_id=bundle.recipe_id,
        )

        return digest

    async def decide(
        self,
        bundle_id: str,
        decision: DigestDecision,
        decided_by: str,
        note: str | None = None,
    ) -> Digest | None:
        digest = await self._digests.get_by_bundle(bundle_id)
        if digest is None or digest.decision != DigestDecision.PENDING:
            return None

        now = datetime.now(timezone.utc)
        updated = await self._digests.update_decision(
            digest.id, decision, decided_by, now
        )

        if decision == DigestDecision.APPROVED:
            bundle = await self._bundles.update_status(
                bundle_id, BundleStatus.DIGESTED, digested_at=now
            )
            event_type = EventType.DIGEST_APPROVED
            event_expires_at = None
            # Create durable tape anchor for approved digest
            tape = TapeManager(self._db, updated.recipe_id)
            await tape.create_digest_anchor(updated)
        else:
            bundle = await self._bundles.update_status(bundle_id, BundleStatus.REJECTED)
            expires_at = now + timedelta(days=self._retention_days)
            await self._events.set_expiry_for_bundle(bundle_id, expires_at)
            event_type = EventType.DIGEST_REJECTED
            event_expires_at = expires_at

        decision_note = note.strip() if note else ""
        decision_event = Event(
            id=f"evt_{uuid.uuid4().hex[:8]}",
            recipe_id=digest.recipe_id,
            bundle_id=bundle_id,
            type=event_type,
            actor_id=decided_by,
            actor_type=ActorType.HUMAN,
            body=(
                f"Digest {decision}. {decision_note}"
                if decision_note
                else f"Digest {decision}."
            ),
            created_at=now,
            expires_at=event_expires_at,
        )
        await self._events.create(decision_event)

        if updated is not None:
            await self._watch.record_resource(
                "digest", updated.id, WatchEventType.MODIFIED, updated,
                recipe_id=digest.recipe_id,
            )
        if bundle is not None:
            await self._watch.record_resource(
                "bundle", bundle_id, WatchEventType.MODIFIED, bundle,
                recipe_id=digest.recipe_id,
            )

        return updated
