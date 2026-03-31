from __future__ import annotations

import uuid
from datetime import datetime, timezone

import aiosqlite

from krewhub.models import (
    ActorType,
    Bundle,
    BundleStatus,
    Event,
    EventType,
    Task,
    TaskStatus,
)
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.services.sse_service import sse_service


class BundleService:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._bundles = BundleRepo(db)
        self._tasks = TaskRepo(db)
        self._events = EventRepo(db)

    async def create_bundle(
        self,
        recipe_id: str,
        prompt: str,
        created_by: str,
        tasks: list[dict],
    ) -> tuple[Bundle, list[Task]]:
        now = datetime.now(timezone.utc)
        bundle_id = f"bun_{uuid.uuid4().hex[:8]}"

        bundle = Bundle(
            id=bundle_id,
            recipe_id=recipe_id,
            prompt=prompt,
            status=BundleStatus.OPEN,
            created_by=created_by,
            created_at=now,
        )
        bundle = await self._bundles.create(bundle)

        created_tasks: list[Task] = []
        for i, t in enumerate(tasks):
            task = Task(
                id=t.get("id", f"task_{uuid.uuid4().hex[:8]}"),
                bundle_id=bundle_id,
                title=t["title"],
                description=t.get("description"),
                status=TaskStatus.OPEN,
                depends_on_task_ids=t.get("depends_on_task_ids", []),
            )
            created_tasks.append(await self._tasks.create(task))

        prompt_event = Event(
            id=f"evt_{uuid.uuid4().hex[:8]}",
            recipe_id=recipe_id,
            bundle_id=bundle_id,
            type=EventType.PROMPT,
            actor_id=created_by,
            actor_type=ActorType.HUMAN,
            body=prompt,
            created_at=now,
        )
        await self._events.create(prompt_event)

        plan_event = Event(
            id=f"evt_{uuid.uuid4().hex[:8]}",
            recipe_id=recipe_id,
            bundle_id=bundle_id,
            type=EventType.PLAN,
            actor_id="system",
            actor_type=ActorType.SYSTEM,
            body=f"Created bundle with {len(created_tasks)} tasks.",
            created_at=now,
        )
        await self._events.create(plan_event)

        await sse_service.publish(recipe_id, "bundle.created", {
            "bundle_id": bundle_id,
            "status": bundle.status,
            "task_count": len(created_tasks),
        })

        return bundle, created_tasks

    async def cancel_bundle(self, bundle_id: str, actor_id: str) -> Bundle | None:
        bundle = await self._bundles.get(bundle_id)
        if bundle is None:
            return None
        if bundle.status not in (BundleStatus.OPEN, BundleStatus.BLOCKED, BundleStatus.CLAIMED):
            return None

        updated = await self._bundles.update_status(bundle_id, BundleStatus.CANCELLED)

        tasks = await self._tasks.list_by_bundle(bundle_id)
        for task in tasks:
            if task.status not in (TaskStatus.DONE, TaskStatus.CANCELLED):
                await self._tasks.update(task.id, status=TaskStatus.CANCELLED)

        return updated

    async def recompute_bundle_status(self, bundle_id: str) -> Bundle | None:
        tasks = await self._tasks.list_by_bundle(bundle_id)
        if not tasks:
            return await self._bundles.get(bundle_id)

        now = datetime.now(timezone.utc)
        all_done = all(t.status == TaskStatus.DONE for t in tasks)
        any_blocked = any(t.status == TaskStatus.BLOCKED for t in tasks)
        any_claimed = any(t.status in (TaskStatus.CLAIMED, TaskStatus.WORKING) for t in tasks)

        if all_done:
            return await self._bundles.update_status(
                bundle_id, BundleStatus.COOKED, cooked_at=now
            )
        elif any_blocked:
            blocked_reasons = [t.blocked_reason for t in tasks if t.blocked_reason]
            return await self._bundles.update_status(
                bundle_id, BundleStatus.BLOCKED,
                blocked_reason="; ".join(blocked_reasons) if blocked_reasons else "Task blocked",
            )
        elif any_claimed:
            return await self._bundles.update_status(
                bundle_id, BundleStatus.CLAIMED, claimed_at=now
            )
        return await self._bundles.update_status(bundle_id, BundleStatus.OPEN)

    async def rerun_blocked_tasks(self, bundle_id: str) -> Bundle | None:
        bundle = await self._bundles.get(bundle_id)
        if bundle is None or bundle.status in (BundleStatus.CANCELLED, BundleStatus.DIGESTED):
            return None

        tasks = await self._tasks.list_by_bundle(bundle_id)
        blocked_tasks = [task for task in tasks if task.status == TaskStatus.BLOCKED]
        if not blocked_tasks:
            return None

        for task in blocked_tasks:
            await self._tasks.reopen_for_rerun(task.id)

        updated_bundle = await self._bundles.reopen_for_rerun(bundle_id)
        if updated_bundle is None:
            return None

        now = datetime.now(timezone.utc)
        rerun_event = Event(
            id=f"evt_{uuid.uuid4().hex[:8]}",
            recipe_id=bundle.recipe_id,
            bundle_id=bundle_id,
            type=EventType.PLAN,
            actor_id="system",
            actor_type=ActorType.SYSTEM,
            body=(
                f"Re-run requested for {len(blocked_tasks)} blocked task"
                f"{'' if len(blocked_tasks) == 1 else 's'}. "
                "Tasks reopened for reassignment."
            ),
            created_at=now,
        )
        await self._events.create(rerun_event)

        for task in blocked_tasks:
            await sse_service.publish(bundle.recipe_id, "task.updated", {
                "task_id": task.id,
                "status": TaskStatus.OPEN,
                "bundle_id": bundle_id,
            })

        return updated_bundle
