from __future__ import annotations

import uuid
from datetime import datetime, timezone

import aiosqlite

from krewhub.models import (
    ActorType,
    CodeRef,
    Event,
    EventType,
    FactRef,
    Task,
    TaskStatus,
    WatchEventType,
)
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.watch.service import WatchService


class TaskService:
    def __init__(self, db: aiosqlite.Connection, watch: WatchService) -> None:
        self._tasks = TaskRepo(db)
        self._events = EventRepo(db)
        self._agents = AgentRepo(db)
        self._bundles = BundleRepo(db)
        self._watch = watch

    async def claim_task(
        self, task_id: str, agent_id: str, recipe_id: str
    ) -> Task | None:
        task = await self._tasks.get(task_id)
        if task is None or task.status != TaskStatus.OPEN:
            return None

        active_tasks = await self._tasks.list_active_by_agent(recipe_id, agent_id)
        if active_tasks:
            return None

        deps = task.depends_on_task_ids
        if deps:
            for dep_id in deps:
                dep = await self._tasks.get(dep_id)
                if dep is None or dep.status != TaskStatus.DONE:
                    return None

        now = datetime.now(timezone.utc)
        updated = await self._tasks.update(
            task_id,
            status=TaskStatus.CLAIMED,
            claimed_by_agent_id=agent_id,
            claimed_at=now,
        )

        claim_event = Event(
            id=f"evt_{uuid.uuid4().hex[:8]}",
            recipe_id=recipe_id,
            bundle_id=task.bundle_id,
            task_id=task_id,
            type=EventType.TASK_CLAIMED,
            actor_id=agent_id,
            actor_type=ActorType.AGENT,
            body=f"Task '{task.title}' claimed.",
            created_at=now,
        )
        await self._events.create(claim_event)

        if updated is not None:
            await self._watch.record_resource(
                "task", task_id, WatchEventType.MODIFIED, updated,
                recipe_id=recipe_id,
            )

        return updated

    async def post_event(
        self,
        task_id: str,
        recipe_id: str,
        event_type: EventType,
        actor_id: str,
        actor_type: ActorType,
        body: str,
        facts: list[FactRef] | None = None,
        code_refs: list[CodeRef] | None = None,
        payload: dict | None = None,
    ) -> Event:
        task = await self._tasks.get(task_id)
        if task is None:
            raise ValueError(f"Task {task_id} not found")

        # Hook events are passive observations and should not flip the
        # task into WORKING. Only first-class agent/system events do.
        if task.status == TaskStatus.CLAIMED and actor_type != ActorType.HOOK:
            updated = await self._tasks.update(task_id, status=TaskStatus.WORKING)
            if updated is not None:
                await self._watch.record_resource(
                    "task", task_id, WatchEventType.MODIFIED, updated,
                    recipe_id=recipe_id,
                )

        now = datetime.now(timezone.utc)
        event = Event(
            id=f"evt_{uuid.uuid4().hex[:8]}",
            recipe_id=recipe_id,
            bundle_id=task.bundle_id,
            task_id=task_id,
            type=event_type,
            actor_id=actor_id,
            actor_type=actor_type,
            body=body,
            facts=facts or [],
            code_refs=code_refs or [],
            payload=payload or {},
            created_at=now,
        )
        await self._events.create(event)

        # Broadcast every event so SSE consumers (cookrew) see hook
        # events live in the bundle feed without an extra round-trip.
        await self._watch.record(
            resource_type="event",
            resource_id=event.id,
            event_type=WatchEventType.ADDED,
            resource_version=1,
            payload=event.model_dump(mode="json"),
            recipe_id=recipe_id,
        )

        return event

    async def mark_done(self, task_id: str) -> Task | None:
        task = await self._tasks.get(task_id)
        if task is None:
            return None

        now = datetime.now(timezone.utc)
        updated = await self._tasks.update(
            task_id, status=TaskStatus.DONE, completed_at=now
        )
        if updated is not None:
            await self._publish_task_update(updated)
        return updated

    async def mark_blocked(self, task_id: str, reason: str) -> Task | None:
        task = await self._tasks.get(task_id)
        if task is None:
            return None

        updated = await self._tasks.update(
            task_id, status=TaskStatus.BLOCKED, blocked_reason=reason
        )
        if updated is not None:
            await self._publish_task_update(updated)
        return updated

    async def add_task(
        self,
        bundle_id: str,
        title: str,
        description: str | None = None,
        depends_on_task_ids: list[str] | None = None,
    ) -> Task:
        task = Task(
            id=f"task_{uuid.uuid4().hex[:8]}",
            bundle_id=bundle_id,
            title=title,
            description=description,
            status=TaskStatus.OPEN,
            depends_on_task_ids=depends_on_task_ids or [],
        )
        created = await self._tasks.create(task)

        bundle = await self._bundles.get(bundle_id)
        if bundle is not None:
            await self._watch.record_resource(
                "task", created.id, WatchEventType.ADDED, created,
                recipe_id=bundle.recipe_id,
            )

        return created

    async def edit_task(
        self,
        task_id: str,
        title: str | None = None,
        description: str | None = None,
        depends_on_task_ids: list[str] | None = None,
    ) -> Task | None:
        updated = await self._tasks.update(
            task_id,
            title=title,
            description=description,
            depends_on_task_ids=depends_on_task_ids,
        )
        if updated is not None:
            await self._publish_task_update(updated)
        return updated

    async def remove_task(self, task_id: str) -> bool:
        task = await self._tasks.get(task_id)
        if task is None or task.status != TaskStatus.OPEN:
            return False

        deleted = await self._tasks.delete(task_id)
        if deleted:
            bundle = await self._bundles.get(task.bundle_id)
            recipe_id = bundle.recipe_id if bundle else None
            await self._watch.record(
                "task", task_id, WatchEventType.DELETED,
                resource_version=task.resource_version,
                payload=task.model_dump(mode="json"),
                recipe_id=recipe_id,
            )
        return deleted

    async def _publish_task_update(self, task: Task) -> None:
        bundle = await self._bundles.get(task.bundle_id)
        if bundle is None:
            return
        await self._watch.record_resource(
            "task", task.id, WatchEventType.MODIFIED, task,
            recipe_id=bundle.recipe_id,
        )
