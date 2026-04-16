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
        payload: dict | None = None,
        facts: list[FactRef] | None = None,
        code_refs: list[CodeRef] | None = None,
        visibility: str | None = None,
    ) -> Event:
        from krewhub.services.event_visibility import classify_visibility

        task = await self._tasks.get(task_id)
        if task is None:
            raise ValueError(f"Task {task_id} not found")

        if task.status == TaskStatus.CLAIMED:
            updated = await self._tasks.update(task_id, status=TaskStatus.WORKING)
            if updated is not None:
                await self._watch.record_resource(
                    "task", task_id, WatchEventType.MODIFIED, updated,
                    recipe_id=recipe_id,
                )
                working_event = Event(
                    id=f"evt_{uuid.uuid4().hex[:8]}",
                    recipe_id=recipe_id,
                    bundle_id=task.bundle_id,
                    task_id=task_id,
                    type=EventType.TASK_WORKING,
                    actor_id=actor_id,
                    actor_type=actor_type,
                    body=f"Task '{task.title}' is now working.",
                    visibility=classify_visibility("task_working"),
                    created_at=datetime.now(timezone.utc),
                )
                await self._events.create(working_event)
                await self._watch.record_resource(
                    "event", working_event.id, WatchEventType.ADDED, working_event,
                    recipe_id=recipe_id,
                )

        sequence = await self._events.next_sequence(task_id)
        now = datetime.now(timezone.utc)
        resolved_visibility = visibility or classify_visibility(str(event_type))
        event = Event(
            id=f"evt_{uuid.uuid4().hex[:8]}",
            recipe_id=recipe_id,
            bundle_id=task.bundle_id,
            task_id=task_id,
            type=event_type,
            actor_id=actor_id,
            actor_type=actor_type,
            body=body,
            payload=payload,
            sequence=sequence,
            facts=facts or [],
            code_refs=code_refs or [],
            visibility=resolved_visibility,
            created_at=now,
        )
        await self._events.create(event)
        await self._watch.record_resource(
            "event", event.id, WatchEventType.ADDED, event,
            recipe_id=recipe_id,
        )

        return event

    async def post_events_batch(
        self,
        task_id: str,
        recipe_id: str,
        events: list[dict],
    ) -> list[Event]:
        """Append a batch of events for a single task.

        Each dict must contain: type, actor_id, actor_type, body (optional),
        payload (optional), facts (optional), code_refs (optional).
        Sequence numbers are assigned server-side, monotonically per task.
        """
        if not events:
            return []

        task = await self._tasks.get(task_id)
        if task is None:
            raise ValueError(f"Task {task_id} not found")

        if task.status == TaskStatus.CLAIMED:
            updated = await self._tasks.update(task_id, status=TaskStatus.WORKING)
            if updated is not None:
                await self._watch.record_resource(
                    "task", task_id, WatchEventType.MODIFIED, updated,
                    recipe_id=recipe_id,
                )
                first = events[0]
                working_event = Event(
                    id=f"evt_{uuid.uuid4().hex[:8]}",
                    recipe_id=recipe_id,
                    bundle_id=task.bundle_id,
                    task_id=task_id,
                    type=EventType.TASK_WORKING,
                    actor_id=first["actor_id"],
                    actor_type=ActorType(first.get("actor_type", "agent")),
                    body=f"Task '{task.title}' is now working.",
                    created_at=datetime.now(timezone.utc),
                )
                await self._events.create(working_event)
                await self._watch.record_resource(
                    "event", working_event.id, WatchEventType.ADDED, working_event,
                    recipe_id=recipe_id,
                )

        from krewhub.services.event_visibility import classify_visibility

        created: list[Event] = []
        for spec in events:
            sequence = await self._events.next_sequence(task_id)
            now = datetime.now(timezone.utc)
            facts_raw = spec.get("facts") or []
            code_refs_raw = spec.get("code_refs") or []
            evt_type = spec["type"]
            vis = spec.get("visibility") or classify_visibility(evt_type)
            event = Event(
                id=f"evt_{uuid.uuid4().hex[:8]}",
                recipe_id=recipe_id,
                bundle_id=task.bundle_id,
                task_id=task_id,
                type=EventType(evt_type),
                actor_id=spec["actor_id"],
                actor_type=ActorType(spec.get("actor_type", "agent")),
                body=spec.get("body", ""),
                payload=spec.get("payload"),
                sequence=sequence,
                facts=[FactRef(**f) for f in facts_raw],
                code_refs=[CodeRef(**c) for c in code_refs_raw],
                visibility=vis,
                created_at=now,
            )
            await self._events.create(event)
            await self._watch.record_resource(
                "event", event.id, WatchEventType.ADDED, event,
                recipe_id=recipe_id,
            )
            created.append(event)

        return created

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

    async def cancel_task(self, task_id: str) -> Task | None:
        """Cancel a task if it's in a cancellable state.

        Returns the updated task, or None if the task doesn't exist
        or is already in a terminal state.
        """
        task = await self._tasks.get(task_id)
        if task is None:
            return None

        # Only open/claimed/working tasks can be cancelled
        if task.status not in (TaskStatus.OPEN, TaskStatus.CLAIMED, TaskStatus.WORKING):
            return None

        updated = await self._tasks.update(task_id, status=TaskStatus.CANCELLED)
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
