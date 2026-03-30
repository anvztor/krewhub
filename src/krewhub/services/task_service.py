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
)
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.services.sse_service import sse_service


class TaskService:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._tasks = TaskRepo(db)
        self._events = EventRepo(db)
        self._agents = AgentRepo(db)

    async def claim_task(
        self, task_id: str, agent_id: str, recipe_id: str
    ) -> Task | None:
        task = await self._tasks.get(task_id)
        if task is None or task.status != TaskStatus.OPEN:
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

        await sse_service.publish(recipe_id, "task.claimed", {
            "task_id": task_id,
            "agent_id": agent_id,
            "bundle_id": task.bundle_id,
        })

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
    ) -> Event:
        task = await self._tasks.get(task_id)
        if task is None:
            raise ValueError(f"Task {task_id} not found")

        if task.status == TaskStatus.CLAIMED:
            await self._tasks.update(task_id, status=TaskStatus.WORKING)

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
            created_at=now,
        )
        await self._events.create(event)

        await sse_service.publish(recipe_id, "task.updated", {
            "task_id": task_id,
            "event_type": event_type,
            "bundle_id": task.bundle_id,
        })

        return event

    async def mark_done(self, task_id: str) -> Task | None:
        now = datetime.now(timezone.utc)
        return await self._tasks.update(
            task_id, status=TaskStatus.DONE, completed_at=now
        )

    async def mark_blocked(self, task_id: str, reason: str) -> Task | None:
        return await self._tasks.update(
            task_id, status=TaskStatus.BLOCKED, blocked_reason=reason
        )

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
        return await self._tasks.create(task)

    async def edit_task(
        self,
        task_id: str,
        title: str | None = None,
        description: str | None = None,
        depends_on_task_ids: list[str] | None = None,
    ) -> Task | None:
        return await self._tasks.update(
            task_id,
            title=title,
            description=description,
            depends_on_task_ids=depends_on_task_ids,
        )

    async def remove_task(self, task_id: str) -> bool:
        task = await self._tasks.get(task_id)
        if task is None or task.status != TaskStatus.OPEN:
            return False
        return await self._tasks.delete(task_id)
