from __future__ import annotations

import logging

from krewhub.controllers.base import BaseController
from krewhub.models import AgentStatus, TaskStatus, WatchEventType
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.task_repo import TaskRepo

logger = logging.getLogger(__name__)


class TaskSchedulerController(BaseController):
    """Assigns open tasks to available agents.

    This is the K8s scheduler equivalent. Instead of agents self-claiming
    tasks, the scheduler examines all open/unassigned tasks and available
    agents, then sets task.assigned_agent_id based on capability matching
    and dependency readiness.

    The agent (via krewcli NodeAgent) watches for tasks assigned to it
    and confirms by claiming. The existing claim API is preserved as
    backward-compatible fallback for agents that don't use watch.

    Level-triggered: safe to restart. Examines current state each cycle.
    """

    async def reconcile(self) -> None:
        agent_repo = AgentRepo(self._db)
        task_repo = TaskRepo(self._db)

        # Get all recipes with online agents
        cursor = await self._db.execute(
            """SELECT DISTINCT recipe_id FROM agent_presence
               WHERE status IN ('online', 'busy')"""
        )
        recipe_rows = await cursor.fetchall()

        for recipe_row in recipe_rows:
            recipe_id = recipe_row["recipe_id"]
            await self._schedule_for_recipe(recipe_id, agent_repo, task_repo)

    async def _schedule_for_recipe(
        self,
        recipe_id: str,
        agent_repo: AgentRepo,
        task_repo: TaskRepo,
    ) -> None:
        # Get open, unassigned tasks
        open_tasks = await task_repo.list_open_by_recipe(recipe_id)
        unassigned = [t for t in open_tasks if t.assigned_agent_id is None]
        if not unassigned:
            return

        # Get available agents
        agents = await agent_repo.list_by_recipe(recipe_id)
        online_agents = [
            a for a in agents if a.status in (AgentStatus.ONLINE, AgentStatus.BUSY)
        ]
        if not online_agents:
            return

        # Build agent capacity map: how many more tasks each can take
        capacity: dict[str, int] = {}
        for agent in online_agents:
            active = await task_repo.list_active_by_agent(recipe_id, agent.agent_id)
            remaining = agent.max_concurrent_tasks - len(active)
            if remaining > 0:
                capacity[agent.agent_id] = remaining

        if not capacity:
            return

        # Assign tasks to agents with capacity
        for task in unassigned:
            if not capacity:
                break

            # Check dependencies are satisfied
            if not await self._deps_satisfied(task_repo, task.depends_on_task_ids):
                continue

            # Find first agent with matching capabilities and capacity
            assigned_agent_id = self._find_agent(
                task, online_agents, capacity
            )
            if assigned_agent_id is None:
                continue

            updated = await task_repo.update(
                task.id, assigned_agent_id=assigned_agent_id
            )
            if updated is not None:
                await self._watch.record_resource(
                    "task", task.id, WatchEventType.MODIFIED, updated,
                    recipe_id=recipe_id,
                )
                logger.info(
                    "TaskScheduler: assigned %s to agent %s",
                    task.id, assigned_agent_id,
                )

            # Decrement capacity
            capacity[assigned_agent_id] -= 1
            if capacity[assigned_agent_id] <= 0:
                del capacity[assigned_agent_id]

    @staticmethod
    async def _deps_satisfied(task_repo: TaskRepo, dep_ids: list[str]) -> bool:
        for dep_id in dep_ids:
            dep = await task_repo.get(dep_id)
            if dep is None or dep.status != TaskStatus.DONE:
                return False
        return True

    @staticmethod
    def _find_agent(
        task,
        agents: list,
        capacity: dict[str, int],
    ) -> str | None:
        """Find the best agent for a task. Simple first-fit for MVP."""
        for agent in agents:
            if agent.agent_id not in capacity:
                continue
            # For now, any online agent with capacity can take any task.
            # Future: match task requirements against agent capabilities.
            return agent.agent_id
        return None
