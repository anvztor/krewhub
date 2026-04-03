from __future__ import annotations

import logging

from krewhub.controllers.base import BaseController
from krewhub.models import AgentStatus, TaskStatus, WatchEventType
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.repositories.task_repo import TaskRepo

logger = logging.getLogger(__name__)


class TaskSchedulerController(BaseController):
    """Assigns open tasks to available agents.

    Agents register at cookbook level. The scheduler finds cookbooks with
    online agents, then schedules tasks per recipe within those cookbooks.

    Level-triggered: safe to restart. Examines current state each cycle.
    """

    async def reconcile(self) -> None:
        agent_repo = AgentRepo(self._db)
        task_repo = TaskRepo(self._db)
        recipe_repo = RecipeRepo(self._db)

        # Get all cookbooks with online agents
        cursor = await self._db.execute(
            """SELECT DISTINCT cookbook_id FROM agent_presence
               WHERE status IN ('online', 'busy')"""
        )
        cookbook_rows = await cursor.fetchall()

        for cookbook_row in cookbook_rows:
            cookbook_id = cookbook_row["cookbook_id"]
            # Get all recipes in this cookbook
            recipes = await recipe_repo.list_by_cookbook(cookbook_id)
            # Get all agents available for this cookbook
            agents = await agent_repo.list_by_cookbook(cookbook_id)
            online_agents = [
                a for a in agents if a.status in (AgentStatus.ONLINE, AgentStatus.BUSY)
            ]
            if not online_agents:
                continue

            for recipe in recipes:
                await self._schedule_for_recipe(
                    recipe.id, online_agents, task_repo,
                )

    async def _schedule_for_recipe(
        self,
        recipe_id: str,
        online_agents: list,
        task_repo: TaskRepo,
    ) -> None:
        # Get open, unassigned tasks
        open_tasks = await task_repo.list_open_by_recipe(recipe_id)
        unassigned = [t for t in open_tasks if t.assigned_agent_id is None]
        if not unassigned:
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

            # Find first agent with capacity
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
            return agent.agent_id
        return None
