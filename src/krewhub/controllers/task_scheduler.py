from __future__ import annotations

import logging

from krewhub.controllers.base import BaseController
from krewhub.models import AgentStatus, TaskStatus, WatchEventType
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.task_repo import TaskRepo

logger = logging.getLogger(__name__)


class TaskSchedulerController(BaseController):
    """Assigns open tasks to available agents.

    Agents register at cookbook level. Step (e): scheduler iterates
    cookbooks directly (recipes are gone) and assigns tasks across
    bundles in each cookbook.
    """

    async def reconcile(self) -> None:
        agent_repo = AgentRepo(self._db)
        task_repo = TaskRepo(self._db)

        cursor = await self._db.execute(
            """SELECT DISTINCT cookbook_id FROM agent_presence
               WHERE status IN ('online', 'busy')"""
        )
        cookbook_rows = await cursor.fetchall()

        for cookbook_row in cookbook_rows:
            cookbook_id = cookbook_row["cookbook_id"]
            agents = await agent_repo.list_by_cookbook(cookbook_id)
            online_agents = [
                a for a in agents if a.status in (AgentStatus.ONLINE, AgentStatus.BUSY)
            ]
            if not online_agents:
                continue
            await self._schedule_for_cookbook(
                cookbook_id, online_agents, task_repo,
            )

    async def _schedule_for_cookbook(
        self,
        cookbook_id: str,
        online_agents: list,
        task_repo: TaskRepo,
    ) -> None:
        open_tasks = await task_repo.list_open_by_cookbook(cookbook_id)
        unassigned = [t for t in open_tasks if t.assigned_agent_id is None]
        if not unassigned:
            return

        capacity: dict[str, int] = {}
        for agent in online_agents:
            active = await task_repo.list_active_by_agent(
                None, agent.agent_id, cookbook_id=cookbook_id,
            )
            pending = await task_repo.list_assigned_unclaimed_by_agent(
                cookbook_id, agent.agent_id,
            )
            remaining = agent.max_concurrent_tasks - len(active) - len(pending)
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
