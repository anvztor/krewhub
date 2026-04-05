from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import aiosqlite

from krewhub.controllers.base import BaseController
from krewhub.models import WatchEventType
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.watch.service import WatchService

logger = logging.getLogger(__name__)


class PresenceController(BaseController):
    """Reconciles agent presence by marking stale agents offline
    and releasing their assigned tasks.

    Agents register at cookbook level. When going offline, we release
    tasks across all recipes in that cookbook.

    Level-triggered: safe to restart at any time.
    """

    def __init__(
        self,
        db: aiosqlite.Connection,
        watch: WatchService,
        *,
        interval: float = 5.0,
        heartbeat_timeout: float = 30.0,
    ) -> None:
        super().__init__(db, watch, interval=interval)
        self._heartbeat_timeout = heartbeat_timeout

    async def reconcile(self) -> None:
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=self._heartbeat_timeout)

        agent_repo = AgentRepo(self._db)
        task_repo = TaskRepo(self._db)
        recipe_repo = RecipeRepo(self._db)

        # Find agents that should be marked offline
        cursor = await self._db.execute(
            """SELECT agent_id, cookbook_id FROM agent_presence
               WHERE last_heartbeat_at < ? AND status != 'offline'""",
            (cutoff.isoformat(),),
        )
        stale_agents = await cursor.fetchall()

        if not stale_agents:
            return

        # Mark all stale agents offline in bulk
        marked = await agent_repo.mark_offline_stale(cutoff)

        # For each stale agent, release their active tasks across all recipes in cookbook
        for row in stale_agents:
            agent_id = row["agent_id"]
            cookbook_id = row["cookbook_id"]

            # Emit watch event for the agent going offline
            updated_agent = await agent_repo.get(agent_id, cookbook_id)
            if updated_agent is not None:
                await self._watch.record_resource(
                    "agent", agent_id, WatchEventType.MODIFIED, updated_agent,
                )

            # Release tasks across all recipes in this cookbook
            recipes = await recipe_repo.list_by_cookbook(cookbook_id)
            for recipe in recipes:
                # Release claimed/working tasks
                active_tasks = await task_repo.list_active_by_agent(recipe.id, agent_id)
                for task in active_tasks:
                    reopened = await task_repo.reopen_for_rerun(task.id)
                    if reopened is not None:
                        await self._watch.record_resource(
                            "task", task.id, WatchEventType.MODIFIED, reopened,
                            recipe_id=recipe.id,
                        )
                        logger.info(
                            "PresenceController: released task %s from offline agent %s",
                            task.id, agent_id,
                        )

                # Release assigned-but-never-claimed tasks (the limbo state)
                orphaned = await task_repo.list_assigned_unclaimed_by_agent(
                    recipe.id, agent_id,
                )
                for task in orphaned:
                    reopened = await task_repo.reopen_for_rerun(task.id)
                    if reopened is not None:
                        await self._watch.record_resource(
                            "task", task.id, WatchEventType.MODIFIED, reopened,
                            recipe_id=recipe.id,
                        )
                        logger.info(
                            "PresenceController: released orphaned assignment %s "
                            "from offline agent %s",
                            task.id, agent_id,
                        )

        if marked > 0:
            logger.info(
                "PresenceController: marked %d agent(s) offline", marked
            )
