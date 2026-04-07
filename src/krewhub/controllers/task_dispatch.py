"""Task dispatch controller — pushes tasks to A2A gateways.

Replaces TaskSchedulerController. Instead of setting assigned_agent_id
and waiting for agents to poll/watch, this controller dispatches tasks
directly to gateway endpoint_urls via A2A JSON-RPC.

Level-triggered: safe to restart. Examines current state each cycle.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

import aiosqlite
import httpx

from krewhub.controllers.base import BaseController
from krewhub.models import AgentStatus, TaskStatus, WatchEventType
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.repositories.task_repo import TaskRepo

logger = logging.getLogger(__name__)

# Timeout for A2A dispatch calls to gateways
_DISPATCH_TIMEOUT = 10.0


class TaskDispatchController(BaseController):
    """Dispatches open tasks to A2A gateways.

    Flow per cycle:
    1. Find cookbooks with online gateways (agent_presence with endpoint_url)
    2. For each recipe in those cookbooks, find open unassigned tasks
    3. Check dependencies are satisfied
    4. POST task to gateway via A2A message/send
    5. On accept → mark task claimed immediately (no assign/claim gap)
    6. On reject/error → skip, try next cycle
    """

    def __init__(
        self,
        db: aiosqlite.Connection,
        watch,
        *,
        interval: float = 2.0,
    ) -> None:
        super().__init__(db, watch, interval=interval)
        self._http = httpx.AsyncClient(timeout=_DISPATCH_TIMEOUT, follow_redirects=True)

    async def stop(self) -> None:
        await super().stop()
        await self._http.aclose()

    async def reconcile(self) -> None:
        agent_repo = AgentRepo(self._db)
        task_repo = TaskRepo(self._db)
        recipe_repo = RecipeRepo(self._db)

        cursor = await self._db.execute(
            """SELECT DISTINCT cookbook_id FROM agent_presence
               WHERE status IN ('online', 'busy')
               AND endpoint_url IS NOT NULL"""
        )
        cookbook_rows = await cursor.fetchall()

        if not cookbook_rows:
            # Log all agents to help diagnose missing endpoint_url
            all_cursor = await self._db.execute(
                "SELECT agent_id, cookbook_id, status, endpoint_url FROM agent_presence WHERE status != 'offline'"
            )
            all_agents = await all_cursor.fetchall()
            if all_agents:
                for a in all_agents:
                    logger.info(
                        "TaskDispatch: agent %s (cookbook=%s) status=%s endpoint_url=%s — %s",
                        a["agent_id"], a["cookbook_id"], a["status"], a["endpoint_url"],
                        "SKIPPED (no endpoint_url)" if not a["endpoint_url"] else "eligible",
                    )
            return

        for cookbook_row in cookbook_rows:
            cookbook_id = cookbook_row["cookbook_id"]
            recipes = await recipe_repo.list_by_cookbook(cookbook_id)
            agents = await agent_repo.list_by_cookbook(cookbook_id)
            gateways = [
                a for a in agents
                if a.status in (AgentStatus.ONLINE, AgentStatus.BUSY)
                and a.endpoint_url
            ]
            if not gateways:
                logger.info(
                    "TaskDispatch: cookbook %s has agents but no gateways with endpoint_url",
                    cookbook_id,
                )
                continue

            logger.info(
                "TaskDispatch: cookbook %s — %d recipes, %d gateways",
                cookbook_id, len(recipes), len(gateways),
            )
            for recipe in recipes:
                await self._dispatch_for_recipe(
                    recipe, gateways, task_repo,
                )

    async def _dispatch_for_recipe(
        self,
        recipe,
        gateways: list,
        task_repo: TaskRepo,
    ) -> None:
        recipe_id = recipe.id
        open_tasks = await task_repo.list_open_by_recipe(recipe_id)
        unassigned = [t for t in open_tasks if t.assigned_agent_id is None]
        if not unassigned:
            logger.info(
                "TaskDispatch: recipe %s — %d open tasks, %d unassigned (nothing to dispatch)",
                recipe_id, len(open_tasks), len(unassigned),
            )
            return

        # Build gateway capacity map
        capacity: dict[str, int] = {}
        for agent in gateways:
            active = await task_repo.list_active_by_agent(recipe_id, agent.agent_id)
            pending = await task_repo.list_assigned_unclaimed_by_agent(
                recipe_id, agent.agent_id,
            )
            remaining = agent.max_concurrent_tasks - len(active) - len(pending)
            if remaining > 0:
                capacity[agent.agent_id] = remaining
            else:
                logger.info(
                    "TaskDispatch: agent %s at capacity (max=%d, active=%d, pending=%d)",
                    agent.agent_id, agent.max_concurrent_tasks, len(active), len(pending),
                )

        if not capacity:
            logger.info("TaskDispatch: recipe %s — no agents with capacity", recipe_id)
            return

        logger.info(
            "TaskDispatch: recipe %s — %d unassigned tasks, capacity=%s",
            recipe_id, len(unassigned), capacity,
        )

        for task in unassigned:
            if not capacity:
                break

            if not await self._deps_satisfied(task_repo, task.depends_on_task_ids):
                logger.info(
                    "TaskDispatch: task %s blocked by unsatisfied deps %s",
                    task.id, task.depends_on_task_ids,
                )
                continue

            logger.info(
                "TaskDispatch: attempting dispatch of task %s (%s)",
                task.id, task.title,
            )
            dispatched = await self._try_dispatch(task, recipe, gateways, capacity)
            if not dispatched:
                continue

            agent_id = dispatched
            now = datetime.now(timezone.utc)
            updated = await task_repo.update(
                task.id,
                status=TaskStatus.CLAIMED,
                assigned_agent_id=agent_id,
                claimed_by_agent_id=agent_id,
                claimed_at=now,
            )
            if updated is not None:
                await self._watch.record_resource(
                    "task", task.id, WatchEventType.MODIFIED, updated,
                    recipe_id=recipe_id,
                )
                logger.info(
                    "TaskDispatch: dispatched %s to gateway %s",
                    task.id, agent_id,
                )

            capacity[agent_id] -= 1
            if capacity[agent_id] <= 0:
                del capacity[agent_id]

    async def _try_dispatch(
        self,
        task,
        recipe,
        gateways: list,
        capacity: dict[str, int],
    ) -> str | None:
        """Try dispatching a task to a gateway. Returns agent_id on success."""
        for agent in gateways:
            if agent.agent_id not in capacity:
                continue

            accepted = await self._send_to_gateway(agent, task, recipe)
            if accepted:
                return agent.agent_id

        return None

    async def _send_to_gateway(self, agent, task, recipe=None) -> bool:
        """Send task to a gateway's A2A endpoint via JSON-RPC message/send."""
        url = agent.endpoint_url
        if not url:
            return False

        metadata = {
            "task_id": task.id,
            "bundle_id": task.bundle_id,
        }
        if recipe is not None:
            metadata["recipe_id"] = getattr(recipe, "id", "")
            metadata["recipe_name"] = getattr(recipe, "name", "")
            metadata["repo_url"] = getattr(recipe, "repo_url", "")
            metadata["branch"] = getattr(recipe, "default_branch", "main")

        payload = {
            "jsonrpc": "2.0",
            "id": task.id,
            "method": "message/send",
            "params": {
                "message": {
                    "messageId": uuid.uuid4().hex,
                    "role": "user",
                    "parts": [
                        {
                            "kind": "text",
                            "text": _build_task_prompt(task),
                        },
                    ],
                    "metadata": metadata,
                },
            },
        }

        try:
            logger.info(
                "TaskDispatch: POST %s task=%s", url, task.id,
            )
            resp = await self._http.post(url, json=payload)
            if resp.status_code == 200:
                body = resp.json()
                # A2A returns a task object with state; anything non-error = accepted
                result = body.get("result", {})
                state = result.get("status", {}).get("state", "")
                if state in ("submitted", "working", "completed"):
                    return True
                # Also accept if there's a task id in result (task was created)
                if result.get("id"):
                    return True
            logger.info(
                "TaskDispatch: gateway %s rejected task %s (status=%d, body=%s)",
                agent.agent_id, task.id, resp.status_code, resp.text[:500],
            )
            return False
        except (httpx.RequestError, httpx.TimeoutException) as exc:
            logger.info(
                "TaskDispatch: gateway %s unreachable for task %s: %s",
                agent.agent_id, task.id, exc,
            )
            return False

    @staticmethod
    async def _deps_satisfied(task_repo: TaskRepo, dep_ids: list[str]) -> bool:
        for dep_id in dep_ids:
            dep = await task_repo.get(dep_id)
            if dep is None or dep.status != TaskStatus.DONE:
                return False
        return True


def _build_task_prompt(task) -> str:
    """Build a prompt string from a task for the A2A message."""
    parts = [task.title]
    if task.description:
        parts.append(task.description)
    return "\n\n".join(parts)
