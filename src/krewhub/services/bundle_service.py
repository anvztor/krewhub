from __future__ import annotations

import logging
import re
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
    WatchEventType,
)
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.event_repo import EventRepo
from krewhub.repositories.task_repo import TaskRepo
from krewhub.services.graph_runtime import OrchestratorDeps, OrchestratorState, dispatch_cycle
from krewhub.services.graph_sandbox import (
    GraphExecError,
    GraphValidationError,
    execute_graph_code,
    extract_graph_structure,
    render_graph,
)
from krewhub.watch.service import WatchService

logger = logging.getLogger(__name__)


class GraphArtifactError(Exception):
    """Raised when an inbound graph artifact can't be attached.

    Carries an HTTP status hint so route handlers can map it directly:
        404 — bundle not found
        409 — bundle already has graph_code
        422 — sandbox rejected the code
    """

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


def _humanize_node(node_id: str) -> str:
    """Convert `scope_review` / `FeatureScope` → `Scope Review` / `Feature Scope`."""
    spaced = re.sub(r"(?<!^)(?=[A-Z])", " ", node_id)
    return spaced.replace("_", " ").title()


class BundleService:
    def __init__(self, db: aiosqlite.Connection, watch: WatchService) -> None:
        self._bundles = BundleRepo(db)
        self._tasks = TaskRepo(db)
        self._events = EventRepo(db)
        self._watch = watch

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

        # Wording reflects the two shapes of bundle creation:
        #   * Empty bundle → PlannerDispatchController will dispatch a
        #     planner agent on its next reconcile; the planner will POST
        #     graph code to /bundles/{id}/graph, which produces its own
        #     PLAN event once the graph is attached.
        #   * Non-empty bundle → the caller (manual seeds, demo store)
        #     baked in tasks up-front; no planner will run.
        plan_body = (
            f"Created bundle with {len(created_tasks)} tasks."
            if created_tasks
            else "Created empty bundle; awaiting planner dispatch."
        )
        plan_event = Event(
            id=f"evt_{uuid.uuid4().hex[:8]}",
            recipe_id=recipe_id,
            bundle_id=bundle_id,
            type=EventType.PLAN,
            actor_id="system",
            actor_type=ActorType.SYSTEM,
            body=plan_body,
            created_at=now,
        )
        await self._events.create(plan_event)

        await self._watch.record_resource(
            "bundle", bundle_id, WatchEventType.ADDED, bundle,
            recipe_id=recipe_id,
        )
        for task in created_tasks:
            await self._watch.record_resource(
                "task", task.id, WatchEventType.ADDED, task,
                recipe_id=recipe_id,
            )

        return bundle, created_tasks

    async def attach_graph_artifact(
        self,
        bundle_id: str,
        code: str,
        *,
        created_by: str = "orchestrator",
    ) -> tuple[Bundle, list[Task]]:
        """Validate, render, and attach LLM-generated graph code to a bundle.

        Pipeline:
            1. Reject if bundle missing or already has graph_code (no rebind).
            2. Sandbox-validate + exec the code.
            3. Extract (node_ids, edges) and render mermaid.
            4. Create one Task per graph node, with graph_node_id set and
               depends_on_task_ids derived from incoming edges.
            5. Persist graph_code + graph_mermaid on the bundle.
            6. Record a PLAN event referencing the graph.
            7. Emit watch ADDED for tasks + MODIFIED for bundle so cookrew
               picks up the topology immediately via SSE.

        On any sandbox failure (validation or exec), the bundle is marked
        BLOCKED with the error and a GraphArtifactError(422, ...) is raised.

        Args:
            bundle_id: existing bundle id (must have status=OPEN, no graph_code).
            code: pydantic-graph source from the orchestrator A2A response.
            created_by: actor id stamped on the PLAN event.

        Raises:
            GraphArtifactError: with status_code 404/409/422 per failure mode.
        """
        bundle = await self._bundles.get(bundle_id)
        if bundle is None:
            raise GraphArtifactError(404, f"bundle {bundle_id} not found")
        if bundle.graph_code is not None:
            raise GraphArtifactError(
                409, f"bundle {bundle_id} already has graph_code attached",
            )

        # 1. Sandbox: validate + exec
        try:
            graph = execute_graph_code(
                code,
                orchestrator_state_cls=OrchestratorState,
                orchestrator_deps_cls=OrchestratorDeps,
                dispatch_cycle=dispatch_cycle,
            )
        except (GraphValidationError, GraphExecError) as exc:
            await self._bundles.update_status(
                bundle_id, BundleStatus.BLOCKED,
                blocked_reason=f"graph artifact rejected: {exc}"[:500],
            )
            logger.warning(
                "attach_graph_artifact: bundle %s sandbox rejected: %s", bundle_id, exc,
            )
            raise GraphArtifactError(422, f"graph code rejected: {exc}") from exc

        # 2. Structure + mermaid
        node_ids, edges = extract_graph_structure(graph)
        if not node_ids:
            await self._bundles.update_status(
                bundle_id, BundleStatus.BLOCKED,
                blocked_reason="graph artifact has no user-step nodes",
            )
            raise GraphArtifactError(422, "graph contains no executable steps")

        rendered = render_graph(graph, direction="LR")

        # 3. Build incoming-edge map for task dependencies.
        incoming: dict[str, list[str]] = {nid: [] for nid in node_ids}
        for src, dst in edges:
            incoming[dst].append(src)

        # 4. Create tasks (no deps yet — we need ids first).
        now = datetime.now(timezone.utc)
        node_to_task_id: dict[str, str] = {}
        created_tasks: list[Task] = []
        for nid in node_ids:
            task = Task(
                id=f"task_{uuid.uuid4().hex[:8]}",
                bundle_id=bundle_id,
                title=_humanize_node(nid),
                description=f"Graph step {nid!r}",
                status=TaskStatus.OPEN,
                graph_node_id=nid,
            )
            persisted = await self._tasks.create(task)
            node_to_task_id[nid] = persisted.id
            created_tasks.append(persisted)

        # 5. Set depends_on_task_ids now that all task ids exist.
        for nid in node_ids:
            dep_ids = [node_to_task_id[src] for src in incoming[nid]]
            if dep_ids:
                updated_task = await self._tasks.update(
                    node_to_task_id[nid],
                    depends_on_task_ids=dep_ids,
                )
                if updated_task is not None:
                    # Replace in created_tasks so the returned list has deps.
                    for i, t in enumerate(created_tasks):
                        if t.id == updated_task.id:
                            created_tasks[i] = updated_task
                            break

        # 6. Attach graph_code + mermaid on the bundle row.
        updated_bundle = await self._bundles.attach_graph(
            bundle_id, graph_code=code, graph_mermaid=rendered.mermaid,
        )
        if updated_bundle is None:
            raise GraphArtifactError(404, f"bundle {bundle_id} disappeared mid-attach")

        # 7. Record a PLAN event so cookrew sees the planning step land.
        plan_event = Event(
            id=f"evt_{uuid.uuid4().hex[:8]}",
            recipe_id=bundle.recipe_id,
            bundle_id=bundle_id,
            type=EventType.PLAN,
            actor_id=created_by,
            actor_type=ActorType.AGENT,
            body=f"Graph attached: {len(node_ids)} steps, {len(edges)} edges",
            payload={
                "node_ids": node_ids,
                "edges": [list(e) for e in edges],
                "mermaid": rendered.mermaid,
            },
            created_at=now,
        )
        await self._events.create(plan_event)

        # 8. Watch events for cookrew SSE.
        await self._watch.record_resource(
            "bundle", bundle_id, WatchEventType.MODIFIED, updated_bundle,
            recipe_id=bundle.recipe_id,
        )
        for task in created_tasks:
            await self._watch.record_resource(
                "task", task.id, WatchEventType.ADDED, task,
                recipe_id=bundle.recipe_id,
            )

        logger.info(
            "attach_graph_artifact: bundle %s — %d nodes, %d edges, %d tasks created",
            bundle_id, len(node_ids), len(edges), len(created_tasks),
        )
        return updated_bundle, created_tasks

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

        if updated is not None:
            await self._watch.record_resource(
                "bundle", bundle_id, WatchEventType.MODIFIED, updated,
                recipe_id=bundle.recipe_id,
            )

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
            updated = await self._bundles.update_status(
                bundle_id, BundleStatus.COOKED, cooked_at=now
            )
        elif any_blocked:
            blocked_reasons = [t.blocked_reason for t in tasks if t.blocked_reason]
            updated = await self._bundles.update_status(
                bundle_id, BundleStatus.BLOCKED,
                blocked_reason="; ".join(blocked_reasons) if blocked_reasons else "Task blocked",
            )
        elif any_claimed:
            updated = await self._bundles.update_status(
                bundle_id, BundleStatus.CLAIMED, claimed_at=now
            )
        else:
            updated = await self._bundles.update_status(bundle_id, BundleStatus.OPEN)

        if updated is not None:
            await self._watch.record_resource(
                "bundle", bundle_id, WatchEventType.MODIFIED, updated,
                recipe_id=updated.recipe_id,
            )

        return updated

    async def rerun_blocked_tasks(self, bundle_id: str) -> Bundle | None:
        bundle = await self._bundles.get(bundle_id)
        if bundle is None or bundle.status in (BundleStatus.CANCELLED, BundleStatus.DIGESTED):
            return None

        tasks = await self._tasks.list_by_bundle(bundle_id)
        blocked_tasks = [task for task in tasks if task.status == TaskStatus.BLOCKED]
        if not blocked_tasks:
            return None

        for task in blocked_tasks:
            reopened = await self._tasks.reopen_for_rerun(task.id)
            if reopened is not None:
                await self._watch.record_resource(
                    "task", task.id, WatchEventType.MODIFIED, reopened,
                    recipe_id=bundle.recipe_id,
                )

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

        await self._watch.record_resource(
            "bundle", bundle_id, WatchEventType.MODIFIED, updated_bundle,
            recipe_id=bundle.recipe_id,
        )

        return updated_bundle
