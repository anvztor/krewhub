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
from krewhub.repositories.recipe_repo import RecipeRepo
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
        recipe_id: str | None,
        prompt: str,
        created_by: str,
        tasks: list[dict],
        *,
        autoplan: bool = False,
        cookbook_id: str | None = None,
        repo_spec: dict | None = None,
    ) -> tuple[Bundle, list[Task]]:
        """Create a bundle.

        Phase 12 dual-write: cookbook_id is now stamped alongside
        recipe_id. Either parameter can be None:
          - recipe_id None → cookbook-scoped bundle (new path)
          - cookbook_id None → resolved from recipe.cookbook_id
        At least one must be set; otherwise the bundle has no parent.
        """
        if recipe_id is None and cookbook_id is None:
            raise ValueError(
                "create_bundle requires at least one of recipe_id or cookbook_id",
            )

        now = datetime.now(timezone.utc)
        bundle_id = f"bun_{uuid.uuid4().hex[:8]}"

        # Resolve cookbook_id from the recipe when the caller only
        # supplied recipe_id. Keeps every legacy POST /recipes/{id}/bundles
        # writing the new column without changing call signatures.
        resolved_cookbook_id = cookbook_id
        if resolved_cookbook_id is None and recipe_id is not None:
            recipe = await RecipeRepo(self._bundles._db).get(recipe_id)  # type: ignore[attr-defined]
            if recipe is not None:
                resolved_cookbook_id = recipe.cookbook_id

        bundle = Bundle(
            id=bundle_id,
            recipe_id=recipe_id,
            cookbook_id=resolved_cookbook_id,
            repo_spec=repo_spec,
            prompt=prompt,
            status=BundleStatus.OPEN,
            created_by=created_by,
            created_at=now,
        )
        bundle = await self._bundles.create(bundle)
        # PlannerDispatchController only picks up bundles where this is 1.
        # Empty "+ NEW" bundles from cookrew-beta stay blank by default.
        if autoplan:
            await self._bundles._db.execute(  # type: ignore[attr-defined]
                "UPDATE bundles SET autoplan_enabled = 1 WHERE id = ?",
                (bundle_id,),
            )

        created_tasks: list[Task] = []
        for _i, t in enumerate(tasks):
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
            cookbook_id=resolved_cookbook_id,
            bundle_id=bundle_id,
            type=EventType.PROMPT,
            actor_id=created_by,
            actor_type=ActorType.HUMAN,
            body=prompt,
            created_at=now,
        )
        await self._events.create(prompt_event)

        # Wording reflects the actual UX path. A plain "+ NEW" bundle is
        # intentionally blank and inert; only explicit autoplan callers are
        # waiting for PlannerDispatchController to attach a graph.
        if created_tasks:
            plan_body = f"Created bundle with {len(created_tasks)} tasks."
        elif autoplan:
            plan_body = "Created empty bundle; awaiting planner dispatch."
        else:
            plan_body = "Created empty bundle; add tasks when ready."
        plan_event = Event(
            id=f"evt_{uuid.uuid4().hex[:8]}",
            recipe_id=recipe_id,
            cookbook_id=resolved_cookbook_id,
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
            # DEPRECATED — writing BundleStatus.BLOCKED on graph
            # validation failure. Under OPEN/CLOSED this should raise
            # without touching bundle status; the failure belongs in
            # an event/log, not the bundle FSM.
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
            # DEPRECATED — same as above; do not move bundle to BLOCKED
            # when migrated to the two-state model.
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

    # DEPRECATED — relies on the CANCELLED terminal state and cascades
    # task cancellation. Under the new OPEN/CLOSED bundle model this
    # should collapse into a simple "close bundle" action with no
    # cascade; the bundle is not the authority over its tasks.
    # Removal target after callers (routes/bundles.py PATCH endpoint,
    # tests/test_controllers.py) migrate.
    async def cancel_bundle(self, bundle_id: str, actor_id: str) -> Bundle | None:
        bundle = await self._bundles.get(bundle_id)
        if bundle is None:
            return None
        if bundle.status not in (BundleStatus.OPEN, BundleStatus.BLOCKED, BundleStatus.CLAIMED):
            return None

        updated = await self._bundles.update_status(bundle_id, BundleStatus.CANCELLED)

        # Cascade cancel to tasks and emit watch events so the agent
        # can pick up the cancellation via SSE.
        tasks = await self._tasks.list_by_bundle(bundle_id)
        for task in tasks:
            if task.status not in (TaskStatus.DONE, TaskStatus.CANCELLED):
                cancelled_task = await self._tasks.update(task.id, status=TaskStatus.CANCELLED)
                if cancelled_task is not None:
                    await self._watch.record_resource(
                        "task", task.id, WatchEventType.MODIFIED, cancelled_task,
                        recipe_id=bundle.recipe_id,
                    )

        if updated is not None:
            await self._watch.record_resource(
                "bundle", bundle_id, WatchEventType.MODIFIED, updated,
                recipe_id=bundle.recipe_id,
            )

        return updated

    # DEPRECATED — folds task aggregate (DONE/BLOCKED/CLAIMED/WORKING)
    # into bundle status CLAIMED/COOKED/BLOCKED/OPEN. Under the new
    # OPEN/CLOSED model the bundle has no derived state; it is just a
    # container. Remove this function and every call site once
    # controllers/bundle_controller.py and graph_runner.py stop
    # consulting middle states.
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

    # DEPRECATED — predicated on bundle-level BLOCKED. The new model
    # has no BLOCKED bundle status, so rerun decisions belong on
    # individual tasks (TaskRepo.reopen_for_rerun). Removal target
    # alongside routes/bundles.py::rerun_blocked_bundle.
    async def rerun_blocked_tasks(self, bundle_id: str) -> Bundle | None:
        """Reopen a bundle for another graph-runner pass.

        Two shapes are supported:
          * Per-task rerun — at least one task is in ``BLOCKED``. We
            reopen every blocked task and clear the bundle's blocked
            state so GraphRunnerController picks it up again.
          * Whole-bundle rerun — the bundle itself is ``BLOCKED`` but
            every task is still ``OPEN``. This happens when the graph
            runner failed the bundle before a single dispatch touched
            any task row (e.g. ``dispatch_cycle`` couldn't find an
            eligible gateway for any step on the first pass). Without
            this branch the caller has no way to unstick the bundle
            short of deleting and recreating it: the old code returned
            ``None`` because no task had ``status == BLOCKED`` yet.

        Returns the reopened bundle, or ``None`` if the bundle is in a
        non-recoverable terminal state (cancelled/digested), missing,
        or already running cleanly.
        """
        bundle = await self._bundles.get(bundle_id)
        if bundle is None or bundle.status in (BundleStatus.CANCELLED, BundleStatus.DIGESTED):
            return None

        tasks = await self._tasks.list_by_bundle(bundle_id)
        blocked_tasks = [task for task in tasks if task.status == TaskStatus.BLOCKED]

        # Whole-bundle recovery: if the bundle as a whole is BLOCKED
        # but no individual task is, the runner failed before touching
        # anything (typically "no eligible gateway"). Reopen the
        # bundle so GraphRunnerController's next reconcile re-runs the
        # graph from the top. Tasks are already OPEN — no reset needed.
        if not blocked_tasks and bundle.status != BundleStatus.BLOCKED:
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
        if blocked_tasks:
            body = (
                f"Re-run requested for {len(blocked_tasks)} blocked task"
                f"{'' if len(blocked_tasks) == 1 else 's'}. "
                "Tasks reopened for reassignment."
            )
        else:
            body = (
                "Re-run requested for blocked bundle. "
                "Bundle reopened; graph runner will retry from the top."
            )
        rerun_event = Event(
            id=f"evt_{uuid.uuid4().hex[:8]}",
            recipe_id=bundle.recipe_id,
            bundle_id=bundle_id,
            type=EventType.PLAN,
            actor_id="system",
            actor_type=ActorType.SYSTEM,
            body=body,
            created_at=now,
        )
        await self._events.create(rerun_event)

        await self._watch.record_resource(
            "bundle", bundle_id, WatchEventType.MODIFIED, updated_bundle,
            recipe_id=bundle.recipe_id,
        )

        return updated_bundle
