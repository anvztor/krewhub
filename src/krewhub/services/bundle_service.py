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

    # ----------------------------------------------------------------------
    # OPEN ↔ CLOSED (step d)
    # ----------------------------------------------------------------------

    async def close_bundle(
        self, bundle_id: str, actor_id: str, reason: str | None = None,
    ) -> Bundle | None:
        """Close a bundle. Idempotent.

        Bundle is a dumb container — closing flips a flag and records an
        audit event. It does NOT cascade to tasks; running work
        continues. Resource cleanup (sandboxes, working trees) is the
        responsibility of separate reapers that observe the close signal.

        Returns the (possibly already-closed) bundle, or None if the
        bundle doesn't exist.
        """
        bundle = await self._bundles.get(bundle_id)
        if bundle is None:
            return None
        # Idempotent: re-closing a closed bundle is a no-op.
        if bundle.status == BundleStatus.CLOSED:
            return bundle

        updated = await self._bundles.update_status(
            bundle_id, BundleStatus.CLOSED,
        )
        if updated is None:
            return None

        await self._emit_lifecycle_event(
            updated, EventType.BUNDLE_CLOSED, actor_id, reason,
        )
        await self._watch.record_resource(
            "bundle", bundle_id, WatchEventType.MODIFIED, updated,
            recipe_id=updated.recipe_id, cookbook_id=updated.cookbook_id,
        )
        return updated

    async def reopen_bundle(
        self, bundle_id: str, actor_id: str, reason: str | None = None,
    ) -> Bundle | None:
        """Reopen a closed bundle. Idempotent.

        Symmetric to close_bundle. CLOSED is a soft state — reopening
        is cheap and reversible. Used both for "I closed by mistake"
        and "I want to add more work to this bundle."
        """
        bundle = await self._bundles.get(bundle_id)
        if bundle is None:
            return None
        if bundle.status == BundleStatus.OPEN:
            return bundle

        updated = await self._bundles.update_status(
            bundle_id, BundleStatus.OPEN,
        )
        if updated is None:
            return None

        await self._emit_lifecycle_event(
            updated, EventType.BUNDLE_REOPENED, actor_id, reason,
        )
        await self._watch.record_resource(
            "bundle", bundle_id, WatchEventType.MODIFIED, updated,
            recipe_id=updated.recipe_id, cookbook_id=updated.cookbook_id,
        )
        return updated

    async def _emit_lifecycle_event(
        self,
        bundle: Bundle,
        event_type: EventType,
        actor_id: str,
        reason: str | None,
    ) -> None:
        body = (
            f"Bundle {event_type.value}"
            + (f": {reason}" if reason else "")
        )
        event = Event(
            id=f"evt_{uuid.uuid4().hex[:8]}",
            recipe_id=bundle.recipe_id,
            cookbook_id=bundle.cookbook_id,
            bundle_id=bundle.id,
            type=event_type,
            actor_id=actor_id,
            actor_type=ActorType.HUMAN,
            body=body,
            created_at=datetime.now(timezone.utc),
        )
        await self._events.create(event)
