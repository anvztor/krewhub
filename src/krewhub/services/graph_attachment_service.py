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


class GraphAttachmentService:
    def __init__(self, db: aiosqlite.Connection, watch: WatchService) -> None:
        self._bundles = BundleRepo(db)
        self._tasks = TaskRepo(db)
        self._events = EventRepo(db)
        self._watch = watch

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
            # Step (d.1): sandbox failures don't move the bundle FSM.
            # Surface as 422; caller can retry with corrected code.
            logger.warning(
                "attach_graph_artifact: bundle %s sandbox rejected: %s", bundle_id, exc,
            )
            raise GraphArtifactError(422, f"graph code rejected: {exc}") from exc

        # 2. Structure + mermaid
        node_ids, edges = extract_graph_structure(graph)
        if not node_ids:
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
        )
        for task in created_tasks:
            await self._watch.record_resource(
                "task", task.id, WatchEventType.ADDED, task,
            )

        logger.info(
            "attach_graph_artifact: bundle %s — %d nodes, %d edges, %d tasks created",
            bundle_id, len(node_ids), len(edges), len(created_tasks),
        )
        return updated_bundle, created_tasks
