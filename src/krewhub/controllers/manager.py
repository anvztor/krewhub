from __future__ import annotations

import logging

import aiosqlite

from krewhub.controllers.base import BaseController
from krewhub.controllers.graph_runner import GraphRunnerController
from krewhub.controllers.link_reconciler import LinkReconcileController
from krewhub.controllers.orch_controller import OrchController
from krewhub.controllers.planner_dispatch import PlannerDispatchController
from krewhub.controllers.presence_controller import PresenceController
from krewhub.controllers.task_dispatch import TaskDispatchController
from krewhub.watch.service import WatchService

logger = logging.getLogger(__name__)


class ControllerManager:
    """Manages the lifecycle of all reconciliation controllers.

    Analogous to the K8s controller-manager: starts all controllers
    as background async tasks and stops them on shutdown.
    """

    def __init__(
        self,
        db: aiosqlite.Connection,
        watch: WatchService,
        *,
        heartbeat_timeout: float = 30.0,
        orch_enabled: bool = True,
        orch_interval: float = 5.0,
        orch_liveness_timeout: float = 60.0,
        orch_max_respawns: int = 3,
    ) -> None:
        # Phase 12 step (d.1): BundleController removed. Bundle FSM is
        # now OPEN ↔ CLOSED and not derived from tasks, so the
        # reconciler has nothing to do.
        self._controllers: list[BaseController] = [
            TaskDispatchController(db, watch, interval=2.0),
            PlannerDispatchController(db, watch, interval=2.0),
            GraphRunnerController(db, watch, interval=2.0, max_concurrent=4),
            PresenceController(db, watch, interval=5.0, heartbeat_timeout=heartbeat_timeout),
            # Link firing (pipe/subagent) is mechanical plumbing — it runs
            # ALWAYS, independent of the orch flag, so a human's manual pipe
            # link fires even with KREWHUB_ORCH_ENABLED=0 (S2 B3, hole #3).
            LinkReconcileController(db, watch, interval=orch_interval),
        ]
        # Orch mode (O2): supervises Brief-managed tasks only (decisions:
        # Report accept/escalate, liveness respawn). Legacy tasks (no
        # brief_json) are untouched. KREWHUB_ORCH_ENABLED=0 disables only
        # the decision layer — link firing above stays on.
        if orch_enabled:
            self._controllers.append(
                OrchController(
                    db, watch,
                    interval=orch_interval,
                    liveness_timeout=orch_liveness_timeout,
                    max_respawns=orch_max_respawns,
                ),
            )

    async def start_all(self) -> None:
        for controller in self._controllers:
            await controller.start()
        names = [c.name for c in self._controllers]
        logger.info("ControllerManager started %d controllers: %s", len(names), names)

    async def stop_all(self) -> None:
        for controller in reversed(self._controllers):
            await controller.stop()
        logger.info("ControllerManager stopped all controllers")

    def health(self) -> dict[str, bool]:
        return {c.name: c.is_running for c in self._controllers}
