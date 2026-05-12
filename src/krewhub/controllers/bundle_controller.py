from __future__ import annotations

import logging
from datetime import datetime, timezone

from krewhub.controllers.base import BaseController
from krewhub.models import BundleStatus, TaskStatus, WatchEventType
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.task_repo import TaskRepo

logger = logging.getLogger(__name__)


class BundleController(BaseController):
    """DEPRECATED — reconciles bundle.status from aggregate task states.

    Bundles are migrating to a two-state OPEN/CLOSED model with no
    derived middle states. Once that lands, the bundle phase no
    longer needs reconciliation — this whole controller should be
    removed. Do not extend.

    Legacy behavior (kept until migration completes):
    K8s-style controller that replaces the inline
    recompute_bundle_status() calls scattered across route handlers.
    Every interval, it scans all non-terminal bundles and recomputes
    their phase from their tasks' current states.
    """

    async def reconcile(self) -> None:
        bundles_repo = BundleRepo(self._db)
        tasks_repo = TaskRepo(self._db)

        # Fetch all recipes, then all bundles in non-terminal states
        cursor = await self._db.execute(
            """SELECT * FROM bundles
               WHERE status NOT IN ('cancelled', 'digested', 'rejected')"""
        )
        rows = await cursor.fetchall()

        for row in rows:
            bundle_id = row["id"]
            recipe_id = row["recipe_id"]
            current_status = row["status"]

            tasks = await tasks_repo.list_by_bundle(bundle_id)
            if not tasks:
                continue

            desired_status = _compute_bundle_phase(tasks)
            if desired_status == current_status:
                continue

            now = datetime.now(timezone.utc)
            kwargs: dict = {}
            if desired_status == BundleStatus.COOKED:
                kwargs["cooked_at"] = now
            elif desired_status == BundleStatus.CLAIMED:
                kwargs["claimed_at"] = now
            elif desired_status == BundleStatus.BLOCKED:
                blocked_reasons = [t.blocked_reason for t in tasks if t.blocked_reason]
                kwargs["blocked_reason"] = (
                    "; ".join(blocked_reasons) if blocked_reasons else "Task blocked"
                )

            updated = await bundles_repo.update_status(
                bundle_id, desired_status, **kwargs
            )

            if updated is not None:
                await self._watch.record_resource(
                    "bundle", bundle_id, WatchEventType.MODIFIED, updated,
                    recipe_id=recipe_id,
                )
                logger.debug(
                    "BundleController: %s status %s -> %s",
                    bundle_id, current_status, desired_status,
                )


# DEPRECATED — derives bundle phase from task aggregate. Under the
# new OPEN/CLOSED bundle model the bundle has no derived state; this
# function (and the controller that calls it) should be removed once
# the routes/UI/tests stop consulting middle states.
def _compute_bundle_phase(tasks: list) -> BundleStatus:
    """Pure function: compute the correct bundle phase from task states."""
    all_done = all(t.status == TaskStatus.DONE for t in tasks)
    any_blocked = any(t.status == TaskStatus.BLOCKED for t in tasks)
    any_active = any(
        t.status in (TaskStatus.CLAIMED, TaskStatus.WORKING) for t in tasks
    )

    if all_done:
        return BundleStatus.COOKED
    if any_blocked:
        return BundleStatus.BLOCKED
    if any_active:
        return BundleStatus.CLAIMED
    return BundleStatus.OPEN
