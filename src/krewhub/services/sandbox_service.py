"""SandboxService — bridges the e2b orchestrator and the sandboxes table.

Lifecycle:
    create_for_task: provision a fresh sandbox for an assigned task.
        Calls E2bClient.create_sandbox, persists a row with status='ready'.
        On e2b failure no row is written (caller bubbles the exception
        and the route returns 503 sandbox_provision_timeout).

    terminate: idempotent. Calls E2bClient.terminate then sets status=
        'terminated'. Terminated sandboxes return early (no second call).
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

import aiosqlite

from krewhub.models.sandbox import Sandbox
from krewhub.repositories.sandbox_repo import SandboxRepo
from krewhub.services.e2b_client import E2bClient

logger = logging.getLogger(__name__)


class SandboxService:
    def __init__(self, db: aiosqlite.Connection, e2b: E2bClient) -> None:
        self._db = db
        self._e2b = e2b
        self._repo = SandboxRepo(db)

    async def create_for_task(
        self,
        *,
        task_id: str,
        owner_account_id: str,
        template: str,
    ) -> Sandbox:
        e2b_id = await self._e2b.create_sandbox(template=template)
        now = datetime.now(timezone.utc)
        sandbox = Sandbox(
            id=f"sbx_{uuid.uuid4().hex[:12]}",
            task_id=task_id,
            owner_account_id=owner_account_id,
            e2b_sandbox_id=e2b_id,
            template=template,
            status="ready",
            created_at=now,
            updated_at=now,
            last_event_at=now,
        )
        await self._repo.create(sandbox)
        return sandbox

    async def create_for_bundle(
        self,
        *,
        bundle_id: str,
        owner_account_id: str,
        template: str,
    ) -> Sandbox:
        """Provision an e2b sandbox tied to a whole bundle.

        Every task in this bundle reuses the resulting sandbox so the
        agent's working tree (cloned repo, generated files, edits)
        survives across tasks. Distinct from create_for_task, which
        provisions a fresh sandbox per task — used only on the legacy
        path when a bundle has no sandbox yet.

        The original sandboxes table was created with task_id NOT NULL.
        We use the bundle_id value in the task_id slot as a sentinel
        so the column has a value; the new bundle_id column flags this
        row as bundle-scoped.
        """
        e2b_id = await self._e2b.create_sandbox(template=template)
        now = datetime.now(timezone.utc)
        sandbox = Sandbox(
            id=f"sbx_{uuid.uuid4().hex[:12]}",
            task_id=bundle_id,  # sentinel — see docstring above
            bundle_id=bundle_id,
            owner_account_id=owner_account_id,
            e2b_sandbox_id=e2b_id,
            template=template,
            status="ready",
            created_at=now,
            updated_at=now,
            last_event_at=now,
        )
        await self._repo.create(sandbox)
        return sandbox

    async def terminate(self, sandbox_id: str) -> None:
        sandbox = await self._repo.get(sandbox_id)
        if sandbox is None or sandbox.status == "terminated":
            return
        try:
            await self._e2b.terminate(sandbox.e2b_sandbox_id)
        except Exception:
            logger.exception(
                "sandbox %s: e2b terminate failed, marking row terminated anyway",
                sandbox_id,
            )
        await self._repo.update_status(sandbox_id, "terminated")

    async def mark_event(self, sandbox_id: str) -> None:
        await self._repo.mark_event(sandbox_id)

    async def reprovision_for_bundle(
        self,
        bundle_id: str,
        *,
        dead_sandbox_id: str | None = None,
    ) -> Sandbox:
        """`provision({resources})` for a bundle whose sandbox has died.

        Per Anthropic's Managed Agents framing — containers are cattle.
        Terminate the dead sandbox, create a fresh one with the same
        template, atomically update bundle.sandbox_id, return the new
        Sandbox. SandboxHand calls this when an op detects "sandbox not
        found"; the brain never sees the failure.

        Concurrency: when SandboxHand ops detect a dead sandbox in
        parallel, both will call this method. The `dead_sandbox_id`
        argument is the caller's idempotency token — the id it just saw
        die. If by the time we get here `bundle.sandbox_id` has already
        been swapped to something else, we return that existing
        replacement instead of provisioning yet another. Without this
        fence, N concurrent recoveries → N fresh sandboxes → N-1 orphans.
        """
        from krewhub.repositories.bundle_repo import BundleRepo
        repo_b = BundleRepo(self._db)
        bundle = await repo_b.get(bundle_id)
        if bundle is None:
            raise ValueError(f"reprovision: bundle {bundle_id} not found")

        # Idempotency fence: if a parallel call has already swapped
        # bundle.sandbox_id since the caller looked, return THAT one.
        if (
            dead_sandbox_id is not None
            and bundle.sandbox_id is not None
            and bundle.sandbox_id != dead_sandbox_id
        ):
            current = await self._repo.get(bundle.sandbox_id)
            if current is not None and current.status == "ready":
                logger.info(
                    "reprovision: bundle %s already has fresh sandbox %s "
                    "(caller saw dead %s); returning existing replacement",
                    bundle_id, current.id, dead_sandbox_id,
                )
                return current

        # The "dead" sandbox to terminate is whichever id we'd be
        # provisioning under. Fall back to bundle.sandbox_id if caller
        # didn't specify (legacy compat / standalone reprovision).
        to_terminate = dead_sandbox_id or bundle.sandbox_id

        if to_terminate is not None:
            try:
                await self.terminate(to_terminate)
            except Exception:
                logger.exception(
                    "reprovision: terminate of %s failed (continuing)",
                    to_terminate,
                )

        template = (
            (await self._template_for_bundle(bundle_id)) or "base"
        )
        owner = bundle.owner_account_id or ""

        fresh = await self.create_for_bundle(
            bundle_id=bundle_id,
            owner_account_id=owner,
            template=template,
        )
        await repo_b.set_sandbox(bundle_id, fresh.id)
        logger.info(
            "reprovisioned bundle %s: %s → %s (e2b %s)",
            bundle_id, to_terminate, fresh.id, fresh.e2b_sandbox_id,
        )
        return fresh

    async def _template_for_bundle(self, bundle_id: str) -> str | None:
        """Look up the most recent sandbox template used by this bundle.
        Falls back to None if no prior sandbox exists; caller should
        substitute a default."""
        cursor = await self._db.execute(
            "SELECT template FROM sandboxes "
            "WHERE bundle_id = ? "
            "ORDER BY created_at DESC LIMIT 1",
            (bundle_id,),
        )
        row = await cursor.fetchone()
        return row[0] if row else None
