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
