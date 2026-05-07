"""Sandbox model — e2b-backed runtime sandbox attached to a task.

Lifecycle:
    provisioning -> ready -> running -> terminated
                                   |--> error

The model mirrors the `sandboxes` table; status transitions are owned by
SandboxService and SandboxSweeper.
"""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel


SandboxStatus = Literal["provisioning", "ready", "running", "terminated", "error"]


class Sandbox(BaseModel, frozen=True):
    id: str
    # task_id is now optional — bundle-scoped sandboxes don't bind to a
    # specific task. Existing per-task rows still set it.
    task_id: str | None = None
    bundle_id: str | None = None
    owner_account_id: str
    e2b_sandbox_id: str
    template: str
    status: SandboxStatus
    created_at: datetime
    updated_at: datetime
    terminated_at: datetime | None = None
    last_event_at: datetime | None = None
