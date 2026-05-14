"""BundleRepo.list_by_cookbook returns latest_task_activity_at per bundle."""
from __future__ import annotations

import asyncio

import pytest

from krewhub.models import Task, TaskStatus
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.task_repo import TaskRepo


@pytest.mark.asyncio
async def test_empty_bundle_uses_created_at(seeded_db):
    """A bundle with zero tasks falls back to bundle.created_at."""
    bundles = await BundleRepo(seeded_db).list_by_cookbook("cb-seed")
    [b] = [x for x in bundles if x.id == "b-seed"]
    assert b.latest_task_activity_at == b.created_at


@pytest.mark.asyncio
async def test_with_tasks_uses_max_updated_at(seeded_db):
    """Bundle activity == MAX(tasks.updated_at)."""
    repo = TaskRepo(seeded_db)
    await repo.create(Task(id="t1", bundle_id="b-seed", title="a", status=TaskStatus.OPEN))
    await asyncio.sleep(0.01)
    await repo.create(Task(id="t2", bundle_id="b-seed", title="b", status=TaskStatus.OPEN))
    t2_updated = (await repo.get("t2")).updated_at

    bundles = await BundleRepo(seeded_db).list_by_cookbook("cb-seed")
    [b] = [x for x in bundles if x.id == "b-seed"]
    assert b.latest_task_activity_at == t2_updated
