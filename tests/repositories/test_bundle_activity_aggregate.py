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


@pytest.mark.asyncio
async def test_max_scoped_per_bundle(seeded_db):
    """Each bundle's MAX(updated_at) is scoped to its own tasks, not the cookbook's."""
    # Add a second bundle in the same cookbook.
    await seeded_db.execute(
        "INSERT INTO bundles (id, cookbook_id, prompt, status, "
        "created_by, created_at, resource_version, generation) "
        "VALUES ('b-other', 'cb-seed', '', 'open', 'u1', "
        "        '2026-05-01T00:00:00+00:00', 1, 1)"
    )
    await seeded_db.commit()

    repo = TaskRepo(seeded_db)
    # Old activity on b-seed
    await repo.create(Task(id="t-seed-old", bundle_id="b-seed", title="x", status=TaskStatus.OPEN))
    t_seed_updated = (await repo.get("t-seed-old")).updated_at

    # Newer activity on b-other (later wall-clock)
    await asyncio.sleep(0.01)
    await repo.create(Task(id="t-other-new", bundle_id="b-other", title="y", status=TaskStatus.OPEN))
    t_other_updated = (await repo.get("t-other-new")).updated_at

    bundles = {b.id: b for b in await BundleRepo(seeded_db).list_by_cookbook("cb-seed")}
    assert bundles["b-seed"].latest_task_activity_at == t_seed_updated
    assert bundles["b-other"].latest_task_activity_at == t_other_updated
    assert bundles["b-seed"].latest_task_activity_at != bundles["b-other"].latest_task_activity_at
