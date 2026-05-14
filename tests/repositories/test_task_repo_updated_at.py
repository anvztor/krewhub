"""TaskRepo bumps tasks.updated_at on create and on every update."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from krewhub.models import Task, TaskStatus
from krewhub.repositories.task_repo import TaskRepo


@pytest.mark.asyncio
async def test_create_stamps_updated_at(seeded_db):
    """create() sets updated_at to now() at insert time."""
    repo = TaskRepo(seeded_db)
    before = datetime.now(timezone.utc)
    t = await repo.create(Task(
        id="t-x",
        bundle_id="b-seed",
        title="x",
        status=TaskStatus.OPEN,
    ))
    after = datetime.now(timezone.utc)

    fetched = await repo.get(t.id)
    assert fetched is not None
    assert fetched.updated_at is not None
    assert before <= fetched.updated_at <= after


@pytest.mark.asyncio
async def test_update_bumps_updated_at(seeded_db):
    """Every update() call bumps updated_at to now()."""
    repo = TaskRepo(seeded_db)
    t = await repo.create(Task(
        id="t-y",
        bundle_id="b-seed",
        title="y",
        status=TaskStatus.OPEN,
    ))
    initial = (await repo.get(t.id)).updated_at
    assert initial is not None

    await asyncio.sleep(0.01)  # ensure clock advances on fast machines
    await repo.update(t.id, status=TaskStatus.WORKING)
    bumped = (await repo.get(t.id)).updated_at
    assert bumped is not None
    assert bumped > initial


@pytest.mark.asyncio
async def test_update_with_no_field_changes_does_not_bump(seeded_db):
    """An update() with no actual field changes returns early - no bump.

    Matches existing behavior at task_repo.py:187-189; we don't churn
    updated_at on no-op calls.
    """
    repo = TaskRepo(seeded_db)
    t = await repo.create(Task(
        id="t-z",
        bundle_id="b-seed",
        title="z",
        status=TaskStatus.OPEN,
    ))
    initial = (await repo.get(t.id)).updated_at
    await asyncio.sleep(0.01)
    await repo.update(t.id)  # no kwargs -> no-op
    after = (await repo.get(t.id)).updated_at
    assert after == initial


@pytest.mark.asyncio
async def test_reopen_for_rerun_bumps_updated_at(seeded_db):
    """reopen_for_rerun() also bumps updated_at."""
    repo = TaskRepo(seeded_db)
    t = await repo.create(Task(
        id="t-r",
        bundle_id="b-seed",
        title="r",
        status=TaskStatus.DONE,
    ))
    initial = (await repo.get(t.id)).updated_at
    await asyncio.sleep(0.01)
    await repo.reopen_for_rerun(t.id)
    after = (await repo.get(t.id)).updated_at
    assert after is not None and after > initial
