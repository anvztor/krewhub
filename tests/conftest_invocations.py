"""Shared fixtures for the Invocation Contract test files.

Imported by `tests/conftest.py` so they're auto-discovered by pytest.
"""
from __future__ import annotations

import pytest_asyncio


@pytest_asyncio.fixture
async def _install_fake_hand(_setup_db):
    """Install a FakeHand registry on a fresh app's invocation service.

    Yields (app, fake_hand). The app's `state.invocations` is wired to
    a service whose only registered Hand is target_type='fake'.
    """
    from tests.test_invocation_service import FakeHand
    from krewhub.app import create_app
    from krewhub.services.invocation_service import InvocationService
    from krewhub.db.connection import get_db
    from krewhub.watch.globals import get_watch_service

    app = create_app()
    db = await get_db()
    fake = FakeHand(target_type="fake")
    app.state.invocations = InvocationService(
        db, hands={"fake": fake}, watch=get_watch_service(),
    )
    yield app, fake


@pytest_asyncio.fixture
async def inv_client(_install_fake_hand):
    """ASGI client wired to the app whose invocation service has FakeHand."""
    from httpx import ASGITransport, AsyncClient

    app, _ = _install_fake_hand
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-API-Key": "test-key"},
    ) as ac:
        yield ac
