from __future__ import annotations

import os

os.environ["KREWHUB_DATABASE_PATH"] = ":memory:"
os.environ["KREWHUB_API_KEY"] = "test-key"

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from krewhub.config import get_settings

# Clear the lru_cache so test settings take effect
get_settings.cache_clear()

from krewhub.app import create_app
from krewhub.db.connection import init_db, close_db, get_db
from krewhub.watch.service import WatchService
from krewhub.watch.globals import set_watch_service, clear_watch_service


@pytest_asyncio.fixture(autouse=True)
async def _setup_db():
    db = await init_db()
    set_watch_service(WatchService(db))
    yield
    clear_watch_service()
    await close_db()


@pytest_asyncio.fixture
async def client(_setup_db):
    app = create_app()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-API-Key": "test-key"},
    ) as ac:
        yield ac
