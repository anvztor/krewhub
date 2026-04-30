from __future__ import annotations

import os

os.environ["KREWHUB_DATABASE_PATH"] = ":memory:"
os.environ["KREWHUB_API_KEY"] = "test-key"
os.environ["KREWHUB_JWT_SECRET"] = "test-jwt-secret-at-least-32-bytes-long-for-hs256"
os.environ["KREWHUB_JWKS_URL"] = ""  # disable JWKS in tests (no krewauth running)

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
async def test_db(_setup_db):
    """Direct handle to the in-memory db (auth track A2 unit tests)."""
    return await get_db()


@pytest_asyncio.fixture
async def client(_setup_db):
    """Client authenticated with legacy API key (backward compat)."""
    app = create_app()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-API-Key": "test-key"},
    ) as ac:
        yield ac


@pytest_asyncio.fixture
async def anon_client(_setup_db):
    """Unauthenticated client for testing auth endpoints."""
    app = create_app()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as ac:
        yield ac


@pytest_asyncio.fixture
async def cookie_client(_setup_db):
    """Client authenticated via krew_session cookie (simulates browser after BFF elimination)."""
    import jwt

    settings = get_settings()
    token = jwt.encode(
        {"sub": "acc_test_cookie", "username": "cookie_tester", "method": "passkey"},
        settings.jwt_secret,
        algorithm="HS256",
    )
    app = create_app()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={"krew_session": token},
    ) as ac:
        yield ac
