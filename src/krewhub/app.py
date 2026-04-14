from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Ensure krewhub loggers are visible (uvicorn only shows its own by default)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

from krewhub.db.connection import close_db, init_db
from krewhub.routes import (
    a2a_callback, a2a_gateway, agents, aggregate, auth_web, bundles,
    cookbooks, git_http, hooks, proxy_krewauth, recipes, stream, tapes, tasks,
)
from krewhub.watch.service import WatchService
from krewhub.watch.globals import set_watch_service, clear_watch_service
from krewhub.controllers.manager import ControllerManager
from krewhub.controllers.globals import set_controller_manager, clear_controller_manager


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    db = await init_db()
    watch = WatchService(db)
    set_watch_service(watch)

    from krewhub.config import get_settings
    settings = get_settings()

    # Init JWKS client for krewauth JWT verification
    if settings.jwks_url:
        from krewhub.auth import init_jwk_client
        try:
            init_jwk_client(settings.jwks_url)
        except Exception:
            import logging
            logging.getLogger(__name__).warning(
                "Failed to init JWKS client at %s — ES256 verification disabled",
                settings.jwks_url,
            )

    manager = ControllerManager(
        db, watch,
        heartbeat_timeout=settings.heartbeat_timeout_seconds,
    )
    set_controller_manager(manager)
    await manager.start_all()

    yield

    await manager.stop_all()
    clear_controller_manager()
    clear_watch_service()
    await close_db()


def create_app() -> FastAPI:
    app = FastAPI(
        title="KrewHub",
        version="0.1.0",
        description="Control plane and system of record for Cookrew",
        lifespan=lifespan,
    )

    # CORS — allow the frontend origin with credentials (cookies)
    from krewhub.config import get_settings
    settings = get_settings()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[settings.app_origin],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Browser-facing auth (cookie-based, no prefix)
    app.include_router(auth_web.router)

    # Proxied krewauth routes
    app.include_router(proxy_krewauth.router, prefix="/api/v1")

    # Aggregate endpoints (BFF elimination)
    app.include_router(aggregate.router, prefix="/api/v1")

    # Existing API routes
    app.include_router(cookbooks.router, prefix="/api/v1")
    app.include_router(recipes.router, prefix="/api/v1")
    app.include_router(bundles.router, prefix="/api/v1")
    app.include_router(tasks.router, prefix="/api/v1")
    app.include_router(agents.router, prefix="/api/v1")
    app.include_router(tapes.router, prefix="/api/v1")
    app.include_router(stream.router, prefix="/api/v1")
    app.include_router(a2a_callback.router, prefix="/api/v1")
    app.include_router(hooks.router, prefix="/api/v1")

    # A2A hub gateway — public agent endpoints
    app.include_router(a2a_gateway.router)

    # Git smart HTTP
    app.include_router(git_http.router)

    return app
