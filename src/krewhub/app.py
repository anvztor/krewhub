from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from krewhub.db.connection import close_db, init_db
from krewhub.routes import agents, bundles, recipes, stream, tasks
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

    app.include_router(recipes.router, prefix="/api/v1")
    app.include_router(bundles.router, prefix="/api/v1")
    app.include_router(tasks.router, prefix="/api/v1")
    app.include_router(agents.router, prefix="/api/v1")
    app.include_router(stream.router, prefix="/api/v1")

    return app
