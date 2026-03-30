from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from krewhub.db.connection import close_db, init_db
from krewhub.routes import agents, bundles, recipes, stream, tasks


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    await init_db()
    yield
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
