from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Ensure krewhub loggers are visible (uvicorn only shows its own by default)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

from krewhub.db.connection import close_db, init_db
from krewhub.routes import (
    a2a_callback, a2a_gateway, agent_runtimes, agents, aggregate, auth_web,
    bundles, cookbook_sharing, cookbooks, credential_relay, credentials,
    git_http, hooks, invocations, links, oauth, proxy_krewauth, stream,
    tapes, tasks,
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

    # Auth track A2 — provision a single E2bClient and stash on app state.
    # SandboxService and SandboxSweeper resolve it via deps.get_e2b.
    from krewhub.services.e2b_client import E2bClient
    _app.state.e2b = E2bClient(
        base_url=settings.e2b_api_url,
        api_key=settings.e2b_api_key,
        proxy_url=settings.e2b_client_proxy_url or None,
        envd_proxy_domain=settings.e2b_envd_proxy_domain,
    )

    manager = ControllerManager(
        db, watch,
        heartbeat_timeout=settings.heartbeat_timeout_seconds,
        orch_enabled=settings.orch_enabled,
        orch_interval=settings.orch_interval_seconds,
        orch_liveness_timeout=settings.orch_liveness_timeout_seconds,
        orch_max_respawns=settings.orch_max_respawns,
    )
    set_controller_manager(manager)
    await manager.start_all()

    # Auth track A2 — sweep idle/aged sandboxes so cost stays bounded.
    from krewhub.controllers.sandbox_sweeper import SandboxSweeper
    from krewhub.db.connection import get_db as _get_singleton_db

    sweeper = SandboxSweeper(
        get_db=_get_singleton_db,
        e2b=_app.state.e2b,
        idle_seconds=settings.sandbox_idle_timeout_seconds,
        max_age_seconds=settings.sandbox_max_age_seconds,
    )
    sweeper.start()
    _app.state.sandbox_sweeper = sweeper

    # Invocation Contract — register the InvocationService singleton with
    # the production Hand registry. Routes resolve it via app.state.
    from krewhub.services.invocation_service import InvocationService
    from krewhub.services.sandbox_service import SandboxService
    from krewhub.workers import AgentHand, HumanHand, SandboxHand
    # SandboxHand auto-recovers from dead-sandbox 502s by reprovisioning
    # via SandboxService.reprovision_for_bundle (the `provision({resources})`
    # primitive from Anthropic Managed Agents). Without `sandbox_service`
    # injected, SandboxHand falls back to surfacing the raw error to the
    # brain — used in tests but not in production.
    sandbox_service = SandboxService(db, _app.state.e2b)
    # CredentialsService: at-rest credential store, decrypted at op:exec
    # dispatch time and merged into the sandbox's env. See
    # services/credentials_service.py for the encryption model.
    from krewhub.services.credentials_service import (
        CredentialsService, resolve_encryption_key,
    )
    credentials_service = CredentialsService(
        db, resolve_encryption_key(settings.credentials_encryption_key),
    )
    _app.state.invocations = InvocationService(
        db, watch=watch,
        hands={
            "sandbox": SandboxHand(
                _app.state.e2b, db=db, sandbox_service=sandbox_service,
                credentials_service=credentials_service,
            ),
            "human": HumanHand(db),
            # AgentHand bridges to the existing A2A queue. The krewcli
            # daemon must implement method="delegate" to actually run a
            # sub-Brain; without that, agent invocations time out and
            # return action=cancel reason=a2a_deadline_exceeded.
            "agent": AgentHand(db),
        },
    )

    # Auth Phase 0 — sweep expired elicit leases so timed-out inject
    # attempts don't permanently block the SPA from retrying.
    from krewhub.services.elicit_sweeper import run_elicit_sweep_loop
    elicit_sweep_task = asyncio.create_task(
        run_elicit_sweep_loop(_get_singleton_db, interval_s=10.0),
        name="elicit-sweeper",
    )
    _app.state.elicit_sweep_task = elicit_sweep_task

    yield

    elicit_sweep_task.cancel()
    try:
        await elicit_sweep_task
    except asyncio.CancelledError:
        pass

    await sweeper.stop()
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

    # Auth track A2 — pre-init E2bClient on app.state so dependency
    # injection works even when ASGITransport tests skip lifespan. The
    # production lifespan re-assigns a fresh client at startup.
    from krewhub.services.e2b_client import E2bClient
    app.state.e2b = E2bClient(
        base_url=settings.e2b_api_url,
        api_key=settings.e2b_api_key,
    )
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
    app.include_router(cookbook_sharing.router, prefix="/api/v1")
    app.include_router(bundles.router, prefix="/api/v1")
    app.include_router(tasks.router, prefix="/api/v1")
    app.include_router(links.router, prefix="/api/v1")
    app.include_router(agents.router, prefix="/api/v1")
    app.include_router(agent_runtimes.router, prefix="/api/v1")
    app.include_router(tapes.router, prefix="/api/v1")
    app.include_router(stream.router, prefix="/api/v1")
    app.include_router(a2a_callback.router, prefix="/api/v1")
    app.include_router(hooks.router, prefix="/api/v1")
    app.include_router(invocations.router, prefix="/api/v1")
    app.include_router(credentials.router, prefix="/api/v1")
    app.include_router(credential_relay.router)
    app.include_router(oauth.router, prefix="/api/v1")

    # A2A hub gateway — public agent endpoints
    app.include_router(a2a_gateway.router)

    # Git smart HTTP
    app.include_router(git_http.router)

    return app
