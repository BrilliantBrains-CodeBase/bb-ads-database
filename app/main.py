from contextlib import asynccontextmanager
from typing import AsyncIterator

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.core.database import connect_db, disconnect_db
from app.core.error_handlers import register_error_handlers
from app.core.logging import configure_logging
from app.core.redis import connect_redis, disconnect_redis

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Startup and shutdown logic for the application."""
    settings = get_settings()

    # ── Startup ──────────────────────────────────────────────────
    configure_logging(
        json_logs=settings.is_production or settings.app_env == "staging",
        log_level="DEBUG" if settings.app_debug else "INFO",
    )
    logger.info("app.starting", env=settings.app_env, instance=settings.app_instance)
    await connect_db(settings)
    await connect_redis(settings)

    # Start the scheduler when this process is designated as the worker.
    # In multi-replica deployments only the `worker` instance runs jobs;
    # API replicas skip this to avoid duplicate runs.
    _scheduler_enabled = (
        settings.app_instance == "worker"
        or settings.app_env == "development"
    )
    if _scheduler_enabled:
        from app.worker.scheduler import start_scheduler
        await start_scheduler()

    logger.info("app.ready")
    yield

    # ── Shutdown ─────────────────────────────────────────────────
    logger.info("app.shutting_down")
    if _scheduler_enabled:
        from app.worker.scheduler import stop_scheduler
        await stop_scheduler()
    await disconnect_redis()
    await disconnect_db()
    logger.info("app.stopped")


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="BB Ads Analytics API",
        description="Multi-tenant ad performance platform for digital agencies.",
        version="0.1.0",
        docs_url="/docs" if not settings.is_production else None,
        redoc_url="/redoc" if not settings.is_production else None,
        openapi_url="/openapi.json" if not settings.is_production else None,
        lifespan=lifespan,
    )

    # ── CORS ─────────────────────────────────────────────────────
    # Explicit allowed origins — never "*" (Gap fix from spec)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-Correlation-ID"],
        expose_headers=["X-Correlation-ID"],
    )

    # ── Error handlers ───────────────────────────────────────────
    register_error_handlers(app)

    # ── Middleware ────────────────────────────────────────────────
    # Starlette applies middleware in LIFO order, so CorrelationMiddleware
    # (added last) runs first — correlation_id is bound before anything else.
    from app.middleware.correlation import CorrelationMiddleware

    app.add_middleware(CorrelationMiddleware)

    # Auth middleware added in Phase 1 Week 1
    # Metrics middleware added in Phase 2 Week 10

    # ── Routers ──────────────────────────────────────────────────
    _register_routers(app)

    # ── Health endpoint (public, no auth, no rate limit) ─────────
    @app.get("/health", tags=["health"], include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def _register_routers(app: FastAPI) -> None:
    from app.api.v1.routers import (
        admin,
        anomalies,
        auth,
        brands,
        campaigns,
        claude,
        ingestion,
        performance,
        reports,
        webhooks,
    )

    api_prefix = "/api/v1"

    app.include_router(auth.router, prefix=api_prefix)
    app.include_router(brands.router, prefix=api_prefix)
    app.include_router(campaigns.router, prefix=api_prefix)
    app.include_router(performance.router, prefix=api_prefix)
    app.include_router(ingestion.router, prefix=api_prefix)
    app.include_router(anomalies.router, prefix=api_prefix)
    app.include_router(reports.router, prefix=api_prefix)
    app.include_router(claude.router, prefix=api_prefix)
    app.include_router(admin.router, prefix=api_prefix)
    app.include_router(webhooks.router, prefix=api_prefix)


# Module-level app instance for uvicorn / gunicorn
app = create_app()
