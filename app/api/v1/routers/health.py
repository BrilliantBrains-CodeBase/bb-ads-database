"""
GET /health — public system health check

Response shape
──────────────
{
  "status":   "ok" | "degraded" | "down",
  "mongodb":  { "status": "ok" | "down", "latency_ms": 4 },
  "redis":    { "status": "ok" | "down" },
  "ingestion": { "status": "ok" | "degraded" | "unknown",
                 "hours_since_last_success": 3.2 | null }
}

Status rules (evaluated in order, first match wins)
────────────────────────────────────────────────────
  "down"     — MongoDB or Redis is unreachable
  "degraded" — reachable but last successful ingestion > 26 h ago
  "ok"       — all checks pass

Probe timeouts
──────────────
  MongoDB: 2 s command timeout (serverSelectionTimeoutMS already set at
           client creation; the ping command itself uses commandtimeout)
  Redis:   2 s socket timeout on the PING
  Both are caught and treated as "down" without bubbling to the caller.

This endpoint is public — no authentication, no rate limiting — and is
deliberately excluded from the OpenAPI schema so it does not appear in
the Swagger UI.  It is used by Docker HEALTHCHECK and the nginx upstream
health probe.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.core.database import get_db_direct, get_motor_client
from app.core.redis import get_redis_client

router = APIRouter(tags=["health"])
logger = structlog.get_logger(__name__)

_STALE_HOURS = 26   # matches _INGESTION_STALE_HOURS in tasks.py


# ── Response builder helpers ───────────────────────────────────────────────────

def _ok_response(body: dict[str, Any]) -> JSONResponse:
    return JSONResponse(content=body, status_code=200)


def _degraded_response(body: dict[str, Any]) -> JSONResponse:
    return JSONResponse(content=body, status_code=200)   # 200 — monitors read the body


def _down_response(body: dict[str, Any]) -> JSONResponse:
    return JSONResponse(content=body, status_code=503)


# ── Probes ─────────────────────────────────────────────────────────────────────

async def _check_mongodb() -> dict[str, Any]:
    """Ping MongoDB and measure round-trip latency.

    Returns {"status": "ok"|"down", "latency_ms": int|null}.
    """
    try:
        client = get_motor_client()

        t0 = datetime.now(UTC)
        await client.admin.command("ping", maxTimeMS=2000)
        latency_ms = round((datetime.now(UTC) - t0).total_seconds() * 1000)
        return {"status": "ok", "latency_ms": latency_ms}
    except Exception as exc:
        logger.warning("health.mongodb.down", error=str(exc))
        return {"status": "down", "latency_ms": None}


async def _check_redis() -> dict[str, Any]:
    """PING Redis.

    Returns {"status": "ok"|"down"}.
    """
    try:
        redis = get_redis_client()
        await redis.ping()
        return {"status": "ok"}
    except Exception as exc:
        logger.warning("health.redis.down", error=str(exc))
        return {"status": "down"}


async def _check_ingestion() -> dict[str, Any]:
    """Find the most recent successful ingestion across all brands and sources.

    Returns:
        {
          "status": "ok" | "degraded" | "unknown",
          "hours_since_last_success": float | null
        }

    "unknown" means the ingestion_logs collection is empty or unreachable
    (e.g. first boot before any ingestion has run).
    """
    try:
        db = get_db_direct()

        doc = await db["ingestion_logs"].find_one(
            {"status": "success"},
            sort=[("completed_at", -1)],
            projection={"completed_at": 1},
        )

        if doc is None:
            return {"status": "unknown", "hours_since_last_success": None}

        completed_at: datetime = doc["completed_at"]
        if completed_at.tzinfo is None:
            completed_at = completed_at.replace(tzinfo=UTC)

        hours_since = (datetime.now(UTC) - completed_at).total_seconds() / 3600
        hours_since = round(hours_since, 1)

        status = "ok" if hours_since <= _STALE_HOURS else "degraded"
        return {"status": status, "hours_since_last_success": hours_since}

    except Exception as exc:
        logger.warning("health.ingestion.check_failed", error=str(exc))
        return {"status": "unknown", "hours_since_last_success": None}


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.get("/health", include_in_schema=False)
async def health_check() -> JSONResponse:
    """System health check — public, no auth.

    Probes MongoDB, Redis, and the ingestion log in parallel, then
    derives an overall status.  Responds with 200 for ok/degraded and
    503 for down so load-balancer health probes act correctly.
    """
    import asyncio

    mongo_result, redis_result, ingestion_result = await asyncio.gather(
        _check_mongodb(),
        _check_redis(),
        _check_ingestion(),
    )

    body = {
        "status": "ok",          # overridden below
        "mongodb":   mongo_result,
        "redis":     redis_result,
        "ingestion": ingestion_result,
    }

    # ── Derive overall status ─────────────────────────────────────────────────
    if mongo_result["status"] == "down" or redis_result["status"] == "down":
        body["status"] = "down"
        return _down_response(body)

    if ingestion_result["status"] == "degraded":
        body["status"] = "degraded"
        return _degraded_response(body)

    body["status"] = "ok"
    return _ok_response(body)
