"""
Unit tests for GET /health.

Strategy
────────
• MongoDB, Redis, and the ingestion DB query are all mocked at the module
  level so no real connections are needed.
• The endpoint is exercised via FastAPI TestClient (synchronous wrapper
  around an async app).
• Each probe helper (_check_mongodb, _check_redis, _check_ingestion) is
  also tested in isolation to keep coverage granular.

Coverage
────────
  _check_mongodb:
    - Returns {"status": "ok", "latency_ms": <int>} on success
    - Returns {"status": "down", "latency_ms": null} when unreachable
    - latency_ms is a non-negative integer

  _check_redis:
    - Returns {"status": "ok"} on successful PING
    - Returns {"status": "down"} on any exception

  _check_ingestion:
    - Returns status "ok" when last success is < 26 h ago
    - Returns status "degraded" when last success is > 26 h ago
    - Returns hours_since_last_success as a float rounded to 1 decimal
    - Returns status "unknown" when no success doc exists
    - Returns status "unknown" when DB is unreachable

  GET /health — overall status logic:
    - "ok" when all probes pass and ingestion is fresh
    - "degraded" (HTTP 200) when ingestion is stale but infra is up
    - "down" (HTTP 503) when MongoDB is unreachable
    - "down" (HTTP 503) when Redis is unreachable
    - "down" (HTTP 503) when both MongoDB and Redis are unreachable
    - Response body contains mongodb / redis / ingestion sub-keys
    - Response is valid JSON with correct Content-Type
    - "ok" when ingestion status is "unknown" (first boot — don't alarm)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.routers.health import (
    _check_ingestion,
    _check_mongodb,
    _check_redis,
    _STALE_HOURS,
)

# ── TestClient fixture ─────────────────────────────────────────────────────────

def _build_client() -> TestClient:
    from app.api.v1.routers.health import router
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


CLIENT = _build_client()


# ── Helpers ────────────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(UTC)


def _hours_ago(h: float) -> datetime:
    return _now() - timedelta(hours=h)


def _make_mongo_ok():
    """Patch get_motor_client to return a mock that responds to ping."""
    mock_client = AsyncMock()
    mock_client.admin.command = AsyncMock(return_value={"ok": 1})
    return patch("app.api.v1.routers.health.get_motor_client", return_value=mock_client)


# ── _check_mongodb ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
class TestCheckMongodb:
    async def test_ok_returns_status_and_latency(self):
        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(return_value={"ok": 1})
        with patch("app.api.v1.routers.health.get_motor_client", return_value=mock_client):
            result = await _check_mongodb()
        assert result["status"] == "ok"
        assert isinstance(result["latency_ms"], int)
        assert result["latency_ms"] >= 0

    async def test_down_on_connection_error(self):
        with patch(
            "app.api.v1.routers.health.get_motor_client",
            side_effect=RuntimeError("not connected"),
        ):
            result = await _check_mongodb()
        assert result["status"] == "down"
        assert result["latency_ms"] is None

    async def test_down_on_ping_failure(self):
        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(side_effect=Exception("timeout"))
        with patch("app.api.v1.routers.health.get_motor_client", return_value=mock_client):
            result = await _check_mongodb()
        assert result["status"] == "down"
        assert result["latency_ms"] is None

    async def test_latency_is_integer(self):
        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(return_value={"ok": 1})
        with patch("app.api.v1.routers.health.get_motor_client", return_value=mock_client):
            result = await _check_mongodb()
        assert isinstance(result["latency_ms"], int)


# ── _check_redis ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
class TestCheckRedis:
    async def test_ok_on_successful_ping(self):
        mock_redis = AsyncMock()
        mock_redis.ping = AsyncMock(return_value=True)
        with patch("app.api.v1.routers.health.get_redis_client", return_value=mock_redis):
            result = await _check_redis()
        assert result == {"status": "ok"}

    async def test_down_on_connection_error(self):
        with patch(
            "app.api.v1.routers.health.get_redis_client",
            side_effect=RuntimeError("not connected"),
        ):
            result = await _check_redis()
        assert result == {"status": "down"}

    async def test_down_on_ping_exception(self):
        mock_redis = AsyncMock()
        mock_redis.ping = AsyncMock(side_effect=ConnectionError("refused"))
        with patch("app.api.v1.routers.health.get_redis_client", return_value=mock_redis):
            result = await _check_redis()
        assert result == {"status": "down"}


# ── _check_ingestion ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
class TestCheckIngestion:
    def _patch_db(self, doc):
        """Patch get_db_direct to return a mock DB where ['ingestion_logs'].find_one returns doc."""
        from unittest.mock import MagicMock
        mock_col = AsyncMock()
        mock_col.find_one = AsyncMock(return_value=doc)
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_col)
        return patch("app.api.v1.routers.health.get_db_direct", return_value=mock_db)

    async def test_ok_when_recent_success(self):
        doc = {"completed_at": _hours_ago(3)}
        with self._patch_db(doc):
            result = await _check_ingestion()
        assert result["status"] == "ok"
        assert result["hours_since_last_success"] == pytest.approx(3.0, abs=0.2)

    async def test_degraded_when_stale(self):
        doc = {"completed_at": _hours_ago(_STALE_HOURS + 2)}
        with self._patch_db(doc):
            result = await _check_ingestion()
        assert result["status"] == "degraded"
        assert result["hours_since_last_success"] > _STALE_HOURS

    async def test_exactly_at_threshold_is_ok(self):
        doc = {"completed_at": _hours_ago(_STALE_HOURS - 0.5)}
        with self._patch_db(doc):
            result = await _check_ingestion()
        assert result["status"] == "ok"

    async def test_hours_rounded_to_one_decimal(self):
        doc = {"completed_at": _hours_ago(5.666)}
        with self._patch_db(doc):
            result = await _check_ingestion()
        hours = result["hours_since_last_success"]
        assert hours == round(hours, 1)

    async def test_unknown_when_no_doc(self):
        with self._patch_db(None):
            result = await _check_ingestion()
        assert result["status"] == "unknown"
        assert result["hours_since_last_success"] is None

    async def test_unknown_on_db_error(self):
        with patch(
            "app.api.v1.routers.health.get_db_direct",
            side_effect=RuntimeError("mongo down"),
        ):
            result = await _check_ingestion()
        assert result["status"] == "unknown"
        assert result["hours_since_last_success"] is None

    async def test_naive_datetime_handled(self):
        """completed_at without tzinfo should not raise."""
        doc = {"completed_at": (datetime.now(UTC) - timedelta(hours=2)).replace(tzinfo=None)}
        with self._patch_db(doc):
            result = await _check_ingestion()
        assert result["status"] == "ok"


# ── GET /health — full endpoint ────────────────────────────────────────────────

class TestHealthEndpoint:
    def _patch_all(self, mongo_result, redis_result, ingestion_result):
        return (
            patch("app.api.v1.routers.health._check_mongodb",
                  new=AsyncMock(return_value=mongo_result)),
            patch("app.api.v1.routers.health._check_redis",
                  new=AsyncMock(return_value=redis_result)),
            patch("app.api.v1.routers.health._check_ingestion",
                  new=AsyncMock(return_value=ingestion_result)),
        )

    def _apply(self, mongo, redis, ingestion):
        p1, p2, p3 = self._patch_all(mongo, redis, ingestion)
        with p1, p2, p3:
            return CLIENT.get("/health")

    # ── Status: ok ──────────────────────────────────────────────────────
    def test_ok_status_200(self):
        resp = self._apply(
            mongo={"status": "ok", "latency_ms": 2},
            redis={"status": "ok"},
            ingestion={"status": "ok", "hours_since_last_success": 3.0},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_ok_body_has_all_keys(self):
        resp = self._apply(
            mongo={"status": "ok", "latency_ms": 2},
            redis={"status": "ok"},
            ingestion={"status": "ok", "hours_since_last_success": 1.0},
        )
        body = resp.json()
        assert "mongodb" in body
        assert "redis" in body
        assert "ingestion" in body

    def test_ok_when_ingestion_unknown_first_boot(self):
        """On first boot with no ingestion history, status is ok (don't alarm)."""
        resp = self._apply(
            mongo={"status": "ok", "latency_ms": 1},
            redis={"status": "ok"},
            ingestion={"status": "unknown", "hours_since_last_success": None},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    # ── Status: degraded ─────────────────────────────────────────────────
    def test_degraded_status_200(self):
        resp = self._apply(
            mongo={"status": "ok", "latency_ms": 2},
            redis={"status": "ok"},
            ingestion={"status": "degraded", "hours_since_last_success": 30.0},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "degraded"

    def test_degraded_body_has_ingestion_hours(self):
        resp = self._apply(
            mongo={"status": "ok", "latency_ms": 2},
            redis={"status": "ok"},
            ingestion={"status": "degraded", "hours_since_last_success": 30.0},
        )
        assert resp.json()["ingestion"]["hours_since_last_success"] == 30.0

    # ── Status: down — MongoDB ────────────────────────────────────────────
    def test_down_503_when_mongo_down(self):
        resp = self._apply(
            mongo={"status": "down", "latency_ms": None},
            redis={"status": "ok"},
            ingestion={"status": "ok", "hours_since_last_success": 1.0},
        )
        assert resp.status_code == 503
        assert resp.json()["status"] == "down"

    def test_down_503_when_redis_down(self):
        resp = self._apply(
            mongo={"status": "ok", "latency_ms": 2},
            redis={"status": "down"},
            ingestion={"status": "ok", "hours_since_last_success": 1.0},
        )
        assert resp.status_code == 503
        assert resp.json()["status"] == "down"

    def test_down_503_when_both_down(self):
        resp = self._apply(
            mongo={"status": "down", "latency_ms": None},
            redis={"status": "down"},
            ingestion={"status": "unknown", "hours_since_last_success": None},
        )
        assert resp.status_code == 503
        assert resp.json()["status"] == "down"

    def test_down_body_still_has_sub_keys(self):
        """Even on down, the body must include mongodb/redis/ingestion for diagnostics."""
        resp = self._apply(
            mongo={"status": "down", "latency_ms": None},
            redis={"status": "ok"},
            ingestion={"status": "ok", "hours_since_last_success": 1.0},
        )
        body = resp.json()
        assert body["mongodb"]["status"] == "down"
        assert "redis" in body
        assert "ingestion" in body

    # ── Content type ─────────────────────────────────────────────────────
    def test_content_type_json(self):
        resp = self._apply(
            mongo={"status": "ok", "latency_ms": 1},
            redis={"status": "ok"},
            ingestion={"status": "ok", "hours_since_last_success": 1.0},
        )
        assert "application/json" in resp.headers["content-type"]

    # ── Down overrides degraded ───────────────────────────────────────────
    def test_down_overrides_degraded_ingestion(self):
        """If infra is down AND ingestion is degraded, overall is still 'down'."""
        resp = self._apply(
            mongo={"status": "down", "latency_ms": None},
            redis={"status": "ok"},
            ingestion={"status": "degraded", "hours_since_last_success": 30.0},
        )
        assert resp.status_code == 503
        assert resp.json()["status"] == "down"

    # ── MongoDB latency forwarded ─────────────────────────────────────────
    def test_mongodb_latency_forwarded(self):
        resp = self._apply(
            mongo={"status": "ok", "latency_ms": 7},
            redis={"status": "ok"},
            ingestion={"status": "ok", "hours_since_last_success": 1.0},
        )
        assert resp.json()["mongodb"]["latency_ms"] == 7
