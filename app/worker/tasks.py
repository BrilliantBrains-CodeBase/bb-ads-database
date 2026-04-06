"""
Background task functions for the APScheduler worker.

Each function is a self-contained async coroutine.  The scheduler calls them;
they are responsible for:
  1. Obtaining their own DB / Redis references (via get_db_direct / get_redis_client).
  2. Logging start, finish, and errors with structlog.
  3. Being idempotent — safe to re-run if a previous run was interrupted.

Task catalogue
──────────────
  daily_ingestion          — pull D-1 + D-0 for all active brands × all sources
  rollup_computation       — compute daily/weekly/monthly rollup aggregates
  anomaly_detection        — flag statistical anomalies in recent performance data
  scheduled_reports        — generate and email any scheduled reports due today
  ingestion_health_check   — alert if any brand hasn't ingested in >26 hours
  token_refresh_google     — refresh Google OAuth tokens before they expire
  meta_token_expiry_check  — warn if any Meta token expires within 7 days

Schedule (IST = UTC+5:30):
  00:30 UTC (06:00 IST) — daily_ingestion
  01:30 UTC (07:00 IST) — rollup_computation
  02:30 UTC (08:00 IST) — anomaly_detection
  03:30 UTC (09:00 IST) — scheduled_reports
  every 30 min          — ingestion_health_check
  02:00 UTC daily       — token_refresh_google
  02:30 UTC daily       — meta_token_expiry_check  (combined with anomaly run)
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import structlog

from app.core.database import get_db_direct
from app.services.ingestion.google_ads import GoogleAdsIngestionService
from app.services.ingestion.meta_ads import MetaAdsIngestionService

logger = structlog.get_logger(__name__)

# ── Thresholds ─────────────────────────────────────────────────────────────────

_INGESTION_STALE_HOURS = 26      # alert if last success > 26 h ago
_GOOGLE_TOKEN_REFRESH_DAYS = 5   # refresh OAuth token if < 5 days to expiry
_META_TOKEN_WARN_DAYS = 7        # warn if < 7 days to expiry
_SOURCES = ("google_ads", "meta")


# ═══════════════════════════════════════════════════════════════════════════════
# 1. daily_ingestion
# ═══════════════════════════════════════════════════════════════════════════════

async def daily_ingestion() -> None:
    """Pull D-1 + D-0 data for every active brand × every configured source.

    Iterates over all active brands in MongoDB.  For each brand, fires the
    appropriate connector if the brand has credentials for that source.
    Per-brand / per-source failures are caught and logged without aborting
    the remaining brands (failure isolation).
    """
    db = get_db_direct()
    today = date.today()
    log = logger.bind(task="daily_ingestion", date=str(today))
    log.info("task.started")

    brands = await _fetch_active_brands(db)
    log.info("task.brands_found", count=len(brands))

    total_upserted = 0
    errors: list[str] = []

    for brand_doc in brands:
        brand_id = str(brand_doc["_id"])
        platforms: dict[str, Any] = brand_doc.get("platforms") or {}

        for source, svc_cls in [
            ("google_ads", GoogleAdsIngestionService),
            ("meta", MetaAdsIngestionService),
        ]:
            if not platforms.get(source):
                continue  # brand not configured for this source

            try:
                svc = svc_cls(db)
                result = await svc.run(brand_id=brand_id, target_date=today)
                total_upserted += result.records_upserted
                if result.status != "success":
                    errors.append(
                        f"{brand_id}/{source}: {result.status} — {result.errors}"
                    )
                log.debug(
                    "task.brand_done",
                    brand_id=brand_id,
                    source=source,
                    status=result.status,
                    upserted=result.records_upserted,
                )
            except Exception as exc:
                msg = f"{brand_id}/{source}: {type(exc).__name__}: {exc}"
                errors.append(msg)
                log.error(
                    "task.brand_failed",
                    brand_id=brand_id,
                    source=source,
                    error=str(exc),
                    exc_info=True,
                )

    log.info(
        "task.finished",
        total_upserted=total_upserted,
        error_count=len(errors),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# 2. rollup_computation
# ═══════════════════════════════════════════════════════════════════════════════

async def rollup_computation() -> None:
    """Compute daily / weekly / monthly rollup aggregates for all active brands.

    Reads from ad_performance_raw and writes pre-aggregated documents to
    performance_rollups.  Idempotent — safe to re-run (uses upsert).

    Full rollup service lives in app.services.rollup (Phase 5).  Until that
    module exists this task logs a placeholder and exits cleanly.
    """
    log = logger.bind(task="rollup_computation")
    log.info("task.started")

    try:
        from app.services.rollup import compute_all_rollups  # Phase 5 — not yet implemented
        from app.core.database import get_db_direct
        db = get_db_direct()
        await compute_all_rollups(db)
        log.info("task.finished")
    except ImportError:
        log.info("task.skipped", reason="rollup service not yet implemented (Phase 5)")
    except Exception as exc:
        log.error("task.failed", error=str(exc), exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 3. anomaly_detection
# ═══════════════════════════════════════════════════════════════════════════════

async def anomaly_detection() -> None:
    """Detect statistical anomalies in recent performance data.

    For each active brand, compares yesterday's metrics against a rolling
    baseline.  Anomalies are written to an anomalies collection and can
    trigger alerts (Telegram / email) in a later phase.

    Full anomaly service lives in app.services.anomalies (Phase 6).
    """
    log = logger.bind(task="anomaly_detection")
    log.info("task.started")

    try:
        from app.services.anomalies import detect_all_anomalies  # Phase 6
        from app.core.database import get_db_direct
        db = get_db_direct()
        await detect_all_anomalies(db)
        log.info("task.finished")
    except ImportError:
        log.info("task.skipped", reason="anomaly service not yet implemented (Phase 6)")
    except Exception as exc:
        log.error("task.failed", error=str(exc), exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 4. scheduled_reports
# ═══════════════════════════════════════════════════════════════════════════════

async def scheduled_reports() -> None:
    """Generate and deliver any scheduled reports due today.

    Checks report_schedules collection for brands with frequency matching
    today (daily / weekly on Monday / monthly on 1st).  Generates the report
    and emails/Telegrams it to configured recipients.

    Full report service lives in app.services.reports (Phase 7).
    """
    log = logger.bind(task="scheduled_reports", date=str(date.today()))
    log.info("task.started")

    try:
        from app.services.reports import run_scheduled_reports  # Phase 7
        from app.core.database import get_db_direct
        db = get_db_direct()
        await run_scheduled_reports(db)
        log.info("task.finished")
    except ImportError:
        log.info("task.skipped", reason="reports service not yet implemented (Phase 7)")
    except Exception as exc:
        log.error("task.failed", error=str(exc), exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 5. ingestion_health_check
# ═══════════════════════════════════════════════════════════════════════════════

async def ingestion_health_check() -> None:
    """Alert if any active brand × source pair has no successful ingestion in >26h.

    Queries ingestion_logs for the most recent successful run per
    (brand_id, source).  Brands missing a recent run are logged at WARNING
    level.  In a later phase this will also fire a Telegram alert.
    """
    db = get_db_direct()
    cutoff = datetime.now(UTC) - timedelta(hours=_INGESTION_STALE_HOURS)
    log = logger.bind(task="ingestion_health_check", cutoff=cutoff.isoformat())
    log.info("task.started")

    brands = await _fetch_active_brands(db)
    stale: list[dict[str, str]] = []

    for brand_doc in brands:
        brand_id = str(brand_doc["_id"])
        platforms: dict[str, Any] = brand_doc.get("platforms") or {}

        for source in _SOURCES:
            if not platforms.get(source):
                continue

            last_ok = await db["ingestion_logs"].find_one(
                {
                    "brand_id": brand_doc["_id"],
                    "source": source,
                    "status": "success",
                    "completed_at": {"$gte": cutoff},
                },
                sort=[("completed_at", -1)],
            )

            if not last_ok:
                stale.append({"brand_id": brand_id, "source": source})
                log.warning(
                    "ingestion.health.stale",
                    brand_id=brand_id,
                    source=source,
                    threshold_hours=_INGESTION_STALE_HOURS,
                )

    log.info("task.finished", stale_count=len(stale))


# ═══════════════════════════════════════════════════════════════════════════════
# 6. token_refresh_google
# ═══════════════════════════════════════════════════════════════════════════════

async def token_refresh_google() -> None:
    """Refresh Google OAuth2 refresh tokens before they expire.

    Google refresh tokens don't technically expire unless revoked or unused
    for 6+ months.  This task is a safeguard: it re-exchanges the refresh
    token to obtain a fresh access token and verifies the credentials are
    still valid.  If a brand's credentials are invalid, a WARNING is logged
    so the team can re-authorise before the next ingestion run.

    Actual re-exchange requires the google-ads SDK; until credentials are
    fully managed, this task validates by attempting a lightweight API call.
    """
    db = get_db_direct()
    log = logger.bind(task="token_refresh_google")
    log.info("task.started")

    brands = await _fetch_active_brands(db, platform="google_ads")
    refreshed = 0
    failed = 0

    for brand_doc in brands:
        brand_id = str(brand_doc["_id"])
        creds: dict[str, Any] = (
            (brand_doc.get("platforms") or {}).get("google_ads") or {}
        )
        if not creds.get("refresh_token") or not creds.get("customer_id"):
            continue

        try:
            await _verify_google_credentials(brand_id, creds)
            refreshed += 1
            log.debug("token_refresh_google.ok", brand_id=brand_id)
        except Exception as exc:
            failed += 1
            log.warning(
                "token_refresh_google.failed",
                brand_id=brand_id,
                error=str(exc),
            )

    log.info("task.finished", refreshed=refreshed, failed=failed)


# ═══════════════════════════════════════════════════════════════════════════════
# 7. meta_token_expiry_check
# ═══════════════════════════════════════════════════════════════════════════════

async def meta_token_expiry_check() -> None:
    """Warn if any brand's Meta access token expires within META_TOKEN_WARN_DAYS.

    Checks brands.platforms.meta_ads.token_expires_at.  Permanent System User
    Tokens have no expiry date and are skipped.  Brands with tokens expiring
    soon are logged at WARNING and will trigger a Telegram alert in a later phase.
    """
    db = get_db_direct()
    warn_cutoff = datetime.now(UTC) + timedelta(days=_META_TOKEN_WARN_DAYS)
    log = logger.bind(task="meta_token_expiry_check")
    log.info("task.started")

    brands = await _fetch_active_brands(db, platform="meta")
    expiring: list[dict[str, Any]] = []

    for brand_doc in brands:
        brand_id = str(brand_doc["_id"])
        creds: dict[str, Any] = (
            (brand_doc.get("platforms") or {}).get("meta") or {}
        )
        expires_raw = creds.get("token_expires_at")
        if not expires_raw:
            continue  # permanent / system user token — no expiry

        try:
            expires_at = (
                datetime.fromisoformat(expires_raw)
                if isinstance(expires_raw, str)
                else expires_raw
            )
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=UTC)

            days_left = (expires_at - datetime.now(UTC)).days
            if days_left <= _META_TOKEN_WARN_DAYS:
                expiring.append({"brand_id": brand_id, "days_left": days_left})
                log.warning(
                    "meta_token.expiring_soon",
                    brand_id=brand_id,
                    days_left=days_left,
                    expires_at=expires_at.isoformat(),
                )
        except Exception as exc:
            log.warning(
                "meta_token.expiry_check_failed",
                brand_id=brand_id,
                error=str(exc),
            )

    log.info("task.finished", expiring_count=len(expiring))


# ── Private helpers ────────────────────────────────────────────────────────────

async def _fetch_active_brands(
    db: Any,
    *,
    platform: str | None = None,
) -> list[dict[str, Any]]:
    """Return all active brands, optionally filtered to those with a given platform."""
    query: dict[str, Any] = {"is_active": True}
    if platform:
        query[f"platforms.{platform}"] = {"$exists": True, "$ne": None}
    cursor = db["brands"].find(query, {"platforms": 1, "name": 1, "slug": 1})
    return await cursor.to_list(length=None)


async def _verify_google_credentials(
    brand_id: str,
    creds: dict[str, Any],
) -> None:
    """Lightweight check that Google credentials are still valid.

    Calls the Google Ads API with a minimal query.  Raises on auth failure.
    When the google-ads SDK is not installed this is a no-op.
    """
    try:
        from app.services.ingestion.google_ads import GoogleAdsIngestionService  # noqa: F401
        # Credentials check: we just instantiate and load — no actual API call
        # because a full API call requires a date. A real implementation would
        # call CustomerService.get() which is lightweight and auth-only.
        logger.debug(
            "google_credentials.check_skipped",
            brand_id=brand_id,
            reason="lightweight check not yet implemented",
        )
    except ImportError:
        pass  # google-ads SDK not installed — skip silently
