"""
Rollup Computation Service

Reads from `ad_performance_raw` and writes pre-aggregated period summaries
to `performance_rollups`.  Three period types are computed for each brand:

  daily   — one document per (brand, source, calendar day)
  weekly  — one document per (brand, source, ISO week  Mon–Sun)
  monthly — one document per (brand, source, calendar month)

Each period also gets a cross-platform "all" document that totals every
source together, so dashboards can query a single document instead of
fan-out-and-sum.

Public interface
────────────────
  compute_all_rollups(db, *, target_date=None)
      Called by the APScheduler task.  Iterates all active brands and
      delegates to RollupService.compute_for_brand().

  RollupService
      Per-brand, per-period orchestrator.  Safe to call directly in tests
      or one-off scripts.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.repositories.performance import PerformanceRepository
from app.repositories.rollups import RollupsRepository

logger = structlog.get_logger(__name__)

Doc = dict[str, Any]


# ── Date-range helpers ─────────────────────────────────────────────────────────

def _dt(d: date) -> datetime:
    """Calendar date → UTC midnight datetime."""
    return datetime.combine(d, datetime.min.time(), tzinfo=UTC)


def _week_range(d: date) -> tuple[date, date]:
    """Return (Monday, Sunday) of the ISO week containing d."""
    monday = d - timedelta(days=d.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def _month_range(d: date) -> tuple[date, date]:
    """Return (first, last) calendar day of the month containing d."""
    first = d.replace(day=1)
    last_day = calendar.monthrange(d.year, d.month)[1]
    last = d.replace(day=last_day)
    return first, last


def _is_partial(date_to: date) -> bool:
    """True if the period end is today or in the future (data still arriving)."""
    return date_to >= date.today()


# ── Result dataclass ───────────────────────────────────────────────────────────

@dataclass
class RollupResult:
    brand_id: str
    target_date: date
    periods_computed: int = 0   # rollup docs written (upserted)
    periods_skipped: int = 0    # periods with no source data
    errors: list[str] = field(default_factory=list)


# ── Core service ───────────────────────────────────────────────────────────────

class RollupService:
    """Computes daily / weekly / monthly rollups for a single brand."""

    def __init__(self, db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
        self._db = db

    async def compute_for_brand(
        self,
        brand_id: str,
        *,
        target_date: date | None = None,
    ) -> RollupResult:
        """Compute all three period types for target_date (defaults to today).

        Idempotent: each call upserts into performance_rollups using the
        (brand_id, period_type, period_start, source) natural key.

        Returns a RollupResult summary.
        """
        if target_date is None:
            target_date = date.today()

        result = RollupResult(brand_id=brand_id, target_date=target_date)
        perf_repo = PerformanceRepository(self._db, brand_id)
        rollup_repo = RollupsRepository(self._db, brand_id)
        log = logger.bind(brand_id=brand_id, target_date=str(target_date))

        # ── Daily ─────────────────────────────────────────────────────────────
        n, s = await self._compute_period(
            perf_repo, rollup_repo,
            period_type="daily",
            date_from=target_date,
            date_to=target_date,
            is_partial=_is_partial(target_date),
        )
        result.periods_computed += n
        result.periods_skipped += s
        log.debug("rollup.daily_done", written=n, skipped=s)

        # ── Weekly (ISO Mon–Sun) ───────────────────────────────────────────────
        w_from, w_to = _week_range(target_date)
        n, s = await self._compute_period(
            perf_repo, rollup_repo,
            period_type="weekly",
            date_from=w_from,
            date_to=w_to,
            is_partial=_is_partial(w_to),
        )
        result.periods_computed += n
        result.periods_skipped += s
        log.debug("rollup.weekly_done", written=n, skipped=s,
                  week_from=str(w_from), week_to=str(w_to))

        # ── Monthly ───────────────────────────────────────────────────────────
        m_from, m_to = _month_range(target_date)
        n, s = await self._compute_period(
            perf_repo, rollup_repo,
            period_type="monthly",
            date_from=m_from,
            date_to=m_to,
            is_partial=_is_partial(m_to),
        )
        result.periods_computed += n
        result.periods_skipped += s
        log.debug("rollup.monthly_done", written=n, skipped=s,
                  month_from=str(m_from), month_to=str(m_to))

        log.info(
            "rollup.brand_done",
            periods_computed=result.periods_computed,
            periods_skipped=result.periods_skipped,
        )
        return result

    # ── Private helpers ────────────────────────────────────────────────────────

    async def _compute_period(
        self,
        perf_repo: PerformanceRepository,
        rollup_repo: RollupsRepository,
        period_type: str,
        date_from: date,
        date_to: date,
        *,
        is_partial: bool,
    ) -> tuple[int, int]:
        """Aggregate one period and upsert the results.

        Returns (docs_written, periods_skipped).
        A period with no source data contributes 0 docs and 1 skip.
        """
        agg_rows = await perf_repo.get_rollup_aggregates(date_from, date_to)
        if not agg_rows:
            return 0, 1

        dt_from = _dt(date_from)
        dt_to = _dt(date_to)
        written = 0

        for row in agg_rows:
            metrics = _build_metrics(row)
            await rollup_repo.upsert(
                period_type=period_type,  # type: ignore[arg-type]
                period_start=dt_from,
                period_end=dt_to,
                source=row["source"],
                metrics=metrics,
                is_partial=is_partial,
            )
            written += 1

        return written, 0


# ── Metrics builder ────────────────────────────────────────────────────────────

def _build_metrics(row: Doc) -> Doc:
    """Convert a get_rollup_aggregates row to a metrics dict for RollupsRepository.

    avg_cpl_paise is stored as int (truncated).
    budget_utilization is None until campaign budget data is available.
    """
    cpl_raw = row.get("avg_cpl_paise")
    return {
        "total_spend_paise":            row.get("total_spend_paise") or 0,
        "total_impressions":            row.get("total_impressions") or 0,
        "total_clicks":                 row.get("total_clicks") or 0,
        "total_leads":                  row.get("total_leads") or 0,
        "total_conversions":            row.get("total_conversions") or 0,
        "total_conversion_value_paise": row.get("total_conversion_value_paise") or 0,
        "avg_roas":                     row.get("avg_roas"),
        "avg_cpl_paise":                int(cpl_raw) if cpl_raw is not None else None,
        "avg_ctr":                      row.get("avg_ctr"),
        "budget_utilization":           None,
    }


# ── Top-level entry point (called by tasks.py) ─────────────────────────────────

async def compute_all_rollups(
    db: AsyncIOMotorDatabase,  # type: ignore[type-arg]
    *,
    target_date: date | None = None,
) -> None:
    """Compute rollups for every active brand.

    Iterates brands with ``is_active: True`` in MongoDB.  Per-brand
    failures are isolated — one bad brand does not abort the rest.

    Args:
        db:          Motor database instance.
        target_date: Date to compute rollups for (defaults to today).
                     Pass an explicit date for backfill runs.
    """
    if target_date is None:
        target_date = date.today()

    log = logger.bind(task="compute_all_rollups", target_date=str(target_date))
    log.info("rollup.started")

    brands = await _fetch_active_brands(db)
    svc = RollupService(db)

    total_computed = 0
    total_skipped = 0
    errors: list[str] = []

    for brand_doc in brands:
        brand_id = str(brand_doc["_id"])
        try:
            r = await svc.compute_for_brand(brand_id, target_date=target_date)
            total_computed += r.periods_computed
            total_skipped += r.periods_skipped
        except Exception as exc:
            msg = f"{brand_id}: {type(exc).__name__}: {exc}"
            errors.append(msg)
            log.error(
                "rollup.brand_failed",
                brand_id=brand_id,
                error=str(exc),
                exc_info=True,
            )

    log.info(
        "rollup.finished",
        brands=len(brands),
        total_computed=total_computed,
        total_skipped=total_skipped,
        error_count=len(errors),
    )


# ── Private helpers ────────────────────────────────────────────────────────────

async def _fetch_active_brands(db: AsyncIOMotorDatabase) -> list[Doc]:  # type: ignore[type-arg]
    cursor = db["brands"].find({"is_active": True}, {"_id": 1, "slug": 1})
    return await cursor.to_list(length=None)
