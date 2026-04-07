"""
Performance router

  GET /brands/{id}/performance/daily        — raw rows, filterable by date/source/campaign
  GET /brands/{id}/performance/rollup       — pre-computed period rollups
  GET /brands/{id}/performance/summary      — KPI card (spend, ROAS, CPL, CTR)
  GET /brands/{id}/performance/top-campaigns — top N campaigns by metric
  GET /brands/{id}/performance/trend        — daily time series for charting
  GET /brands/{id}/performance/attribution  — spend/metrics split by source

All endpoints are brand-scoped (BrandAccess dependency) and require auth.
Date params default to the last 30 days when omitted.
Unknown source values return empty results (not 422) to stay consistent
with the campaigns router convention.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Annotated, Literal

import structlog
from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api.v1.schemas.performance import (
    AttributionResponse,
    AttributionSource,
    DailyResponse,
    DailyRow,
    KpiSummary,
    RollupItem,
    RollupResponse,
    TopCampaignItem,
    TopCampaignsResponse,
    TrendPoint,
    TrendResponse,
)
from app.core.database import get_database
from app.middleware.brand_scope import BrandAccess
from app.repositories.performance import PerformanceRepository
from app.repositories.rollups import RollupsRepository

router = APIRouter(prefix="/brands", tags=["performance"])
logger = structlog.get_logger(__name__)

# ── Valid source literals ──────────────────────────────────────────────────────

_VALID_SOURCES = {"google_ads", "meta", "interakt", "manual"}
_VALID_METRICS = {"spend_paise", "roas", "cpl_paise", "ctr", "conversions", "leads", "impressions", "clicks"}


def _default_date_from() -> date:
    return date.today() - timedelta(days=29)  # last 30 days inclusive


# ── GET /brands/{brand_id}/performance/daily ──────────────────────────────────

@router.get("/{brand_id}/performance/daily", response_model=DailyResponse)
async def get_daily(
    brand_id: Annotated[str, Depends(BrandAccess)],
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
    date_from: date = Query(default_factory=_default_date_from, description="Inclusive start date (YYYY-MM-DD)"),
    date_to: date = Query(default_factory=date.today, description="Inclusive end date (YYYY-MM-DD)"),
    source: str | None = Query(default=None, description="Filter by platform source"),
    campaign_id: str | None = Query(default=None, description="Filter by campaign ObjectId"),
) -> DailyResponse:
    """Return raw daily performance rows with optional filters.

    Rows are sorted by date descending.  Unknown source values return an
    empty list rather than a 422 error.
    """
    if source and source not in _VALID_SOURCES:
        return DailyResponse(
            items=[], total=0,
            date_from=date_from, date_to=date_to,
            source=source, campaign_id=campaign_id,
        )

    repo = PerformanceRepository(db, brand_id)
    docs = await repo.find_by_date_range(
        date_from, date_to,
        source=source,
        campaign_id=campaign_id,
    )
    items = [DailyRow.from_doc(d) for d in docs]
    return DailyResponse(
        items=items,
        total=len(items),
        date_from=date_from,
        date_to=date_to,
        source=source,
        campaign_id=campaign_id,
    )


# ── GET /brands/{brand_id}/performance/rollup ─────────────────────────────────

@router.get("/{brand_id}/performance/rollup", response_model=RollupResponse)
async def get_rollup(
    brand_id: Annotated[str, Depends(BrandAccess)],
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
    period_type: Literal["daily", "weekly", "monthly"] = Query(
        default="daily", description="Aggregation period"
    ),
    date_from: date = Query(default_factory=_default_date_from),
    date_to: date = Query(default_factory=date.today),
    source: str | None = Query(default=None, description="Filter by source; omit for 'all'"),
) -> RollupResponse:
    """Return pre-computed rollup aggregates.

    Rollups are written by the nightly rollup_computation worker task.
    If no rollup data exists for the requested range, an empty list is returned
    (not an error — the caller should fall back to the /daily endpoint or wait
    for the next worker run).
    """
    from datetime import datetime, timezone

    repo = RollupsRepository(db, brand_id)

    dt_from = datetime.combine(date_from, datetime.min.time(), tzinfo=timezone.utc)
    dt_to = datetime.combine(date_to, datetime.min.time(), tzinfo=timezone.utc)

    # "all" is the special cross-platform rollup; a specific source filters to that source
    effective_source: str | None = source if source and source in _VALID_SOURCES else (
        "all" if source is None else None
    )
    if effective_source is None:
        # Unknown source value — return empty
        return RollupResponse(
            items=[], total=0,
            period_type=period_type,
            date_from=date_from, date_to=date_to,
            source=source,
        )

    docs = await repo.find_by_period(period_type, dt_from, dt_to, source=effective_source)
    items = [RollupItem.from_doc(d) for d in docs]
    return RollupResponse(
        items=items,
        total=len(items),
        period_type=period_type,
        date_from=date_from,
        date_to=date_to,
        source=source,
    )


# ── GET /brands/{brand_id}/performance/summary ────────────────────────────────

@router.get("/{brand_id}/performance/summary", response_model=KpiSummary)
async def get_summary(
    brand_id: Annotated[str, Depends(BrandAccess)],
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
    date_from: date = Query(default_factory=_default_date_from),
    date_to: date = Query(default_factory=date.today),
    source: str | None = Query(default=None),
) -> KpiSummary:
    """Return a single aggregate KPI card for the date range.

    Computes: total spend, impressions, clicks, reach, leads, conversions,
    conversion value, and derived KPIs (ROAS, CTR, CPC, CPM, CPL).
    """
    if source and source not in _VALID_SOURCES:
        return KpiSummary(date_from=date_from, date_to=date_to, source=source)

    repo = PerformanceRepository(db, brand_id)
    raw = await repo.get_kpi_summary(date_from, date_to, source=source)

    spend = raw.get("total_spend_paise") or 0
    impressions = raw.get("total_impressions") or 0
    clicks = raw.get("total_clicks") or 0
    leads = raw.get("total_leads") or 0
    conv_value = raw.get("total_conversion_value_paise") or 0

    return KpiSummary(
        date_from=date_from,
        date_to=date_to,
        source=source,
        total_spend_paise=spend,
        total_impressions=impressions,
        total_clicks=clicks,
        total_reach=raw.get("total_reach") or 0,
        total_leads=leads,
        total_conversions=raw.get("total_conversions") or 0,
        total_conversion_value_paise=conv_value,
        roas=conv_value / spend if spend else None,
        ctr=clicks / impressions if impressions else None,
        cpc_paise=spend // clicks if clicks else None,
        cpm_paise=int(spend * 1000 // impressions) if impressions else None,
        cpl_paise=spend // leads if leads else None,
        days_with_data=raw.get("days_with_data") or 0,
    )


# ── GET /brands/{brand_id}/performance/top-campaigns ─────────────────────────

@router.get("/{brand_id}/performance/top-campaigns", response_model=TopCampaignsResponse)
async def get_top_campaigns(
    brand_id: Annotated[str, Depends(BrandAccess)],
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
    date_from: date = Query(default_factory=_default_date_from),
    date_to: date = Query(default_factory=date.today),
    metric: str = Query(default="spend_paise", description="Ranking metric"),
    limit: int = Query(default=10, ge=1, le=50, description="Number of campaigns to return"),
    source: str | None = Query(default=None),
) -> TopCampaignsResponse:
    """Return the top N campaigns ranked by the given metric.

    Valid metrics: spend_paise, roas, cpl_paise, ctr, conversions,
    leads, impressions, clicks.  Unrecognised metric defaults to spend_paise.
    """
    if source and source not in _VALID_SOURCES:
        return TopCampaignsResponse(
            items=[], metric=metric, limit=limit,
            date_from=date_from, date_to=date_to, source=source,
        )

    effective_metric = metric if metric in _VALID_METRICS else "spend_paise"

    repo = PerformanceRepository(db, brand_id)
    docs = await repo.get_top_campaigns(
        date_from, date_to,
        metric=effective_metric,
        limit=limit,
        source=source,
    )
    items = [TopCampaignItem.from_doc(d) for d in docs]
    return TopCampaignsResponse(
        items=items,
        metric=effective_metric,
        limit=limit,
        date_from=date_from,
        date_to=date_to,
        source=source,
    )


# ── GET /brands/{brand_id}/performance/trend ─────────────────────────────────

@router.get("/{brand_id}/performance/trend", response_model=TrendResponse)
async def get_trend(
    brand_id: Annotated[str, Depends(BrandAccess)],
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
    date_from: date = Query(default_factory=_default_date_from),
    date_to: date = Query(default_factory=date.today),
    source: str | None = Query(default=None, description="Omit to aggregate all sources"),
) -> TrendResponse:
    """Return a daily time series for charting.

    When `source` is omitted the response contains one point per day across
    all sources (totals).  When `source` is specified the response contains
    one point per day for that source only.

    Sorted by date ascending.
    """
    if source and source not in _VALID_SOURCES:
        return TrendResponse(
            points=[], date_from=date_from, date_to=date_to, source=source
        )

    repo = PerformanceRepository(db, brand_id)
    docs = await repo.get_daily_summary(date_from, date_to, source=source)
    points = [TrendPoint.from_doc(d) for d in docs]
    return TrendResponse(
        points=points,
        date_from=date_from,
        date_to=date_to,
        source=source,
    )


# ── GET /brands/{brand_id}/performance/attribution ────────────────────────────

@router.get("/{brand_id}/performance/attribution", response_model=AttributionResponse)
async def get_attribution(
    brand_id: Annotated[str, Depends(BrandAccess)],
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
    date_from: date = Query(default_factory=_default_date_from),
    date_to: date = Query(default_factory=date.today),
) -> AttributionResponse:
    """Return spend and key metrics broken down by source (channel).

    Each source entry includes its percentage share of total spend
    (`spend_share_pct`) so callers can build attribution/pie charts directly.
    Sources with no data in the period are excluded from the response.
    """
    repo = PerformanceRepository(db, brand_id)
    docs = await repo.get_source_attribution(date_from, date_to)

    total_spend = sum(d.get("total_spend_paise") or 0 for d in docs)
    sources = [AttributionSource.from_doc(d, total_spend) for d in docs]

    return AttributionResponse(
        sources=sources,
        total_spend_paise=total_spend,
        date_from=date_from,
        date_to=date_to,
    )
