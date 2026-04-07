"""
Performance API — request/response schemas.

Monetary fields are always in INR paise (int) to stay consistent with the
database layer.  Callers that need rupees divide by 100.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field, model_validator


# ── Shared ─────────────────────────────────────────────────────────────────────

class _PaiseMetrics(BaseModel):
    """Mixin with the full set of numeric performance metrics."""
    spend_paise: int = 0
    impressions: int = 0
    clicks: int = 0
    reach: int = 0
    leads: int = 0
    conversions: int = 0
    conversion_value_paise: int = 0
    # Derived
    ctr: float | None = None
    cpc_paise: int | None = None
    cpm_paise: int | None = None
    cpl_paise: int | None = None
    roas: float | None = None


# ── GET /performance/daily ─────────────────────────────────────────────────────

class DailyRow(_PaiseMetrics):
    id: str = Field(alias="_id_str")
    brand_id: str
    campaign_id: str
    source: str
    date: datetime
    ingested_at: datetime
    ingestion_run_id: str

    model_config = {"populate_by_name": True}

    @classmethod
    def from_doc(cls, doc: dict[str, Any]) -> "DailyRow":
        return cls(
            _id_str=str(doc["_id"]),
            brand_id=str(doc["brand_id"]),
            campaign_id=str(doc["campaign_id"]),
            source=doc["source"],
            date=doc["date"],
            ingested_at=doc.get("ingested_at", doc["date"]),
            ingestion_run_id=doc.get("ingestion_run_id", ""),
            spend_paise=doc.get("spend_paise") or 0,
            impressions=doc.get("impressions") or 0,
            clicks=doc.get("clicks") or 0,
            reach=doc.get("reach") or 0,
            leads=doc.get("leads") or 0,
            conversions=doc.get("conversions") or 0,
            conversion_value_paise=doc.get("conversion_value_paise") or 0,
            ctr=doc.get("ctr"),
            cpc_paise=doc.get("cpc_paise"),
            cpm_paise=doc.get("cpm_paise"),
            cpl_paise=doc.get("cpl_paise"),
            roas=doc.get("roas"),
        )


class DailyResponse(BaseModel):
    items: list[DailyRow]
    total: int
    date_from: date
    date_to: date
    source: str | None
    campaign_id: str | None


# ── GET /performance/rollup ────────────────────────────────────────────────────

class RollupItem(BaseModel):
    period_type: str
    period_start: datetime
    period_end: datetime
    source: str
    total_spend_paise: int = 0
    total_impressions: int = 0
    total_clicks: int = 0
    total_leads: int = 0
    total_conversions: int = 0
    avg_roas: float | None = None
    avg_cpl_paise: int | None = None
    avg_ctr: float | None = None
    budget_utilization: float | None = None
    computed_at: datetime
    is_partial: bool = False

    @classmethod
    def from_doc(cls, doc: dict[str, Any]) -> "RollupItem":
        return cls(
            period_type=doc["period_type"],
            period_start=doc["period_start"],
            period_end=doc["period_end"],
            source=doc.get("source", "all"),
            total_spend_paise=doc.get("total_spend_paise") or 0,
            total_impressions=doc.get("total_impressions") or 0,
            total_clicks=doc.get("total_clicks") or 0,
            total_leads=doc.get("total_leads") or 0,
            total_conversions=doc.get("total_conversions") or 0,
            avg_roas=doc.get("avg_roas"),
            avg_cpl_paise=doc.get("avg_cpl_paise"),
            avg_ctr=doc.get("avg_ctr"),
            budget_utilization=doc.get("budget_utilization"),
            computed_at=doc.get("computed_at", doc["period_start"]),
            is_partial=doc.get("is_partial", False),
        )


class RollupResponse(BaseModel):
    items: list[RollupItem]
    total: int
    period_type: str
    date_from: date
    date_to: date
    source: str | None


# ── GET /performance/summary ───────────────────────────────────────────────────

class KpiSummary(BaseModel):
    """Aggregate KPI card for the requested date range."""
    date_from: date
    date_to: date
    source: str | None

    # Totals
    total_spend_paise: int = 0
    total_impressions: int = 0
    total_clicks: int = 0
    total_reach: int = 0
    total_leads: int = 0
    total_conversions: int = 0
    total_conversion_value_paise: int = 0

    # Computed KPIs (None when denominator is zero)
    roas: float | None = None
    ctr: float | None = None
    cpc_paise: int | None = None
    cpm_paise: int | None = None
    cpl_paise: int | None = None

    days_with_data: int = 0


# ── GET /performance/top-campaigns ────────────────────────────────────────────

class TopCampaignItem(BaseModel):
    campaign_id: str
    campaign_name: str | None = None
    source: str
    total_spend_paise: int = 0
    total_impressions: int = 0
    total_clicks: int = 0
    total_leads: int = 0
    total_conversions: int = 0
    total_conversion_value_paise: int = 0
    roas: float | None = None
    ctr: float | None = None
    cpc_paise: int | None = None
    cpl_paise: int | None = None

    @classmethod
    def from_doc(cls, doc: dict[str, Any]) -> "TopCampaignItem":
        spend = doc.get("total_spend_paise") or 0
        clicks = doc.get("total_clicks") or 0
        leads = doc.get("total_leads") or 0
        impressions = doc.get("total_impressions") or 0
        conv_value = doc.get("total_conversion_value_paise") or 0

        roas = conv_value / spend if spend else None
        ctr = clicks / impressions if impressions else None
        cpc = spend // clicks if clicks else None
        cpl = spend // leads if leads else None

        return cls(
            campaign_id=str(doc["campaign_id"]),
            campaign_name=doc.get("campaign_name"),
            source=doc.get("source", ""),
            total_spend_paise=spend,
            total_impressions=impressions,
            total_clicks=clicks,
            total_leads=leads,
            total_conversions=doc.get("total_conversions") or 0,
            total_conversion_value_paise=conv_value,
            roas=roas,
            ctr=ctr,
            cpc_paise=cpc,
            cpl_paise=cpl,
        )


class TopCampaignsResponse(BaseModel):
    items: list[TopCampaignItem]
    metric: str
    limit: int
    date_from: date
    date_to: date
    source: str | None


# ── GET /performance/trend ────────────────────────────────────────────────────

class TrendPoint(BaseModel):
    date: datetime
    source: str | None = None
    total_spend_paise: int = 0
    total_impressions: int = 0
    total_clicks: int = 0
    total_leads: int = 0
    total_conversions: int = 0
    avg_roas: float | None = None
    avg_ctr: float | None = None
    record_count: int = 0

    @classmethod
    def from_doc(cls, doc: dict[str, Any]) -> "TrendPoint":
        return cls(
            date=doc["date"],
            source=doc.get("source"),
            total_spend_paise=doc.get("total_spend_paise") or 0,
            total_impressions=doc.get("total_impressions") or 0,
            total_clicks=doc.get("total_clicks") or 0,
            total_leads=doc.get("total_leads") or 0,
            total_conversions=doc.get("total_conversions") or 0,
            avg_roas=doc.get("avg_roas"),
            avg_ctr=doc.get("avg_ctr"),
            record_count=doc.get("record_count") or 0,
        )


class TrendResponse(BaseModel):
    points: list[TrendPoint]
    date_from: date
    date_to: date
    source: str | None


# ── GET /performance/attribution ──────────────────────────────────────────────

class AttributionSource(BaseModel):
    source: str
    total_spend_paise: int = 0
    total_impressions: int = 0
    total_clicks: int = 0
    total_leads: int = 0
    total_conversions: int = 0
    total_conversion_value_paise: int = 0
    spend_share_pct: float = 0.0   # percentage of total spend
    roas: float | None = None
    ctr: float | None = None
    cpl_paise: int | None = None

    @classmethod
    def from_doc(cls, doc: dict[str, Any], total_spend: int) -> "AttributionSource":
        spend = doc.get("total_spend_paise") or 0
        impressions = doc.get("total_impressions") or 0
        clicks = doc.get("total_clicks") or 0
        leads = doc.get("total_leads") or 0
        conv_value = doc.get("total_conversion_value_paise") or 0

        return cls(
            source=doc["source"],
            total_spend_paise=spend,
            total_impressions=impressions,
            total_clicks=clicks,
            total_leads=leads,
            total_conversions=doc.get("total_conversions") or 0,
            total_conversion_value_paise=conv_value,
            spend_share_pct=round(spend / total_spend * 100, 2) if total_spend else 0.0,
            roas=conv_value / spend if spend else None,
            ctr=clicks / impressions if impressions else None,
            cpl_paise=spend // leads if leads else None,
        )


class AttributionResponse(BaseModel):
    sources: list[AttributionSource]
    total_spend_paise: int
    date_from: date
    date_to: date
