"""
PerformanceRepository  (collection: ad_performance_raw)

Brand-scoped. One document per (brand_id, source, campaign_id, date) —
the unique natural key that makes ingestion idempotent.

All monetary values are stored in INR paise (Int64) as per the schema.
Date values represent IST midnight stored as UTC (conversion is the
ingestion layer's responsibility).

Key methods
───────────
  upsert()              — idempotent ingest write
  find_by_date_range()  — raw row query with optional source / campaign filter
  delete_by_run_id()    — rollback a bad ingestion run atomically
  get_daily_summary()   — aggregation: per-day totals across sources
  get_campaign_summary()— aggregation: per-campaign totals for a date range
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any, Literal

import structlog
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.repositories.base import BrandScopedRepository

logger = structlog.get_logger(__name__)

Doc = dict[str, Any]
Source = Literal["google_ads", "meta", "interakt", "manual"]


def _day_to_utc(d: date) -> datetime:
    """Convert a date to UTC midnight datetime (IST midnight = UTC 18:30 prev day
    but we store as UTC midnight for simplicity; ingestion layer handles IST)."""
    return datetime.combine(d, time.min, tzinfo=timezone.utc)


class PerformanceRepository(BrandScopedRepository):
    def __init__(self, db: AsyncIOMotorDatabase, brand_id: str) -> None:  # type: ignore[type-arg]
        super().__init__(db["ad_performance_raw"], brand_id)

    # ── Idempotent upsert (ingestion path) ────────────────────────────────────

    async def upsert(
        self,
        source: str,
        campaign_id: str,
        record_date: date,
        metrics: Doc,
        *,
        ingestion_run_id: str,
    ) -> str:
        """Write or update a single performance record.

        Natural key: (brand_id, source, campaign_id, date).
        `metrics` should contain the numeric fields from the platform
        (spend_paise, impressions, clicks, …).

        Returns the document's string _id.
        """
        now = datetime.now(timezone.utc)
        date_dt = _day_to_utc(record_date)

        set_fields: Doc = {
            **{k: v for k, v in metrics.items() if k not in ("_id", "brand_id")},
            "brand_id": self._brand_id,
            "source": source,
            "campaign_id": ObjectId(campaign_id),
            "date": date_dt,
            "ingested_at": now,
            "ingestion_run_id": ingestion_run_id,
        }

        result = await self._col.update_one(
            self._scope({
                "source": source,
                "campaign_id": ObjectId(campaign_id),
                "date": date_dt,
            }),
            {"$set": set_fields},
            upsert=True,
        )
        if result.upserted_id:
            return str(result.upserted_id)
        doc = await self._col.find_one(
            self._scope({
                "source": source,
                "campaign_id": ObjectId(campaign_id),
                "date": date_dt,
            }),
            {"_id": 1},
        )
        return str(doc["_id"]) if doc else ""

    # ── Queries ───────────────────────────────────────────────────────────────

    async def find_by_date_range(
        self,
        date_from: date,
        date_to: date,
        *,
        source: str | None = None,
        campaign_id: str | None = None,
    ) -> list[Doc]:
        """Return raw performance rows for a date range (inclusive).

        Optionally filter by `source` and/or `campaign_id`.
        Results are sorted by date descending, then source.
        """
        q: Doc = {
            "date": {
                "$gte": _day_to_utc(date_from),
                "$lte": _day_to_utc(date_to),
            }
        }
        if source:
            q["source"] = source
        if campaign_id:
            q["campaign_id"] = ObjectId(campaign_id)
        return await self.find(q, sort=[("date", -1), ("source", 1)])

    async def delete_by_run_id(self, ingestion_run_id: str) -> int:
        """Delete all rows written by a specific ingestion run.

        Used to atomically roll back a failed or corrupt ingestion.
        Returns the number of documents deleted.
        """
        count = await self.delete_many({"ingestion_run_id": ingestion_run_id})
        logger.info(
            "performance.rollback",
            run_id=ingestion_run_id,
            deleted=count,
            brand_id=self._brand_id,
        )
        return count

    # ── Aggregations ──────────────────────────────────────────────────────────

    async def get_daily_summary(
        self,
        date_from: date,
        date_to: date,
        *,
        source: str | None = None,
    ) -> list[Doc]:
        """Per-day totals across all campaigns (or a specific source).

        Returns list of:
          { date, source?, total_spend_paise, total_impressions,
            total_clicks, total_leads, total_conversions, avg_roas, avg_ctr }
        sorted by date ascending.
        """
        match: Doc = {
            "date": {
                "$gte": _day_to_utc(date_from),
                "$lte": _day_to_utc(date_to),
            }
        }
        if source:
            match["source"] = source

        group_id: Doc = {"date": "$date"}
        if not source:
            group_id["source"] = "$source"

        pipeline: list[Doc] = [
            {"$match": match},
            {
                "$group": {
                    "_id": group_id,
                    "total_spend_paise":   {"$sum": "$spend_paise"},
                    "total_impressions":   {"$sum": "$impressions"},
                    "total_clicks":        {"$sum": "$clicks"},
                    "total_leads":         {"$sum": "$leads"},
                    "total_conversions":   {"$sum": "$conversions"},
                    "avg_roas":            {"$avg": "$roas"},
                    "avg_ctr":             {"$avg": "$ctr"},
                    "record_count":        {"$sum": 1},
                }
            },
            {"$sort": {"_id.date": 1}},
            {
                "$project": {
                    "_id": 0,
                    "date":                "$_id.date",
                    "source":              "$_id.source",
                    "total_spend_paise":   1,
                    "total_impressions":   1,
                    "total_clicks":        1,
                    "total_leads":         1,
                    "total_conversions":   1,
                    "avg_roas":            1,
                    "avg_ctr":             1,
                    "record_count":        1,
                }
            },
        ]
        return await self.aggregate(pipeline)

    async def get_campaign_summary(
        self,
        campaign_id: str,
        date_from: date,
        date_to: date,
    ) -> Doc | None:
        """Aggregate totals for a single campaign over a date range.

        Returns a single summary dict or None if no data found.
        """
        pipeline: list[Doc] = [
            {
                "$match": {
                    "campaign_id": ObjectId(campaign_id),
                    "date": {
                        "$gte": _day_to_utc(date_from),
                        "$lte": _day_to_utc(date_to),
                    },
                }
            },
            {
                "$group": {
                    "_id": "$campaign_id",
                    "total_spend_paise":            {"$sum": "$spend_paise"},
                    "total_impressions":            {"$sum": "$impressions"},
                    "total_clicks":                 {"$sum": "$clicks"},
                    "total_leads":                  {"$sum": "$leads"},
                    "total_conversions":            {"$sum": "$conversions"},
                    "total_conversion_value_paise": {"$sum": "$conversion_value_paise"},
                    "avg_roas":                     {"$avg": "$roas"},
                    "avg_ctr":                      {"$avg": "$ctr"},
                    "avg_cpc_paise":                {"$avg": "$cpc_paise"},
                    "days_with_data":               {"$sum": 1},
                }
            },
            {"$project": {"_id": 0}},
        ]
        results = await self.aggregate(pipeline)
        return results[0] if results else None
