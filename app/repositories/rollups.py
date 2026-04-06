"""
RollupsRepository  (collection: performance_rollups)

Brand-scoped. Stores pre-computed period summaries so dashboard queries
hit O(1) lookups instead of expensive aggregations at request time.

Natural key: (brand_id, period_type, period_start, source)
  — enforced by the unique index `rollups_period_unique`.

`source` is either a platform name ("google_ads", "meta", "interakt")
or the special value "all" for cross-platform totals.

The rollup computation job (app/worker/tasks.py) calls `upsert()` after
each daily ingestion run. `computed_at` and `is_partial` track freshness.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.repositories.base import BrandScopedRepository

logger = structlog.get_logger(__name__)

Doc = dict[str, Any]
PeriodType = Literal["daily", "weekly", "monthly"]


class RollupsRepository(BrandScopedRepository):
    def __init__(self, db: AsyncIOMotorDatabase, brand_id: str) -> None:  # type: ignore[type-arg]
        super().__init__(db["performance_rollups"], brand_id)

    # ── Upsert (rollup computation path) ─────────────────────────────────────

    async def upsert(
        self,
        period_type: PeriodType,
        period_start: datetime,
        period_end: datetime,
        source: str,
        metrics: Doc,
        *,
        is_partial: bool = False,
    ) -> str:
        """Write or replace a rollup for the given period + source.

        Natural key: (brand_id, period_type, period_start, source).
        Replaces ALL metric fields on each call — rollups are fully
        recomputed, not incrementally updated.

        Returns the document's string _id.
        """
        now = datetime.now(UTC)
        set_fields: Doc = {
            **{k: v for k, v in metrics.items() if k not in ("_id", "brand_id")},
            "brand_id": self._brand_id,
            "period_type": period_type,
            "period_start": period_start,
            "period_end": period_end,
            "source": source,
            "computed_at": now,
            "is_partial": is_partial,
        }

        result = await self._col.update_one(
            self._scope({
                "period_type": period_type,
                "period_start": period_start,
                "source": source,
            }),
            {"$set": set_fields},
            upsert=True,
        )
        if result.upserted_id:
            logger.info(
                "rollup.created",
                brand_id=self._brand_id,
                period_type=period_type,
                period_start=period_start.isoformat(),
                source=source,
            )
            return str(result.upserted_id)

        doc = await self._col.find_one(
            self._scope({
                "period_type": period_type,
                "period_start": period_start,
                "source": source,
            }),
            {"_id": 1},
        )
        return str(doc["_id"]) if doc else ""

    # ── Queries ───────────────────────────────────────────────────────────────

    async def find_by_period(
        self,
        period_type: PeriodType,
        date_from: datetime,
        date_to: datetime,
        *,
        source: str | None = None,
    ) -> list[Doc]:
        """Return rollups whose period_start falls in [date_from, date_to].

        Optionally filter by source. Results sorted by period_start ascending.
        Omit `source` to get rollups for all sources (including "all").
        """
        q: Doc = {
            "period_type": period_type,
            "period_start": {"$gte": date_from, "$lte": date_to},
        }
        if source is not None:
            q["source"] = source
        return await self.find(q, sort=[("period_start", 1)])

    async def find_latest(
        self,
        period_type: PeriodType,
        *,
        source: str = "all",
    ) -> Doc | None:
        """Return the most recent rollup for a period type + source."""
        results = await self.find(
            {"period_type": period_type, "source": source},
            sort=[("period_start", -1)],
            limit=1,
        )
        return results[0] if results else None

    async def find_for_dashboard(
        self,
        period_type: PeriodType,
        date_from: datetime,
        date_to: datetime,
    ) -> list[Doc]:
        """Convenience: returns cross-platform ("all") rollups for the date range.

        Used by the performance summary endpoint to avoid per-source fan-out.
        """
        return await self.find_by_period(
            period_type, date_from, date_to, source="all"
        )

    async def delete_range(
        self,
        period_type: PeriodType,
        date_from: datetime,
        date_to: datetime,
        *,
        source: str | None = None,
    ) -> int:
        """Delete rollups in a date range (used before recomputation).

        Returns the number of documents deleted.
        """
        q: Doc = {
            "period_type": period_type,
            "period_start": {"$gte": date_from, "$lte": date_to},
        }
        if source is not None:
            q["source"] = source
        count = await self.delete_many(q)
        logger.info(
            "rollup.deleted_range",
            brand_id=self._brand_id,
            period_type=period_type,
            deleted=count,
        )
        return count
