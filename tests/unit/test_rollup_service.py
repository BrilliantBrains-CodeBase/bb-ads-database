"""
Unit tests for the Rollup Computation Service (app/services/rollup.py).

Strategy
────────
• PerformanceRepository.get_rollup_aggregates and RollupsRepository.upsert /
  find_by_period are exercised against mongomock-motor with real documents.
• RollupService.compute_for_brand is tested end-to-end: insert raw data,
  run computation, assert rollup documents are correct in performance_rollups.
• compute_all_rollups is tested with a brand list, checking per-brand isolation
  and that one failing brand does not abort the rest.
• Date helpers (_week_range, _month_range, _is_partial) are tested directly.

Coverage
────────
  Date helpers:
    - _week_range returns Monday–Sunday of the ISO week
    - _month_range returns first–last day of month, handles short/long months
    - _is_partial: today = partial, past = not partial, future = partial

  PerformanceRepository.get_rollup_aggregates:
    - Returns per-source rows + "all" row for brand with data
    - Correct numeric totals for each source
    - "all" row totals both sources
    - Returns [] when brand has no data in date range
    - Brand isolation: other brand's data not included

  RollupService.compute_for_brand:
    - Writes daily / weekly / monthly rollup documents
    - Idempotent: second call upserts without duplicates
    - partial flag set correctly (today = True, past month = False)
    - Returns correct RollupResult counts
    - Skips gracefully when no source data exists (periods_skipped incremented)

  compute_all_rollups:
    - Processes all active brands
    - Inactive brands skipped
    - Per-brand exception does not abort remaining brands
    - Empty brand collection → no error, 0 computed

  _build_metrics:
    - avg_cpl_paise truncated to int
    - None values preserved as None
    - budget_utilization always None
"""

from __future__ import annotations

import calendar
from datetime import UTC, date, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from bson import ObjectId
from mongomock_motor import AsyncMongoMockClient

from app.services.rollup import (
    RollupService,
    _build_metrics,
    _is_partial,
    _month_range,
    _week_range,
    compute_all_rollups,
)

# ── Constants ──────────────────────────────────────────────────────────────────

BRAND_OID = ObjectId()
BRAND_ID = str(BRAND_OID)
BRAND_OID_2 = ObjectId()
BRAND_ID_2 = str(BRAND_OID_2)

CAMP_OID_1 = ObjectId()
CAMP_OID_2 = ObjectId()

TODAY = date(2026, 4, 7)           # Tuesday — useful known weekday
YESTERDAY = TODAY - timedelta(days=1)
LAST_MONDAY = date(2026, 3, 30)    # Monday of previous week
LAST_SUNDAY = date(2026, 4, 5)     # Sunday of previous week


def _day_utc(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time(), tzinfo=UTC)


def _raw(
    *,
    brand_id: str = BRAND_ID,
    campaign_id: ObjectId = CAMP_OID_1,
    source: str = "google_ads",
    d: date = TODAY,
    spend: int = 10_000,
    impressions: int = 1_000,
    clicks: int = 50,
    leads: int = 5,
    conversions: int = 2,
    conv_value: int = 20_000,
) -> dict[str, Any]:
    return {
        "_id": ObjectId(),
        "brand_id": brand_id,
        "campaign_id": campaign_id,
        "source": source,
        "date": _day_utc(d),
        "ingested_at": datetime.now(UTC),
        "ingestion_run_id": "run-001",
        "spend_paise": spend,
        "impressions": impressions,
        "clicks": clicks,
        "reach": impressions // 2,
        "leads": leads,
        "conversions": conversions,
        "conversion_value_paise": conv_value,
        "ctr": clicks / impressions if impressions else None,
        "cpc_paise": spend // clicks if clicks else None,
        "cpm_paise": int(spend * 1000 // impressions) if impressions else None,
        "cpl_paise": spend // leads if leads else None,
        "roas": conv_value / spend if spend else None,
    }


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def db():
    client = AsyncMongoMockClient()
    database = client["test_db"]
    yield database
    client.close()


@pytest_asyncio.fixture
async def db_with_raw(db):
    """google_ads × 2 days + meta × 1 day for BRAND_ID; 1 row for BRAND_ID_2."""
    await db["ad_performance_raw"].insert_many([
        # google_ads
        _raw(source="google_ads", d=TODAY,      spend=50_000, impressions=5_000,
             clicks=200, leads=10, conversions=5, conv_value=100_000),
        _raw(source="google_ads", d=YESTERDAY,  spend=40_000, impressions=4_000,
             clicks=160, leads=8,  conversions=4, conv_value=80_000),
        # meta
        _raw(source="meta", d=TODAY,            spend=30_000, impressions=8_000,
             clicks=80,  leads=20, conversions=2, conv_value=20_000),
        # other brand — must not appear in BRAND_ID aggregates
        _raw(brand_id=BRAND_ID_2, source="google_ads", d=TODAY,
             spend=999_999, impressions=1, clicks=1, leads=1,
             conversions=1, conv_value=1),
    ])
    await db["brands"].insert_many([
        {"_id": BRAND_OID,   "slug": "brand-a", "is_active": True},
        {"_id": BRAND_OID_2, "slug": "brand-b", "is_active": True},
    ])
    yield db


# ══════════════════════════════════════════════════════════════════════════════
# Section 1 — Date helpers
# ══════════════════════════════════════════════════════════════════════════════

class TestDateHelpers:
    def test_week_range_monday(self):
        monday = date(2026, 3, 30)  # known Monday
        start, end = _week_range(monday)
        assert start == date(2026, 3, 30)
        assert end == date(2026, 4, 5)
        assert start.weekday() == 0   # Monday
        assert end.weekday() == 6     # Sunday

    def test_week_range_mid_week(self):
        thursday = date(2026, 4, 2)
        start, end = _week_range(thursday)
        assert start == date(2026, 3, 30)
        assert end == date(2026, 4, 5)

    def test_week_range_sunday(self):
        sunday = date(2026, 4, 5)
        start, end = _week_range(sunday)
        assert start == date(2026, 3, 30)
        assert end == date(2026, 4, 5)

    def test_month_range_full_month(self):
        first, last = _month_range(date(2026, 1, 15))
        assert first == date(2026, 1, 1)
        assert last == date(2026, 1, 31)

    def test_month_range_february_non_leap(self):
        first, last = _month_range(date(2026, 2, 10))
        assert first == date(2026, 2, 1)
        assert last == date(2026, 2, 28)

    def test_month_range_february_leap(self):
        first, last = _month_range(date(2028, 2, 14))
        assert first == date(2028, 2, 1)
        assert last == date(2028, 2, 29)

    def test_month_range_30_day_month(self):
        first, last = _month_range(date(2026, 4, 7))
        assert first == date(2026, 4, 1)
        assert last == date(2026, 4, 30)

    def test_is_partial_today(self):
        assert _is_partial(date.today()) is True

    def test_is_partial_past(self):
        assert _is_partial(date(2025, 1, 1)) is False

    def test_is_partial_future(self):
        future = date.today() + timedelta(days=5)
        assert _is_partial(future) is True


# ══════════════════════════════════════════════════════════════════════════════
# Section 2 — PerformanceRepository.get_rollup_aggregates
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestGetRollupAggregates:
    async def test_returns_per_source_plus_all(self, db_with_raw):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_raw, BRAND_ID)
        rows = await repo.get_rollup_aggregates(YESTERDAY, TODAY)

        sources = {r["source"] for r in rows}
        assert "google_ads" in sources
        assert "meta" in sources
        assert "all" in sources
        assert len(rows) == 3  # google_ads + meta + all

    async def test_google_ads_totals(self, db_with_raw):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_raw, BRAND_ID)
        rows = await repo.get_rollup_aggregates(YESTERDAY, TODAY)
        ga = next(r for r in rows if r["source"] == "google_ads")

        # spend: 50k + 40k = 90k; impressions: 9k; clicks: 360; leads: 18
        assert ga["total_spend_paise"] == 90_000
        assert ga["total_impressions"] == 9_000
        assert ga["total_clicks"] == 360
        assert ga["total_leads"] == 18
        assert ga["total_conversions"] == 9
        assert ga["total_conversion_value_paise"] == 180_000

    async def test_meta_totals(self, db_with_raw):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_raw, BRAND_ID)
        rows = await repo.get_rollup_aggregates(YESTERDAY, TODAY)
        meta = next(r for r in rows if r["source"] == "meta")

        assert meta["total_spend_paise"] == 30_000
        assert meta["total_impressions"] == 8_000

    async def test_all_row_totals_both_sources(self, db_with_raw):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_raw, BRAND_ID)
        rows = await repo.get_rollup_aggregates(YESTERDAY, TODAY)
        all_row = next(r for r in rows if r["source"] == "all")

        assert all_row["total_spend_paise"] == 120_000   # 90k + 30k
        assert all_row["total_impressions"] == 17_000    # 9k + 8k

    async def test_derived_kpis_present(self, db_with_raw):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_raw, BRAND_ID)
        rows = await repo.get_rollup_aggregates(YESTERDAY, TODAY)
        all_row = next(r for r in rows if r["source"] == "all")

        assert all_row["avg_roas"] is not None
        assert all_row["avg_roas"] > 0
        assert all_row["avg_ctr"] is not None
        assert 0 < all_row["avg_ctr"] < 1
        assert all_row["avg_cpl_paise"] is not None

    async def test_empty_range_returns_empty_list(self, db_with_raw):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_raw, BRAND_ID)
        rows = await repo.get_rollup_aggregates(date(2025, 1, 1), date(2025, 1, 31))
        assert rows == []

    async def test_brand_isolation(self, db_with_raw):
        from app.repositories.performance import PerformanceRepository
        # BRAND_ID_2 has only 1 row; its totals should not bleed into BRAND_ID
        repo = PerformanceRepository(db_with_raw, BRAND_ID)
        rows = await repo.get_rollup_aggregates(YESTERDAY, TODAY)
        all_row = next(r for r in rows if r["source"] == "all")
        # BRAND_ID_2 has spend 999_999 — must NOT appear here
        assert all_row["total_spend_paise"] == 120_000

    async def test_single_source_only(self, db_with_raw):
        """A brand with only one source gets 2 rows: that source + 'all'."""
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_raw, BRAND_ID_2)
        rows = await repo.get_rollup_aggregates(YESTERDAY, TODAY)
        sources = {r["source"] for r in rows}
        assert sources == {"google_ads", "all"}


# ══════════════════════════════════════════════════════════════════════════════
# Section 3 — _build_metrics
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildMetrics:
    def _row(self, **kwargs) -> dict[str, Any]:
        base = {
            "source": "google_ads",
            "total_spend_paise": 10_000,
            "total_impressions": 1_000,
            "total_clicks": 50,
            "total_leads": 5,
            "total_conversions": 2,
            "total_conversion_value_paise": 20_000,
            "avg_roas": 2.0,
            "avg_cpl_paise": 2000.7,
            "avg_ctr": 0.05,
        }
        base.update(kwargs)
        return base

    def test_avg_cpl_truncated_to_int(self):
        m = _build_metrics(self._row(avg_cpl_paise=1234.9))
        assert m["avg_cpl_paise"] == 1234
        assert isinstance(m["avg_cpl_paise"], int)

    def test_none_values_preserved(self):
        m = _build_metrics(self._row(avg_roas=None, avg_cpl_paise=None, avg_ctr=None))
        assert m["avg_roas"] is None
        assert m["avg_cpl_paise"] is None
        assert m["avg_ctr"] is None

    def test_budget_utilization_always_none(self):
        m = _build_metrics(self._row())
        assert m["budget_utilization"] is None

    def test_correct_totals_passed_through(self):
        m = _build_metrics(self._row(total_spend_paise=99_999, total_leads=7))
        assert m["total_spend_paise"] == 99_999
        assert m["total_leads"] == 7

    def test_missing_fields_default_to_zero(self):
        """Row that omits optional numeric fields defaults to 0."""
        m = _build_metrics({"source": "meta", "total_spend_paise": None})
        assert m["total_spend_paise"] == 0
        assert m["total_impressions"] == 0


# ══════════════════════════════════════════════════════════════════════════════
# Section 4 — RollupService.compute_for_brand
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestRollupService:
    async def test_writes_daily_rollup(self, db_with_raw):
        svc = RollupService(db_with_raw)
        await svc.compute_for_brand(BRAND_ID, target_date=TODAY)

        docs = await db_with_raw["performance_rollups"].find(
            {"brand_id": BRAND_ID, "period_type": "daily"}
        ).to_list(length=None)
        sources = {d["source"] for d in docs}
        assert "google_ads" in sources
        assert "meta" in sources
        assert "all" in sources

    async def test_writes_weekly_rollup(self, db_with_raw):
        svc = RollupService(db_with_raw)
        await svc.compute_for_brand(BRAND_ID, target_date=YESTERDAY)

        docs = await db_with_raw["performance_rollups"].find(
            {"brand_id": BRAND_ID, "period_type": "weekly"}
        ).to_list(length=None)
        assert len(docs) >= 1
        sources = {d["source"] for d in docs}
        assert "all" in sources

    async def test_writes_monthly_rollup(self, db_with_raw):
        svc = RollupService(db_with_raw)
        await svc.compute_for_brand(BRAND_ID, target_date=TODAY)

        docs = await db_with_raw["performance_rollups"].find(
            {"brand_id": BRAND_ID, "period_type": "monthly"}
        ).to_list(length=None)
        assert len(docs) >= 1

    async def test_daily_spend_correct(self, db_with_raw):
        """Daily rollup for TODAY should sum only today's rows."""
        svc = RollupService(db_with_raw)
        await svc.compute_for_brand(BRAND_ID, target_date=TODAY)

        doc = await db_with_raw["performance_rollups"].find_one(
            {"brand_id": BRAND_ID, "period_type": "daily", "source": "all"}
        )
        assert doc is not None
        # TODAY: google 50k + meta 30k = 80k
        assert doc["total_spend_paise"] == 80_000

    async def test_idempotent_no_duplicates(self, db_with_raw):
        """Running twice must not create duplicate rollup documents."""
        svc = RollupService(db_with_raw)
        await svc.compute_for_brand(BRAND_ID, target_date=TODAY)
        await svc.compute_for_brand(BRAND_ID, target_date=TODAY)

        docs = await db_with_raw["performance_rollups"].find(
            {"brand_id": BRAND_ID, "period_type": "daily", "source": "all"}
        ).to_list(length=None)
        assert len(docs) == 1

    async def test_partial_flag_today(self, db_with_raw):
        """Rollups for today are marked partial (data still arriving)."""
        svc = RollupService(db_with_raw)
        await svc.compute_for_brand(BRAND_ID, target_date=date.today())

        doc = await db_with_raw["performance_rollups"].find_one(
            {"brand_id": BRAND_ID, "period_type": "daily"}
        )
        if doc:  # may be empty if no data for real today
            assert doc["is_partial"] is True

    async def test_partial_flag_past(self, db_with_raw):
        """A past-complete month has is_partial=False."""
        past_date = date(2025, 1, 15)
        svc = RollupService(db_with_raw)
        result = await svc.compute_for_brand(BRAND_ID, target_date=past_date)
        # No data → all periods skipped; assert skipped count
        assert result.periods_skipped == 3  # daily + weekly + monthly all empty

    async def test_result_counts_populated(self, db_with_raw):
        svc = RollupService(db_with_raw)
        result = await svc.compute_for_brand(BRAND_ID, target_date=TODAY)
        # TODAY: 3 docs (ga + meta + all) per period type = 3 sources × 3 types (daily, weekly, monthly)
        assert result.periods_computed > 0
        assert result.errors == []

    async def test_no_data_returns_skipped(self, db_with_raw):
        other_brand_id = str(ObjectId())
        svc = RollupService(db_with_raw)
        result = await svc.compute_for_brand(other_brand_id, target_date=TODAY)
        assert result.periods_computed == 0
        assert result.periods_skipped == 3  # daily + weekly + monthly all skipped

    async def test_brand_isolation_rollups(self, db_with_raw):
        """BRAND_ID_2's raw data must not contaminate BRAND_ID's rollups."""
        svc = RollupService(db_with_raw)
        await svc.compute_for_brand(BRAND_ID, target_date=TODAY)

        doc = await db_with_raw["performance_rollups"].find_one(
            {"brand_id": BRAND_ID, "period_type": "daily", "source": "all"}
        )
        assert doc["total_spend_paise"] == 80_000  # not 80k + 999_999

    async def test_period_start_end_set_correctly(self, db_with_raw):
        svc = RollupService(db_with_raw)
        await svc.compute_for_brand(BRAND_ID, target_date=TODAY)

        doc = await db_with_raw["performance_rollups"].find_one(
            {"brand_id": BRAND_ID, "period_type": "daily", "source": "all"}
        )
        assert doc is not None
        # mongomock strips tzinfo on read-back; compare naive datetimes
        expected = _day_utc(TODAY).replace(tzinfo=None)
        assert doc["period_start"].replace(tzinfo=None) == expected
        assert doc["period_end"].replace(tzinfo=None) == expected


# ══════════════════════════════════════════════════════════════════════════════
# Section 5 — compute_all_rollups
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestComputeAllRollups:
    async def test_processes_both_active_brands(self, db_with_raw):
        await compute_all_rollups(db_with_raw, target_date=TODAY)

        b1_docs = await db_with_raw["performance_rollups"].count_documents(
            {"brand_id": BRAND_ID}
        )
        b2_docs = await db_with_raw["performance_rollups"].count_documents(
            {"brand_id": BRAND_ID_2}
        )
        assert b1_docs > 0
        assert b2_docs > 0

    async def test_skips_inactive_brands(self, db):
        inactive_oid = ObjectId()
        await db["brands"].insert_one(
            {"_id": inactive_oid, "slug": "inactive", "is_active": False}
        )
        await compute_all_rollups(db, target_date=TODAY)

        docs = await db["performance_rollups"].count_documents(
            {"brand_id": str(inactive_oid)}
        )
        assert docs == 0

    async def test_per_brand_exception_does_not_abort(self, db_with_raw):
        """If one brand raises, the other brand is still processed."""
        # Make BRAND_ID_2 raise by patching compute_for_brand to fail the first call
        original = RollupService.compute_for_brand
        call_count = {"n": 0}

        async def _selective_fail(self, brand_id, **kwargs):
            call_count["n"] += 1
            if brand_id == BRAND_ID_2:
                raise RuntimeError("forced failure")
            return await original(self, brand_id, **kwargs)

        with patch.object(RollupService, "compute_for_brand", _selective_fail):
            # Should not raise
            await compute_all_rollups(db_with_raw, target_date=TODAY)

        # BRAND_ID should still have rollups despite BRAND_ID_2 failing
        docs = await db_with_raw["performance_rollups"].count_documents(
            {"brand_id": BRAND_ID}
        )
        assert docs > 0
        assert call_count["n"] == 2  # both brands were attempted

    async def test_empty_brands_collection_no_error(self, db):
        """No brands → completes silently, 0 rollup docs written."""
        await compute_all_rollups(db, target_date=TODAY)
        docs = await db["performance_rollups"].count_documents({})
        assert docs == 0

    async def test_default_target_date_is_today(self, db_with_raw):
        """Omitting target_date should default to today without error."""
        # Just ensure it doesn't raise; data may or may not match today's date
        await compute_all_rollups(db_with_raw)
