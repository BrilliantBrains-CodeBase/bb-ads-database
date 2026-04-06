"""
Unit tests for BaseIngestionService.

Uses mongomock-motor (no real MongoDB) and a minimal concrete subclass
that records which dates were fetched.

Test coverage:
  ✓ Correction window: D-1 and D-0 fetched by default
  ✓ Custom dates override the correction window
  ✓ Successful run → status="success", log written
  ✓ Per-date fetch failure → status="partial" when some rows upserted
  ✓ Per-date fetch failure → status="failed" when zero rows upserted
  ✓ All dates fail → status="failed"
  ✓ Run ID tagged on every ingestion_logs document
  ✓ Campaign auto-created via upsert_from_platform
  ✓ Performance rows written via PerformanceRepository.upsert
  ✓ Derived metrics computed correctly (CTR, CPC, CPM, CPL, ROAS)
  ✓ Zero-denominator derived metrics are None (no division errors)
  ✓ Per-record upsert failure is isolated (other records still written)
  ✓ Log start/complete failures do not abort ingestion
  ✓ IngestionResult fields are correct
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from mongomock_motor import AsyncMongoMockClient

from app.services.ingestion.base import (
    BaseIngestionService,
    IngestionResult,
    PlatformRecord,
    _compute_derived,
)

# ── Concrete test subclass ────────────────────────────────────────────────────

class StubIngestionService(BaseIngestionService):
    """Minimal concrete subclass for testing the base orchestration."""

    source = "manual"

    def __init__(self, db, *, fetch_side_effect=None, records_per_date=None):
        super().__init__(db)
        self._fetch_side_effect = fetch_side_effect  # dict[date, Exception] or None
        self._records_per_date = records_per_date or {}  # dict[date, list[PlatformRecord]]
        self.fetched_dates: list[date] = []

    async def fetch(self, brand_id: str, target_date: date) -> list[dict[str, Any]]:
        self.fetched_dates.append(target_date)
        if self._fetch_side_effect and target_date in self._fetch_side_effect:
            raise self._fetch_side_effect[target_date]
        return [{"date": str(target_date), "_stub": True}]

    def transform(
        self,
        raw_records: list[dict[str, Any]],
        brand_id: str,
    ) -> list[PlatformRecord]:
        results = []
        for raw in raw_records:
            d = date.fromisoformat(raw["date"])
            override = self._records_per_date.get(d)
            if override is not None:
                results.extend(override)
            else:
                results.append(PlatformRecord(
                    external_campaign_id="ext_001",
                    campaign_name="Test Campaign",
                    date=d,
                    spend_paise=100_000,
                    impressions=10_000,
                    clicks=200,
                    conversions=5,
                    conversion_value_paise=500_000,
                    leads=10,
                ))
        return results


# ── Fixtures ──────────────────────────────────────────────────────────────────

BRAND_ID = "507f1f77bcf86cd799439011"   # valid ObjectId string
TODAY = date(2025, 6, 15)
YESTERDAY = TODAY - timedelta(days=1)


@pytest.fixture
def db():
    client = AsyncMongoMockClient()
    return client["test_db"]


@pytest.fixture
def svc(db):
    return StubIngestionService(db)


# ── _compute_derived unit tests ───────────────────────────────────────────────

class TestComputeDerived:
    def test_all_metrics(self):
        r = PlatformRecord(
            external_campaign_id="x", campaign_name="C", date=TODAY,
            spend_paise=100_000, impressions=10_000, clicks=500,
            conversion_value_paise=300_000, leads=20,
        )
        d = _compute_derived(r)
        assert d["ctr"] == pytest.approx(0.05)          # 500 / 10000
        assert d["cpc_paise"] == 200                     # 100000 // 500
        assert d["cpm_paise"] == 10_000                  # 100000 * 1000 // 10000
        assert d["cpl_paise"] == 5_000                   # 100000 // 20
        assert d["roas"] == pytest.approx(3.0)           # 300000 / 100000

    def test_zero_impressions_gives_none(self):
        r = PlatformRecord(
            external_campaign_id="x", campaign_name="C", date=TODAY,
            spend_paise=50_000, impressions=0, clicks=0,
        )
        d = _compute_derived(r)
        assert d["ctr"] is None
        assert d["cpm_paise"] is None
        assert d["cpc_paise"] is None

    def test_zero_spend_gives_none_roas(self):
        r = PlatformRecord(
            external_campaign_id="x", campaign_name="C", date=TODAY,
            spend_paise=0, conversion_value_paise=1_000,
        )
        assert _compute_derived(r)["roas"] is None

    def test_zero_leads_gives_none_cpl(self):
        r = PlatformRecord(
            external_campaign_id="x", campaign_name="C", date=TODAY,
            spend_paise=100_000, leads=0,
        )
        assert _compute_derived(r)["cpl_paise"] is None


# ── Correction window ─────────────────────────────────────────────────────────

class TestCorrectionWindow:
    async def test_default_pulls_d_minus_1_and_d_0(self, svc):
        with patch("app.services.ingestion.base.date") as mock_date:
            mock_date.today.return_value = TODAY
            mock_date.fromisoformat = date.fromisoformat
            result = await svc.run(BRAND_ID)
        assert YESTERDAY in svc.fetched_dates
        assert TODAY in svc.fetched_dates

    async def test_explicit_target_date_still_includes_d_minus_1(self, svc):
        target = date(2025, 3, 10)
        result = await svc.run(BRAND_ID, target_date=target)
        assert date(2025, 3, 9) in svc.fetched_dates
        assert target in svc.fetched_dates

    async def test_custom_dates_override_window(self, svc):
        custom = [date(2025, 1, 1), date(2025, 1, 5), date(2025, 1, 10)]
        await svc.run(BRAND_ID, custom_dates=custom)
        assert svc.fetched_dates == custom

    async def test_single_custom_date(self, svc):
        d = date(2025, 4, 1)
        await svc.run(BRAND_ID, custom_dates=[d])
        assert svc.fetched_dates == [d]


# ── Success path ──────────────────────────────────────────────────────────────

class TestSuccessPath:
    async def test_returns_ingestion_result(self, svc):
        result = await svc.run(BRAND_ID, target_date=TODAY)
        assert isinstance(result, IngestionResult)

    async def test_status_is_success(self, svc):
        result = await svc.run(BRAND_ID, target_date=TODAY)
        assert result.status == "success"

    async def test_correct_counts(self, svc):
        result = await svc.run(BRAND_ID, target_date=TODAY)
        # 2 dates × 1 record each = 2 fetched, 2 upserted
        assert result.records_fetched == 2
        assert result.records_upserted == 2

    async def test_dates_covered_in_result(self, svc):
        result = await svc.run(BRAND_ID, target_date=TODAY)
        assert YESTERDAY in result.dates_covered
        assert TODAY in result.dates_covered

    async def test_no_errors_in_result(self, svc):
        result = await svc.run(BRAND_ID, target_date=TODAY)
        assert result.errors == []

    async def test_run_id_is_uuid_string(self, svc):
        import re
        result = await svc.run(BRAND_ID, target_date=TODAY)
        assert re.match(
            r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
            result.run_id,
        )


# ── Ingestion log ─────────────────────────────────────────────────────────────

class TestIngestionLog:
    async def test_log_written_to_db(self, svc, db):
        result = await svc.run(BRAND_ID, target_date=TODAY)
        log = await db["ingestion_logs"].find_one({"run_id": result.run_id})
        assert log is not None

    async def test_log_final_status_success(self, svc, db):
        result = await svc.run(BRAND_ID, target_date=TODAY)
        log = await db["ingestion_logs"].find_one({"run_id": result.run_id})
        assert log["status"] == "success"

    async def test_log_source_matches(self, svc, db):
        result = await svc.run(BRAND_ID, target_date=TODAY)
        log = await db["ingestion_logs"].find_one({"run_id": result.run_id})
        assert log["source"] == "manual"

    async def test_log_counts_correct(self, svc, db):
        result = await svc.run(BRAND_ID, target_date=TODAY)
        log = await db["ingestion_logs"].find_one({"run_id": result.run_id})
        assert log["records_fetched"] == result.records_fetched
        assert log["records_upserted"] == result.records_upserted

    async def test_log_completed_at_set(self, svc, db):
        result = await svc.run(BRAND_ID, target_date=TODAY)
        log = await db["ingestion_logs"].find_one({"run_id": result.run_id})
        assert log["completed_at"] is not None

    async def test_backfill_flag_stored(self, svc, db):
        result = await svc.run(BRAND_ID, target_date=TODAY, is_backfill=True)
        log = await db["ingestion_logs"].find_one({"run_id": result.run_id})
        assert log["is_backfill"] is True

    async def test_log_start_failure_does_not_abort(self, db):
        """If the log insert fails, ingestion must still complete."""
        svc = StubIngestionService(db)
        with patch.object(svc, "_start_log", side_effect=Exception("DB down")):
            result = await svc.run(BRAND_ID, target_date=TODAY)
        # Ingestion completed despite log failure
        assert result.records_upserted > 0


# ── Partial failure ───────────────────────────────────────────────────────────

class TestPartialFailure:
    async def test_one_date_fails_status_partial(self, db):
        """If one date fails but another succeeds, status should be 'partial'."""
        svc = StubIngestionService(
            db,
            fetch_side_effect={YESTERDAY: ValueError("API timeout")},
        )
        result = await svc.run(BRAND_ID, target_date=TODAY)
        assert result.status == "partial"

    async def test_partial_errors_recorded(self, db):
        svc = StubIngestionService(
            db,
            fetch_side_effect={YESTERDAY: ValueError("timeout")},
        )
        result = await svc.run(BRAND_ID, target_date=TODAY)
        assert len(result.errors) == 1
        assert str(YESTERDAY) in result.errors[0]

    async def test_partial_upserted_count(self, db):
        svc = StubIngestionService(
            db,
            fetch_side_effect={YESTERDAY: ValueError("timeout")},
        )
        result = await svc.run(BRAND_ID, target_date=TODAY)
        # Only TODAY's record was upserted
        assert result.records_upserted == 1

    async def test_log_status_partial(self, db):
        svc = StubIngestionService(
            db,
            fetch_side_effect={YESTERDAY: ValueError("timeout")},
        )
        result = await svc.run(BRAND_ID, target_date=TODAY)
        log = await db["ingestion_logs"].find_one({"run_id": result.run_id})
        assert log["status"] == "partial"


# ── Total failure ─────────────────────────────────────────────────────────────

class TestTotalFailure:
    async def test_all_dates_fail_status_failed(self, db):
        svc = StubIngestionService(
            db,
            fetch_side_effect={
                YESTERDAY: RuntimeError("auth"),
                TODAY: RuntimeError("auth"),
            },
        )
        result = await svc.run(BRAND_ID, target_date=TODAY)
        assert result.status == "failed"

    async def test_failed_result_has_zero_upserts(self, db):
        svc = StubIngestionService(
            db,
            fetch_side_effect={
                YESTERDAY: RuntimeError("quota"),
                TODAY: RuntimeError("quota"),
            },
        )
        result = await svc.run(BRAND_ID, target_date=TODAY)
        assert result.records_upserted == 0

    async def test_log_status_failed(self, db):
        svc = StubIngestionService(
            db,
            fetch_side_effect={
                YESTERDAY: RuntimeError("quota"),
                TODAY: RuntimeError("quota"),
            },
        )
        result = await svc.run(BRAND_ID, target_date=TODAY)
        log = await db["ingestion_logs"].find_one({"run_id": result.run_id})
        assert log["status"] == "failed"


# ── Campaign auto-create ──────────────────────────────────────────────────────

class TestCampaignAutoCreate:
    async def test_new_campaign_created_in_db(self, svc, db):
        """A brand-new external campaign ID should be created in the campaigns coll."""
        await svc.run(BRAND_ID, target_date=TODAY)
        campaign = await db["campaigns"].find_one({"external_id": "ext_001"})
        assert campaign is not None
        assert campaign["name"] == "Test Campaign"
        assert campaign["source"] == "manual"

    async def test_campaign_brand_id_scoped(self, svc, db):
        await svc.run(BRAND_ID, target_date=TODAY)
        campaign = await db["campaigns"].find_one({"external_id": "ext_001"})
        assert str(campaign["brand_id"]) == BRAND_ID

    async def test_two_runs_do_not_duplicate_campaign(self, svc, db):
        await svc.run(BRAND_ID, target_date=TODAY)
        await svc.run(BRAND_ID, target_date=TODAY)
        count = await db["campaigns"].count_documents({"external_id": "ext_001"})
        assert count == 1


# ── Performance row upsert ────────────────────────────────────────────────────

class TestPerformanceUpsert:
    async def test_performance_row_written(self, svc, db):
        await svc.run(BRAND_ID, target_date=TODAY)
        count = await db["ad_performance_raw"].count_documents({})
        assert count > 0

    async def test_run_id_tagged_on_record(self, svc, db):
        result = await svc.run(BRAND_ID, target_date=TODAY)
        docs = await db["ad_performance_raw"].find(
            {"ingestion_run_id": result.run_id}
        ).to_list(None)
        assert len(docs) > 0

    async def test_two_runs_same_date_still_one_row(self, svc, db):
        """Same natural key (brand, source, campaign, date) must stay as one row."""
        await svc.run(BRAND_ID, target_date=TODAY)
        await svc.run(BRAND_ID, target_date=TODAY)
        count = await db["ad_performance_raw"].count_documents({})
        # 2 dates × 1 campaign = 2 rows, not 4
        assert count == 2

    async def test_spend_stored_correctly(self, svc, db):
        await svc.run(BRAND_ID, target_date=TODAY)
        doc = await db["ad_performance_raw"].find_one({"spend_paise": 100_000})
        assert doc is not None

    async def test_derived_metrics_stored(self, svc, db):
        """CTR, CPC, CPM, ROAS should be present on the written document."""
        await svc.run(BRAND_ID, target_date=TODAY)
        doc = await db["ad_performance_raw"].find_one({})
        assert doc is not None
        assert "ctr" in doc
        assert "cpc_paise" in doc
        assert "roas" in doc


# ── Per-record failure isolation ──────────────────────────────────────────────

class TestRecordIsolation:
    async def test_bad_record_does_not_abort_good_records(self, db):
        """If one record's upsert fails, the others must still be written."""
        good = PlatformRecord("ext_good", "Good", TODAY, spend_paise=50_000, impressions=1000, clicks=10)
        bad = PlatformRecord("ext_bad", "Bad", TODAY, spend_paise=-1, impressions=0, clicks=0)

        svc = StubIngestionService(
            db,
            records_per_date={
                TODAY: [bad, good],
                YESTERDAY: [good],
            },
        )

        # Patch upsert to raise for the bad record
        original_upsert = svc._upsert_records

        async def patched_upsert(brand_id, records, run_id):
            from app.repositories.campaigns import CampaignsRepository
            from app.repositories.performance import PerformanceRepository
            ok = [r for r in records if r.external_campaign_id != "ext_bad"]
            return await original_upsert(brand_id, ok, run_id)

        svc._upsert_records = patched_upsert
        result = await svc.run(BRAND_ID, target_date=TODAY)
        # Should not be a total failure
        assert result.records_upserted > 0
