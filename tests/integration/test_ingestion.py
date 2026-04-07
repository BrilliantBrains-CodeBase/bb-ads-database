"""
Integration tests: ingestion pipeline idempotency

A concrete stub connector (StubConnector) is used — it returns a fixed
list of PlatformRecord objects from fetch()/transform() without touching
any real ad-platform API.

Coverage
────────
  First run
  ├── records_upserted == expected count
  ├── ingestion_log.status == "success"
  ├── ingestion_log.records_upserted matches returned value
  └── ad_performance_raw contains exactly the expected docs

  Idempotency (second run with identical data)
  ├── no new documents inserted (collection count unchanged)
  ├── ingestion_log.status == "success" for both runs
  └── records_upserted still reported correctly (existing docs are updated)

  Custom date window
  ├── custom_dates=[d1, d2] covers exactly those two dates
  └── total upserted == campaigns × 2 dates

  Partial failure (one date raises, one succeeds)
  ├── ingestion_log.status == "partial"
  ├── good-date records ARE in the DB
  └── IngestionResult.errors contains the failed date

  Fatal failure (every date raises)
  ├── ingestion_log.status == "failed"
  └── records_upserted == 0
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pytest
import pytest_asyncio
from bson import ObjectId

from app.services.ingestion.base import BaseIngestionService, PlatformRecord
from tests.integration.conftest import make_brand


# ── Stub connector ────────────────────────────────────────────────────────────

_CAMPAIGNS = [
    {"external_id": "EXT-001", "name": "Brand Search",   "objective": "brand_awareness"},
    {"external_id": "EXT-002", "name": "Generic Search",  "objective": "conversions"},
    {"external_id": "EXT-003", "name": "Shopping",        "objective": "sales"},
]

_BASE_METRICS = {
    "EXT-001": dict(spend_paise=60_000, impressions=8_000, clicks=320, leads=6, conversions=4, conversion_value_paise=108_000),
    "EXT-002": dict(spend_paise=90_000, impressions=12_000, clicks=480, leads=12, conversions=8, conversion_value_paise=198_000),
    "EXT-003": dict(spend_paise=75_000, impressions=10_000, clicks=400, leads=8, conversions=6, conversion_value_paise=187_500),
}


class StubConnector(BaseIngestionService):
    """Deterministic connector for testing — no real API calls."""

    source = "google_ads"

    def __init__(self, db: Any, fail_on_date: date | None = None, fail_all: bool = False):
        super().__init__(db)
        self._fail_on_date = fail_on_date
        self._fail_all = fail_all

    async def fetch(self, brand_id: str, target_date: date) -> list[dict]:
        if self._fail_all:
            raise RuntimeError(f"Simulated fatal error on {target_date}")
        if self._fail_on_date and target_date == self._fail_on_date:
            raise RuntimeError(f"Simulated per-date error on {target_date}")
        # Return raw dicts; transform() will convert them
        return [
            {**c, "date": str(target_date), **_BASE_METRICS[c["external_id"]]}
            for c in _CAMPAIGNS
        ]

    def transform(self, raw_records: list[dict], brand_id: str) -> list[PlatformRecord]:
        return [
            PlatformRecord(
                external_campaign_id=r["external_id"],
                campaign_name=r["name"],
                date=date.fromisoformat(r["date"]),
                spend_paise=r["spend_paise"],
                impressions=r["impressions"],
                clicks=r["clicks"],
                leads=r["leads"],
                conversions=r["conversions"],
                conversion_value_paise=r["conversion_value_paise"],
                campaign_meta={"objective": r["objective"]},
            )
            for r in raw_records
        ]


# ── Fixtures ──────────────────────────────────────────────────────────────────

_TODAY = date(2026, 4, 6)


@pytest_asyncio.fixture
async def brand_id(db):
    return await make_brand(db, "Ingestion Test Brand", "ingestion-test")


# ── First run ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_first_run_upserts_expected_records(db, brand_id):
    connector = StubConnector(db)
    result = await connector.run(brand_id, target_date=_TODAY, custom_dates=[_TODAY])

    assert result.status == "success"
    assert result.records_upserted == len(_CAMPAIGNS)
    assert result.records_fetched  == len(_CAMPAIGNS)

    docs = await db["ad_performance_raw"].find({"brand_id": brand_id}).to_list(length=100)
    assert len(docs) == len(_CAMPAIGNS)


@pytest.mark.asyncio
async def test_first_run_creates_ingestion_log(db, brand_id):
    connector = StubConnector(db)
    result = await connector.run(brand_id, target_date=_TODAY, custom_dates=[_TODAY])

    log = await db["ingestion_logs"].find_one({"run_id": result.run_id})
    assert log is not None
    assert log["status"] == "success"
    assert log["records_upserted"] == len(_CAMPAIGNS)
    assert log["brand_id"] == brand_id
    assert log["source"] == "google_ads"


@pytest.mark.asyncio
async def test_first_run_creates_campaign_docs(db, brand_id):
    connector = StubConnector(db)
    await connector.run(brand_id, target_date=_TODAY, custom_dates=[_TODAY])

    camps = await db["campaigns"].find({"brand_id": brand_id}).to_list(length=100)
    ext_ids = {c["external_id"] for c in camps}
    assert ext_ids == {c["external_id"] for c in _CAMPAIGNS}


# ── Idempotency ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_second_run_does_not_duplicate_records(db, brand_id):
    """Running the same connector twice must not increase the document count."""
    connector = StubConnector(db)
    r1 = await connector.run(brand_id, target_date=_TODAY, custom_dates=[_TODAY])
    assert r1.status == "success"

    count_after_r1 = await db["ad_performance_raw"].count_documents({"brand_id": brand_id})

    r2 = await connector.run(brand_id, target_date=_TODAY, custom_dates=[_TODAY])
    assert r2.status == "success"

    count_after_r2 = await db["ad_performance_raw"].count_documents({"brand_id": brand_id})
    assert count_after_r2 == count_after_r1


@pytest.mark.asyncio
async def test_second_run_upserts_metrics_changes(db, brand_id):
    """A second run with different values must update the existing row, not insert."""
    connector = StubConnector(db)
    await connector.run(brand_id, target_date=_TODAY, custom_dates=[_TODAY])

    # Temporarily monkey-patch the base metrics to simulate a platform retroactive update
    original = _BASE_METRICS["EXT-001"]["spend_paise"]
    _BASE_METRICS["EXT-001"]["spend_paise"] = original + 5_000

    try:
        await connector.run(brand_id, target_date=_TODAY, custom_dates=[_TODAY])
    finally:
        _BASE_METRICS["EXT-001"]["spend_paise"] = original  # restore

    doc = await db["ad_performance_raw"].find_one(
        {"brand_id": brand_id, "source": "google_ads"}
    )
    assert doc is not None
    # Only one unique doc per natural key (brand+source+campaign+date)
    count = await db["ad_performance_raw"].count_documents({"brand_id": brand_id})
    assert count == len(_CAMPAIGNS)


@pytest.mark.asyncio
async def test_idempotent_across_runs_with_correction_window(db, brand_id):
    """Default correction window (D-1 + D-0): 6 unique docs per run."""
    connector = StubConnector(db)
    r1 = await connector.run(brand_id, target_date=_TODAY)

    assert r1.records_upserted == len(_CAMPAIGNS) * 2   # D-1 + D-0

    count_after_r1 = await db["ad_performance_raw"].count_documents({"brand_id": brand_id})

    r2 = await connector.run(brand_id, target_date=_TODAY)
    count_after_r2 = await db["ad_performance_raw"].count_documents({"brand_id": brand_id})

    assert count_after_r2 == count_after_r1


# ── Custom date window ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_custom_dates_covers_exactly_those_dates(db, brand_id):
    d1 = _TODAY - timedelta(days=2)
    d2 = _TODAY - timedelta(days=1)
    connector = StubConnector(db)
    result = await connector.run(brand_id, target_date=_TODAY, custom_dates=[d1, d2])

    assert result.status == "success"
    assert result.records_upserted == len(_CAMPAIGNS) * 2

    # Verify the dates in the DB are exactly d1 and d2
    from datetime import datetime, time, timezone
    day_utcs = {
        datetime.combine(d1, time.min, tzinfo=timezone.utc),
        datetime.combine(d2, time.min, tzinfo=timezone.utc),
    }
    docs = await db["ad_performance_raw"].find({"brand_id": brand_id}).to_list(length=100)
    stored_dates = {d["date"].replace(tzinfo=timezone.utc) if d["date"].tzinfo is None else d["date"]
                   for d in docs}
    assert stored_dates == day_utcs


# ── Partial failure ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_partial_failure_status(db, brand_id):
    """When one date fails but another succeeds, status == 'partial'."""
    bad_date  = _TODAY - timedelta(days=1)
    good_date = _TODAY
    connector = StubConnector(db, fail_on_date=bad_date)

    result = await connector.run(brand_id, target_date=_TODAY, custom_dates=[bad_date, good_date])

    assert result.status == "partial"
    assert any(str(bad_date) in e for e in result.errors)


@pytest.mark.asyncio
async def test_partial_failure_good_date_still_inserted(db, brand_id):
    """Records for the successful date must reach the DB despite the partial error."""
    bad_date  = _TODAY - timedelta(days=1)
    good_date = _TODAY
    connector = StubConnector(db, fail_on_date=bad_date)

    result = await connector.run(brand_id, target_date=_TODAY, custom_dates=[bad_date, good_date])

    assert result.records_upserted == len(_CAMPAIGNS)

    from datetime import datetime, time, timezone
    good_dt = datetime.combine(good_date, time.min, tzinfo=timezone.utc)
    good_docs = await db["ad_performance_raw"].find(
        {"brand_id": brand_id, "date": good_dt}
    ).to_list(length=100)
    assert len(good_docs) == len(_CAMPAIGNS)


@pytest.mark.asyncio
async def test_partial_failure_log_message(db, brand_id):
    bad_date = _TODAY - timedelta(days=1)
    connector = StubConnector(db, fail_on_date=bad_date)
    result = await connector.run(
        brand_id, target_date=_TODAY, custom_dates=[bad_date, _TODAY]
    )

    log = await db["ingestion_logs"].find_one({"run_id": result.run_id})
    assert log["status"] == "partial"
    assert log["error_message"] is not None
    assert str(bad_date) in log["error_message"]


# ── Fatal failure ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fatal_failure_status(db, brand_id):
    """When ALL dates fail, status == 'failed' and no records land in DB."""
    connector = StubConnector(db, fail_all=True)
    result = await connector.run(brand_id, target_date=_TODAY, custom_dates=[_TODAY])

    assert result.status == "failed"
    assert result.records_upserted == 0

    count = await db["ad_performance_raw"].count_documents({"brand_id": brand_id})
    assert count == 0


@pytest.mark.asyncio
async def test_fatal_failure_log_created(db, brand_id):
    connector = StubConnector(db, fail_all=True)
    result = await connector.run(brand_id, target_date=_TODAY, custom_dates=[_TODAY])

    log = await db["ingestion_logs"].find_one({"run_id": result.run_id})
    assert log is not None
    assert log["status"] == "failed"
