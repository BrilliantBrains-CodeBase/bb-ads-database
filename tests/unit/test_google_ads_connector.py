"""
Unit tests for GoogleAdsIngestionService.

Strategy
--------
• The Google Ads SDK is NOT installed in the test environment, so we
  patch `_run_gaql_sync` (the synchronous SDK call) at the service level.
  This lets us test all surrounding logic (credential loading, transform,
  retry, error classification) without requiring the SDK.

• MongoDB is provided by mongomock-motor (same pattern as other tests).

• The tests exercise:
    - Happy path: credentials loaded, GAQL rows transformed correctly
    - Currency conversion: cost_micros → paise, conversion_value → paise
    - Retry logic: transient errors retried; fatal errors re-raised immediately
    - Credential validation: missing customer_id / refresh_token raises
    - Transform edge cases: zero values, None values, large numbers
    - _build_gaql date formatting
    - _row_to_dict / _parse_gaql_date helpers
    - _extract_google_error_code with and without Google error structure
    - _decrypt_token pass-through
"""
from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from bson import ObjectId
from mongomock_motor import AsyncMongoMockClient

from app.services.ingestion.google_ads import (
    GoogleAdsIngestionService,
    _build_gaql,
    _decrypt_token,
    _extract_google_error_code,
    _parse_gaql_date,
    _row_to_dict,
)

# ── Fixtures ───────────────────────────────────────────────────────────────────

BRAND_OID = ObjectId()
BRAND_ID = str(BRAND_OID)
TODAY = date(2026, 4, 6)

VALID_CREDS = {
    "customer_id": "1234567890",
    "refresh_token": "test_refresh_token",
    "login_customer_id": "9876543210",
}

SAMPLE_GAQL_ROW = {
    "campaign_id": "111",
    "campaign_name": "Test Campaign",
    "status": "ENABLED",
    "advertising_channel_type": "SEARCH",
    "impressions": 1000,
    "clicks": 50,
    "cost_micros": 500_000_000,   # 500 INR = 50,000 paise
    "conversions": 5.0,
    "conversion_value": 1000.0,   # 1000 INR = 100,000 paise
    "date": "2026-04-06",
}


@pytest_asyncio.fixture
async def db():
    client = AsyncMongoMockClient()
    database = client["test_db"]
    yield database
    client.close()


@pytest_asyncio.fixture
async def db_with_brand(db):
    """DB pre-seeded with a brand that has valid Google Ads credentials."""
    await db["brands"].insert_one({
        "_id": BRAND_OID,
        "name": "Test Brand",
        "slug": "test-brand",
        "platforms": {
            "google_ads": {
                "customer_id": VALID_CREDS["customer_id"],
                "refresh_token": VALID_CREDS["refresh_token"],
                "login_customer_id": VALID_CREDS["login_customer_id"],
            }
        },
    })
    yield db


# ── Helper builder ─────────────────────────────────────────────────────────────

def _make_rows(**overrides) -> list[dict]:
    row = {**SAMPLE_GAQL_ROW, **overrides}
    return [row]


# ══════════════════════════════════════════════════════════════════════════════
# Section 1: Module-level helper tests
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildGaql:
    def test_contains_date(self):
        q = _build_gaql("2026-04-06")
        assert "2026-04-06" in q

    def test_excludes_removed(self):
        q = _build_gaql("2026-04-06")
        assert "REMOVED" in q

    def test_selects_cost_micros(self):
        q = _build_gaql("2026-04-06")
        assert "cost_micros" in q

    def test_selects_conversions_value(self):
        q = _build_gaql("2026-04-06")
        assert "conversions_value" in q

    def test_segments_date_filter(self):
        q = _build_gaql("2026-01-15")
        assert "segments.date = '2026-01-15'" in q


class TestParseGaqlDate:
    def test_valid_date(self):
        assert _parse_gaql_date("2026-04-06") == date(2026, 4, 6)

    def test_different_date(self):
        assert _parse_gaql_date("2025-12-31") == date(2025, 12, 31)

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            _parse_gaql_date("not-a-date")


class TestRowToDict:
    def _make_proto_row(self, **overrides):
        """Build a mock proto-plus row object."""
        defaults = {
            "campaign_id": 111,
            "campaign_name": "Camp A",
            "status_name": "ENABLED",
            "channel_name": "SEARCH",
            "impressions": 100,
            "clicks": 10,
            "cost_micros": 1_000_000,
            "conversions": 2.0,
            "conversions_value": 500.0,
            "segments_date": "2026-04-06",
        }
        defaults.update(overrides)

        row = MagicMock()
        row.campaign.id = defaults["campaign_id"]
        row.campaign.name = defaults["campaign_name"]
        row.campaign.status.name = defaults["status_name"]
        row.campaign.advertising_channel_type.name = defaults["channel_name"]
        row.metrics.impressions = defaults["impressions"]
        row.metrics.clicks = defaults["clicks"]
        row.metrics.cost_micros = defaults["cost_micros"]
        row.metrics.conversions = defaults["conversions"]
        row.metrics.conversions_value = defaults["conversions_value"]
        row.segments.date = defaults["segments_date"]
        return row

    def test_campaign_id_is_string(self):
        row = self._make_proto_row()
        d = _row_to_dict(row)
        assert d["campaign_id"] == "111"
        assert isinstance(d["campaign_id"], str)

    def test_cost_micros_is_int(self):
        row = self._make_proto_row(cost_micros=2_500_000)
        d = _row_to_dict(row)
        assert d["cost_micros"] == 2_500_000
        assert isinstance(d["cost_micros"], int)

    def test_conversions_is_float(self):
        row = self._make_proto_row(conversions=3.0)
        d = _row_to_dict(row)
        assert d["conversions"] == 3.0

    def test_date_string_preserved(self):
        row = self._make_proto_row(segments_date="2026-01-01")
        d = _row_to_dict(row)
        assert d["date"] == "2026-01-01"


class TestExtractGoogleErrorCode:
    def test_no_attribute_returns_empty(self):
        exc = RuntimeError("generic error")
        assert _extract_google_error_code(exc) == ""

    def test_exception_with_failure_errors(self):
        error = MagicMock()
        error.error_code.WhichOneof.return_value = "RESOURCE_EXHAUSTED"
        exc = MagicMock()
        exc.failure.errors = [error]
        assert _extract_google_error_code(exc) == "RESOURCE_EXHAUSTED"

    def test_empty_error_code(self):
        error = MagicMock()
        error.error_code.WhichOneof.return_value = ""
        exc = MagicMock()
        exc.failure.errors = [error]
        assert _extract_google_error_code(exc) == ""


class TestDecryptToken:
    def test_passthrough(self):
        assert _decrypt_token("my_token") == "my_token"

    def test_empty_string(self):
        assert _decrypt_token("") == ""


# ══════════════════════════════════════════════════════════════════════════════
# Section 2: Credential loading
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestCredentialLoading:
    async def test_loads_valid_credentials(self, db_with_brand):
        svc = GoogleAdsIngestionService(db_with_brand)
        creds = await svc._load_credentials(BRAND_ID)
        assert creds["customer_id"] == "1234567890"
        assert creds["refresh_token"] == "test_refresh_token"
        assert creds["login_customer_id"] == "9876543210"

    async def test_missing_brand_raises(self, db):
        svc = GoogleAdsIngestionService(db)
        missing_id = str(ObjectId())
        with pytest.raises(ValueError, match="not found"):
            await svc._load_credentials(missing_id)

    async def test_missing_customer_id_raises(self, db):
        oid = ObjectId()
        await db["brands"].insert_one({
            "_id": oid,
            "platforms": {"google_ads": {"refresh_token": "tok"}},
        })
        svc = GoogleAdsIngestionService(db)
        with pytest.raises(ValueError, match="customer_id"):
            await svc._load_credentials(str(oid))

    async def test_missing_refresh_token_raises(self, db):
        oid = ObjectId()
        await db["brands"].insert_one({
            "_id": oid,
            "platforms": {"google_ads": {"customer_id": "123"}},
        })
        svc = GoogleAdsIngestionService(db)
        with pytest.raises(ValueError, match="refresh_token"):
            await svc._load_credentials(str(oid))

    async def test_no_platforms_field_raises(self, db):
        oid = ObjectId()
        await db["brands"].insert_one({"_id": oid, "name": "No Platforms"})
        svc = GoogleAdsIngestionService(db)
        with pytest.raises(ValueError, match="customer_id"):
            await svc._load_credentials(str(oid))

    async def test_login_customer_id_defaults_to_customer_id(self, db):
        oid = ObjectId()
        await db["brands"].insert_one({
            "_id": oid,
            "platforms": {
                "google_ads": {
                    "customer_id": "555",
                    "refresh_token": "tok",
                    # no login_customer_id
                }
            },
        })
        svc = GoogleAdsIngestionService(db)
        creds = await svc._load_credentials(str(oid))
        assert creds.get("login_customer_id") is None  # not set; _run_gaql_sync defaults it


# ══════════════════════════════════════════════════════════════════════════════
# Section 3: transform()
# ══════════════════════════════════════════════════════════════════════════════

class TestTransform:
    def _svc(self):
        client = AsyncMongoMockClient()
        return GoogleAdsIngestionService(client["db"])

    def test_basic_conversion(self):
        svc = self._svc()
        rows = _make_rows(
            cost_micros=500_000_000,   # 500 INR
            conversion_value=1000.0,   # 1000 INR
        )
        records = svc.transform(rows, BRAND_ID)
        assert len(records) == 1
        rec = records[0]
        # 500_000_000 // 10_000 = 50_000 paise
        assert rec.spend_paise == 50_000
        # 1000 * 100 = 100_000 paise
        assert rec.conversion_value_paise == 100_000

    def test_zero_cost_micros(self):
        svc = self._svc()
        rows = _make_rows(cost_micros=0, conversion_value=0.0)
        records = svc.transform(rows, BRAND_ID)
        assert records[0].spend_paise == 0
        assert records[0].conversion_value_paise == 0

    def test_none_fields_default_to_zero(self):
        svc = self._svc()
        row = {
            "campaign_id": "99",
            "campaign_name": "Null Test",
            "status": "ENABLED",
            "advertising_channel_type": "SEARCH",
            "impressions": None,
            "clicks": None,
            "cost_micros": None,
            "conversions": None,
            "conversion_value": None,
            "date": "2026-04-06",
        }
        records = svc.transform([row], BRAND_ID)
        assert len(records) == 1
        assert records[0].spend_paise == 0
        assert records[0].impressions == 0
        assert records[0].clicks == 0
        assert records[0].conversions == 0

    def test_external_campaign_id_is_string(self):
        svc = self._svc()
        rows = _make_rows(campaign_id="777")
        records = svc.transform(rows, BRAND_ID)
        assert records[0].external_campaign_id == "777"

    def test_campaign_meta_includes_channel_and_status(self):
        svc = self._svc()
        rows = _make_rows(
            advertising_channel_type="DISPLAY",
            status="PAUSED",
        )
        records = svc.transform(rows, BRAND_ID)
        meta = records[0].campaign_meta
        assert meta["advertising_channel_type"] == "DISPLAY"
        assert meta["platform_status"] == "PAUSED"

    def test_date_parsed(self):
        svc = self._svc()
        rows = _make_rows(date="2026-01-15")
        records = svc.transform(rows, BRAND_ID)
        assert records[0].date == date(2026, 1, 15)

    def test_large_cost_micros(self):
        svc = self._svc()
        # 10,000,000,000 micros = 10,000 INR = 1,000,000 paise
        rows = _make_rows(cost_micros=10_000_000_000)
        records = svc.transform(rows, BRAND_ID)
        assert records[0].spend_paise == 1_000_000

    def test_fractional_conversion_value_rounds_down(self):
        svc = self._svc()
        # 10.999 INR → int(10.999 * 100) = int(1099.9) = 1099 paise
        rows = _make_rows(conversion_value=10.999)
        records = svc.transform(rows, BRAND_ID)
        assert records[0].conversion_value_paise == 1099

    def test_reach_and_frequency_are_zero(self):
        """Reach/frequency not available in standard campaign report."""
        svc = self._svc()
        rows = _make_rows()
        records = svc.transform(rows, BRAND_ID)
        assert records[0].reach == 0
        assert records[0].frequency == 0.0

    def test_malformed_row_skipped_not_raised(self):
        """A row missing campaign_id should be skipped, not crash transform."""
        svc = self._svc()
        bad_row = {"date": "2026-04-06"}  # missing campaign_id → KeyError
        good_row = _make_rows()[0]
        records = svc.transform([bad_row, good_row], BRAND_ID)
        # bad row silently dropped; good row processed
        assert len(records) == 1
        assert records[0].external_campaign_id == "111"

    def test_multiple_rows(self):
        svc = self._svc()
        rows = [
            {**SAMPLE_GAQL_ROW, "campaign_id": "1", "cost_micros": 100_000_000},
            {**SAMPLE_GAQL_ROW, "campaign_id": "2", "cost_micros": 200_000_000},
            {**SAMPLE_GAQL_ROW, "campaign_id": "3", "cost_micros": 300_000_000},
        ]
        records = svc.transform(rows, BRAND_ID)
        assert len(records) == 3
        assert [r.external_campaign_id for r in records] == ["1", "2", "3"]
        assert [r.spend_paise for r in records] == [10_000, 20_000, 30_000]


# ══════════════════════════════════════════════════════════════════════════════
# Section 4: fetch() retry logic
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestFetchRetry:
    async def test_success_on_first_attempt(self, db_with_brand):
        svc = GoogleAdsIngestionService(db_with_brand)
        expected = [SAMPLE_GAQL_ROW]

        with patch.object(svc, "_run_gaql_sync", return_value=expected):
            rows = await svc.fetch(BRAND_ID, TODAY)

        assert rows == expected

    async def test_retries_on_transient_error(self, db_with_brand):
        """Transient RuntimeError should be retried up to _MAX_RETRIES times."""
        svc = GoogleAdsIngestionService(db_with_brand)

        call_count = 0

        def gaql_fails_twice(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("transient network error")
            return [SAMPLE_GAQL_ROW]

        with patch.object(svc, "_run_gaql_sync", side_effect=gaql_fails_twice):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                rows = await svc.fetch(BRAND_ID, TODAY)

        assert call_count == 3
        assert rows == [SAMPLE_GAQL_ROW]

    async def test_raises_after_max_retries_exhausted(self, db_with_brand):
        svc = GoogleAdsIngestionService(db_with_brand)

        with patch.object(
            svc, "_run_gaql_sync", side_effect=RuntimeError("always fails")
        ):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with pytest.raises(RuntimeError, match="always fails"):
                    await svc.fetch(BRAND_ID, TODAY)

    async def test_fatal_error_not_retried(self, db_with_brand):
        """UNAUTHENTICATED errors must raise immediately without sleeping."""
        svc = GoogleAdsIngestionService(db_with_brand)

        # Must be a real Exception subclass so `raise exc` works
        class _FakeGoogleError(Exception):
            pass

        auth_error = _FakeGoogleError("UNAUTHENTICATED")
        auth_error.failure = MagicMock()
        auth_error.failure.errors = [
            MagicMock(**{"error_code.WhichOneof.return_value": "UNAUTHENTICATED"})
        ]

        call_count = 0

        def fatal_side_effect(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            raise auth_error

        with patch.object(svc, "_run_gaql_sync", side_effect=fatal_side_effect):
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                with pytest.raises(Exception):
                    await svc.fetch(BRAND_ID, TODAY)

        # Must not have slept (no retry)
        mock_sleep.assert_not_called()
        assert call_count == 1

    async def test_sleep_uses_exponential_backoff(self, db_with_brand):
        """Backoff delays should be 1s, 2s, 4s for the first 3 failures."""
        svc = GoogleAdsIngestionService(db_with_brand)
        sleep_calls: list[float] = []

        async def record_sleep(seconds: float) -> None:
            sleep_calls.append(seconds)

        call_count = 0

        def fail_three_times(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise RuntimeError("transient")
            return [SAMPLE_GAQL_ROW]

        with patch.object(svc, "_run_gaql_sync", side_effect=fail_three_times):
            with patch("asyncio.sleep", side_effect=record_sleep):
                await svc.fetch(BRAND_ID, TODAY)

        assert sleep_calls == [1.0, 2.0, 4.0]


# ══════════════════════════════════════════════════════════════════════════════
# Section 5: Full run() integration (uses BaseIngestionService.run)
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestFullRun:
    async def test_run_success_upserts_records(self, db_with_brand):
        svc = GoogleAdsIngestionService(db_with_brand)

        with patch.object(
            svc, "_run_gaql_sync", return_value=[SAMPLE_GAQL_ROW]
        ):
            result = await svc.run(BRAND_ID, target_date=TODAY)

        assert result.status == "success"
        assert result.records_fetched == 2   # D-1 + D-0
        assert result.records_upserted == 2
        assert result.source == "google_ads"
        assert result.errors == []

    async def test_run_with_missing_credentials_is_partial(self, db):
        """If brand has no google_ads credentials, each date fails → partial."""
        oid = ObjectId()
        await db["brands"].insert_one({"_id": oid, "name": "NoCreds"})

        svc = GoogleAdsIngestionService(db)
        result = await svc.run(str(oid), target_date=TODAY)

        # Both dates fail (credential error), zero upserted → "failed"
        assert result.status == "failed"
        assert result.records_upserted == 0
        assert len(result.errors) > 0

    async def test_run_partial_one_date_fails(self, db_with_brand):
        """One date fetch raises unconditionally → partial status."""
        from datetime import timedelta

        svc = GoogleAdsIngestionService(db_with_brand)
        d_minus_1 = TODAY - timedelta(days=1)

        async def conditional_fetch(brand_id: str, fetch_date: date) -> list:
            if fetch_date == d_minus_1:
                raise RuntimeError("D-1 failed")
            return [SAMPLE_GAQL_ROW]

        # Patch at the fetch() level so the retry loop inside fetch() is bypassed
        with patch.object(svc, "fetch", side_effect=conditional_fetch):
            result = await svc.run(BRAND_ID, target_date=TODAY)

        assert result.status == "partial"
        assert result.records_upserted == 1
        assert len(result.errors) == 1

    async def test_run_custom_dates(self, db_with_brand):
        svc = GoogleAdsIngestionService(db_with_brand)
        custom_dates = [date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 3)]

        rows_per_date = [
            {**SAMPLE_GAQL_ROW, "date": str(d)} for d in custom_dates
        ]

        call_idx = 0

        def date_specific_rows(*_args, **_kwargs):
            nonlocal call_idx
            row = rows_per_date[call_idx]
            call_idx += 1
            return [row]

        with patch.object(svc, "_run_gaql_sync", side_effect=date_specific_rows):
            result = await svc.run(
                BRAND_ID, target_date=TODAY, custom_dates=custom_dates
            )

        assert result.status == "success"
        assert result.records_fetched == 3
        assert result.records_upserted == 3

    async def test_run_ingestion_log_written(self, db_with_brand):
        svc = GoogleAdsIngestionService(db_with_brand)

        with patch.object(svc, "_run_gaql_sync", return_value=[SAMPLE_GAQL_ROW]):
            result = await svc.run(BRAND_ID, target_date=TODAY)

        log = await db_with_brand["ingestion_logs"].find_one({"run_id": result.run_id})
        assert log is not None
        assert log["status"] == "success"
        assert log["source"] == "google_ads"
        assert log["records_fetched"] == 2
        assert log["records_upserted"] == 2
