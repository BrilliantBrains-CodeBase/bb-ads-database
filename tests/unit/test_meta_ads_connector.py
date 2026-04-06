"""
Unit tests for MetaAdsIngestionService.

Strategy
--------
• The facebook-business SDK is NOT installed in the test environment, so we
  patch `_run_insights_sync` at the service level.

• MongoDB is provided by mongomock-motor.

• Tests cover:
    - Happy path: credentials loaded, Insights rows transformed correctly
    - Currency conversion: INR spend/conversion_value → paise
    - Non-INR currency: returns 0 paise + warning
    - Retry logic: transient errors retried; fatal codes raise immediately
    - Token expiry warning (logged, not raised)
    - Credential validation: missing access_token / ad_account_id raises
    - ad_account_id normalisation (act_ prefix)
    - Transform edge cases: zero values, None values, missing actions
    - _extract_action_value: 7d_click preference, fallback to value, missing
    - _to_paise: INR, non-INR, zero, None, invalid string
    - _parse_meta_date, _warn_if_token_expiring, _insight_to_dict
    - Full run() integration: success, failed, partial
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from bson import ObjectId
from mongomock_motor import AsyncMongoMockClient

from app.services.ingestion.meta_ads import (
    MetaAdsIngestionService,
    _extract_action_value,
    _extract_meta_error_code,
    _insight_to_dict,
    _parse_meta_date,
    _to_paise,
    _warn_if_token_expiring,
)

# ── Fixtures ───────────────────────────────────────────────────────────────────

BRAND_OID = ObjectId()
BRAND_ID = str(BRAND_OID)
TODAY = date(2026, 4, 6)

VALID_CREDS = {
    "access_token": "test_access_token",
    "ad_account_id": "act_1234567890",
    "currency": "INR",
}

SAMPLE_INSIGHT_ROW = {
    "campaign_id": "222",
    "campaign_name": "Meta Campaign",
    "date": "2026-04-06",
    "currency": "INR",
    "impressions": 2000,
    "clicks": 80,
    "spend": 500.0,       # 500 INR = 50,000 paise
    "reach": 1500,
    "frequency": 1.33,
    "leads": 10,
    "conversions": 5,
    "conversion_value": 2000.0,  # 2000 INR = 200,000 paise
}


@pytest_asyncio.fixture
async def db():
    client = AsyncMongoMockClient()
    database = client["test_db"]
    yield database
    client.close()


@pytest_asyncio.fixture
async def db_with_brand(db):
    """DB pre-seeded with a brand that has valid Meta credentials."""
    await db["brands"].insert_one({
        "_id": BRAND_OID,
        "name": "Meta Brand",
        "slug": "meta-brand",
        "platforms": {
            "meta_ads": {
                "access_token": VALID_CREDS["access_token"],
                "ad_account_id": VALID_CREDS["ad_account_id"],
                "currency": "INR",
            }
        },
    })
    yield db


# ══════════════════════════════════════════════════════════════════════════════
# Section 1: Module-level helpers
# ══════════════════════════════════════════════════════════════════════════════

class TestExtractActionValue:
    def test_returns_7d_click_when_present(self):
        actions = [{"action_type": "lead", "7d_click": "3", "value": "10"}]
        assert _extract_action_value(actions, "lead") == 3.0

    def test_falls_back_to_value(self):
        actions = [{"action_type": "lead", "value": "7"}]
        assert _extract_action_value(actions, "lead") == 7.0

    def test_missing_action_type_returns_zero(self):
        actions = [{"action_type": "other_action", "value": "5"}]
        assert _extract_action_value(actions, "lead") == 0.0

    def test_empty_list_returns_zero(self):
        assert _extract_action_value([], "lead") == 0.0

    def test_purchase_conversion(self):
        action_type = "offsite_conversion.fb_pixel_purchase"
        actions = [{"action_type": action_type, "7d_click": "2.5"}]
        assert _extract_action_value(actions, action_type) == 2.5

    def test_invalid_value_returns_zero(self):
        actions = [{"action_type": "lead", "value": "not_a_number"}]
        assert _extract_action_value(actions, "lead") == 0.0


class TestToPaise:
    def test_inr_converts_correctly(self):
        assert _to_paise(500.0, "INR", BRAND_ID, "spend") == 50_000

    def test_inr_lowercase(self):
        assert _to_paise(100.0, "inr", BRAND_ID, "spend") == 10_000

    def test_zero_returns_zero(self):
        assert _to_paise(0.0, "INR", BRAND_ID, "spend") == 0

    def test_none_returns_zero(self):
        assert _to_paise(None, "INR", BRAND_ID, "spend") == 0

    def test_non_inr_returns_zero(self):
        result = _to_paise(500.0, "USD", BRAND_ID, "spend")
        assert result == 0

    def test_fractional_inr(self):
        # 10.505 INR → int(10.505 * 100) = int(1050.5) = 1050 paise
        assert _to_paise(10.505, "INR", BRAND_ID, "spend") == 1050

    def test_string_amount(self):
        assert _to_paise("250.5", "INR", BRAND_ID, "spend") == 25_050

    def test_invalid_string_returns_zero(self):
        assert _to_paise("abc", "INR", BRAND_ID, "spend") == 0

    def test_integer_amount(self):
        assert _to_paise(1000, "INR", BRAND_ID, "spend") == 100_000


class TestParsMetaDate:
    def test_valid_date(self):
        assert _parse_meta_date("2026-04-06") == date(2026, 4, 6)

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            _parse_meta_date("06-04-2026")


class TestWarnIfTokenExpiring:
    def test_no_expiry_field_no_warning(self, caplog):
        creds = {"access_token": "tok"}
        _warn_if_token_expiring(creds, BRAND_ID)  # should not raise

    def test_expiring_soon_logs_warning(self, caplog):
        expires = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
        creds = {"token_expires_at": expires}
        import structlog
        # just ensure no exception is raised — structlog doesn't use caplog
        _warn_if_token_expiring(creds, BRAND_ID)

    def test_not_expiring_soon_no_warning(self):
        expires = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        creds = {"token_expires_at": expires}
        _warn_if_token_expiring(creds, BRAND_ID)  # should not raise

    def test_datetime_object_expiry(self):
        expires = datetime.now(timezone.utc) + timedelta(days=5)
        creds = {"token_expires_at": expires}
        _warn_if_token_expiring(creds, BRAND_ID)  # should not raise

    def test_malformed_expiry_does_not_raise(self):
        creds = {"token_expires_at": "not-a-date"}
        _warn_if_token_expiring(creds, BRAND_ID)  # should not raise


class TestInsightToDict:
    def _make_insight(self, **overrides):
        defaults = {
            "campaign_id": "222",
            "campaign_name": "Camp",
            "date_start": "2026-04-06",
            "impressions": "1000",
            "clicks": "50",
            "spend": "500.00",
            "reach": "800",
            "frequency": "1.25",
            "actions": [
                {"action_type": "lead", "7d_click": "5"},
                {"action_type": "offsite_conversion.fb_pixel_purchase", "7d_click": "3"},
            ],
            "action_values": [
                {"action_type": "offsite_conversion.fb_pixel_purchase", "7d_click": "1500.00"},
            ],
        }
        m = MagicMock()
        merged = {**defaults, **overrides}
        m.get = lambda k, default=None: merged.get(k, default)
        return m

    def test_leads_extracted(self):
        insight = self._make_insight()
        d = _insight_to_dict(insight, "INR")
        assert d["leads"] == 5

    def test_conversions_extracted(self):
        insight = self._make_insight()
        d = _insight_to_dict(insight, "INR")
        assert d["conversions"] == 3

    def test_conversion_value_extracted(self):
        insight = self._make_insight()
        d = _insight_to_dict(insight, "INR")
        assert d["conversion_value"] == 1500.0

    def test_no_actions_defaults_to_zero(self):
        insight = self._make_insight(actions=None, action_values=None)
        d = _insight_to_dict(insight, "INR")
        assert d["leads"] == 0
        assert d["conversions"] == 0
        assert d["conversion_value"] == 0.0

    def test_currency_included(self):
        insight = self._make_insight()
        d = _insight_to_dict(insight, "USD")
        assert d["currency"] == "USD"


class TestExtractMetaErrorCode:
    def test_no_attribute_returns_zero(self):
        exc = RuntimeError("generic")
        assert _extract_meta_error_code(exc) == 0

    def test_api_error_code_method(self):
        exc = MagicMock()
        exc.api_error_code.return_value = 190
        assert _extract_meta_error_code(exc) == 190

    def test_private_attribute_fallback(self):
        class _FakeError(Exception):
            _api_error_code = 17
        exc = _FakeError("rate limit")
        assert _extract_meta_error_code(exc) == 17


# ══════════════════════════════════════════════════════════════════════════════
# Section 2: Credential loading
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestCredentialLoading:
    async def test_loads_valid_credentials(self, db_with_brand):
        svc = MetaAdsIngestionService(db_with_brand)
        creds = await svc._load_credentials(BRAND_ID)
        assert creds["access_token"] == "test_access_token"
        assert creds["ad_account_id"] == "act_1234567890"
        assert creds["currency"] == "INR"

    async def test_act_prefix_added_when_missing(self, db):
        oid = ObjectId()
        await db["brands"].insert_one({
            "_id": oid,
            "platforms": {
                "meta_ads": {
                    "access_token": "tok",
                    "ad_account_id": "9999",  # no act_ prefix
                }
            },
        })
        svc = MetaAdsIngestionService(db)
        creds = await svc._load_credentials(str(oid))
        assert creds["ad_account_id"] == "act_9999"

    async def test_act_prefix_not_doubled(self, db):
        oid = ObjectId()
        await db["brands"].insert_one({
            "_id": oid,
            "platforms": {
                "meta_ads": {
                    "access_token": "tok",
                    "ad_account_id": "act_9999",
                }
            },
        })
        svc = MetaAdsIngestionService(db)
        creds = await svc._load_credentials(str(oid))
        assert creds["ad_account_id"] == "act_9999"

    async def test_currency_defaults_to_inr(self, db):
        oid = ObjectId()
        await db["brands"].insert_one({
            "_id": oid,
            "platforms": {
                "meta_ads": {
                    "access_token": "tok",
                    "ad_account_id": "act_1",
                    # no currency
                }
            },
        })
        svc = MetaAdsIngestionService(db)
        creds = await svc._load_credentials(str(oid))
        assert creds["currency"] == "INR"

    async def test_missing_brand_raises(self, db):
        svc = MetaAdsIngestionService(db)
        with pytest.raises(ValueError, match="not found"):
            await svc._load_credentials(str(ObjectId()))

    async def test_missing_access_token_raises(self, db):
        oid = ObjectId()
        await db["brands"].insert_one({
            "_id": oid,
            "platforms": {"meta_ads": {"ad_account_id": "act_1"}},
        })
        svc = MetaAdsIngestionService(db)
        with pytest.raises(ValueError, match="access_token"):
            await svc._load_credentials(str(oid))

    async def test_missing_ad_account_id_raises(self, db):
        oid = ObjectId()
        await db["brands"].insert_one({
            "_id": oid,
            "platforms": {"meta_ads": {"access_token": "tok"}},
        })
        svc = MetaAdsIngestionService(db)
        with pytest.raises(ValueError, match="ad_account_id"):
            await svc._load_credentials(str(oid))


# ══════════════════════════════════════════════════════════════════════════════
# Section 3: transform()
# ══════════════════════════════════════════════════════════════════════════════

class TestTransform:
    def _svc(self):
        client = AsyncMongoMockClient()
        return MetaAdsIngestionService(client["db"])

    def test_basic_inr_conversion(self):
        svc = self._svc()
        rows = [{**SAMPLE_INSIGHT_ROW}]
        records = svc.transform(rows, BRAND_ID)
        assert len(records) == 1
        rec = records[0]
        assert rec.spend_paise == 50_000       # 500 INR
        assert rec.conversion_value_paise == 200_000  # 2000 INR

    def test_impressions_clicks_reach_frequency(self):
        svc = self._svc()
        rows = [{**SAMPLE_INSIGHT_ROW}]
        records = svc.transform(rows, BRAND_ID)
        rec = records[0]
        assert rec.impressions == 2000
        assert rec.clicks == 80
        assert rec.reach == 1500
        assert rec.frequency == 1.33

    def test_leads_and_conversions(self):
        svc = self._svc()
        rows = [{**SAMPLE_INSIGHT_ROW}]
        records = svc.transform(rows, BRAND_ID)
        rec = records[0]
        assert rec.leads == 10
        assert rec.conversions == 5

    def test_non_inr_currency_returns_zero_paise(self):
        svc = self._svc()
        row = {**SAMPLE_INSIGHT_ROW, "currency": "USD"}
        records = svc.transform([row], BRAND_ID)
        assert records[0].spend_paise == 0
        assert records[0].conversion_value_paise == 0

    def test_zero_spend(self):
        svc = self._svc()
        row = {**SAMPLE_INSIGHT_ROW, "spend": 0.0}
        records = svc.transform([row], BRAND_ID)
        assert records[0].spend_paise == 0

    def test_none_fields_default_to_zero(self):
        svc = self._svc()
        row = {
            "campaign_id": "333",
            "campaign_name": "Null Cam",
            "date": "2026-04-06",
            "currency": "INR",
            "impressions": None,
            "clicks": None,
            "spend": None,
            "reach": None,
            "frequency": None,
            "leads": None,
            "conversions": None,
            "conversion_value": None,
        }
        records = svc.transform([row], BRAND_ID)
        assert len(records) == 1
        rec = records[0]
        assert rec.spend_paise == 0
        assert rec.impressions == 0

    def test_date_parsed(self):
        svc = self._svc()
        row = {**SAMPLE_INSIGHT_ROW, "date": "2026-01-20"}
        records = svc.transform([row], BRAND_ID)
        assert records[0].date == date(2026, 1, 20)

    def test_external_campaign_id_is_string(self):
        svc = self._svc()
        row = {**SAMPLE_INSIGHT_ROW, "campaign_id": 999}
        records = svc.transform([row], BRAND_ID)
        assert records[0].external_campaign_id == "999"

    def test_campaign_meta_includes_currency(self):
        svc = self._svc()
        rows = [{**SAMPLE_INSIGHT_ROW}]
        records = svc.transform(rows, BRAND_ID)
        assert records[0].campaign_meta["currency"] == "INR"

    def test_malformed_row_skipped(self):
        svc = self._svc()
        bad = {"date": "2026-04-06"}  # missing campaign_id
        good = {**SAMPLE_INSIGHT_ROW}
        records = svc.transform([bad, good], BRAND_ID)
        assert len(records) == 1

    def test_multiple_rows(self):
        svc = self._svc()
        rows = [
            {**SAMPLE_INSIGHT_ROW, "campaign_id": "1", "spend": 100.0},
            {**SAMPLE_INSIGHT_ROW, "campaign_id": "2", "spend": 200.0},
        ]
        records = svc.transform(rows, BRAND_ID)
        assert len(records) == 2
        assert [r.spend_paise for r in records] == [10_000, 20_000]


# ══════════════════════════════════════════════════════════════════════════════
# Section 4: fetch() retry logic
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestFetchRetry:
    async def test_success_on_first_attempt(self, db_with_brand):
        svc = MetaAdsIngestionService(db_with_brand)

        with patch.object(svc, "_run_insights_sync", return_value=[SAMPLE_INSIGHT_ROW]):
            rows = await svc.fetch(BRAND_ID, TODAY)

        assert rows == [SAMPLE_INSIGHT_ROW]

    async def test_retries_on_transient_error(self, db_with_brand):
        svc = MetaAdsIngestionService(db_with_brand)
        call_count = 0

        def fail_twice(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("transient")
            return [SAMPLE_INSIGHT_ROW]

        with patch.object(svc, "_run_insights_sync", side_effect=fail_twice):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                rows = await svc.fetch(BRAND_ID, TODAY)

        assert call_count == 3
        assert rows == [SAMPLE_INSIGHT_ROW]

    async def test_raises_after_max_retries(self, db_with_brand):
        svc = MetaAdsIngestionService(db_with_brand)

        with patch.object(svc, "_run_insights_sync", side_effect=RuntimeError("always")):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with pytest.raises(RuntimeError):
                    await svc.fetch(BRAND_ID, TODAY)

    async def test_fatal_oauth_error_not_retried(self, db_with_brand):
        svc = MetaAdsIngestionService(db_with_brand)

        class _OAuthError(Exception):
            def api_error_code(self):
                return 190

        call_count = 0

        def oauth_fail(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            raise _OAuthError("invalid token")

        with patch.object(svc, "_run_insights_sync", side_effect=oauth_fail):
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                with pytest.raises(Exception):
                    await svc.fetch(BRAND_ID, TODAY)

        mock_sleep.assert_not_called()
        assert call_count == 1

    async def test_exponential_backoff_delays(self, db_with_brand):
        svc = MetaAdsIngestionService(db_with_brand)
        sleep_calls: list[float] = []

        async def record_sleep(s: float) -> None:
            sleep_calls.append(s)

        call_count = 0

        def fail_three_times(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise RuntimeError("transient")
            return [SAMPLE_INSIGHT_ROW]

        with patch.object(svc, "_run_insights_sync", side_effect=fail_three_times):
            with patch("asyncio.sleep", side_effect=record_sleep):
                await svc.fetch(BRAND_ID, TODAY)

        assert sleep_calls == [1.0, 2.0, 4.0]


# ══════════════════════════════════════════════════════════════════════════════
# Section 5: Full run() integration
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestFullRun:
    async def test_run_success(self, db_with_brand):
        svc = MetaAdsIngestionService(db_with_brand)

        with patch.object(
            svc, "_run_insights_sync", return_value=[SAMPLE_INSIGHT_ROW]
        ):
            result = await svc.run(BRAND_ID, target_date=TODAY)

        assert result.status == "success"
        assert result.records_fetched == 2   # D-1 + D-0
        assert result.records_upserted == 2
        assert result.source == "meta"

    async def test_run_failed_when_no_credentials(self, db):
        oid = ObjectId()
        await db["brands"].insert_one({"_id": oid, "name": "NoCreds"})
        svc = MetaAdsIngestionService(db)
        result = await svc.run(str(oid), target_date=TODAY)
        assert result.status == "failed"
        assert result.records_upserted == 0

    async def test_run_partial_one_date_fails(self, db_with_brand):
        svc = MetaAdsIngestionService(db_with_brand)
        d_minus_1 = TODAY - timedelta(days=1)

        async def conditional_fetch(brand_id: str, fetch_date: date) -> list:
            if fetch_date == d_minus_1:
                raise RuntimeError("D-1 unavailable")
            return [SAMPLE_INSIGHT_ROW]

        with patch.object(svc, "fetch", side_effect=conditional_fetch):
            result = await svc.run(BRAND_ID, target_date=TODAY)

        assert result.status == "partial"
        assert result.records_upserted == 1

    async def test_run_ingestion_log_written(self, db_with_brand):
        svc = MetaAdsIngestionService(db_with_brand)

        with patch.object(
            svc, "_run_insights_sync", return_value=[SAMPLE_INSIGHT_ROW]
        ):
            result = await svc.run(BRAND_ID, target_date=TODAY)

        log = await db_with_brand["ingestion_logs"].find_one({"run_id": result.run_id})
        assert log is not None
        assert log["status"] == "success"
        assert log["source"] == "meta"
