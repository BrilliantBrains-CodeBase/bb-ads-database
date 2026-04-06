"""
Unit tests for CSVIngestionService and parse_csv().

Coverage:
  - parse_csv: happy path, empty file, missing headers, required columns,
    alias resolution, multiple date formats, BOM, encoding error
  - Field validation: negative values, invalid floats, invalid ints,
    float-as-int (e.g. "5.0"), comma-separated thousands
  - Currency conversion: INR rupees → paise
  - CSVIngestionService.fetch: payload not set, date filtering, validation errors
  - CSVIngestionService.transform: happy path, malformed raw rows
  - CSVIngestionService.ingest_csv: success, validation failure, multi-date
  - Full run() integration via ingest_csv
"""
from __future__ import annotations

import textwrap
from datetime import date

import pytest
import pytest_asyncio
from bson import ObjectId
from mongomock_motor import AsyncMongoMockClient

from app.services.ingestion.csv_upload import (
    CSVIngestionService,
    CSVParseResult,
    RowValidationError,
    _normalise_column,
    _parse_date,
    _parse_float,
    _parse_int,
    parse_csv,
)

# ── Fixtures ───────────────────────────────────────────────────────────────────

BRAND_OID = ObjectId()
BRAND_ID = str(BRAND_OID)
TODAY = date(2026, 4, 6)

MINIMAL_CSV = textwrap.dedent("""\
    campaign_id,campaign_name,date,spend,impressions,clicks
    camp1,Campaign One,2026-04-06,500.00,1000,50
""").encode()

FULL_CSV = textwrap.dedent("""\
    campaign_id,campaign_name,date,spend,impressions,clicks,reach,frequency,conversions,conversion_value,leads
    camp1,Campaign One,2026-04-06,500.00,1000,50,800,1.25,5,2000.00,10
    camp2,Campaign Two,2026-04-06,250.50,500,20,300,1.67,2,800.00,4
""").encode()

MULTI_DATE_CSV = textwrap.dedent("""\
    campaign_id,campaign_name,date,spend
    camp1,Camp One,2026-04-05,100.00
    camp1,Camp One,2026-04-06,200.00
""").encode()


@pytest_asyncio.fixture
async def db():
    client = AsyncMongoMockClient()
    database = client["test_db"]
    yield database
    client.close()


@pytest_asyncio.fixture
async def db_with_brand(db):
    await db["brands"].insert_one({"_id": BRAND_OID, "name": "Test Brand"})
    yield db


# ══════════════════════════════════════════════════════════════════════════════
# Section 1: _normalise_column
# ══════════════════════════════════════════════════════════════════════════════

class TestNormaliseColumn:
    def test_lowercase_and_strip(self):
        assert _normalise_column("  SPEND  ") == "spend"

    def test_alias_resolution(self):
        assert _normalise_column("Cost") == "spend"
        assert _normalise_column("amount_spent") == "spend"
        assert _normalise_column("Campaign ID") == "campaign_id"
        assert _normalise_column("day") == "date"
        assert _normalise_column("impr") == "impressions"
        assert _normalise_column("link_clicks") == "clicks"
        assert _normalise_column("lead_gen") == "leads"
        assert _normalise_column("conv") == "conversions"
        assert _normalise_column("revenue") == "conversion_value"

    def test_hyphen_to_underscore(self):
        assert _normalise_column("campaign-id") == "campaign_id"

    def test_unknown_column_passes_through(self):
        assert _normalise_column("custom_metric") == "custom_metric"


# ══════════════════════════════════════════════════════════════════════════════
# Section 2: _parse_date
# ══════════════════════════════════════════════════════════════════════════════

class TestParseDate:
    def test_iso_format(self):
        d, err = _parse_date("2026-04-06", 2)
        assert d == date(2026, 4, 6)
        assert err is None

    def test_dd_mm_yyyy(self):
        d, err = _parse_date("06-04-2026", 2)
        assert d == date(2026, 4, 6)
        assert err is None

    def test_slash_separator_dd_mm_yyyy(self):
        d, err = _parse_date("06/04/2026", 2)
        assert d == date(2026, 4, 6)
        assert err is None

    def test_slash_separator_yyyy_mm_dd(self):
        d, err = _parse_date("2026/04/06", 2)
        assert d == date(2026, 4, 6)
        assert err is None

    def test_empty_returns_error(self):
        d, err = _parse_date("", 2)
        assert d is None
        assert err is not None
        assert "required" in err.message

    def test_unrecognised_format_returns_error(self):
        d, err = _parse_date("April 6 2026", 2)
        assert d is None
        assert err is not None
        assert "Unrecognised" in err.message

    def test_strips_whitespace(self):
        d, err = _parse_date("  2026-04-06  ", 2)
        assert d == date(2026, 4, 6)
        assert err is None


# ══════════════════════════════════════════════════════════════════════════════
# Section 3: _parse_float and _parse_int
# ══════════════════════════════════════════════════════════════════════════════

class TestParseFloat:
    def test_valid_float(self):
        v, err = _parse_float("500.50", 2, "spend")
        assert v == 500.50
        assert err is None

    def test_empty_string_returns_zero(self):
        v, err = _parse_float("", 2, "spend")
        assert v == 0.0
        assert err is None

    def test_negative_returns_error(self):
        v, err = _parse_float("-10.0", 2, "spend")
        assert v is None
        assert err is not None

    def test_invalid_string_returns_error(self):
        v, err = _parse_float("abc", 2, "spend")
        assert v is None
        assert err is not None

    def test_comma_thousands_separator(self):
        v, err = _parse_float("1,500.00", 2, "spend")
        assert v == 1500.0
        assert err is None


class TestParseInt:
    def test_valid_int(self):
        v, err = _parse_int("1000", 2, "impressions")
        assert v == 1000
        assert err is None

    def test_float_string_truncated(self):
        v, err = _parse_int("5.0", 2, "impressions")
        assert v == 5
        assert err is None

    def test_empty_string_returns_zero(self):
        v, err = _parse_int("", 2, "impressions")
        assert v == 0
        assert err is None

    def test_negative_returns_error(self):
        v, err = _parse_int("-1", 2, "impressions")
        assert v is None
        assert err is not None

    def test_invalid_string_returns_error(self):
        v, err = _parse_int("xxx", 2, "impressions")
        assert v is None
        assert err is not None

    def test_comma_thousands_separator(self):
        v, err = _parse_int("10,000", 2, "impressions")
        assert v == 10000
        assert err is None


# ══════════════════════════════════════════════════════════════════════════════
# Section 4: parse_csv — happy path
# ══════════════════════════════════════════════════════════════════════════════

class TestParseCsvHappyPath:
    def test_minimal_csv_parses(self):
        result = parse_csv(MINIMAL_CSV)
        assert not result.has_errors
        assert len(result.records) == 1

    def test_full_csv_parses(self):
        result = parse_csv(FULL_CSV)
        assert not result.has_errors
        assert len(result.records) == 2

    def test_spend_converted_to_paise(self):
        result = parse_csv(MINIMAL_CSV)
        # 500.00 INR = 50,000 paise
        assert result.records[0].spend_paise == 50_000

    def test_conversion_value_converted_to_paise(self):
        result = parse_csv(FULL_CSV)
        # 2000.00 INR = 200,000 paise
        assert result.records[0].conversion_value_paise == 200_000

    def test_all_fields_populated(self):
        result = parse_csv(FULL_CSV)
        rec = result.records[0]
        assert rec.external_campaign_id == "camp1"
        assert rec.campaign_name == "Campaign One"
        assert rec.date == date(2026, 4, 6)
        assert rec.impressions == 1000
        assert rec.clicks == 50
        assert rec.reach == 800
        assert rec.frequency == 1.25
        assert rec.conversions == 5
        assert rec.leads == 10

    def test_two_rows_parsed(self):
        result = parse_csv(FULL_CSV)
        assert result.total_rows == 2
        assert result.valid_rows == 2

    def test_bom_stripped(self):
        bom_csv = b"\xef\xbb\xbf" + MINIMAL_CSV
        result = parse_csv(bom_csv)
        assert not result.has_errors
        assert len(result.records) == 1

    def test_campaign_name_defaults_to_campaign_id(self):
        csv_bytes = b"campaign_id,date,spend\ncamp1,2026-04-06,100.00\n"
        result = parse_csv(csv_bytes)
        assert result.records[0].campaign_name == "camp1"


# ══════════════════════════════════════════════════════════════════════════════
# Section 5: parse_csv — validation errors
# ══════════════════════════════════════════════════════════════════════════════

class TestParseCsvValidationErrors:
    def test_empty_file_returns_error(self):
        result = parse_csv(b"")
        assert result.has_errors
        assert any("empty" in e.message.lower() for e in result.errors)

    def test_missing_campaign_id_column_returns_error(self):
        csv_bytes = b"date,spend\n2026-04-06,100\n"
        result = parse_csv(csv_bytes)
        assert result.has_errors
        assert any("campaign_id" in e.message for e in result.errors)

    def test_missing_date_column_returns_error(self):
        csv_bytes = b"campaign_id,spend\ncamp1,100\n"
        result = parse_csv(csv_bytes)
        assert result.has_errors
        assert any("date" in e.message for e in result.errors)

    def test_row_missing_campaign_id_value(self):
        csv_bytes = b"campaign_id,date,spend\n,2026-04-06,100\n"
        result = parse_csv(csv_bytes)
        assert result.has_errors
        assert result.valid_rows == 0

    def test_row_missing_date_value(self):
        csv_bytes = b"campaign_id,date,spend\ncamp1,,100\n"
        result = parse_csv(csv_bytes)
        assert result.has_errors
        assert result.valid_rows == 0

    def test_invalid_date_format(self):
        csv_bytes = b"campaign_id,date,spend\ncamp1,April 6 2026,100\n"
        result = parse_csv(csv_bytes)
        assert result.has_errors
        err = result.errors[0]
        assert err.field == "date"

    def test_negative_spend(self):
        csv_bytes = b"campaign_id,date,spend\ncamp1,2026-04-06,-50\n"
        result = parse_csv(csv_bytes)
        assert result.has_errors
        err = result.errors[0]
        assert "negative" in err.message

    def test_negative_impressions(self):
        csv_bytes = b"campaign_id,date,impressions\ncamp1,2026-04-06,-100\n"
        result = parse_csv(csv_bytes)
        assert result.has_errors

    def test_invalid_spend_string(self):
        csv_bytes = b"campaign_id,date,spend\ncamp1,2026-04-06,abc\n"
        result = parse_csv(csv_bytes)
        assert result.has_errors

    def test_partial_rows_good_and_bad(self):
        csv_bytes = textwrap.dedent("""\
            campaign_id,date,spend
            camp1,2026-04-06,100.00
            camp2,INVALID_DATE,200.00
            camp3,2026-04-06,300.00
        """).encode()
        result = parse_csv(csv_bytes)
        # bad row is recorded in errors; good rows are in records
        assert result.valid_rows == 2
        assert len(result.errors) == 1

    def test_unicode_decode_error(self):
        bad_bytes = b"\xff\xfe invalid latin-1"
        result = parse_csv(bad_bytes)
        # Should either parse (if latin-1 is attempted) or return encoding error
        # We just ensure no exception is raised
        assert isinstance(result, CSVParseResult)


# ══════════════════════════════════════════════════════════════════════════════
# Section 6: Column aliases
# ══════════════════════════════════════════════════════════════════════════════

class TestColumnAliases:
    def test_cost_alias_for_spend(self):
        csv_bytes = b"campaign_id,date,cost\ncamp1,2026-04-06,100.00\n"
        result = parse_csv(csv_bytes)
        assert not result.has_errors
        assert result.records[0].spend_paise == 10_000

    def test_revenue_alias_for_conversion_value(self):
        csv_bytes = b"campaign_id,date,revenue\ncamp1,2026-04-06,500.00\n"
        result = parse_csv(csv_bytes)
        assert not result.has_errors
        assert result.records[0].conversion_value_paise == 50_000

    def test_impr_alias_for_impressions(self):
        csv_bytes = b"campaign_id,date,impr\ncamp1,2026-04-06,2000\n"
        result = parse_csv(csv_bytes)
        assert not result.has_errors
        assert result.records[0].impressions == 2000

    def test_lead_gen_alias_for_leads(self):
        csv_bytes = b"campaign_id,date,lead_gen\ncamp1,2026-04-06,7\n"
        result = parse_csv(csv_bytes)
        assert not result.has_errors
        assert result.records[0].leads == 7

    def test_day_alias_for_date(self):
        csv_bytes = b"campaign_id,day,spend\ncamp1,2026-04-06,100\n"
        result = parse_csv(csv_bytes)
        assert not result.has_errors
        assert result.records[0].date == date(2026, 4, 6)

    def test_case_insensitive_headers(self):
        csv_bytes = b"Campaign_ID,DATE,SPEND\ncamp1,2026-04-06,100\n"
        result = parse_csv(csv_bytes)
        assert not result.has_errors

    def test_campaign_id_alias(self):
        csv_bytes = b"external_campaign_id,date\ncamp1,2026-04-06\n"
        result = parse_csv(csv_bytes)
        assert not result.has_errors
        assert result.records[0].external_campaign_id == "camp1"


# ══════════════════════════════════════════════════════════════════════════════
# Section 7: CSVIngestionService.fetch
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestCSVServiceFetch:
    async def test_payload_not_set_raises(self, db):
        svc = CSVIngestionService(db)
        with pytest.raises(ValueError, match="payload not set"):
            await svc.fetch(BRAND_ID, TODAY)

    async def test_filters_by_date(self, db):
        svc = CSVIngestionService(db)
        svc.set_csv_payload(MULTI_DATE_CSV)

        rows_today = await svc.fetch(BRAND_ID, TODAY)
        rows_yesterday = await svc.fetch(BRAND_ID, date(2026, 4, 5))

        assert len(rows_today) == 1
        assert rows_today[0]["date"] == "2026-04-06"

        assert len(rows_yesterday) == 1
        assert rows_yesterday[0]["date"] == "2026-04-05"

    async def test_validation_errors_raise(self, db):
        svc = CSVIngestionService(db)
        bad_csv = b"campaign_id,date,spend\ncamp1,INVALID,abc\n"
        svc.set_csv_payload(bad_csv)
        with pytest.raises(ValueError, match="validation errors"):
            await svc.fetch(BRAND_ID, TODAY)

    async def test_no_rows_for_unmatched_date(self, db):
        svc = CSVIngestionService(db)
        svc.set_csv_payload(MINIMAL_CSV)
        rows = await svc.fetch(BRAND_ID, date(2025, 1, 1))
        assert rows == []


# ══════════════════════════════════════════════════════════════════════════════
# Section 8: CSVIngestionService.transform
# ══════════════════════════════════════════════════════════════════════════════

class TestCSVServiceTransform:
    def _svc(self):
        client = AsyncMongoMockClient()
        return CSVIngestionService(client["db"])

    def test_converts_raw_back_to_platform_record(self):
        svc = self._svc()
        raw = [
            {
                "campaign_id": "c1",
                "campaign_name": "Camp",
                "date": "2026-04-06",
                "spend_paise": 50_000,
                "impressions": 1000,
                "clicks": 50,
                "reach": 0,
                "frequency": 0.0,
                "conversions": 5,
                "conversion_value_paise": 100_000,
                "leads": 3,
            }
        ]
        records = svc.transform(raw, BRAND_ID)
        assert len(records) == 1
        rec = records[0]
        assert rec.spend_paise == 50_000
        assert rec.conversion_value_paise == 100_000
        assert rec.date == date(2026, 4, 6)

    def test_malformed_row_skipped(self):
        svc = self._svc()
        # Missing 'campaign_id' key
        raw = [{"date": "2026-04-06", "spend_paise": 100}]
        records = svc.transform(raw, BRAND_ID)
        assert records == []


# ══════════════════════════════════════════════════════════════════════════════
# Section 9: ingest_csv — full end-to-end
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestIngestCsv:
    async def test_success_single_date(self, db_with_brand):
        svc = CSVIngestionService(db_with_brand)
        result, parse_result = await svc.ingest_csv(BRAND_ID, MINIMAL_CSV)

        assert not parse_result.has_errors
        assert result is not None
        assert result.status == "success"
        assert result.records_upserted == 1

    async def test_success_multi_row(self, db_with_brand):
        svc = CSVIngestionService(db_with_brand)
        result, parse_result = await svc.ingest_csv(BRAND_ID, FULL_CSV)

        assert not parse_result.has_errors
        assert result.records_upserted == 2

    async def test_validation_failure_returns_none_result(self, db_with_brand):
        bad_csv = b"campaign_id,date\ncamp1,INVALID_DATE\n"
        svc = CSVIngestionService(db_with_brand)
        result, parse_result = await svc.ingest_csv(BRAND_ID, bad_csv)

        assert result is None
        assert parse_result.has_errors

    async def test_multi_date_ingests_all_dates(self, db_with_brand):
        svc = CSVIngestionService(db_with_brand)
        result, parse_result = await svc.ingest_csv(BRAND_ID, MULTI_DATE_CSV)

        assert not parse_result.has_errors
        assert result.records_upserted == 2

    async def test_ingestion_log_written(self, db_with_brand):
        svc = CSVIngestionService(db_with_brand)
        result, _ = await svc.ingest_csv(BRAND_ID, MINIMAL_CSV)

        log = await db_with_brand["ingestion_logs"].find_one({"run_id": result.run_id})
        assert log is not None
        assert log["status"] == "success"
        assert log["source"] == "manual"

    async def test_idempotent_double_upload(self, db_with_brand):
        svc1 = CSVIngestionService(db_with_brand)
        result1, _ = await svc1.ingest_csv(BRAND_ID, FULL_CSV)

        svc2 = CSVIngestionService(db_with_brand)
        result2, _ = await svc2.ingest_csv(BRAND_ID, FULL_CSV)

        assert result1.records_upserted == 2
        assert result2.records_upserted == 2

        # Only 2 performance rows should exist (upsert, not insert)
        count = await db_with_brand["ad_performance_raw"].count_documents({})
        assert count == 2

    async def test_source_is_manual(self, db_with_brand):
        svc = CSVIngestionService(db_with_brand)
        result, _ = await svc.ingest_csv(BRAND_ID, MINIMAL_CSV)
        assert result.source == "manual"
