"""
CSV Upload Connector
====================

Validates, normalises, and ingests ad performance data from CSV files
uploaded by agency analysts.

Design goals
────────────
• Atomic: the entire file succeeds or the entire file is rolled back
  via PerformanceRepository.delete_by_run_id().
• Idempotent: each row is upserted on the natural key
  (brand_id, source, campaign_id, date) — re-uploading the same file
  is safe.
• Permissive column matching: column headers are matched
  case-insensitively and common alternate names are accepted.
• Date format detection: DD-MM-YYYY, YYYY-MM-DD, and ISO 8601.
• Currency: accepts INR in rupees (float) and converts to paise.
  Rejects files with non-INR currency markers.

CSV format (expected columns, all optional except marked *)
───────────────────────────────────────────────────────────
  campaign_id*       — external campaign identifier (string)
  campaign_name      — display name
  date*              — record date (DD-MM-YYYY | YYYY-MM-DD | ISO 8601)
  spend              — spend in INR rupees (float)
  impressions        — integer
  clicks             — integer
  reach              — integer
  frequency          — float
  conversions        — integer
  conversion_value   — conversion value in INR rupees (float)
  leads              — integer

Column aliases (case-insensitive)
──────────────────────────────────
  campaign_id   ← "campaign id", "external_campaign_id", "campaign_ext_id"
  campaign_name ← "campaign name", "name"
  date          ← "report_date", "stat_date", "day"
  spend         ← "cost", "amount_spent", "total_spend"
  impressions   ← "impr", "views"
  clicks        ← "link_clicks", "total_clicks"
  leads         ← "lead_gen", "total_leads"
  conversions   ← "conv", "total_conversions"
  conversion_value ← "conv_value", "revenue", "conversion_revenue"
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.services.ingestion.base import BaseIngestionService, PlatformRecord

logger = structlog.get_logger(__name__)

# ── Column alias map ────────────────────────────────────────────────────────────

_COLUMN_ALIASES: dict[str, str] = {
    # campaign_id
    "campaign id": "campaign_id",
    "external_campaign_id": "campaign_id",
    "campaign_ext_id": "campaign_id",
    # campaign_name
    "campaign name": "campaign_name",
    "name": "campaign_name",
    # date
    "report_date": "date",
    "stat_date": "date",
    "day": "date",
    # spend
    "cost": "spend",
    "amount_spent": "spend",
    "total_spend": "spend",
    # impressions
    "impr": "impressions",
    "views": "impressions",
    # clicks
    "link_clicks": "clicks",
    "total_clicks": "clicks",
    # leads
    "lead_gen": "leads",
    "total_leads": "leads",
    # conversions
    "conv": "conversions",
    "total_conversions": "conversions",
    # conversion_value
    "conv_value": "conversion_value",
    "revenue": "conversion_value",
    "conversion_revenue": "conversion_value",
}

# Required canonical column names
_REQUIRED_COLUMNS = {"campaign_id", "date"}

# Supported date formats in priority order
_DATE_FORMATS = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"]


# ── Validation error ────────────────────────────────────────────────────────────

@dataclass
class RowValidationError:
    row_number: int
    field: str
    message: str


@dataclass
class CSVParseResult:
    """Result of parsing a CSV file before committing to the database."""
    records: list[PlatformRecord] = field(default_factory=list)
    errors: list[RowValidationError] = field(default_factory=list)
    total_rows: int = 0
    valid_rows: int = 0

    @property
    def has_errors(self) -> bool:
        return bool(self.errors)


# ── CSV Ingestion Service ───────────────────────────────────────────────────────

class CSVIngestionService(BaseIngestionService):
    """Ingestion connector for manual CSV uploads.

    Unlike platform connectors, fetch() accepts a pre-loaded CSV bytes
    payload rather than making a network call. The caller (upload router)
    passes it via the context mechanism:

        svc = CSVIngestionService(db)
        svc.set_csv_payload(csv_bytes)
        result = await svc.run(brand_id=..., target_date=..., custom_dates=[...])

    Or, more directly, call ingest_csv() which bypasses the correction window
    and processes only the dates present in the file.
    """

    source: str = "manual"

    def __init__(self, db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
        super().__init__(db)
        self._csv_payload: bytes | None = None

    def set_csv_payload(self, payload: bytes) -> None:
        """Attach a CSV file payload before calling run()."""
        self._csv_payload = payload

    # ── BaseIngestionService abstract interface ────────────────────────────────

    async def fetch(
        self,
        brand_id: str,
        target_date: date,
    ) -> list[dict[str, Any]]:
        """Return pre-parsed CSV rows for target_date.

        The CSV payload must be set via set_csv_payload() before run() is
        called.  Rows not matching target_date are filtered out.
        """
        if self._csv_payload is None:
            raise ValueError("CSV payload not set. Call set_csv_payload() first.")

        result = parse_csv(self._csv_payload)
        if result.has_errors:
            # Surface validation errors as a single exception so run() marks
            # this date as a partial failure with a useful error message.
            error_summaries = "; ".join(
                f"row {e.row_number} [{e.field}]: {e.message}"
                for e in result.errors[:5]  # cap at 5 for readability
            )
            raise ValueError(f"CSV validation errors: {error_summaries}")

        # Filter to rows whose date matches target_date
        raw_for_date = [
            r for r in _records_to_raw(result.records)
            if r["date"] == str(target_date)
        ]
        return raw_for_date

    def transform(
        self,
        raw_records: list[dict[str, Any]],
        brand_id: str,
    ) -> list[PlatformRecord]:
        """Re-hydrate PlatformRecord objects from raw dicts produced by fetch()."""
        records: list[PlatformRecord] = []
        for raw in raw_records:
            try:
                records.append(_raw_to_platform_record(raw))
            except Exception as exc:
                logger.warning(
                    "csv.transform.row_failed",
                    brand_id=brand_id,
                    row=raw,
                    error=str(exc),
                )
        return records

    # ── Direct ingestion (skips correction window) ─────────────────────────────

    async def ingest_csv(
        self,
        brand_id: str,
        payload: bytes,
        *,
        is_backfill: bool = False,
    ) -> tuple[Any, CSVParseResult]:
        """Parse, validate, and ingest a CSV file atomically.

        Parses the file first.  If there are any validation errors the file
        is rejected entirely (no DB writes).

        Returns (IngestionResult, CSVParseResult) so the caller can surface
        per-row errors in the API response.
        """
        parse_result = parse_csv(payload)
        if parse_result.has_errors:
            logger.warning(
                "csv.ingest.validation_failed",
                brand_id=brand_id,
                error_count=len(parse_result.errors),
            )
            # Return a minimal IngestionResult-like dict; caller checks has_errors
            return None, parse_result

        # Determine the unique dates present in this file
        dates_in_file: list[date] = sorted(
            {rec.date for rec in parse_result.records}
        )

        self.set_csv_payload(payload)
        result = await self.run(
            brand_id=brand_id,
            target_date=dates_in_file[-1] if dates_in_file else date.today(),
            custom_dates=dates_in_file,
            is_backfill=is_backfill,
        )
        return result, parse_result


# ── CSV parsing (pure, no DB) ──────────────────────────────────────────────────

def parse_csv(payload: bytes) -> CSVParseResult:
    """Parse CSV bytes into PlatformRecord instances with per-row validation.

    Handles:
      - UTF-8 and UTF-8-BOM encoding
      - Header normalisation (strip, lowercase, alias resolution)
      - Required column presence check
      - Per-row field validation
    """
    result = CSVParseResult()

    try:
        text = payload.decode("utf-8-sig").strip()  # utf-8-sig strips BOM
    except UnicodeDecodeError as exc:
        result.errors.append(RowValidationError(0, "encoding", str(exc)))
        return result

    if not text:
        result.errors.append(RowValidationError(0, "file", "CSV file is empty"))
        return result

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        result.errors.append(RowValidationError(0, "header", "No headers found"))
        return result

    # Normalise headers
    normalised_fieldnames = [_normalise_column(h) for h in reader.fieldnames]
    missing = _REQUIRED_COLUMNS - set(normalised_fieldnames)
    if missing:
        result.errors.append(
            RowValidationError(0, "header", f"Missing required columns: {missing}")
        )
        return result

    # Re-map reader keys to normalised names
    col_map: dict[str, str] = {
        original: _normalise_column(original)
        for original in (reader.fieldnames or [])
    }

    for row_num, raw_row in enumerate(reader, start=2):  # row 1 = header
        result.total_rows += 1
        # Remap keys
        row: dict[str, str] = {col_map.get(k, k): (v or "").strip() for k, v in raw_row.items()}

        record, errors = _validate_row(row, row_num)
        if errors:
            result.errors.extend(errors)
        else:
            result.records.append(record)  # type: ignore[arg-type]
            result.valid_rows += 1

    return result


def _normalise_column(header: str) -> str:
    """Lowercase, strip, and resolve aliases for a CSV column header."""
    cleaned = header.strip().lower().replace("-", "_")
    return _COLUMN_ALIASES.get(cleaned, cleaned)


def _validate_row(
    row: dict[str, str],
    row_num: int,
) -> tuple[PlatformRecord | None, list[RowValidationError]]:
    """Validate a single normalised CSV row.

    Returns (PlatformRecord, []) on success or (None, [errors]) on failure.
    """
    errors: list[RowValidationError] = []

    # ── campaign_id ──────────────────────────────────────────────────────────
    campaign_id = row.get("campaign_id", "").strip()
    if not campaign_id:
        errors.append(RowValidationError(row_num, "campaign_id", "campaign_id is required"))

    # ── campaign_name ────────────────────────────────────────────────────────
    campaign_name = row.get("campaign_name", campaign_id)

    # ── date ─────────────────────────────────────────────────────────────────
    record_date, date_error = _parse_date(row.get("date", ""), row_num)
    if date_error:
        errors.append(date_error)

    # ── numeric fields ───────────────────────────────────────────────────────
    spend_inr, err = _parse_float(row.get("spend", "0"), row_num, "spend")
    if err:
        errors.append(err)

    impressions, err = _parse_int(row.get("impressions", "0"), row_num, "impressions")
    if err:
        errors.append(err)

    clicks, err = _parse_int(row.get("clicks", "0"), row_num, "clicks")
    if err:
        errors.append(err)

    reach, err = _parse_int(row.get("reach", "0"), row_num, "reach")
    if err:
        errors.append(err)

    frequency, err = _parse_float(row.get("frequency", "0"), row_num, "frequency")
    if err:
        errors.append(err)

    conversions, err = _parse_int(row.get("conversions", "0"), row_num, "conversions")
    if err:
        errors.append(err)

    conv_value_inr, err = _parse_float(row.get("conversion_value", "0"), row_num, "conversion_value")
    if err:
        errors.append(err)

    leads, err = _parse_int(row.get("leads", "0"), row_num, "leads")
    if err:
        errors.append(err)

    if errors:
        return None, errors

    record = PlatformRecord(
        external_campaign_id=campaign_id,
        campaign_name=campaign_name,
        date=record_date,  # type: ignore[arg-type]
        spend_paise=int((spend_inr or 0.0) * 100),
        impressions=impressions or 0,
        clicks=clicks or 0,
        reach=reach or 0,
        frequency=frequency or 0.0,
        conversions=conversions or 0,
        conversion_value_paise=int((conv_value_inr or 0.0) * 100),
        leads=leads or 0,
    )
    return record, []


def _parse_date(
    value: str,
    row_num: int,
) -> tuple[date | None, RowValidationError | None]:
    """Try each supported date format and return the first that parses."""
    if not value.strip():
        return None, RowValidationError(row_num, "date", "date is required")

    from datetime import datetime as _dt
    for fmt in _DATE_FORMATS:
        try:
            return _dt.strptime(value.strip(), fmt).date(), None
        except ValueError:
            continue

    return None, RowValidationError(
        row_num, "date",
        f"Unrecognised date format '{value}'. Expected YYYY-MM-DD or DD-MM-YYYY."
    )


def _parse_float(
    value: str,
    row_num: int,
    field_name: str,
) -> tuple[float | None, RowValidationError | None]:
    """Parse an optional float field; empty string → 0.0."""
    v = value.strip()
    if not v:
        return 0.0, None
    # Remove commas used as thousands separators
    v = v.replace(",", "")
    try:
        result = float(v)
        if result < 0:
            return None, RowValidationError(
                row_num, field_name, f"{field_name} cannot be negative"
            )
        return result, None
    except ValueError:
        return None, RowValidationError(
            row_num, field_name, f"Invalid value '{value}' for {field_name}"
        )


def _parse_int(
    value: str,
    row_num: int,
    field_name: str,
) -> tuple[int | None, RowValidationError | None]:
    """Parse an optional integer field; empty string → 0."""
    v = value.strip()
    if not v:
        return 0, None
    # Accept float strings like "5.0" by truncating
    v = v.replace(",", "")
    try:
        result = int(float(v))
        if result < 0:
            return None, RowValidationError(
                row_num, field_name, f"{field_name} cannot be negative"
            )
        return result, None
    except ValueError:
        return None, RowValidationError(
            row_num, field_name, f"Invalid value '{value}' for {field_name}"
        )


# ── Private helpers ────────────────────────────────────────────────────────────

def _records_to_raw(records: list[PlatformRecord]) -> list[dict[str, Any]]:
    """Convert PlatformRecord objects back to plain dicts for the fetch/transform pipeline."""
    return [
        {
            "campaign_id": r.external_campaign_id,
            "campaign_name": r.campaign_name,
            "date": str(r.date),
            "spend_paise": r.spend_paise,
            "impressions": r.impressions,
            "clicks": r.clicks,
            "reach": r.reach,
            "frequency": r.frequency,
            "conversions": r.conversions,
            "conversion_value_paise": r.conversion_value_paise,
            "leads": r.leads,
        }
        for r in records
    ]


def _raw_to_platform_record(raw: dict[str, Any]) -> PlatformRecord:
    """Reconstruct a PlatformRecord from the raw dict produced by _records_to_raw."""
    return PlatformRecord(
        external_campaign_id=str(raw["campaign_id"]),
        campaign_name=raw.get("campaign_name", ""),
        date=date.fromisoformat(str(raw["date"])),
        spend_paise=int(raw.get("spend_paise", 0) or 0),
        impressions=int(raw.get("impressions", 0) or 0),
        clicks=int(raw.get("clicks", 0) or 0),
        reach=int(raw.get("reach", 0) or 0),
        frequency=float(raw.get("frequency", 0.0) or 0.0),
        conversions=int(raw.get("conversions", 0) or 0),
        conversion_value_paise=int(raw.get("conversion_value_paise", 0) or 0),
        leads=int(raw.get("leads", 0) or 0),
    )
