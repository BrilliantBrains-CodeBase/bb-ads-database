"""
BaseIngestionService — Template Method pattern for all ad-platform connectors.

Each connector (Google Ads, Meta, CSV) subclasses this and implements two
abstract methods:

    async def fetch(brand_id, target_date) -> list[RawRecord]
    def transform(raw_records, brand_id) -> list[PlatformRecord]

The base class owns the entire orchestration:

    run()
     ├── _start_log()          — insert ingestion_logs doc (status="running")
     ├── for D-1, D-0:
     │    ├── fetch()           — platform HTTP / SDK call (subclass)
     │    ├── transform()       — field mapping + unit conversion (subclass)
     │    └── _upsert_records() — campaign upsert + perf upsert (base)
     └── _complete_log()        — update log (success / partial / failed)

Design invariants
─────────────────
• Idempotency: every write uses the natural key
  (brand_id, source, campaign_id, date) via PerformanceRepository.upsert().
• Run ID: a UUID4 is tagged on every ad_performance_raw document so an entire
  run can be rolled back atomically via PerformanceRepository.delete_by_run_id().
• Correction window: both D-1 and D-0 are always fetched because platforms
  retroactively update the previous day's data (late-arriving conversions).
• Failure isolation: a per-date fetch/transform error is caught, recorded in
  `partial_errors`, and execution continues with the next date.  A platform-
  wide error (auth failure, quota exhausted) raises into `run()` and sets
  status = "failed".
• Campaign auto-create: if a platform returns a campaign never seen before,
  CampaignsRepository.upsert_from_platform() creates it automatically.
"""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import structlog
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.repositories.campaigns import CampaignsRepository
from app.repositories.performance import PerformanceRepository

logger = structlog.get_logger(__name__)


# ── Data transfer objects ─────────────────────────────────────────────────────

@dataclass
class PlatformRecord:
    """Normalized record produced by each connector's transform() method.

    The base class resolves external_campaign_id → our MongoDB campaign _id
    before persisting, so subclasses never need to touch CampaignsRepository.

    Monetary fields must be in INR paise (Int64).
    """
    external_campaign_id: str        # platform's own campaign identifier
    campaign_name: str               # current name from platform
    date: date                       # IST calendar date for this row

    # Core metrics — paise for monetary fields
    spend_paise: int = 0
    impressions: int = 0
    clicks: int = 0
    reach: int = 0
    frequency: float = 0.0
    conversions: int = 0
    conversion_value_paise: int = 0
    leads: int = 0

    # Extra campaign-level metadata forwarded to CampaignsRepository
    # (objective, platform_status, budget_type, budget_paise, …)
    campaign_meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class IngestionResult:
    """Returned by BaseIngestionService.run()."""
    run_id: str
    brand_id: str
    source: str
    status: str                           # "success" | "partial" | "failed"
    target_date: date
    dates_covered: list[date]
    records_fetched: int
    records_upserted: int
    errors: list[str]
    duration_seconds: float


# ── Derived-metric computation ────────────────────────────────────────────────

def _compute_derived(r: PlatformRecord) -> dict[str, Any]:
    """Compute CTR, CPC, CPM, CPL, ROAS from base metrics.

    All paise fields are integers (floor division); ratio fields are float.
    None is stored when the denominator is zero to avoid division errors.
    """
    ctr: float | None = r.clicks / r.impressions if r.impressions else None
    cpc: int | None   = r.spend_paise // r.clicks if r.clicks else None
    cpm: int | None   = int(r.spend_paise * 1000 // r.impressions) if r.impressions else None
    cpl: int | None   = r.spend_paise // r.leads if r.leads else None
    roas: float | None = (
        r.conversion_value_paise / r.spend_paise if r.spend_paise else None
    )
    return {
        "ctr": ctr,
        "cpc_paise": cpc,
        "cpm_paise": cpm,
        "cpl_paise": cpl,
        "roas": roas,
    }


# ── Base class ────────────────────────────────────────────────────────────────

class BaseIngestionService(ABC):
    """Abstract base for all ad-platform ingestion connectors.

    Subclasses must:
      1. Set the class attribute `source` (matches MongoDB schema enum).
      2. Implement `async fetch(brand_id, target_date)`.
      3. Implement `transform(raw_records, brand_id)`.

    Subclasses may override `_upsert_records()` for platform-specific
    batch-write optimisations, but this is rarely needed.
    """

    #: Must be overridden: "google_ads" | "meta" | "interakt" | "manual"
    source: str

    def __init__(self, db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
        self._db = db

    # ── Abstract interface (implement in subclasses) ───────────────────────────

    @abstractmethod
    async def fetch(
        self,
        brand_id: str,
        target_date: date,
    ) -> list[dict[str, Any]]:
        """Call the platform API / SDK and return raw response records.

        Implementors are responsible for:
          - Loading brand credentials from the DB (encrypted token lookup).
          - Handling platform-specific pagination.
          - Raising on unrecoverable errors (auth failure, quota exhausted).
            Per-date transient errors should also raise; the base `run()` will
            catch them and mark that date as a partial failure.
        """
        ...

    @abstractmethod
    def transform(
        self,
        raw_records: list[dict[str, Any]],
        brand_id: str,
    ) -> list[PlatformRecord]:
        """Convert platform raw records into PlatformRecord instances.

        Implementors are responsible for:
          - Mapping platform field names to PlatformRecord attributes.
          - Converting currency units to INR paise (e.g. micros ÷ 10 for Google).
          - Handling missing / null fields gracefully (default to 0).
          - NOT resolving external_campaign_id → MongoDB ObjectId (base handles it).
        """
        ...

    # ── Orchestration (do not override unless necessary) ──────────────────────

    async def run(
        self,
        brand_id: str,
        target_date: date | None = None,
        *,
        is_backfill: bool = False,
        custom_dates: list[date] | None = None,
    ) -> IngestionResult:
        """Execute the full ingestion pipeline for one brand × source.

        Args:
            brand_id:     MongoDB brand _id string.
            target_date:  Primary date (defaults to today UTC).  D-1 is always
                          added automatically unless custom_dates is provided.
            is_backfill:  Flagged in the ingestion log; does not change behaviour.
            custom_dates: Override the correction window with an explicit list
                          (used for historical backfills covering many days).

        Returns:
            IngestionResult with final status and record counts.
        """
        run_id = str(uuid.uuid4())
        primary_date = target_date or date.today()
        dates_to_pull = custom_dates or [
            primary_date - timedelta(days=1),  # D-1 correction window
            primary_date,                       # D-0 primary
        ]

        log = logger.bind(
            run_id=run_id,
            brand_id=brand_id,
            source=self.source,
            dates=[str(d) for d in dates_to_pull],
        )
        log.info("ingestion.run.started")

        try:
            await self._start_log(run_id, brand_id, primary_date, is_backfill)
        except Exception as exc:
            logger.warning("ingestion.log.start_failed", run_id=run_id, error=str(exc))

        started_at = datetime.now(UTC)
        total_fetched = 0
        total_upserted = 0
        partial_errors: list[str] = []
        fatal_error: str | None = None

        try:
            for fetch_date in dates_to_pull:
                try:
                    raw = await self.fetch(brand_id, fetch_date)
                    records = self.transform(raw, brand_id)
                    total_fetched += len(records)

                    upserted = await self._upsert_records(brand_id, records, run_id)
                    total_upserted += upserted

                    log.debug(
                        "ingestion.date.done",
                        date=str(fetch_date),
                        fetched=len(records),
                        upserted=upserted,
                    )

                except Exception as exc:
                    # Per-date failure: record and continue with next date
                    msg = f"{fetch_date}: {type(exc).__name__}: {exc}"
                    partial_errors.append(msg)
                    log.warning("ingestion.date.failed", date=str(fetch_date), error=str(exc))

        except Exception as exc:
            # Catastrophic failure outside the per-date loop (extremely rare)
            fatal_error = f"fatal: {type(exc).__name__}: {exc}"
            log.error("ingestion.run.fatal", error=str(exc), exc_info=True)

        duration = (datetime.now(UTC) - started_at).total_seconds()

        # Determine final status
        all_errors = ([fatal_error] if fatal_error else []) + partial_errors
        if fatal_error or (partial_errors and total_upserted == 0):
            final_status = "failed"
        elif partial_errors:
            final_status = "partial"
        else:
            final_status = "success"

        error_msg: str | None = "; ".join(all_errors) if all_errors else None
        await self._complete_log(
            run_id, final_status, total_fetched, total_upserted, error_msg
        )

        log.info(
            "ingestion.run.finished",
            status=final_status,
            fetched=total_fetched,
            upserted=total_upserted,
            duration_s=round(duration, 2),
        )

        return IngestionResult(
            run_id=run_id,
            brand_id=brand_id,
            source=self.source,
            status=final_status,
            target_date=primary_date,
            dates_covered=dates_to_pull,
            records_fetched=total_fetched,
            records_upserted=total_upserted,
            errors=all_errors,
            duration_seconds=duration,
        )

    # ── Internal helpers ───────────────────────────────────────────────────────

    async def _upsert_records(
        self,
        brand_id: str,
        records: list[PlatformRecord],
        run_id: str,
    ) -> int:
        """Resolve campaigns and write performance rows for a list of records.

        For each PlatformRecord:
          1. Upsert the campaign (create if new, update name/meta if changed).
          2. Compute derived metrics (CTR, CPC, CPM, CPL, ROAS).
          3. Upsert the performance row (idempotent on natural key).

        Returns the number of rows written.
        """
        if not records:
            return 0

        campaigns_repo = CampaignsRepository(self._db, brand_id)
        perf_repo = PerformanceRepository(self._db, brand_id)

        upserted = 0
        for rec in records:
            try:
                # Ensure the campaign exists in our catalog
                campaign_id = await campaigns_repo.upsert_from_platform(
                    source=self.source,
                    external_id=rec.external_campaign_id,
                    data={"name": rec.campaign_name, **rec.campaign_meta},
                )

                metrics: dict[str, Any] = {
                    "spend_paise": rec.spend_paise,
                    "impressions": rec.impressions,
                    "clicks": rec.clicks,
                    "reach": rec.reach,
                    "frequency": rec.frequency,
                    "conversions": rec.conversions,
                    "conversion_value_paise": rec.conversion_value_paise,
                    "leads": rec.leads,
                    **_compute_derived(rec),
                }

                await perf_repo.upsert(
                    source=self.source,
                    campaign_id=campaign_id,
                    record_date=rec.date,
                    metrics=metrics,
                    ingestion_run_id=run_id,
                )
                upserted += 1

            except Exception as exc:
                # Per-record failure: log and continue
                logger.warning(
                    "ingestion.record.upsert_failed",
                    brand_id=brand_id,
                    run_id=run_id,
                    campaign_ext_id=rec.external_campaign_id,
                    date=str(rec.date),
                    error=str(exc),
                )

        return upserted

    async def _start_log(
        self,
        run_id: str,
        brand_id: str,
        target_date: date,
        is_backfill: bool,
    ) -> None:
        """Insert a running ingestion_logs document."""
        try:
            await self._db["ingestion_logs"].insert_one({
                "run_id": run_id,
                "brand_id": ObjectId(brand_id),
                "source": self.source,
                "target_date": datetime.combine(target_date, time.min, tzinfo=UTC),
                "status": "running",
                "started_at": datetime.now(UTC),
                "completed_at": None,
                "records_fetched": 0,
                "records_upserted": 0,
                "error_message": None,
                "retry_count": 0,
                "is_backfill": is_backfill,
            })
        except Exception as exc:
            # Log write failure must never abort the ingestion itself
            logger.warning("ingestion.log.start_failed", run_id=run_id, error=str(exc))

    async def _complete_log(
        self,
        run_id: str,
        status: str,
        records_fetched: int,
        records_upserted: int,
        error_message: str | None,
    ) -> None:
        """Update the ingestion_logs document with final status and counts."""
        try:
            await self._db["ingestion_logs"].update_one(
                {"run_id": run_id},
                {
                    "$set": {
                        "status": status,
                        "completed_at": datetime.now(UTC),
                        "records_fetched": records_fetched,
                        "records_upserted": records_upserted,
                        "error_message": error_message,
                    }
                },
            )
        except Exception as exc:
            logger.warning("ingestion.log.complete_failed", run_id=run_id, error=str(exc))
