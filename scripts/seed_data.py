#!/usr/bin/env python3
"""
Seed script — populates a fresh MongoDB instance with realistic test data.

Creates
───────
  Agency   : BB Digital Agency
  Brands   : 3 brands with different platform configurations
  Users    : 4 users, one per role (super_admin / admin / analyst / viewer)
  Campaigns: 2–3 per brand × source
  Performance data: 30 days × brand × source × campaign
  Ingestion logs: one success log per brand × source for yesterday

Default credentials (change in .env for staging/production)
─────────────────────────────────────────────────────────────
  super_admin@bb.local  /  SeedPass1!
  admin@bb.local        /  SeedPass1!
  analyst@bb.local      /  SeedPass1!
  viewer@bb.local       /  SeedPass1!   (viewer — access to Acme Corp only)

Usage
─────
  # Against local MongoDB (default)
  python scripts/seed_data.py

  # Against a specific URI
  MONGODB_URI=mongodb://user:pass@host:27017 python scripts/seed_data.py

  # Dry-run — prints what would be inserted without writing
  python scripts/seed_data.py --dry-run

Idempotency
───────────
  Re-running when data already exists skips each object (matched on unique
  natural key) rather than failing.  Pass --force to drop and re-seed.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import sys
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

# ── Path setup — allow importing from the project root ───────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

# Import password hashing from the app (passlib bcrypt)
from app.core.security import hash_password

# ── Configuration ─────────────────────────────────────────────────────────────

MONGODB_URI = os.getenv("MONGODB_URI", os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
MONGODB_DB  = os.getenv("MONGODB_DB_NAME", "bb_ads")
SEED_PASSWORD = "SeedPass1!"

# ── Deterministic ObjectIds ────────────────────────────────────────────────────
# Fixed seeds make the script idempotent: the same ObjectIds are generated on
# every run, so upsert-by-_id works without querying first.

def _oid(hex_suffix: str) -> ObjectId:
    """Build a deterministic ObjectId from a 24-char hex string."""
    return ObjectId(hex_suffix.ljust(24, "0")[:24])


AGENCY_OID      = _oid("aa000000000000000000000a")
BRAND_OID_1     = _oid("bb000000000000000000001b")  # Acme Corp  — google_ads + meta
BRAND_OID_2     = _oid("bb000000000000000000002b")  # Globex Corp — meta only
BRAND_OID_3     = _oid("bb000000000000000000003b")  # Initech     — google_ads only

USER_SUPER_OID  = _oid("uu000000000000000000001u")
USER_ADMIN_OID  = _oid("uu000000000000000000002u")
USER_ANALYST_OID= _oid("uu000000000000000000003u")
USER_VIEWER_OID = _oid("uu000000000000000000004u")

# Campaign OIDs — 3 per brand per source (some brands have 2 sources)
_CAMP_IDS: dict[str, ObjectId] = {
    # Acme google_ads
    "acme_ga_1": _oid("cc000000000000000000001c"),
    "acme_ga_2": _oid("cc000000000000000000002c"),
    "acme_ga_3": _oid("cc000000000000000000003c"),
    # Acme meta
    "acme_mt_1": _oid("cc000000000000000000004c"),
    "acme_mt_2": _oid("cc000000000000000000005c"),
    # Globex meta
    "globex_mt_1": _oid("cc000000000000000000006c"),
    "globex_mt_2": _oid("cc000000000000000000007c"),
    "globex_mt_3": _oid("cc000000000000000000008c"),
    # Initech google_ads
    "initech_ga_1": _oid("cc000000000000000000009c"),
    "initech_ga_2": _oid("cc00000000000000000000ac"),
}


# ── Fixture data ───────────────────────────────────────────────────────────────

NOW = datetime.now(UTC)
TODAY = date.today()


def _agency() -> dict[str, Any]:
    return {
        "_id": AGENCY_OID,
        "name": "BB Digital Agency",
        "slug": "bb-digital",
        "created_at": NOW,
        "settings": {
            "fiscal_year_start_month": 4,
            "default_timezone": "Asia/Kolkata",
            "default_currency": "INR",
        },
    }


def _brands() -> list[dict[str, Any]]:
    base: dict[str, Any] = {
        "agency_id": AGENCY_OID,
        "is_active": True,
        "created_at": NOW,
        "created_by": USER_SUPER_OID,
        "onboarding_status": "completed",
        "settings": {
            "target_roas": 3.5,
            "target_cpl": 50000,          # 500 INR in paise
            "budget_alert_threshold": 0.9,
            "anomaly_sensitivity": "medium",
        },
    }
    return [
        {
            **base,
            "_id": BRAND_OID_1,
            "name": "Acme Corp",
            "slug": "acme-corp",
            "industry": "ecommerce",
            "storage_path": "brands/acme-corp",
            "platforms": {
                "google_ads": {
                    "customer_id": "123-456-7890",
                    "refresh_token": "seed_refresh_token_google",
                },
                "meta": {
                    "ad_account_id": "act_111222333",
                    "access_token": "seed_access_token_meta",
                },
            },
        },
        {
            **base,
            "_id": BRAND_OID_2,
            "name": "Globex Corp",
            "slug": "globex-corp",
            "industry": "saas",
            "storage_path": "brands/globex-corp",
            "platforms": {
                "meta": {
                    "ad_account_id": "act_444555666",
                    "access_token": "seed_access_token_meta_2",
                },
            },
        },
        {
            **base,
            "_id": BRAND_OID_3,
            "name": "Initech",
            "slug": "initech",
            "industry": "fintech",
            "storage_path": "brands/initech",
            "platforms": {
                "google_ads": {
                    "customer_id": "987-654-3210",
                    "refresh_token": "seed_refresh_token_google_2",
                },
            },
        },
    ]


def _users() -> list[dict[str, Any]]:
    hashed = hash_password(SEED_PASSWORD)
    base: dict[str, Any] = {
        "agency_id": AGENCY_OID,
        "hashed_password": hashed,
        "is_active": True,
        "created_at": NOW,
        "api_keys": [],
    }
    return [
        {
            **base,
            "_id": USER_SUPER_OID,
            "email": "super_admin@bb.local",
            "role": "super_admin",
            "full_name": "Super Admin",
            "allowed_brands": [],
        },
        {
            **base,
            "_id": USER_ADMIN_OID,
            "email": "admin@bb.local",
            "role": "admin",
            "full_name": "Agency Admin",
            "allowed_brands": [],
        },
        {
            **base,
            "_id": USER_ANALYST_OID,
            "email": "analyst@bb.local",
            "role": "analyst",
            "full_name": "Data Analyst",
            "allowed_brands": [BRAND_OID_1, BRAND_OID_2, BRAND_OID_3],
        },
        {
            **base,
            "_id": USER_VIEWER_OID,
            "email": "viewer@bb.local",
            "role": "viewer",
            "full_name": "Client Viewer",
            "allowed_brands": [BRAND_OID_1],   # access to Acme Corp only
        },
    ]


def _campaigns() -> list[dict[str, Any]]:
    now = NOW
    base: dict[str, Any] = {
        "created_at": now,
        "created_by": USER_ADMIN_OID,
        "our_status": "active",
        "platform_status": "ENABLED",
        "budget_type": "daily",
    }
    return [
        # ── Acme Corp — Google Ads ────────────────────────────────────────────
        {**base, "_id": _CAMP_IDS["acme_ga_1"],
         "brand_id": BRAND_OID_1, "source": "google_ads",
         "external_id": "ga_11001", "name": "Acme — Brand Search",
         "objective": "brand_awareness", "budget_paise": 200_000},
        {**base, "_id": _CAMP_IDS["acme_ga_2"],
         "brand_id": BRAND_OID_1, "source": "google_ads",
         "external_id": "ga_11002", "name": "Acme — Generic Search",
         "objective": "conversions",    "budget_paise": 500_000},
        {**base, "_id": _CAMP_IDS["acme_ga_3"],
         "brand_id": BRAND_OID_1, "source": "google_ads",
         "external_id": "ga_11003", "name": "Acme — Shopping",
         "objective": "sales",          "budget_paise": 350_000},
        # ── Acme Corp — Meta ──────────────────────────────────────────────────
        {**base, "_id": _CAMP_IDS["acme_mt_1"],
         "brand_id": BRAND_OID_1, "source": "meta",
         "external_id": "mt_21001", "name": "Acme — FB Traffic",
         "objective": "traffic",        "budget_paise": 300_000},
        {**base, "_id": _CAMP_IDS["acme_mt_2"],
         "brand_id": BRAND_OID_1, "source": "meta",
         "external_id": "mt_21002", "name": "Acme — Retargeting",
         "objective": "conversions",    "budget_paise": 150_000},
        # ── Globex Corp — Meta ────────────────────────────────────────────────
        {**base, "_id": _CAMP_IDS["globex_mt_1"],
         "brand_id": BRAND_OID_2, "source": "meta",
         "external_id": "mt_31001", "name": "Globex — Lead Gen",
         "objective": "lead_generation","budget_paise": 400_000},
        {**base, "_id": _CAMP_IDS["globex_mt_2"],
         "brand_id": BRAND_OID_2, "source": "meta",
         "external_id": "mt_31002", "name": "Globex — Lookalike",
         "objective": "conversions",    "budget_paise": 250_000},
        {**base, "_id": _CAMP_IDS["globex_mt_3"],
         "brand_id": BRAND_OID_2, "source": "meta",
         "external_id": "mt_31003", "name": "Globex — Brand Awareness",
         "objective": "brand_awareness","budget_paise": 100_000},
        # ── Initech — Google Ads ──────────────────────────────────────────────
        {**base, "_id": _CAMP_IDS["initech_ga_1"],
         "brand_id": BRAND_OID_3, "source": "google_ads",
         "external_id": "ga_41001", "name": "Initech — Search",
         "objective": "leads",          "budget_paise": 600_000},
        {**base, "_id": _CAMP_IDS["initech_ga_2"],
         "brand_id": BRAND_OID_3, "source": "google_ads",
         "external_id": "ga_41002", "name": "Initech — Performance Max",
         "objective": "conversions",    "budget_paise": 800_000},
    ]


# ── Performance data generator ────────────────────────────────────────────────

# (brand_oid, source, campaign_key, base_spend, base_impressions, base_clicks,
#  base_leads, base_conversions, conv_value_multiplier)
_CAMPAIGN_PROFILES: list[tuple[Any, ...]] = [
    (BRAND_OID_1, "google_ads", "acme_ga_1",  60_000,  8_000, 320,  6, 4,  1.8),
    (BRAND_OID_1, "google_ads", "acme_ga_2",  90_000, 12_000, 480, 12, 8,  2.2),
    (BRAND_OID_1, "google_ads", "acme_ga_3",  75_000, 10_000, 400,  8, 6,  2.5),
    (BRAND_OID_1, "meta",       "acme_mt_1",  50_000, 20_000, 200, 10, 3,  1.5),
    (BRAND_OID_1, "meta",       "acme_mt_2",  30_000,  8_000, 150, 15, 7,  3.0),
    (BRAND_OID_2, "meta",       "globex_mt_1",70_000, 25_000, 250, 25, 5,  1.2),
    (BRAND_OID_2, "meta",       "globex_mt_2",55_000, 18_000, 180, 18, 6,  2.0),
    (BRAND_OID_2, "meta",       "globex_mt_3",20_000, 30_000,  80,  3, 1,  0.8),
    (BRAND_OID_3, "google_ads", "initech_ga_1",100_000,15_000,600, 20,12,  2.8),
    (BRAND_OID_3, "google_ads", "initech_ga_2",130_000,18_000,700, 25,15,  3.2),
]


def _day_utc(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time(), tzinfo=UTC)


def _perf_rows(days: int = 30, run_id: str | None = None) -> list[dict[str, Any]]:
    """Generate `days` × len(_CAMPAIGN_PROFILES) performance documents."""
    if run_id is None:
        run_id = str(uuid.uuid4())

    docs: list[dict[str, Any]] = []
    now = NOW

    for brand_oid, source, camp_key, base_spend, base_imp, base_clicks, \
            base_leads, base_convs, cv_mult in _CAMPAIGN_PROFILES:

        camp_oid = _CAMP_IDS[camp_key]

        for offset in range(days):
            record_date = TODAY - timedelta(days=offset)
            # Deterministic RNG per (campaign, date) so re-runs produce same numbers
            rng = random.Random(hash(f"{camp_key}:{record_date}") & 0xFFFF_FFFF)

            # ±20% noise
            def vary(base: int) -> int:
                return max(0, int(base * rng.uniform(0.8, 1.2)))

            spend     = vary(base_spend)
            imp       = vary(base_imp)
            clicks    = min(vary(base_clicks), imp)
            leads     = vary(base_leads)
            convs     = min(vary(base_convs), leads)
            cv        = int(spend * cv_mult * rng.uniform(0.85, 1.15)) if spend else 0
            reach     = int(imp * rng.uniform(0.6, 0.85))

            ctr        = clicks / imp if imp else 0.0
            cpc        = spend // clicks if clicks else 0
            cpm        = int(spend * 1000 // imp) if imp else 0
            cpl        = spend // leads if leads else 0
            roas_val   = cv / spend if spend else 0.0

            docs.append({
                "_id": ObjectId(),
                "brand_id": brand_oid,
                "campaign_id": camp_oid,
                "source": source,
                "date": _day_utc(record_date),
                "ingested_at": now,
                "ingestion_run_id": run_id,
                "spend_paise": spend,
                "impressions": imp,
                "clicks": clicks,
                "reach": reach,
                "frequency": round(imp / reach, 2) if reach else 1.0,
                "leads": leads,
                "conversions": convs,
                "conversion_value_paise": cv,
                "ctr": round(ctr, 6),
                "cpc_paise": cpc,
                "cpm_paise": cpm,
                "cpl_paise": cpl,
                "roas": round(roas_val, 4),
            })

    return docs


def _ingestion_log(brand_oid: ObjectId, source: str) -> dict[str, Any]:
    """Simulate a successful ingestion run for yesterday."""
    now = NOW
    started = now - timedelta(minutes=3)
    return {
        "_id": ObjectId(),
        "run_id": str(uuid.uuid4()),
        "brand_id": brand_oid,
        "source": source,
        "target_date": _day_utc(TODAY - timedelta(days=1)),
        "status": "success",
        "started_at": started,
        "completed_at": now,
        "records_fetched": 3,
        "records_upserted": 3,
        "error_message": None,
        "retry_count": 0,
        "is_backfill": False,
    }


# ── DB write helpers ──────────────────────────────────────────────────────────

async def _upsert(col: Any, filter_: dict, doc: dict, *, dry_run: bool) -> str:
    """Insert or replace a document; skip if filter already matches."""
    if dry_run:
        return "dry-run"
    existing = await col.find_one(filter_, {"_id": 1})
    if existing:
        return "exists"
    await col.insert_one(doc)
    return "created"


async def _bulk_upsert_perf(
    col: Any,
    docs: list[dict],
    *,
    dry_run: bool,
) -> tuple[int, int]:
    """Insert performance docs; skip those whose natural key already exists.

    Returns (inserted, skipped).
    """
    if dry_run:
        return 0, len(docs)

    inserted = skipped = 0
    for doc in docs:
        key = {
            "brand_id": doc["brand_id"],
            "source":   doc["source"],
            "campaign_id": doc["campaign_id"],
            "date":     doc["date"],
        }
        existing = await col.find_one(key, {"_id": 1})
        if existing:
            skipped += 1
            continue
        await col.insert_one(doc)
        inserted += 1

    return inserted, skipped


# ── Main ───────────────────────────────────────────────────────────────────────

async def seed(db: AsyncIOMotorDatabase, *, dry_run: bool = False) -> None:  # type: ignore[type-arg]
    prefix = "[DRY-RUN] " if dry_run else ""

    # 1. Agency
    r = await _upsert(db["agencies"], {"slug": "bb-digital"}, _agency(), dry_run=dry_run)
    print(f"  {prefix}agency:   BB Digital Agency — {r}")

    # 2. Brands
    for brand in _brands():
        r = await _upsert(
            db["brands"],
            {"agency_id": AGENCY_OID, "slug": brand["slug"]},
            brand,
            dry_run=dry_run,
        )
        print(f"  {prefix}brand:    {brand['name']} — {r}")

    # 3. Users
    for user in _users():
        r = await _upsert(
            db["users"],
            {"email": user["email"]},
            user,
            dry_run=dry_run,
        )
        print(f"  {prefix}user:     {user['email']} [{user['role']}] — {r}")

    # 4. Campaigns
    created = skipped = 0
    for camp in _campaigns():
        r = await _upsert(
            db["campaigns"],
            {"brand_id": camp["brand_id"], "source": camp["source"],
             "external_id": camp["external_id"]},
            camp,
            dry_run=dry_run,
        )
        if r == "created":
            created += 1
        else:
            skipped += 1
    print(f"  {prefix}campaigns: {created} created, {skipped} skipped")

    # 5. Performance data — 30 days
    run_id = str(uuid.uuid4())
    perf_docs = _perf_rows(days=30, run_id=run_id)
    ins, skp = await _bulk_upsert_perf(db["ad_performance_raw"], perf_docs, dry_run=dry_run)
    print(f"  {prefix}performance: {ins} inserted, {skp} skipped  ({len(perf_docs)} total rows)")

    # 6. Ingestion logs — one per brand × source (yesterday)
    log_specs = [
        (BRAND_OID_1, "google_ads"),
        (BRAND_OID_1, "meta"),
        (BRAND_OID_2, "meta"),
        (BRAND_OID_3, "google_ads"),
    ]
    for brand_oid, source in log_specs:
        log_doc = _ingestion_log(brand_oid, source)
        r = await _upsert(
            db["ingestion_logs"],
            {"brand_id": brand_oid, "source": source,
             "target_date": _day_utc(TODAY - timedelta(days=1)),
             "status": "success"},
            log_doc,
            dry_run=dry_run,
        )
        print(f"  {prefix}ingestion_log: {source}/{brand_oid} — {r}")


async def main(dry_run: bool = False) -> None:
    print(f"Connecting to {MONGODB_URI} / {MONGODB_DB} …")
    client: AsyncIOMotorClient = AsyncIOMotorClient(  # type: ignore[type-arg]
        MONGODB_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
    )
    try:
        await client.admin.command("ping")
        print("Connected.\n")
    except Exception as exc:
        print(f"ERROR: Cannot connect to MongoDB: {exc}")
        sys.exit(1)

    db = client[MONGODB_DB]

    print("Seeding …")
    await seed(db, dry_run=dry_run)
    print("\nDone.")

    if not dry_run:
        print("\nCredentials:")
        print(f"  super_admin@bb.local  /  {SEED_PASSWORD}")
        print(f"  admin@bb.local        /  {SEED_PASSWORD}")
        print(f"  analyst@bb.local      /  {SEED_PASSWORD}")
        print(f"  viewer@bb.local       /  {SEED_PASSWORD}  (Acme Corp only)")

    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed the BB Ads database with test data.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be inserted without writing to the DB.")
    args = parser.parse_args()
    asyncio.run(main(dry_run=args.dry_run))
