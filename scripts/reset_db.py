#!/usr/bin/env python3
"""
reset_db.py — Drop all collections, recreate indexes, and re-seed.

WARNING: This is destructive.  All data in the target database will be
permanently deleted.  Never run against a staging or production database
unless you really mean it.

Usage
─────
  # Default (mongodb://localhost:27017 / bb_ads)
  python scripts/reset_db.py

  # Specific URI / DB
  MONGODB_URI=mongodb://user:pass@host:27017 MONGODB_DB_NAME=bb_ads_test \
      python scripts/reset_db.py

  # Skip the "are you sure?" prompt (CI / automation)
  python scripts/reset_db.py --yes

  # Drop + recreate indexes only (skip seed)
  python scripts/reset_db.py --no-seed

  # Dry-run: print what would happen, touch nothing
  python scripts/reset_db.py --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Any

# ── Path setup ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

# ── Configuration ─────────────────────────────────────────────────────────────

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB  = os.getenv("MONGODB_DB_NAME", "bb_ads")

# All collections defined in mongo/init/01_create_indexes.js, drop order matters
# (children before parents to avoid FK-style issues in application logic)
_ALL_COLLECTIONS = [
    "ad_performance_raw",
    "performance_rollups",
    "ingestion_logs",
    "anomalies",
    "claude_conversations",
    "scheduled_reports",
    "campaigns",
    "users",
    "brands",
    "agencies",
]


# ── Drop ───────────────────────────────────────────────────────────────────────

async def drop_all(db: AsyncIOMotorDatabase, *, dry_run: bool) -> None:  # type: ignore[type-arg]
    """Drop every application collection."""
    existing: list[str] = await db.list_collection_names()
    for name in _ALL_COLLECTIONS:
        if name in existing:
            if dry_run:
                print(f"  [DRY-RUN] would drop: {name}")
            else:
                await db.drop_collection(name)
                print(f"  dropped: {name}")
        else:
            print(f"  skip (not found): {name}")


# ── Index creation ─────────────────────────────────────────────────────────────
# Mirrors mongo/init/01_create_indexes.js in Python so we don't need mongosh.

async def _ci(col: Any, keys: list[tuple[str, Any]], **kwargs: Any) -> None:
    """create_index wrapper — silently skips if index already exists."""
    await col.create_index(keys, **kwargs)


async def create_indexes(db: AsyncIOMotorDatabase, *, dry_run: bool) -> None:  # type: ignore[type-arg]
    if dry_run:
        print("  [DRY-RUN] would recreate all indexes")
        return

    from pymongo import ASCENDING, DESCENDING

    # agencies
    await _ci(db["agencies"], [("slug", ASCENDING)],
              unique=True, name="agencies_slug_unique")

    # brands
    await _ci(db["brands"], [("agency_id", ASCENDING), ("slug", ASCENDING)],
              unique=True, name="brands_agency_slug_unique")
    await _ci(db["brands"], [("agency_id", ASCENDING), ("is_active", ASCENDING)],
              name="brands_agency_active")

    # users
    await _ci(db["users"], [("email", ASCENDING)],
              unique=True, name="users_email_unique")
    await _ci(db["users"], [("agency_id", ASCENDING), ("role", ASCENDING)],
              name="users_agency_role")
    await _ci(db["users"], [("api_keys.key_hash", ASCENDING)],
              sparse=True, name="users_api_key_hash_sparse")

    # campaigns
    await _ci(db["campaigns"],
              [("brand_id", ASCENDING), ("source", ASCENDING), ("external_id", ASCENDING)],
              unique=True, name="campaigns_brand_source_ext_unique")
    await _ci(db["campaigns"],
              [("brand_id", ASCENDING), ("our_status", ASCENDING)],
              name="campaigns_brand_status")
    await _ci(db["campaigns"],
              [("brand_id", ASCENDING), ("created_by", ASCENDING)],
              name="campaigns_brand_creator")
    await _ci(db["campaigns"],
              [("brand_id", ASCENDING), ("start_date", ASCENDING), ("end_date", ASCENDING)],
              name="campaigns_brand_dates")

    # ad_performance_raw
    await _ci(db["ad_performance_raw"],
              [("brand_id", ASCENDING), ("source", ASCENDING),
               ("campaign_id", ASCENDING), ("date", ASCENDING)],
              unique=True, name="perf_raw_natural_key_unique")
    await _ci(db["ad_performance_raw"],
              [("brand_id", ASCENDING), ("date", DESCENDING), ("source", ASCENDING)],
              name="perf_raw_brand_date_source")
    await _ci(db["ad_performance_raw"],
              [("brand_id", ASCENDING), ("campaign_id", ASCENDING), ("date", DESCENDING)],
              name="perf_raw_brand_campaign_date")
    await _ci(db["ad_performance_raw"],
              [("brand_id", ASCENDING), ("date", DESCENDING)],
              partialFilterExpression={"spend_paise": {"$gt": 0}},
              name="perf_raw_brand_date_partial")
    await _ci(db["ad_performance_raw"],
              [("ingestion_run_id", ASCENDING)],
              name="perf_raw_run_id")

    # performance_rollups
    await _ci(db["performance_rollups"],
              [("brand_id", ASCENDING), ("period_type", ASCENDING),
               ("period_start", ASCENDING), ("source", ASCENDING)],
              unique=True, name="rollups_period_unique")
    await _ci(db["performance_rollups"],
              [("brand_id", ASCENDING), ("period_type", ASCENDING),
               ("period_start", DESCENDING)],
              name="rollups_brand_period_date")

    # ingestion_logs
    await _ci(db["ingestion_logs"],
              [("brand_id", ASCENDING), ("source", ASCENDING), ("target_date", ASCENDING)],
              name="ingest_logs_brand_source_date")
    await _ci(db["ingestion_logs"],
              [("status", ASCENDING), ("started_at", DESCENDING)],
              name="ingest_logs_status_time")
    await _ci(db["ingestion_logs"],
              [("started_at", ASCENDING)],
              expireAfterSeconds=90 * 24 * 60 * 60,
              name="ingest_logs_ttl_90d")

    # anomalies
    await _ci(db["anomalies"],
              [("brand_id", ASCENDING), ("detected_at", DESCENDING)],
              name="anomalies_brand_time")
    await _ci(db["anomalies"],
              [("brand_id", ASCENDING), ("acknowledged", ASCENDING), ("severity", ASCENDING)],
              name="anomalies_brand_ack_severity")
    await _ci(db["anomalies"],
              [("detected_at", ASCENDING)],
              expireAfterSeconds=180 * 24 * 60 * 60,
              name="anomalies_ttl_180d")

    # claude_conversations
    await _ci(db["claude_conversations"],
              [("brand_id", ASCENDING), ("user_id", ASCENDING), ("updated_at", DESCENDING)],
              name="conv_brand_user_updated")
    await _ci(db["claude_conversations"],
              [("updated_at", ASCENDING)],
              expireAfterSeconds=365 * 24 * 60 * 60,
              name="conv_ttl_365d")

    # scheduled_reports
    await _ci(db["scheduled_reports"],
              [("brand_id", ASCENDING), ("is_active", ASCENDING)],
              name="reports_brand_active")
    await _ci(db["scheduled_reports"],
              [("next_run", ASCENDING), ("is_active", ASCENDING)],
              name="reports_next_run_active")

    print("  indexes created (27 total)")


# ── Main ───────────────────────────────────────────────────────────────────────

async def reset(
    db: AsyncIOMotorDatabase,  # type: ignore[type-arg]
    *,
    seed: bool = True,
    dry_run: bool = False,
) -> None:
    print("\n── Dropping collections ──────────────────────────────────────────")
    await drop_all(db, dry_run=dry_run)

    print("\n── Recreating indexes ────────────────────────────────────────────")
    await create_indexes(db, dry_run=dry_run)

    if seed:
        print("\n── Seeding data ──────────────────────────────────────────────────")
        from scripts.seed_data import seed as run_seed
        await run_seed(db, dry_run=dry_run)

    print("\nReset complete.")


async def main(*, seed: bool, dry_run: bool, yes: bool) -> None:
    if not dry_run and not yes:
        print(f"This will PERMANENTLY DELETE all data in '{MONGODB_DB}' on:")
        print(f"  {MONGODB_URI}")
        answer = input("Type 'yes' to continue: ").strip()
        if answer.lower() != "yes":
            print("Aborted.")
            sys.exit(0)

    print(f"Connecting to {MONGODB_URI} / {MONGODB_DB} …")
    client: AsyncIOMotorClient = AsyncIOMotorClient(  # type: ignore[type-arg]
        MONGODB_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
    )
    try:
        await client.admin.command("ping")
        print("Connected.")
    except Exception as exc:
        print(f"ERROR: Cannot connect to MongoDB: {exc}")
        sys.exit(1)

    db = client[MONGODB_DB]
    await reset(db, seed=seed, dry_run=dry_run)
    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Drop all collections, recreate indexes, and re-seed the BB Ads database."
    )
    parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Skip the confirmation prompt (useful for CI).",
    )
    parser.add_argument(
        "--no-seed",
        action="store_true",
        help="Drop and recreate indexes only; skip seeding.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen without making any changes.",
    )
    args = parser.parse_args()

    asyncio.run(main(seed=not args.no_seed, dry_run=args.dry_run, yes=args.yes))
