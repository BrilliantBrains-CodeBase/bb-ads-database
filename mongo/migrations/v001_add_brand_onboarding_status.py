"""
v001_add_brand_onboarding_status.py

What: Backfill onboarding fields on existing brand documents that were
      created before the Phase 2.5 schema update.

Fields added
────────────
  onboarding_status  — default "pending" for brands that don't have it
  clickup_task_id    — left absent (optional, set when ClickUp task is created)
  storage_path       — left absent (optional, set when folders are provisioned)
  onboarded_at       — left absent (set only when status transitions to "completed")
  onboarded_by       — left absent (set only when onboarding is completed)

Why:  The 01_create_indexes.js schema validator already declares these fields,
      but documents inserted before this migration don't carry onboarding_status.
      Without it, GET /brands/{id}/onboarding-status returns None instead of
      the expected "pending" default, and the onboarding router crashes on
      brands.get("onboarding_status", "pending") — technically safe but
      inconsistent with newer documents.

Safe to re-run: yes — uses $exists guard so already-migrated docs are untouched.
Estimated time: < 1 s on a dataset of < 10 000 brands.

Usage
─────
  # Staging / local
  python mongo/migrations/v001_add_brand_onboarding_status.py

  # Production (pass URI explicitly)
  python mongo/migrations/v001_add_brand_onboarding_status.py \\
      mongodb+srv://user:pass@cluster/bb_ads
"""

from __future__ import annotations

import asyncio
import sys
from datetime import UTC, datetime

from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URI = sys.argv[1] if len(sys.argv) > 1 else "mongodb://localhost:27017"
DB_NAME = "bb_ads"


async def run() -> None:
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]

    print(f"Connecting to: {MONGO_URI} / {DB_NAME}")

    # ── 1. Backfill onboarding_status = "pending" ─────────────────────────────
    #    Only touches documents that don't already have the field.
    result = await db["brands"].update_many(
        {"onboarding_status": {"$exists": False}},
        {
            "$set": {
                "onboarding_status": "pending",
                "updated_at": datetime.now(UTC),
            }
        },
    )
    print(f"onboarding_status backfilled: {result.modified_count} documents updated")

    # ── 2. Verify ─────────────────────────────────────────────────────────────
    missing = await db["brands"].count_documents(
        {"onboarding_status": {"$exists": False}}
    )
    if missing:
        print(f"WARNING: {missing} brand(s) still lack onboarding_status — investigate")
        sys.exit(1)

    total = await db["brands"].count_documents({})
    print(f"Verification passed: all {total} brand(s) have onboarding_status")

    # ── 3. Report onboarding_status distribution ──────────────────────────────
    pipeline = [
        {"$group": {"_id": "$onboarding_status", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    cursor = db["brands"].aggregate(pipeline)
    rows = await cursor.to_list(length=None)
    print("\nonboarding_status distribution:")
    for row in rows:
        print(f"  {row['_id']:20s}  {row['count']}")

    client.close()
    print("\nMigration v001 complete.")


if __name__ == "__main__":
    asyncio.run(run())
