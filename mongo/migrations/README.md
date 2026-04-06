# MongoDB Migrations

Schema changes and data migrations for the Agency Analytics Platform.

## Convention

```
mongo/migrations/
  v001_add_brand_onboarding_status.py
  v002_backfill_roas_field.py
  v003_rename_target_cpl_field.py
```

**Naming:** `v{NNN}_{short_description}.py` — zero-padded 3-digit version.

## Rules

1. **Never edit `01_create_indexes.js`** after initial deployment. All schema changes go through a versioned migration script.

2. **Migrations run manually** — there is no auto-runner. Every migration requires deliberate review before deployment:
   ```bash
   python mongo/migrations/v001_add_brand_onboarding_status.py --env production
   ```

3. **One script, one concern.** A migration should do exactly one logical thing (add a field, rename a field, backfill values, add an index).

4. **Idempotent.** Scripts must be safe to run more than once. Use `updateMany` with `$set` and check before modifying.

5. **Index changes** use `background: true` (MongoDB 4.x) / are always non-blocking in MongoDB 7.0. Still, schedule index builds during low-traffic windows.

6. **Test on staging first.** Run against a staging DB with a prod-size dataset before production.

## Script Template

```python
"""
v{NNN}_{description}.py

What: <what this migration does>
Why:  <why it's needed / which ticket>
Safe to re-run: yes
Estimated time: <seconds/minutes on prod dataset>
"""
import asyncio
import sys
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URI = sys.argv[1] if len(sys.argv) > 1 else "mongodb://localhost:27017"
DB_NAME   = "bb_ads"


async def run() -> None:
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]

    # --- migration logic here ---
    result = await db.brands.update_many(
        { "new_field": { "$exists": False } },   # idempotent guard
        { "$set": { "new_field": None } }
    )
    print(f"Modified {result.modified_count} documents")

    client.close()


if __name__ == "__main__":
    asyncio.run(run())
```

## Migration Log

| Version | Description | Applied | Applied By |
|---------|-------------|---------|------------|
| —       | Initial schema via 01_create_indexes.js | Phase 0 | bootstrap |
