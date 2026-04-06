"""
CampaignsRepository

Brand-scoped — extends BrandScopedRepository so brand_id is injected
into every query automatically.

Key behaviour:
  - `upsert_from_platform()` uses the natural key (brand_id, source,
    external_id) so ingestion can be retried without duplicates.
  - `created_at` is only set on first insert (`$setOnInsert`); subsequent
    upserts only update mutable fields.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.repositories.base import BrandScopedRepository

logger = structlog.get_logger(__name__)

Doc = dict[str, Any]


class CampaignsRepository(BrandScopedRepository):
    def __init__(self, db: AsyncIOMotorDatabase, brand_id: str) -> None:  # type: ignore[type-arg]
        super().__init__(db["campaigns"], brand_id)

    # ── Queries ───────────────────────────────────────────────────────────────

    async def find_by_id(self, campaign_id: str) -> Doc | None:
        """Find a campaign by its MongoDB _id within this brand."""
        try:
            oid = ObjectId(campaign_id)
        except Exception:
            return None
        return await self.find_one({"_id": oid})

    async def find_by_external_id(
        self, source: str, external_id: str
    ) -> Doc | None:
        """Look up a campaign by its platform-assigned ID (e.g. Google campaign ID)."""
        return await self.find_one({"source": source, "external_id": external_id})

    async def find_active(self) -> list[Doc]:
        """Return all active campaigns for this brand, sorted by name."""
        return await self.find({"our_status": "active"}, sort=[("name", 1)])

    async def find_by_source(self, source: str) -> list[Doc]:
        return await self.find({"source": source}, sort=[("name", 1)])

    # ── Upsert (ingestion path) ───────────────────────────────────────────────

    async def upsert_from_platform(
        self,
        source: str,
        external_id: str,
        data: Doc,
        *,
        created_by: str | None = None,
    ) -> str:
        """Idempotent upsert keyed on (brand_id, source, external_id).

        `data` should contain platform metadata: name, objective,
        platform_status, start_date, end_date, budget_type, budget_paise, etc.

        Returns the campaign's string _id (inserted or existing).
        """
        now = datetime.now(UTC)
        set_fields: Doc = {
            **{k: v for k, v in data.items() if k not in ("_id", "brand_id")},
            "brand_id": self._brand_id,
            "source": source,
            "external_id": external_id,
            "updated_at": now,
        }
        on_insert: Doc = {
            "created_at": now,
            "our_status": data.get("our_status", "active"),
        }
        if created_by:
            on_insert["created_by"] = ObjectId(created_by)

        result = await self._col.update_one(
            self._scope({"source": source, "external_id": external_id}),
            {"$set": set_fields, "$setOnInsert": on_insert},
            upsert=True,
        )
        if result.upserted_id:
            return str(result.upserted_id)
        # Was an update — retrieve the existing _id
        doc = await self._col.find_one(
            self._scope({"source": source, "external_id": external_id}),
            {"_id": 1},
        )
        return str(doc["_id"]) if doc else ""

    # ── Mutations ─────────────────────────────────────────────────────────────

    async def update(self, campaign_id: str, fields: Doc) -> bool:
        """Partial update by _id within this brand. Returns True if modified."""
        try:
            oid = ObjectId(campaign_id)
        except Exception:
            return False
        fields = {k: v for k, v in fields.items() if k not in ("_id", "brand_id")}
        fields["updated_at"] = datetime.now(UTC)
        return (
            await self.update_one({"_id": oid}, {"$set": fields}) > 0
        )

    async def update_status(self, campaign_id: str, our_status: str) -> bool:
        """Set our_status (active | paused | archived)."""
        return await self.update(campaign_id, {"our_status": our_status})
