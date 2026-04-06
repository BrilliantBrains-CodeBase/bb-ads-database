"""
BrandScopedRepository — the security boundary between tenants.

Every repository that stores brand data MUST subclass this. The sole
constructor argument beyond the collection is `brand_id`, which is
injected into every single read/write/delete operation automatically.

Design contract
───────────────
• `brand_id` is ALWAYS sourced from the authenticated user's JWT (via
  `CurrentUser.user_id` or an explicit parameter passed from the route).
  It is never trusted from request bodies.
• The internal `_col` attribute is private. Subclasses call the public
  methods below; raw `_col` access is intentionally avoided outside this
  file.
• For every method the caller's filter is sanitised: any `brand_id` key
  the caller supplies is silently replaced by the instance's own value.
  This means even a bug in a subclass cannot leak cross-tenant data.
• `aggregate()` prepends `{"$match": {"brand_id": <own>}}` as the first
  pipeline stage so brand isolation holds even for complex aggregations.

Subclass pattern
────────────────
    class CampaignsRepository(BrandScopedRepository):
        def __init__(self, db: AsyncIOMotorDatabase, brand_id: str) -> None:
            super().__init__(db["campaigns"], brand_id)

        async def find_active(self) -> list[dict]:
            return await self.find({"status": "active"})
"""

from __future__ import annotations

from typing import Any

from motor.motor_asyncio import AsyncIOMotorCollection


class BrandScopedRepository:
    """Base class for all brand-data repositories.

    Args:
        collection: The Motor collection to operate on.
        brand_id:   The brand this repository instance is scoped to.
                    Must come from a validated JWT — never from user input.
    """

    def __init__(
        self,
        collection: AsyncIOMotorCollection,  # type: ignore[type-arg]
        brand_id: str,
    ) -> None:
        self._col = collection
        self._brand_id = brand_id

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _scope(self, filter: dict[str, Any] | None = None) -> dict[str, Any]:
        """Return filter with brand_id always set to this instance's value.

        Any `brand_id` key supplied by the caller is stripped and replaced,
        making cross-tenant queries structurally impossible.
        """
        scoped: dict[str, Any] = {"brand_id": self._brand_id}
        if filter:
            for k, v in filter.items():
                if k != "brand_id":          # caller cannot override brand_id
                    scoped[k] = v
        return scoped

    def _inject_brand(self, document: dict[str, Any]) -> dict[str, Any]:
        """Return a copy of document with brand_id forced to this instance."""
        return {**document, "brand_id": self._brand_id}

    # ── Read ──────────────────────────────────────────────────────────────────

    async def find(
        self,
        filter: dict[str, Any] | None = None,
        *,
        sort: list[tuple[str, int]] | None = None,
        skip: int = 0,
        limit: int = 0,
        projection: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Return all documents matching filter within this brand."""
        cursor = self._col.find(self._scope(filter), projection)
        if sort:
            cursor = cursor.sort(sort)
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        return await cursor.to_list(length=None if not limit else limit)

    async def find_one(
        self,
        filter: dict[str, Any] | None = None,
        *,
        projection: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Return the first document matching filter within this brand."""
        return await self._col.find_one(self._scope(filter), projection)

    async def count(self, filter: dict[str, Any] | None = None) -> int:
        """Count documents matching filter within this brand."""
        return await self._col.count_documents(self._scope(filter))

    # ── Write ─────────────────────────────────────────────────────────────────

    async def insert_one(self, document: dict[str, Any]) -> str:
        """Insert a single document; returns the inserted _id as a string."""
        result = await self._col.insert_one(self._inject_brand(document))
        return str(result.inserted_id)

    async def insert_many(self, documents: list[dict[str, Any]]) -> list[str]:
        """Insert multiple documents; returns inserted _ids as strings."""
        if not documents:
            return []
        scoped = [self._inject_brand(doc) for doc in documents]
        result = await self._col.insert_many(scoped)
        return [str(oid) for oid in result.inserted_ids]

    async def update_one(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        *,
        upsert: bool = False,
    ) -> int:
        """Update a single matching document. Returns modified_count."""
        result = await self._col.update_one(
            self._scope(filter), update, upsert=upsert
        )
        return result.modified_count

    async def update_many(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
    ) -> int:
        """Update all matching documents. Returns modified_count."""
        result = await self._col.update_many(self._scope(filter), update)
        return result.modified_count

    # ── Delete ────────────────────────────────────────────────────────────────

    async def delete_one(self, filter: dict[str, Any]) -> int:
        """Delete a single matching document. Returns deleted_count."""
        result = await self._col.delete_one(self._scope(filter))
        return result.deleted_count

    async def delete_many(self, filter: dict[str, Any]) -> int:
        """Delete all matching documents. Returns deleted_count."""
        result = await self._col.delete_many(self._scope(filter))
        return result.deleted_count

    # ── Aggregate ─────────────────────────────────────────────────────────────

    async def aggregate(
        self, pipeline: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Run an aggregation pipeline scoped to this brand.

        A `$match` on `brand_id` is unconditionally prepended as stage 0
        so no pipeline can accidentally read another tenant's data.
        """
        scoped_pipeline = [{"$match": {"brand_id": self._brand_id}}, *pipeline]
        cursor = self._col.aggregate(scoped_pipeline)
        return await cursor.to_list(length=None)
