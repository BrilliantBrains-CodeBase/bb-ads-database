"""
Unit tests for BrandScopedRepository.

Coverage targets (per plan: 100% on BrandScopedRepository):
  ✓ brand_id injected on every write method (insert_one, insert_many)
  ✓ brand_id injected / enforced on every read method (find, find_one, count)
  ✓ brand_id injected / enforced on every update method (update_one, update_many)
  ✓ brand_id injected / enforced on every delete method (delete_one, delete_many)
  ✓ brand_id prepended to aggregate pipeline
  ✓ caller-supplied brand_id in filter is silently replaced (cannot override)
  ✓ cross-tenant query is structurally impossible across all methods
  ✓ insert_many with empty list is a no-op

Uses mongomock-motor (in-process, no real MongoDB needed).
"""

import pytest
from mongomock_motor import AsyncMongoMockClient

from app.repositories.base import BrandScopedRepository


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def db():
    """In-process mock MongoDB database."""
    client = AsyncMongoMockClient()
    return client["test_db"]


@pytest.fixture
def repo_a(db):
    """BrandScopedRepository scoped to brand_a."""
    return BrandScopedRepository(db["items"], "brand_a")


@pytest.fixture
def repo_b(db):
    """BrandScopedRepository scoped to brand_b — shares the same collection."""
    return BrandScopedRepository(db["items"], "brand_b")


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _insert_raw(db, brand_id: str, name: str) -> None:
    """Bypass the scoped repo to seed data directly into the collection."""
    await db["items"].insert_one({"brand_id": brand_id, "name": name})


# ── _scope() unit tests ───────────────────────────────────────────────────────

class TestScope:
    def test_adds_brand_id_to_empty_filter(self, repo_a):
        assert repo_a._scope() == {"brand_id": "brand_a"}

    def test_merges_caller_filter(self, repo_a):
        result = repo_a._scope({"status": "active"})
        assert result == {"brand_id": "brand_a", "status": "active"}

    def test_strips_caller_brand_id(self, repo_a):
        """Caller cannot override brand_id through the filter."""
        result = repo_a._scope({"brand_id": "brand_b", "status": "active"})
        assert result["brand_id"] == "brand_a"
        assert result["status"] == "active"

    def test_strips_malicious_brand_id_operator(self, repo_a):
        """brand_id with operator expression is also stripped."""
        result = repo_a._scope({"brand_id": {"$in": ["brand_b", "brand_c"]}})
        assert result["brand_id"] == "brand_a"


# ── _inject_brand() unit tests ────────────────────────────────────────────────

class TestInjectBrand:
    def test_adds_brand_id(self, repo_a):
        result = repo_a._inject_brand({"name": "x"})
        assert result == {"name": "x", "brand_id": "brand_a"}

    def test_overrides_wrong_brand_id(self, repo_a):
        result = repo_a._inject_brand({"name": "x", "brand_id": "brand_b"})
        assert result["brand_id"] == "brand_a"

    def test_does_not_mutate_original(self, repo_a):
        original = {"name": "x"}
        repo_a._inject_brand(original)
        assert "brand_id" not in original


# ── insert_one ────────────────────────────────────────────────────────────────

class TestInsertOne:
    async def test_returns_string_id(self, repo_a):
        oid = await repo_a.insert_one({"name": "alpha"})
        assert isinstance(oid, str)
        assert len(oid) == 24  # hex ObjectId

    async def test_injects_brand_id(self, repo_a, db):
        await repo_a.insert_one({"name": "alpha"})
        doc = await db["items"].find_one({"name": "alpha"})
        assert doc is not None
        assert doc["brand_id"] == "brand_a"

    async def test_overrides_wrong_brand_id_in_document(self, repo_a, db):
        await repo_a.insert_one({"name": "beta", "brand_id": "brand_b"})
        doc = await db["items"].find_one({"name": "beta"})
        assert doc["brand_id"] == "brand_a"


# ── insert_many ───────────────────────────────────────────────────────────────

class TestInsertMany:
    async def test_returns_list_of_string_ids(self, repo_a):
        ids = await repo_a.insert_many([{"n": 1}, {"n": 2}])
        assert len(ids) == 2
        assert all(isinstance(i, str) and len(i) == 24 for i in ids)

    async def test_all_documents_get_brand_id(self, repo_a, db):
        await repo_a.insert_many([{"n": 1}, {"n": 2}, {"n": 3}])
        docs = await db["items"].find({"brand_id": "brand_a"}).to_list(None)
        assert len(docs) == 3

    async def test_empty_list_is_noop(self, repo_a):
        ids = await repo_a.insert_many([])
        assert ids == []


# ── find ──────────────────────────────────────────────────────────────────────

class TestFind:
    async def test_returns_only_own_brand(self, repo_a, repo_b, db):
        await _insert_raw(db, "brand_a", "a1")
        await _insert_raw(db, "brand_a", "a2")
        await _insert_raw(db, "brand_b", "b1")

        results = await repo_a.find()
        names = {r["name"] for r in results}
        assert names == {"a1", "a2"}

    async def test_empty_when_no_match(self, repo_a, db):
        await _insert_raw(db, "brand_b", "b1")
        assert await repo_a.find() == []

    async def test_additional_filter_applied(self, repo_a, db):
        await _insert_raw(db, "brand_a", "active")
        await _insert_raw(db, "brand_a", "inactive")
        results = await repo_a.find({"name": "active"})
        assert len(results) == 1
        assert results[0]["name"] == "active"

    async def test_limit_respected(self, repo_a):
        await repo_a.insert_many([{"n": i} for i in range(5)])
        results = await repo_a.find(limit=2)
        assert len(results) == 2

    async def test_projection_applied(self, repo_a):
        await repo_a.insert_one({"name": "x", "secret": "hidden"})
        results = await repo_a.find(projection={"name": 1, "_id": 0})
        assert "secret" not in results[0]
        assert results[0]["name"] == "x"


# ── find_one ──────────────────────────────────────────────────────────────────

class TestFindOne:
    async def test_returns_own_brand_doc(self, repo_a, db):
        await _insert_raw(db, "brand_a", "mine")
        doc = await repo_a.find_one({"name": "mine"})
        assert doc is not None
        assert doc["brand_id"] == "brand_a"

    async def test_returns_none_for_other_brand(self, repo_a, db):
        await _insert_raw(db, "brand_b", "theirs")
        doc = await repo_a.find_one({"name": "theirs"})
        assert doc is None

    async def test_returns_none_when_empty(self, repo_a):
        assert await repo_a.find_one() is None


# ── count ─────────────────────────────────────────────────────────────────────

class TestCount:
    async def test_counts_only_own_brand(self, repo_a, repo_b, db):
        await _insert_raw(db, "brand_a", "a1")
        await _insert_raw(db, "brand_a", "a2")
        await _insert_raw(db, "brand_b", "b1")

        assert await repo_a.count() == 2
        assert await repo_b.count() == 1

    async def test_count_with_filter(self, repo_a, db):
        await _insert_raw(db, "brand_a", "x")
        await _insert_raw(db, "brand_a", "y")
        assert await repo_a.count({"name": "x"}) == 1


# ── update_one ────────────────────────────────────────────────────────────────

class TestUpdateOne:
    async def test_updates_own_brand_doc(self, repo_a, db):
        await _insert_raw(db, "brand_a", "old")
        modified = await repo_a.update_one({"name": "old"}, {"$set": {"name": "new"}})
        assert modified == 1
        doc = await db["items"].find_one({"brand_id": "brand_a"})
        assert doc["name"] == "new"

    async def test_does_not_update_other_brand(self, repo_a, repo_b, db):
        await _insert_raw(db, "brand_b", "target")
        modified = await repo_a.update_one({"name": "target"}, {"$set": {"name": "hacked"}})
        assert modified == 0
        doc = await db["items"].find_one({"brand_id": "brand_b"})
        assert doc["name"] == "target"

    async def test_upsert_injects_brand_id(self, repo_a, db):
        await repo_a.update_one(
            {"name": "new_doc"},
            {"$set": {"value": 42}},
            upsert=True,
        )
        doc = await db["items"].find_one({"name": "new_doc"})
        assert doc is not None
        assert doc["brand_id"] == "brand_a"


# ── update_many ───────────────────────────────────────────────────────────────

class TestUpdateMany:
    async def test_updates_all_own_brand_docs(self, repo_a, db):
        await _insert_raw(db, "brand_a", "x")
        await _insert_raw(db, "brand_a", "x")
        await _insert_raw(db, "brand_b", "x")

        modified = await repo_a.update_many({"name": "x"}, {"$set": {"updated": True}})
        assert modified == 2

        # brand_b doc must be untouched
        b_doc = await db["items"].find_one({"brand_id": "brand_b"})
        assert b_doc.get("updated") is None


# ── delete_one ────────────────────────────────────────────────────────────────

class TestDeleteOne:
    async def test_deletes_own_brand_doc(self, repo_a, db):
        await _insert_raw(db, "brand_a", "gone")
        deleted = await repo_a.delete_one({"name": "gone"})
        assert deleted == 1
        assert await db["items"].find_one({"name": "gone"}) is None

    async def test_does_not_delete_other_brand(self, repo_a, db):
        await _insert_raw(db, "brand_b", "safe")
        deleted = await repo_a.delete_one({"name": "safe"})
        assert deleted == 0
        assert await db["items"].find_one({"name": "safe"}) is not None


# ── delete_many ───────────────────────────────────────────────────────────────

class TestDeleteMany:
    async def test_deletes_only_own_brand(self, repo_a, repo_b, db):
        await _insert_raw(db, "brand_a", "a1")
        await _insert_raw(db, "brand_a", "a2")
        await _insert_raw(db, "brand_b", "b1")

        deleted = await repo_a.delete_many({})
        assert deleted == 2
        assert await repo_b.count() == 1


# ── aggregate ─────────────────────────────────────────────────────────────────

class TestAggregate:
    async def test_prepends_brand_match(self, repo_a, repo_b, db):
        await _insert_raw(db, "brand_a", "a1")
        await _insert_raw(db, "brand_a", "a2")
        await _insert_raw(db, "brand_b", "b1")

        results = await repo_a.aggregate([{"$project": {"name": 1, "_id": 0}}])
        names = {r["name"] for r in results}
        assert names == {"a1", "a2"}

    async def test_group_aggregation_scoped(self, repo_a, db):
        await repo_a.insert_many([{"value": 10}, {"value": 20}])
        await _insert_raw(db, "brand_b", "x")  # brand_b has no value field

        results = await repo_a.aggregate([
            {"$group": {"_id": None, "total": {"$sum": "$value"}}}
        ])
        assert results[0]["total"] == 30

    async def test_pipeline_cannot_escape_brand(self, repo_a, db):
        """A $match in the pipeline cannot widen scope to other brands."""
        await _insert_raw(db, "brand_b", "secret")

        # Attempt to match all documents via a pipeline $match
        results = await repo_a.aggregate([
            {"$match": {"brand_id": {"$exists": True}}}  # tries to see all brands
        ])
        # The prepended $match{"brand_id": "brand_a"} AND-filters first,
        # so brand_b docs never enter the pipeline
        assert all(r["brand_id"] == "brand_a" for r in results)


# ── Cross-tenant impossibility ────────────────────────────────────────────────

class TestCrossTenantImpossible:
    """
    Exhaustive proof that a brand_a repository instance cannot read,
    modify, or delete brand_b data, regardless of what filter the caller
    passes.
    """

    async def test_find_cannot_see_other_tenant(self, repo_a, db):
        await _insert_raw(db, "brand_b", "secret")
        assert await repo_a.find({"brand_id": "brand_b"}) == []

    async def test_find_one_cannot_see_other_tenant(self, repo_a, db):
        await _insert_raw(db, "brand_b", "secret")
        assert await repo_a.find_one({"brand_id": "brand_b"}) is None

    async def test_count_cannot_count_other_tenant(self, repo_a, db):
        await _insert_raw(db, "brand_b", "x")
        assert await repo_a.count({"brand_id": "brand_b"}) == 0

    async def test_update_cannot_touch_other_tenant(self, repo_a, db):
        await _insert_raw(db, "brand_b", "immutable")
        modified = await repo_a.update_one(
            {"brand_id": "brand_b"},
            {"$set": {"name": "hacked"}},
        )
        assert modified == 0
        doc = await db["items"].find_one({"brand_id": "brand_b"})
        assert doc["name"] == "immutable"

    async def test_delete_cannot_remove_other_tenant(self, repo_a, db):
        await _insert_raw(db, "brand_b", "protected")
        deleted = await repo_a.delete_one({"brand_id": "brand_b"})
        assert deleted == 0
        assert await db["items"].find_one({"brand_id": "brand_b"}) is not None

    async def test_aggregate_cannot_leak_other_tenant(self, repo_a, db):
        await _insert_raw(db, "brand_b", "hidden")
        results = await repo_a.aggregate([
            {"$match": {}},           # match everything
            {"$project": {"_id": 0, "brand_id": 1, "name": 1}},
        ])
        assert all(r.get("brand_id") == "brand_a" for r in results)
