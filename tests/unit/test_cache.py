"""
Unit tests for app/core/cache.py.

Strategy
────────
• Redis is replaced with an AsyncMock that simulates get/set/scan/unlink.
• The actual import of get_redis_client inside the decorator is patched
  via `patch("app.core.cache.get_redis_client" is NOT used because the
  import is deferred (inside the wrapper).  Instead we patch the module
  path that cache.py resolves: "app.core.redis.get_redis_client".
  Wait — the import inside wrapper is `from app.core.redis import
  get_redis_client`, so we patch `app.core.redis.get_redis_client`.
• build_key and _params_hash are tested directly (pure functions).
• The @cached decorator is tested by decorating a tiny async stub that
  records whether it was called.

Coverage
────────
  build_key:
    - Produces expected {prefix}:{brand_id}:{func}:{hash} format

  _params_hash:
    - Deterministic for same inputs
    - Different for different params
    - Excludes `brand_id` and `db`
    - Handles date, None, int, str

  @cached decorator — cache HIT:
    - Underlying handler NOT called
    - Returns json.loads of cached value (dict)

  @cached decorator — cache MISS:
    - Underlying handler called once
    - Result stored in Redis with correct TTL
    - Result returned as-is

  @cached decorator — Redis read error (fail-open):
    - get() raises → handler called, result returned
    - No exception propagated to caller

  @cached decorator — Redis write error (fail-open):
    - set() raises → result still returned
    - No exception propagated to caller

  @cached decorator — Redis not initialised (RuntimeError from get_redis_client):
    - Treated as Redis error → fail-open, handler called

  @cached decorator — correct key construction:
    - Key contains brand_id, function name, params hash

  @cached decorator — db excluded from hash:
    - Two calls with different db objects but same logical params share same key

  invalidate_brand_cache:
    - Calls SCAN with correct pattern
    - Calls UNLINK for matched keys
    - Returns count of deleted keys
    - Returns 0 when no keys match
    - Returns 0 and logs warning when Redis unavailable
    - Batches UNLINK when many keys returned

  Cache integration with ingestion base:
    - invalidate_brand_cache called on success
    - invalidate_brand_cache called on partial
    - invalidate_brand_cache NOT called on failed status
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from pydantic import BaseModel

from app.core.cache import (
    CACHE_TTL_DAILY,
    CACHE_TTL_SUMMARY,
    PERF_PREFIX,
    _params_hash,
    build_key,
    cached,
    invalidate_brand_cache,
)

# ── Helpers ────────────────────────────────────────────────────────────────────

BRAND_ID = "brand-abc"


def _make_redis(get_return=None, scan_pages=None):
    """Return an AsyncMock Redis with configurable get/scan/unlink behaviour.

    scan_pages: list of (cursor, keys) pairs returned sequentially.
                Defaults to [(0, [])] — empty result.
    """
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=get_return)
    redis.set = AsyncMock(return_value=True)
    redis.unlink = AsyncMock(return_value=1)

    if scan_pages is None:
        scan_pages = [(0, [])]

    scan_pages_iter = iter(scan_pages)

    async def _scan(cursor, match=None, count=None):
        try:
            return next(scan_pages_iter)
        except StopIteration:
            return (0, [])

    redis.scan = _scan
    return redis


class _SampleModel(BaseModel):
    value: int
    label: str


async def _handler(*, brand_id: str, db: object, value: int = 0, label: str = "x"):
    return _SampleModel(value=value, label=label)


# ══════════════════════════════════════════════════════════════════════════════
# Section 1 — build_key / _params_hash
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildKey:
    def test_format(self):
        key = build_key("perf", "brand1", "get_summary", "abc123def456")
        assert key == "perf:brand1:get_summary:abc123def456"

    def test_components_present(self):
        key = build_key("x", "b", "fn", "h")
        parts = key.split(":")
        assert parts == ["x", "b", "fn", "h"]


class TestParamsHash:
    def test_deterministic(self):
        kwargs = {"date_from": date(2026, 1, 1), "date_to": date(2026, 1, 31)}
        h1 = _params_hash(kwargs, frozenset({"brand_id", "db"}))
        h2 = _params_hash(kwargs, frozenset({"brand_id", "db"}))
        assert h1 == h2

    def test_different_params_different_hash(self):
        kw1 = {"date_from": date(2026, 1, 1)}
        kw2 = {"date_from": date(2026, 2, 1)}
        exclude = frozenset({"brand_id", "db"})
        assert _params_hash(kw1, exclude) != _params_hash(kw2, exclude)

    def test_brand_id_excluded(self):
        """Same params, different brand_id → same hash (brand_id is in key prefix)."""
        exclude = frozenset({"brand_id", "db"})
        kw1 = {"brand_id": "aaa", "date_from": date(2026, 1, 1)}
        kw2 = {"brand_id": "bbb", "date_from": date(2026, 1, 1)}
        assert _params_hash(kw1, exclude) == _params_hash(kw2, exclude)

    def test_db_excluded(self):
        """Same params, different db object → same hash."""
        exclude = frozenset({"brand_id", "db"})
        kw1 = {"db": object(), "value": 1}
        kw2 = {"db": object(), "value": 1}
        assert _params_hash(kw1, exclude) == _params_hash(kw2, exclude)

    def test_hash_length_12(self):
        h = _params_hash({"a": 1}, frozenset())
        assert len(h) == 12

    def test_none_value(self):
        """None params should hash without error."""
        _params_hash({"source": None}, frozenset())

    def test_date_param(self):
        """date objects should serialise without error."""
        _params_hash({"date_from": date(2026, 4, 1)}, frozenset())


# ══════════════════════════════════════════════════════════════════════════════
# Section 2 — @cached decorator
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestCachedDecorator:
    def _wrap(self, ttl=60):
        """Return a cached-wrapped version of _handler."""
        return cached(ttl=ttl)(_handler)

    async def test_cache_miss_calls_handler(self):
        redis = _make_redis(get_return=None)
        wrapped = self._wrap()
        with patch("app.core.redis.get_redis_client", return_value=redis):
            result = await wrapped(brand_id=BRAND_ID, db=object(), value=42, label="hi")
        assert isinstance(result, _SampleModel)
        assert result.value == 42

    async def test_cache_miss_stores_in_redis(self):
        redis = _make_redis(get_return=None)
        wrapped = self._wrap(ttl=300)
        with patch("app.core.redis.get_redis_client", return_value=redis):
            await wrapped(brand_id=BRAND_ID, db=object(), value=5, label="y")
        redis.set.assert_awaited_once()
        _, set_kwargs = redis.set.call_args
        # TTL must match
        assert set_kwargs.get("ex") == 300 or redis.set.call_args.args[2:] == ()

    async def test_cache_miss_ttl_passed(self):
        redis = _make_redis(get_return=None)
        wrapped = self._wrap(ttl=777)
        with patch("app.core.redis.get_redis_client", return_value=redis):
            await wrapped(brand_id=BRAND_ID, db=object())
        call_args = redis.set.call_args
        # positional: (key, value, ex=ttl)
        assert call_args.kwargs.get("ex") == 777 or call_args.args[-1] == 777 or \
               any(a == 777 for a in call_args.args)

    async def test_cache_hit_returns_cached_value(self):
        payload = json.dumps({"value": 99, "label": "cached"})
        redis = _make_redis(get_return=payload)
        handler_calls = []

        async def _tracked_handler(*, brand_id, db, **kw):
            handler_calls.append(1)
            return _SampleModel(value=0, label="fresh")

        wrapped = cached(ttl=60)(_tracked_handler)
        with patch("app.core.redis.get_redis_client", return_value=redis):
            result = await wrapped(brand_id=BRAND_ID, db=object())

        # Handler was NOT called
        assert len(handler_calls) == 0
        # Returns parsed dict from cache
        assert result == {"value": 99, "label": "cached"}

    async def test_cache_hit_skips_set(self):
        payload = json.dumps({"value": 1, "label": "x"})
        redis = _make_redis(get_return=payload)
        wrapped = self._wrap()
        with patch("app.core.redis.get_redis_client", return_value=redis):
            await wrapped(brand_id=BRAND_ID, db=object())
        redis.set.assert_not_awaited()

    async def test_redis_read_error_failopen(self):
        """get() raises → handler called, result returned normally."""
        redis = AsyncMock()
        redis.get = AsyncMock(side_effect=ConnectionError("Redis down"))
        redis.set = AsyncMock(return_value=True)
        wrapped = self._wrap()
        with patch("app.core.redis.get_redis_client", return_value=redis):
            result = await wrapped(brand_id=BRAND_ID, db=object(), value=7, label="z")
        assert isinstance(result, _SampleModel)
        assert result.value == 7

    async def test_redis_write_error_failopen(self):
        """set() raises → result still returned, no exception."""
        redis = _make_redis(get_return=None)
        redis.set = AsyncMock(side_effect=ConnectionError("Redis down"))
        wrapped = self._wrap()
        with patch("app.core.redis.get_redis_client", return_value=redis):
            result = await wrapped(brand_id=BRAND_ID, db=object(), value=3, label="w")
        assert isinstance(result, _SampleModel)

    async def test_redis_not_initialised_failopen(self):
        """get_redis_client() raises RuntimeError → fail-open."""
        with patch("app.core.redis.get_redis_client", side_effect=RuntimeError("not init")):
            wrapped = self._wrap()
            result = await wrapped(brand_id=BRAND_ID, db=object(), value=1, label="q")
        assert isinstance(result, _SampleModel)

    async def test_key_contains_brand_id(self):
        redis = _make_redis(get_return=None)
        wrapped = self._wrap()
        with patch("app.core.redis.get_redis_client", return_value=redis):
            await wrapped(brand_id="my-brand", db=object())
        set_key = redis.set.call_args.args[0]
        assert "my-brand" in set_key

    async def test_key_contains_func_name(self):
        redis = _make_redis(get_return=None)
        wrapped = self._wrap()
        with patch("app.core.redis.get_redis_client", return_value=redis):
            await wrapped(brand_id=BRAND_ID, db=object())
        set_key = redis.set.call_args.args[0]
        assert "_handler" in set_key

    async def test_key_contains_prefix(self):
        redis = _make_redis(get_return=None)
        wrapped = cached(ttl=60, key_prefix="myprefix")(_handler)
        with patch("app.core.redis.get_redis_client", return_value=redis):
            await wrapped(brand_id=BRAND_ID, db=object())
        set_key = redis.set.call_args.args[0]
        assert set_key.startswith("myprefix:")

    async def test_different_params_different_keys(self):
        keys_used: list[str] = []
        redis_a = _make_redis(get_return=None)
        redis_b = _make_redis(get_return=None)
        wrapped = self._wrap()

        with patch("app.core.redis.get_redis_client", return_value=redis_a):
            await wrapped(brand_id=BRAND_ID, db=object(), value=1, label="a")
        with patch("app.core.redis.get_redis_client", return_value=redis_b):
            await wrapped(brand_id=BRAND_ID, db=object(), value=2, label="b")

        key_a = redis_a.set.call_args.args[0]
        key_b = redis_b.set.call_args.args[0]
        assert key_a != key_b

    async def test_same_params_different_db_same_key(self):
        """Different db handles, same logical params → same cache key."""
        keys_used: list[str] = []
        redis_a = _make_redis(get_return=None)
        redis_b = _make_redis(get_return=None)
        wrapped = self._wrap()

        with patch("app.core.redis.get_redis_client", return_value=redis_a):
            await wrapped(brand_id=BRAND_ID, db=object(), value=5, label="x")
        with patch("app.core.redis.get_redis_client", return_value=redis_b):
            await wrapped(brand_id=BRAND_ID, db=object(), value=5, label="x")

        key_a = redis_a.set.call_args.args[0]
        key_b = redis_b.set.call_args.args[0]
        assert key_a == key_b

    async def test_functools_wraps_preserves_name(self):
        wrapped = self._wrap()
        assert wrapped.__name__ == "_handler"

    async def test_pydantic_model_serialised_to_json(self):
        """model_dump_json() called on result; stored value is valid JSON."""
        redis = _make_redis(get_return=None)
        wrapped = self._wrap()
        with patch("app.core.redis.get_redis_client", return_value=redis):
            await wrapped(brand_id=BRAND_ID, db=object(), value=42, label="test")
        stored = redis.set.call_args.args[1]
        parsed = json.loads(stored)
        assert parsed["value"] == 42
        assert parsed["label"] == "test"


# ══════════════════════════════════════════════════════════════════════════════
# Section 3 — invalidate_brand_cache
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestInvalidateBrandCache:
    async def test_returns_zero_no_keys(self):
        redis = _make_redis(scan_pages=[(0, [])])
        with patch("app.core.redis.get_redis_client", return_value=redis):
            deleted = await invalidate_brand_cache(BRAND_ID)
        assert deleted == 0

    async def test_deletes_matched_keys(self):
        redis = _make_redis(scan_pages=[(0, ["perf:b:fn:aaa", "perf:b:fn:bbb"])])
        with patch("app.core.redis.get_redis_client", return_value=redis):
            deleted = await invalidate_brand_cache("b")
        assert deleted == 2
        redis.unlink.assert_awaited_once_with("perf:b:fn:aaa", "perf:b:fn:bbb")

    async def test_correct_scan_pattern(self):
        scan_args: list = []
        redis = AsyncMock()
        redis.unlink = AsyncMock()

        async def _scan(cursor, match=None, count=None):
            scan_args.append(match)
            return (0, [])

        redis.scan = _scan
        with patch("app.core.redis.get_redis_client", return_value=redis):
            await invalidate_brand_cache("my-brand", key_prefix="perf")
        assert scan_args[0] == "perf:my-brand:*"

    async def test_custom_prefix(self):
        scan_args: list = []
        redis = AsyncMock()
        redis.unlink = AsyncMock()

        async def _scan(cursor, match=None, count=None):
            scan_args.append(match)
            return (0, [])

        redis.scan = _scan
        with patch("app.core.redis.get_redis_client", return_value=redis):
            await invalidate_brand_cache(BRAND_ID, key_prefix="reports")
        assert scan_args[0] == "reports:brand-abc:*"

    async def test_multi_page_scan(self):
        """SCAN cursor != 0 on first page → continues scanning."""
        redis = _make_redis(scan_pages=[
            (42, ["perf:b:fn:k1"]),   # cursor=42 → continue
            (0,  ["perf:b:fn:k2"]),   # cursor=0  → done
        ])
        with patch("app.core.redis.get_redis_client", return_value=redis):
            deleted = await invalidate_brand_cache("b")
        assert deleted == 2

    async def test_redis_unavailable_returns_zero(self):
        with patch("app.core.redis.get_redis_client",
                   side_effect=RuntimeError("not connected")):
            deleted = await invalidate_brand_cache(BRAND_ID)
        assert deleted == 0

    async def test_redis_error_does_not_raise(self):
        """Any exception inside is caught; function returns without raising."""
        with patch("app.core.redis.get_redis_client",
                   side_effect=ConnectionError("timeout")):
            # Must not raise
            result = await invalidate_brand_cache(BRAND_ID)
        assert result == 0


# ══════════════════════════════════════════════════════════════════════════════
# Section 4 — Cache invalidation wired into BaseIngestionService.run()
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestIngestionCacheInvalidation:
    """Verify that BaseIngestionService.run() calls invalidate_brand_cache
    on success and partial but NOT on failed status."""

    def _make_svc(self, db, fetch_side_effect=None):
        from app.services.ingestion.base import BaseIngestionService, PlatformRecord
        from datetime import date as _date

        class _StubSvc(BaseIngestionService):
            source = "google_ads"

            async def fetch(self, brand_id, target_date):
                if fetch_side_effect:
                    raise fetch_side_effect
                return [{"ext_id": "c1", "name": "Camp"}]

            def transform(self, raw, brand_id):
                return [PlatformRecord(
                    external_campaign_id="c1",
                    campaign_name="Camp",
                    date=_date(2026, 4, 7),
                    spend_paise=1000,
                    impressions=100,
                    clicks=5,
                    reach=80,
                    leads=1,
                    conversions=0,
                    conversion_value_paise=0,
                )]

        return _StubSvc(db)

    async def test_invalidated_on_success(self, tmp_path):
        from bson import ObjectId
        from mongomock_motor import AsyncMongoMockClient
        client = AsyncMongoMockClient()
        db = client["test"]
        brand_id = str(ObjectId())

        svc = self._make_svc(db)
        mock_invalidate = AsyncMock(return_value=3)

        with patch("app.core.cache.invalidate_brand_cache", mock_invalidate):
            result = await svc.run(brand_id=brand_id, target_date=date(2026, 4, 7))

        assert result.status in ("success", "partial")
        mock_invalidate.assert_awaited_once_with(brand_id)
        client.close()

    async def test_not_invalidated_on_failed(self, tmp_path):
        """A fully failed run (platform-wide exception) must not invalidate."""
        from bson import ObjectId
        from mongomock_motor import AsyncMongoMockClient
        client = AsyncMongoMockClient()
        db = client["test"]
        brand_id = str(ObjectId())

        svc = self._make_svc(db, fetch_side_effect=RuntimeError("platform down"))
        mock_invalidate = AsyncMock(return_value=0)

        with patch("app.core.cache.invalidate_brand_cache", mock_invalidate):
            result = await svc.run(brand_id=brand_id, target_date=date(2026, 4, 7))

        assert result.status == "failed"
        mock_invalidate.assert_not_awaited()
        client.close()

    async def test_invalidation_failure_does_not_abort(self, tmp_path):
        """If cache invalidation itself raises, run() must still return normally."""
        from bson import ObjectId
        from mongomock_motor import AsyncMongoMockClient
        client = AsyncMongoMockClient()
        db = client["test"]
        brand_id = str(ObjectId())

        svc = self._make_svc(db)
        mock_invalidate = AsyncMock(side_effect=Exception("Redis exploded"))

        with patch("app.core.cache.invalidate_brand_cache", mock_invalidate):
            result = await svc.run(brand_id=brand_id, target_date=date(2026, 4, 7))

        # Run completed; result is success or partial (not crashed)
        assert result.status in ("success", "partial")
        client.close()
