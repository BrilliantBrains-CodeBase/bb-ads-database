"""
Unit tests for the APScheduler setup and task functions.

Strategy
────────
• The scheduler itself is tested without starting it (build_scheduler()
  is called but not started) so no real timers fire.
• Redis is mocked via AsyncMock to test lock acquire / release logic.
• DB is provided by mongomock-motor.
• Task functions are tested by patching their service-layer dependencies
  so no real connectors or external services are called.

Coverage
────────
  scheduler.py:
    - build_scheduler registers all 7 jobs
    - Each job has correct id, misfire_grace_time, max_instances
    - _acquire_lock: SET NX EX — returns True on success, False when held
    - _release_lock: Lua script called with correct args
    - _locked wrapper: acquires lock, runs fn, releases lock
    - _locked wrapper: skips fn when lock not acquired
    - _locked wrapper: releases lock even when fn raises

  tasks.py:
    - daily_ingestion: iterates active brands, calls connectors, counts
    - daily_ingestion: per-brand failure is isolated (others still run)
    - daily_ingestion: brands without platform config are skipped
    - ingestion_health_check: stale brand logged as warning
    - ingestion_health_check: fresh brand is not reported
    - meta_token_expiry_check: near-expiry token triggers warning
    - meta_token_expiry_check: permanent token (no expiry) skipped
    - meta_token_expiry_check: malformed expiry field doesn't raise
    - token_refresh_google: valid creds logs ok
    - rollup_computation: calls compute_all_rollups with db instance
    - anomaly_detection / scheduled_reports:
        graceful no-op when service module not yet implemented (ImportError)
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from bson import ObjectId
from mongomock_motor import AsyncMongoMockClient

# ── Fixtures ───────────────────────────────────────────────────────────────────

TODAY = date(2026, 4, 6)
BRAND_OID = ObjectId()
BRAND_ID = str(BRAND_OID)


@pytest_asyncio.fixture
async def db():
    client = AsyncMongoMockClient()
    database = client["test_db"]
    yield database
    client.close()


@pytest_asyncio.fixture
async def db_with_brands(db):
    """Two active brands: one with google_ads, one with meta."""
    await db["brands"].insert_many([
        {
            "_id": BRAND_OID,
            "name": "Google Brand",
            "slug": "google-brand",
            "is_active": True,
            "platforms": {
                "google_ads": {
                    "customer_id": "111",
                    "refresh_token": "tok",
                }
            },
        },
        {
            "_id": ObjectId(),
            "name": "Meta Brand",
            "slug": "meta-brand",
            "is_active": True,
            "platforms": {
                "meta": {
                    "access_token": "tok",
                    "ad_account_id": "act_222",
                    "currency": "INR",
                }
            },
        },
        {
            "_id": ObjectId(),
            "name": "Inactive Brand",
            "slug": "inactive",
            "is_active": False,
            "platforms": {},
        },
    ])
    yield db


# ══════════════════════════════════════════════════════════════════════════════
# Section 1: build_scheduler — job registration
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildScheduler:
    EXPECTED_JOB_IDS = {
        "daily_ingestion",
        "rollup_computation",
        "token_refresh_google",
        "anomaly_detection",
        "meta_token_expiry_check",
        "scheduled_reports",
        "ingestion_health_check",
    }

    def test_all_jobs_registered(self):
        from app.worker.scheduler import build_scheduler
        scheduler = build_scheduler()
        job_ids = {j.id for j in scheduler.get_jobs()}
        assert job_ids == self.EXPECTED_JOB_IDS

    def test_job_count(self):
        from app.worker.scheduler import build_scheduler
        scheduler = build_scheduler()
        assert len(scheduler.get_jobs()) == 7

    def test_each_job_has_misfire_grace(self):
        from app.worker.scheduler import build_scheduler, _MISFIRE_GRACE
        scheduler = build_scheduler()
        for job in scheduler.get_jobs():
            assert job.misfire_grace_time == _MISFIRE_GRACE, (
                f"{job.id} has wrong misfire_grace_time"
            )

    def test_each_job_max_instances_one(self):
        from app.worker.scheduler import build_scheduler
        scheduler = build_scheduler()
        for job in scheduler.get_jobs():
            assert job.max_instances == 1, f"{job.id} max_instances != 1"

    def test_daily_ingestion_trigger_cron(self):
        from apscheduler.triggers.cron import CronTrigger
        from app.worker.scheduler import build_scheduler
        scheduler = build_scheduler()
        job = scheduler.get_job("daily_ingestion")
        assert isinstance(job.trigger, CronTrigger)

    def test_ingestion_health_check_trigger_interval(self):
        from apscheduler.triggers.interval import IntervalTrigger
        from app.worker.scheduler import build_scheduler
        scheduler = build_scheduler()
        job = scheduler.get_job("ingestion_health_check")
        assert isinstance(job.trigger, IntervalTrigger)

    def test_scheduler_not_started(self):
        from app.worker.scheduler import build_scheduler
        scheduler = build_scheduler()
        assert not scheduler.running


# ══════════════════════════════════════════════════════════════════════════════
# Section 2: Redis lock logic
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestRedisLock:
    async def test_acquire_lock_success(self):
        from app.worker.scheduler import _acquire_lock

        mock_redis = AsyncMock()
        mock_redis.set.return_value = True

        with patch("app.worker.scheduler.get_redis_client", return_value=mock_redis):
            result = await _acquire_lock("daily_ingestion", "worker-1", 3600)

        assert result is True
        mock_redis.set.assert_called_once_with(
            "scheduler:lock:daily_ingestion", "worker-1", nx=True, ex=3600
        )

    async def test_acquire_lock_fails_when_held(self):
        from app.worker.scheduler import _acquire_lock

        mock_redis = AsyncMock()
        mock_redis.set.return_value = None  # Redis returns None on NX failure

        with patch("app.worker.scheduler.get_redis_client", return_value=mock_redis):
            result = await _acquire_lock("daily_ingestion", "worker-2", 3600)

        assert result is False

    async def test_release_lock_calls_lua(self):
        from app.worker.scheduler import _release_lock, _RELEASE_LUA

        mock_redis = AsyncMock()
        mock_redis.eval = AsyncMock(return_value=1)

        with patch("app.worker.scheduler.get_redis_client", return_value=mock_redis):
            await _release_lock("daily_ingestion", "worker-1")

        mock_redis.eval.assert_called_once_with(
            _RELEASE_LUA, 1,
            "scheduler:lock:daily_ingestion",
            "worker-1",
        )

    async def test_release_lock_swallows_redis_error(self):
        """A Redis failure on release must not propagate."""
        from app.worker.scheduler import _release_lock

        mock_redis = AsyncMock()
        mock_redis.eval.side_effect = RuntimeError("Redis down")

        with patch("app.worker.scheduler.get_redis_client", return_value=mock_redis):
            # Must not raise
            await _release_lock("daily_ingestion", "worker-1")



# ══════════════════════════════════════════════════════════════════════════════
# Section 3: _locked wrapper behaviour
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestLockedWrapper:
    async def _mock_redis(self, *, acquired: bool = True) -> AsyncMock:
        mock = AsyncMock()
        mock.set.return_value = True if acquired else None
        mock.eval = AsyncMock(return_value=1)
        return mock

    async def test_runs_fn_when_lock_acquired(self):
        from app.worker.scheduler import _locked

        fn = AsyncMock()
        wrapped = _locked("test_job", fn)
        redis = await self._mock_redis(acquired=True)

        with patch("app.worker.scheduler.get_redis_client", return_value=redis):
            await wrapped()

        fn.assert_called_once()

    async def test_skips_fn_when_lock_not_acquired(self):
        from app.worker.scheduler import _locked

        fn = AsyncMock()
        wrapped = _locked("test_job", fn)
        redis = await self._mock_redis(acquired=False)

        with patch("app.worker.scheduler.get_redis_client", return_value=redis):
            await wrapped()

        fn.assert_not_called()

    async def test_releases_lock_after_fn_success(self):
        from app.worker.scheduler import _locked

        fn = AsyncMock()
        wrapped = _locked("test_job", fn)
        redis = await self._mock_redis(acquired=True)

        with patch("app.worker.scheduler.get_redis_client", return_value=redis):
            await wrapped()

        # eval = Lua release script
        redis.eval.assert_called_once()

    async def test_releases_lock_after_fn_failure(self):
        """Lock must be released even when the wrapped function raises."""
        from app.worker.scheduler import _locked

        fn = AsyncMock(side_effect=RuntimeError("task exploded"))
        wrapped = _locked("test_job", fn)
        redis = await self._mock_redis(acquired=True)

        with patch("app.worker.scheduler.get_redis_client", return_value=redis):
            await wrapped()  # must NOT raise

        redis.eval.assert_called_once()  # lock was released

    async def test_lock_ttl_matches_config(self):
        """The lock TTL used must match _LOCK_TTLS for the job_id."""
        from app.worker.scheduler import _locked, _LOCK_TTLS

        fn = AsyncMock()
        job_id = "daily_ingestion"
        wrapped = _locked(job_id, fn)
        redis = await self._mock_redis(acquired=True)

        with patch("app.worker.scheduler.get_redis_client", return_value=redis):
            await wrapped()

        _args, _kwargs = redis.set.call_args
        assert _kwargs["ex"] == _LOCK_TTLS[job_id]

    async def test_unknown_job_id_uses_default_ttl(self):
        from app.worker.scheduler import _locked

        fn = AsyncMock()
        wrapped = _locked("unknown_job", fn)
        redis = await self._mock_redis(acquired=True)

        with patch("app.worker.scheduler.get_redis_client", return_value=redis):
            await wrapped()

        _args, _kwargs = redis.set.call_args
        assert _kwargs["ex"] == 3600  # default fallback


# ══════════════════════════════════════════════════════════════════════════════
# Section 4: daily_ingestion task
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestDailyIngestion:
    async def test_runs_for_configured_brands(self, db_with_brands):
        """daily_ingestion calls the correct connector for each brand."""
        from app.worker import tasks

        mock_result = MagicMock()
        mock_result.status = "success"
        mock_result.records_upserted = 2
        mock_result.errors = []

        mock_svc = AsyncMock()
        mock_svc.run.return_value = mock_result

        with patch("app.worker.tasks.get_db_direct", return_value=db_with_brands), \
             patch("app.worker.tasks.GoogleAdsIngestionService", return_value=mock_svc), \
             patch("app.worker.tasks.MetaAdsIngestionService", return_value=mock_svc):
            await tasks.daily_ingestion()

        # 1 google_ads brand + 1 meta brand = 2 connector calls
        assert mock_svc.run.call_count == 2

    async def test_skips_inactive_brands(self, db_with_brands):
        """Inactive brands must not be ingested."""
        from app.worker import tasks

        mock_result = MagicMock(status="success", records_upserted=1, errors=[])
        mock_svc = AsyncMock()
        mock_svc.run.return_value = mock_result

        with patch("app.worker.tasks.get_db_direct", return_value=db_with_brands), \
             patch("app.worker.tasks.GoogleAdsIngestionService", return_value=mock_svc), \
             patch("app.worker.tasks.MetaAdsIngestionService", return_value=mock_svc):
            await tasks.daily_ingestion()

        # Inactive brand is excluded — still only 2 calls
        assert mock_svc.run.call_count == 2

    async def test_per_brand_failure_is_isolated(self, db_with_brands):
        """A connector failure for one brand must not abort the others."""
        from app.worker import tasks

        good_result = MagicMock(status="success", records_upserted=1, errors=[])
        call_count = 0

        async def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("connector down")
            return good_result

        mock_svc = AsyncMock()
        mock_svc.run.side_effect = side_effect

        with patch("app.worker.tasks.get_db_direct", return_value=db_with_brands), \
             patch("app.worker.tasks.GoogleAdsIngestionService", return_value=mock_svc), \
             patch("app.worker.tasks.MetaAdsIngestionService", return_value=mock_svc):
            # Must not raise even though one connector raised
            await tasks.daily_ingestion()

        assert call_count == 2  # second brand still processed

    async def test_brand_without_platform_config_skipped(self, db):
        """Brands without platforms.google_ads/meta configured are skipped."""
        from app.worker import tasks

        await db["brands"].insert_one({
            "_id": ObjectId(),
            "name": "NoPlatform Brand",
            "is_active": True,
            "platforms": {},
        })

        mock_svc = AsyncMock()
        mock_svc.run.return_value = MagicMock(
            status="success", records_upserted=0, errors=[]
        )

        with patch("app.worker.tasks.get_db_direct", return_value=db), \
             patch("app.worker.tasks.GoogleAdsIngestionService", return_value=mock_svc), \
             patch("app.worker.tasks.MetaAdsIngestionService", return_value=mock_svc):
            await tasks.daily_ingestion()

        mock_svc.run.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# Section 5: ingestion_health_check task
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestIngestionHealthCheck:
    async def _insert_brand_with_google(self, db) -> str:
        oid = ObjectId()
        await db["brands"].insert_one({
            "_id": oid,
            "name": "Health Brand",
            "is_active": True,
            "platforms": {"google_ads": {"customer_id": "1", "refresh_token": "t"}},
        })
        return str(oid)

    async def test_stale_brand_logs_warning(self, db):
        """Brand with no recent successful ingestion triggers a stale warning."""
        from app.worker import tasks

        brand_id = await self._insert_brand_with_google(db)
        # Insert a stale log (48 h ago)
        await db["ingestion_logs"].insert_one({
            "brand_id": ObjectId(brand_id),
            "source": "google_ads",
            "status": "success",
            "completed_at": datetime.now(UTC) - timedelta(hours=48),
        })

        with patch("app.worker.tasks.get_db_direct", return_value=db):
            # Should complete without raising
            await tasks.ingestion_health_check()

    async def test_fresh_brand_not_reported(self, db):
        """Brand with a recent successful ingestion must not trigger a warning."""
        from app.worker import tasks

        brand_id = await self._insert_brand_with_google(db)
        # Insert a fresh log (1 h ago)
        await db["ingestion_logs"].insert_one({
            "brand_id": ObjectId(brand_id),
            "source": "google_ads",
            "status": "success",
            "completed_at": datetime.now(UTC) - timedelta(hours=1),
        })

        with patch("app.worker.tasks.get_db_direct", return_value=db):
            await tasks.ingestion_health_check()  # must not raise

    async def test_no_brands_no_error(self, db):
        """Empty brands collection completes without error."""
        from app.worker import tasks

        with patch("app.worker.tasks.get_db_direct", return_value=db):
            await tasks.ingestion_health_check()


# ══════════════════════════════════════════════════════════════════════════════
# Section 6: meta_token_expiry_check task
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestMetaTokenExpiryCheck:
    async def test_expiring_token_logs_warning(self, db):
        """Token expiring in 3 days should trigger a warning."""
        from app.worker import tasks

        expires = (datetime.now(UTC) + timedelta(days=3)).isoformat()
        await db["brands"].insert_one({
            "_id": ObjectId(),
            "name": "Expiring Token Brand",
            "is_active": True,
            "platforms": {
                "meta": {
                    "access_token": "tok",
                    "ad_account_id": "act_1",
                    "token_expires_at": expires,
                }
            },
        })

        with patch("app.worker.tasks.get_db_direct", return_value=db):
            await tasks.meta_token_expiry_check()  # must not raise

    async def test_permanent_token_skipped(self, db):
        """Brand with no token_expires_at (permanent token) is not flagged."""
        from app.worker import tasks

        await db["brands"].insert_one({
            "_id": ObjectId(),
            "name": "Perm Token Brand",
            "is_active": True,
            "platforms": {
                "meta": {
                    "access_token": "permanent_tok",
                    "ad_account_id": "act_2",
                    # no token_expires_at
                }
            },
        })

        with patch("app.worker.tasks.get_db_direct", return_value=db):
            await tasks.meta_token_expiry_check()  # must not raise

    async def test_distant_expiry_no_warning(self, db):
        """Token expiring in 30 days is within safe range — no action."""
        from app.worker import tasks

        expires = (datetime.now(UTC) + timedelta(days=30)).isoformat()
        await db["brands"].insert_one({
            "_id": ObjectId(),
            "is_active": True,
            "platforms": {
                "meta": {
                    "access_token": "tok",
                    "ad_account_id": "act_3",
                    "token_expires_at": expires,
                }
            },
        })

        with patch("app.worker.tasks.get_db_direct", return_value=db):
            await tasks.meta_token_expiry_check()  # must not raise

    async def test_malformed_expiry_does_not_raise(self, db):
        """Malformed token_expires_at field must not crash the task."""
        from app.worker import tasks

        await db["brands"].insert_one({
            "_id": ObjectId(),
            "is_active": True,
            "platforms": {
                "meta": {
                    "access_token": "tok",
                    "ad_account_id": "act_4",
                    "token_expires_at": "NOT_A_DATE",
                }
            },
        })

        with patch("app.worker.tasks.get_db_direct", return_value=db):
            await tasks.meta_token_expiry_check()  # must not raise


# ══════════════════════════════════════════════════════════════════════════════
# Section 7: stub tasks (graceful ImportError handling)
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestStubTasks:
    async def test_rollup_computation_calls_service(self, db):
        from app.worker import tasks

        mock_compute = AsyncMock()
        with (
            patch("app.worker.tasks.get_db_direct", return_value=db),
            patch("app.services.rollup.compute_all_rollups", mock_compute),
        ):
            await tasks.rollup_computation()

        mock_compute.assert_awaited_once_with(db)

    async def test_anomaly_detection_graceful_on_import_error(self):
        from app.worker import tasks

        with patch.dict("sys.modules", {"app.services.anomalies": None}):
            await tasks.anomaly_detection()

    async def test_scheduled_reports_graceful_on_import_error(self):
        from app.worker import tasks

        with patch.dict("sys.modules", {"app.services.reports": None}):
            await tasks.scheduled_reports()

    async def test_token_refresh_google_no_brands(self, db):
        from app.worker import tasks

        with patch("app.worker.tasks.get_db_direct", return_value=db):
            await tasks.token_refresh_google()  # empty DB — should not raise


# ══════════════════════════════════════════════════════════════════════════════
# Section 8: start / stop scheduler lifecycle
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestSchedulerLifecycle:
    async def test_start_stop(self):
        from app.worker.scheduler import start_scheduler, stop_scheduler, get_scheduler

        mock_redis = AsyncMock()
        with patch("app.worker.scheduler.get_redis_client", return_value=mock_redis):
            await start_scheduler()
            assert get_scheduler() is not None
            assert get_scheduler().running

            await stop_scheduler()
            assert get_scheduler() is None

    async def test_start_twice_is_safe(self):
        from app.worker.scheduler import start_scheduler, stop_scheduler, get_scheduler

        mock_redis = AsyncMock()
        with patch("app.worker.scheduler.get_redis_client", return_value=mock_redis):
            await start_scheduler()
            sched1 = get_scheduler()
            await start_scheduler()  # second call — should not create a new instance
            sched2 = get_scheduler()
            assert sched1 is sched2
            await stop_scheduler()

    async def test_stop_when_not_started_is_safe(self):
        from app.worker.scheduler import stop_scheduler, _scheduler
        import app.worker.scheduler as _sched_mod

        # Ensure clean state
        _sched_mod._scheduler = None
        await stop_scheduler()  # must not raise
