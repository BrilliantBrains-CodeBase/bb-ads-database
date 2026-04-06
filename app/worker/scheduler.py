"""
APScheduler-based background job scheduler.

Architecture
────────────
This module can be used in two modes:

1. Standalone worker process (docker-compose `worker` service):

       python -m app.worker.scheduler

   Connects its own DB + Redis, starts the scheduler, and blocks until
   SIGTERM / SIGINT.

2. Embedded in the FastAPI lifespan (single-process deployments or dev):

       from app.worker.scheduler import start_scheduler, stop_scheduler
       await start_scheduler()
       ...
       await stop_scheduler()

   The lifespan in main.py calls these when app_instance == "worker" or
   SCHEDULER_ENABLED=true.

Job deduplication (Redis lock)
──────────────────────────────
Each job acquires a Redis lock before running:

    SET scheduler:lock:{job_id}  {worker_id}  NX  EX {lock_ttl}

• NX — only set if the key does not exist (atomic acquire)
• EX — auto-expire so a crashed worker doesn't hold the lock forever
• lock_ttl is set to the job's maximum expected runtime (generous ceiling)

Release uses a Lua script to ensure only the lock owner can delete it,
preventing a fast job from releasing a lock acquired by a slower parallel run.

If the lock cannot be acquired the job logs a skip at DEBUG level and returns
immediately — this is expected when running multiple replicas.

Misfire grace time
──────────────────
All jobs use misfire_grace_time=3600s (1 hour).  If the scheduler was down
(e.g. restart) and a job's scheduled time passed within the last hour, it will
still fire once when the scheduler comes back up.  Misfires older than 1 hour
are silently discarded.

Schedule (all times UTC)
────────────────────────
  00:30  daily_ingestion         D-1 + D-0 for all brands (6:00 IST)
  01:30  rollup_computation      after ingestion (7:00 IST)
  02:00  token_refresh_google    daily OAuth check
  02:30  anomaly_detection       after rollups (8:00 IST)
  02:30  meta_token_expiry_check combined with anomaly run
  03:30  scheduled_reports       after anomaly detection (9:00 IST)
  */30   ingestion_health_check  every 30 minutes
"""

from __future__ import annotations

import asyncio
import signal
import uuid
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.core.redis import get_redis_client

logger = structlog.get_logger(__name__)

# ── Module-level scheduler instance ───────────────────────────────────────────

_scheduler: AsyncIOScheduler | None = None

# ── Lock configuration ─────────────────────────────────────────────────────────

# Key pattern: scheduler:lock:{job_id}
_LOCK_PREFIX = "scheduler:lock"

# Maximum expected runtimes (lock TTLs in seconds) — generous ceilings
_LOCK_TTLS: dict[str, int] = {
    "daily_ingestion": 4 * 3600,        # 4 h — many brands, slow platforms
    "rollup_computation": 3600,          # 1 h
    "anomaly_detection": 3600,           # 1 h
    "scheduled_reports": 2 * 3600,       # 2 h
    "ingestion_health_check": 300,       # 5 min
    "token_refresh_google": 3600,        # 1 h
    "meta_token_expiry_check": 600,      # 10 min
}

_MISFIRE_GRACE = 3600  # seconds — 1 hour

# Lua script: delete key only if it matches the expected value (atomic release)
_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


# ── Redis lock helpers ─────────────────────────────────────────────────────────

async def _acquire_lock(
    job_id: str,
    worker_id: str,
    ttl: int,
) -> bool:
    """Try to acquire the Redis lock for job_id.  Returns True on success."""
    redis = get_redis_client()
    key = f"{_LOCK_PREFIX}:{job_id}"
    result = await redis.set(key, worker_id, nx=True, ex=ttl)
    return bool(result)


async def _release_lock(job_id: str, worker_id: str) -> None:
    """Release the Redis lock only if this worker owns it."""
    redis = get_redis_client()
    key = f"{_LOCK_PREFIX}:{job_id}"
    try:
        await redis.eval(_RELEASE_LUA, 1, key, worker_id)
    except Exception as exc:
        logger.warning("scheduler.lock_release_failed", job_id=job_id, error=str(exc))


# ── Lock-guarded job wrapper ───────────────────────────────────────────────────

def _locked(
    job_id: str,
    fn: Callable[[], Awaitable[None]],
) -> Callable[[], Awaitable[None]]:
    """Wrap an async task function with Redis-lock deduplication.

    Generates a unique worker_id per process start so locks identify the
    specific worker instance that acquired them.
    """
    _worker_id = f"worker:{uuid.uuid4()}"

    async def _wrapper() -> None:
        ttl = _LOCK_TTLS.get(job_id, 3600)
        acquired = await _acquire_lock(job_id, _worker_id, ttl)
        if not acquired:
            logger.debug(
                "scheduler.job_skipped",
                job_id=job_id,
                reason="lock held by another worker",
            )
            return

        log = logger.bind(job_id=job_id, worker_id=_worker_id)
        started_at = datetime.now(UTC)
        log.info("scheduler.job_started")

        try:
            await fn()
            duration = (datetime.now(UTC) - started_at).total_seconds()
            log.info("scheduler.job_completed", duration_s=round(duration, 2))
        except Exception as exc:
            duration = (datetime.now(UTC) - started_at).total_seconds()
            log.error(
                "scheduler.job_failed",
                error=str(exc),
                duration_s=round(duration, 2),
                exc_info=True,
            )
        finally:
            await _release_lock(job_id, _worker_id)

    _wrapper.__name__ = f"locked_{job_id}"
    return _wrapper


# ── Scheduler lifecycle ────────────────────────────────────────────────────────

def build_scheduler() -> AsyncIOScheduler:
    """Construct and configure the AsyncIOScheduler with all registered jobs.

    Does NOT start it — call start_scheduler() to start.
    """
    from app.worker import tasks

    scheduler = AsyncIOScheduler(timezone="UTC")

    _jobs: list[tuple[str, Any, Callable[[], Awaitable[None]]]] = [
        # (job_id, trigger, task_fn)
        (
            "daily_ingestion",
            CronTrigger(hour=0, minute=30, timezone="UTC"),
            tasks.daily_ingestion,
        ),
        (
            "rollup_computation",
            CronTrigger(hour=1, minute=30, timezone="UTC"),
            tasks.rollup_computation,
        ),
        (
            "token_refresh_google",
            CronTrigger(hour=2, minute=0, timezone="UTC"),
            tasks.token_refresh_google,
        ),
        (
            "anomaly_detection",
            CronTrigger(hour=2, minute=30, timezone="UTC"),
            tasks.anomaly_detection,
        ),
        (
            "meta_token_expiry_check",
            CronTrigger(hour=2, minute=30, timezone="UTC"),
            tasks.meta_token_expiry_check,
        ),
        (
            "scheduled_reports",
            CronTrigger(hour=3, minute=30, timezone="UTC"),
            tasks.scheduled_reports,
        ),
        (
            "ingestion_health_check",
            IntervalTrigger(minutes=30),
            tasks.ingestion_health_check,
        ),
    ]

    for job_id, trigger, fn in _jobs:
        scheduler.add_job(
            _locked(job_id, fn),
            trigger=trigger,
            id=job_id,
            name=job_id,
            misfire_grace_time=_MISFIRE_GRACE,
            max_instances=1,           # APScheduler-level guard on top of Redis lock
            replace_existing=True,
        )
        logger.debug("scheduler.job_registered", job_id=job_id)

    return scheduler


async def start_scheduler() -> None:
    """Build and start the AsyncIOScheduler.  Safe to call multiple times."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        logger.warning("scheduler.already_running")
        return

    _scheduler = build_scheduler()
    _scheduler.start()
    logger.info(
        "scheduler.started",
        job_count=len(_scheduler.get_jobs()),
        jobs=[j.id for j in _scheduler.get_jobs()],
    )


async def stop_scheduler() -> None:
    """Shut down the scheduler gracefully, waiting for running jobs to finish."""
    global _scheduler
    if _scheduler is None or not _scheduler.running:
        return
    _scheduler.shutdown(wait=True)
    _scheduler = None
    logger.info("scheduler.stopped")


def get_scheduler() -> AsyncIOScheduler | None:
    """Return the current scheduler instance (None if not started)."""
    return _scheduler


# ── Standalone worker entry point ──────────────────────────────────────────────

async def _run_worker() -> None:
    """Async entry point for the standalone worker process.

    Connects DB and Redis, starts the scheduler, then blocks until
    SIGTERM or SIGINT is received.
    """
    from app.core.config import get_settings
    from app.core.database import connect_db, disconnect_db
    from app.core.logging import configure_logging
    from app.core.redis import connect_redis, disconnect_redis

    settings = get_settings()
    configure_logging(
        json_logs=settings.is_production or settings.app_env == "staging",
        log_level="DEBUG" if settings.app_debug else "INFO",
    )

    log = logger.bind(instance=settings.app_instance, env=settings.app_env)
    log.info("worker.starting")

    await connect_db(settings)
    await connect_redis(settings)

    await start_scheduler()
    log.info("worker.ready")

    # Block until a shutdown signal is received
    stop_event = asyncio.Event()

    def _handle_signal(signum: int) -> None:
        log.info("worker.signal_received", signum=signum)
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal, sig)

    await stop_event.wait()

    log.info("worker.shutting_down")
    await stop_scheduler()
    await disconnect_redis()
    await disconnect_db()
    log.info("worker.stopped")


def main() -> None:
    """Synchronous entry point: python -m app.worker.scheduler"""
    asyncio.run(_run_worker())


if __name__ == "__main__":
    main()
