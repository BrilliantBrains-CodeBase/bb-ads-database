import argparse
import logging
import signal
import time


logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Background worker scheduler process")
    parser.add_argument("--dev", action="store_true", help="Run scheduler in development mode")
    args = parser.parse_args()

    _configure_logging()

    interval_seconds = 10 if args.dev else 60
    stop_requested = False

    def _handle_signal(signum: int, _frame: object) -> None:
        nonlocal stop_requested
        logger.info("Received signal %s, shutting down worker", signum)
        stop_requested = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    logger.info("Worker scheduler started (dev=%s, interval=%ss)", args.dev, interval_seconds)

    while not stop_requested:
        # Placeholder heartbeat loop until scheduled jobs are implemented.
        logger.info("Worker heartbeat")
        time.sleep(interval_seconds)

    logger.info("Worker scheduler stopped")


if __name__ == "__main__":
    main()