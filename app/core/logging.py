"""
Structured JSON logging via structlog.

Call `configure_logging()` once at application startup (main.py lifespan).
After that, every module does:

    import structlog
    logger = structlog.get_logger(__name__)

Log context (correlation_id, user_id, brand_id) is injected automatically
by the CorrelationMiddleware via structlog's contextvars support.
"""

import logging
import sys

import structlog


def configure_logging(json_logs: bool = True, log_level: str = "INFO") -> None:
    """Configure structlog + stdlib logging for the application.

    Args:
        json_logs: Emit JSON lines (True for staging/prod, False for dev pretty-print).
        log_level: Root log level string (DEBUG / INFO / WARNING / ERROR).
    """
    shared_processors: list[structlog.types.Processor] = [
        # Merge contextvars (correlation_id, user_id, brand_id) into every event
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    if json_logs:
        # Production / staging: one JSON object per line
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        # Development: colourised, human-readable output
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=shared_processors
        + [
            # Prepare the event dict for the stdlib handler
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(log_level.upper())
        ),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        # These run only on records that come from stdlib logging (e.g. uvicorn)
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(log_level.upper())

    # Quieten noisy third-party loggers
    for noisy in ("motor", "pymongo", "httpx", "httpcore", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
