"""
FastAPI exception handlers.

All errors return the same envelope:

    {
        "error": {
            "code":    "not_found",
            "message": "Campaign not found",
            "details": { "campaign_id": "abc123" }   // optional, may be {}
        }
    }

Register with `register_error_handlers(app)` inside `create_app()`.
"""

from __future__ import annotations

from typing import Any

import structlog
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.exceptions import AppError, RateLimitError

logger = structlog.get_logger(__name__)


# ── Response builder ──────────────────────────────────────────────────────────


def _error_response(
    status_code: int,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message, "details": details or {}}},
        headers=headers,
    )


# ── Handlers ──────────────────────────────────────────────────────────────────


async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    """Handles every exception in our AppError hierarchy."""
    log = logger.bind(
        error_code=exc.code,
        http_status=exc.http_status,
        path=request.url.path,
    )
    if exc.http_status >= 500:
        log.exception("app_error.server", error=str(exc))
    else:
        log.warning("app_error.client", error=str(exc))

    headers: dict[str, str] | None = None
    if isinstance(exc, RateLimitError) and exc.retry_after is not None:
        headers = {"Retry-After": str(exc.retry_after)}

    return _error_response(
        status_code=exc.http_status,
        code=exc.code,
        message=exc.message,
        details=exc.details,
        headers=headers,
    )


async def http_exception_handler(
    request: Request, exc: StarletteHTTPException
) -> JSONResponse:
    """Converts Starlette/FastAPI HTTPException to our error envelope."""
    # Map common status codes to stable machine-readable codes
    _status_to_code: dict[int, str] = {
        400: "bad_request",
        401: "unauthorized",
        403: "forbidden",
        404: "not_found",
        405: "method_not_allowed",
        409: "conflict",
        410: "gone",
        422: "validation_error",
        429: "rate_limit_exceeded",
        500: "internal_error",
        502: "external_service_error",
        503: "service_unavailable",
    }
    code = _status_to_code.get(exc.status_code, "http_error")
    message = exc.detail if isinstance(exc.detail, str) else str(exc.detail)

    logger.warning(
        "http_exception",
        status_code=exc.status_code,
        path=request.url.path,
        detail=exc.detail,
    )

    headers = dict(exc.headers) if exc.headers else None
    return _error_response(
        status_code=exc.status_code,
        code=code,
        message=message,
        headers=headers,
    )


async def request_validation_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Converts Pydantic RequestValidationError to our envelope.

    `details.fields` is a list of { loc, msg, type } objects so the client
    knows exactly which fields failed and why.
    """
    fields = [
        {
            "loc": list(err["loc"]),
            "msg": err["msg"],
            "type": err["type"],
        }
        for err in exc.errors()
    ]
    logger.warning(
        "request_validation_error",
        path=request.url.path,
        error_count=len(fields),
    )
    return _error_response(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        code="validation_error",
        message="Request validation failed.",
        details={"fields": fields},
    )


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all for any exception that slipped through."""
    logger.exception(
        "unhandled_exception",
        path=request.url.path,
        exc_type=type(exc).__name__,
        error=str(exc),
    )
    return _error_response(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        code="internal_error",
        message="An unexpected error occurred.",
    )


# ── Registration ──────────────────────────────────────────────────────────────


def register_error_handlers(app: FastAPI) -> None:
    """Attach all exception handlers to the FastAPI app."""
    app.add_exception_handler(AppError, app_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(RequestValidationError, request_validation_handler)  # type: ignore[arg-type]
    app.add_exception_handler(Exception, unhandled_exception_handler)
