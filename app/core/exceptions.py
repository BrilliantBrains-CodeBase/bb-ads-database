"""
Application exception hierarchy.

Every exception carries:
  - code       — machine-readable snake_case string (e.g. "not_found")
  - message    — human-readable description
  - details    — optional dict with extra context (field errors, upstream info, …)
  - http_status — the HTTP status code the error handler should use

Usage:
    raise NotFoundError("Campaign not found", details={"campaign_id": cid})
    raise ForbiddenError("Access denied to brand")
    raise RateLimitError(retry_after=60)
"""

from __future__ import annotations

from typing import Any


class AppError(Exception):
    """Base class for all application errors."""

    http_status: int = 500
    default_code: str = "internal_error"
    default_message: str = "An unexpected error occurred."

    def __init__(
        self,
        message: str | None = None,
        *,
        code: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.message = message or self.default_message
        self.code = code or self.default_code
        self.details = details or {}
        super().__init__(self.message)


class NotFoundError(AppError):
    """Resource does not exist (404)."""

    http_status = 404
    default_code = "not_found"
    default_message = "The requested resource was not found."


class ForbiddenError(AppError):
    """Authenticated but not authorised (403)."""

    http_status = 403
    default_code = "forbidden"
    default_message = "You do not have permission to perform this action."


class UnauthorizedError(AppError):
    """Missing or invalid credentials (401)."""

    http_status = 401
    default_code = "unauthorized"
    default_message = "Authentication is required."


class ValidationError(AppError):
    """Business-logic validation failure (422).

    Distinct from Pydantic's RequestValidationError which is handled
    separately — this is for domain-level rules (e.g. date range invalid,
    duplicate slug, budget below minimum).
    """

    http_status = 422
    default_code = "validation_error"
    default_message = "The request data failed validation."


class RateLimitError(AppError):
    """Too many requests (429)."""

    http_status = 429
    default_code = "rate_limit_exceeded"
    default_message = "Too many requests. Please slow down."

    def __init__(
        self,
        message: str | None = None,
        *,
        retry_after: int | None = None,
        code: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        extra: dict[str, Any] = details or {}
        if retry_after is not None:
            extra["retry_after_seconds"] = retry_after
        self.retry_after = retry_after
        super().__init__(message, code=code, details=extra)


class ConflictError(AppError):
    """Resource already exists or state conflict (409)."""

    http_status = 409
    default_code = "conflict"
    default_message = "The resource already exists or is in a conflicting state."


class ExternalServiceError(AppError):
    """An upstream/external API call failed (502).

    Use `details` to carry the service name and upstream status:
        raise ExternalServiceError("Google Ads API unavailable",
                                   details={"service": "google_ads", "upstream_status": 503})
    """

    http_status = 502
    default_code = "external_service_error"
    default_message = "An external service is unavailable."
