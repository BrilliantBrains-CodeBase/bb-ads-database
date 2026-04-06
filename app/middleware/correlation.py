"""
CorrelationMiddleware

For every incoming request:
1. Reads `X-Correlation-ID` header if present, otherwise generates a new UUID4.
2. Binds correlation_id (+ placeholder user_id / brand_id) into structlog's
   per-request contextvars so every log line emitted during the request
   automatically carries these fields.
3. Echoes `X-Correlation-ID` back in the response headers.
4. Clears the contextvars at the end of the request to prevent leaking state
   between requests on the same worker coroutine.

Log fields guaranteed on every line:
    timestamp, level, logger, correlation_id, user_id, brand_id, event
"""

import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

CORRELATION_ID_HEADER = "X-Correlation-ID"

logger = structlog.get_logger(__name__)


class CorrelationMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: object) -> Response:
        # ── 1. Resolve correlation ID ────────────────────────────
        correlation_id = request.headers.get(CORRELATION_ID_HEADER) or str(uuid.uuid4())

        # ── 2. Bind context for this request ─────────────────────
        # user_id / brand_id start as None; auth middleware will update them
        # once the JWT is validated (later in the middleware chain).
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            correlation_id=correlation_id,
            user_id=None,
            brand_id=None,
        )

        # Store on request.state so downstream code (auth middleware, deps) can
        # call bind_contextvars to fill in user_id / brand_id without re-reading
        # headers.
        request.state.correlation_id = correlation_id

        # ── 3. Process request ───────────────────────────────────
        response: Response = await call_next(request)  # type: ignore[operator]

        # ── 4. Echo header back ──────────────────────────────────
        response.headers[CORRELATION_ID_HEADER] = correlation_id

        # ── 5. Clear contextvars (defensive — contextvars are task-scoped
        #       in asyncio, but explicit cleanup is good practice) ──
        structlog.contextvars.clear_contextvars()

        return response
