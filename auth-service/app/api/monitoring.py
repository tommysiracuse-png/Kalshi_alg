"""Request monitoring for the REST API.

Two complementary signals are produced for every request:

  * A structured JSON access log line on stdout. The CloudWatch agent on the EC2
    host ships these to CloudWatch Logs, so each request is viewable/queryable
    (method, path, status, latency, client IP, request id).
  * Prometheus metrics exposed at ``/metrics`` (request counts by
    method/path/status and a latency histogram), scrapeable by any Prometheus /
    CloudWatch-agent setup.
"""

import json
import time
import uuid
import logging
import sys

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from prometheus_client import Counter, Histogram, CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

# ── Structured access logger (separate from app logging) ──────────────────────
access_logger = logging.getLogger("api.access")
if not access_logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(message)s"))
    access_logger.addHandler(_handler)
    access_logger.setLevel(logging.INFO)
    access_logger.propagate = False

# ── Prometheus metrics ────────────────────────────────────────────────────────
REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "path"],
)


def _route_template(request: Request) -> str:
    """Use the matched route path (e.g. /auth/2fa/{method_type}) to avoid
    high-cardinality metric labels from path params."""
    route = request.scope.get("route")
    if route and getattr(route, "path", None):
        return route.path
    return request.url.path


class MonitoringMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        start = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            elapsed = time.perf_counter() - start
            path = _route_template(request)

            REQUEST_COUNT.labels(request.method, path, str(status_code)).inc()
            REQUEST_LATENCY.labels(request.method, path).observe(elapsed)

            client = request.client.host if request.client else "-"
            access_logger.info(
                json.dumps(
                    {
                        "event": "http_request",
                        "request_id": request_id,
                        "method": request.method,
                        "path": request.url.path,
                        "route": path,
                        "status": status_code,
                        "duration_ms": round(elapsed * 1000, 2),
                        "client_ip": request.headers.get("x-forwarded-for", client),
                        "user_agent": request.headers.get("user-agent", "-"),
                    }
                )
            )


def metrics_endpoint() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
