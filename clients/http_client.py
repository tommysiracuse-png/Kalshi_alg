"""Small venue-independent synchronous JSON-over-HTTP transport."""

from __future__ import annotations

import time
from typing import Any, Mapping, Optional

import requests

from .monitoring import ActivityMonitor


class HTTPClientError(RuntimeError):
    def __init__(self, *, method: str, path: str, status_code: int, response_text: str) -> None:
        self.method = method.upper()
        self.path = path
        self.status_code = int(status_code)
        self.response_text = response_text
        super().__init__(f"{self.method} {path} failed {status_code}: {response_text[:500]}")


class HTTPClient:
    def __init__(self, base_url: str, *, timeout_seconds: float = 15, session: Optional[Any] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = float(timeout_seconds)
        self.session = session or requests.Session()
        self.activity = ActivityMonitor()

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    @staticmethod
    def _decode(response: Any, *, method: str, path: str) -> dict:
        text = str(getattr(response, "text", "") or "")
        status_code = int(getattr(response, "status_code", 0))
        if status_code >= 400:
            raise HTTPClientError(
                method=method,
                path=path,
                status_code=status_code,
                response_text=text,
            )
        return {} if not text.strip() else response.json()

    def get(
        self,
        path: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        params: Optional[Mapping[str, Any]] = None,
        operation: str = "get",
    ) -> dict:
        return self._request("GET", path, headers=headers, params=params, operation=operation)

    def post(
        self,
        path: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        body: Optional[Mapping[str, Any]] = None,
        params: Optional[Mapping[str, Any]] = None,
        operation: str = "post",
    ) -> dict:
        return self._request("POST", path, headers=headers, params=params, body=body, operation=operation)

    def delete(
        self,
        path: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        params: Optional[Mapping[str, Any]] = None,
        operation: str = "delete",
    ) -> dict:
        return self._request("DELETE", path, headers=headers, params=params, operation=operation)

    def _request(
        self,
        method: str,
        path: str,
        *,
        headers: Optional[Mapping[str, str]],
        params: Optional[Mapping[str, Any]],
        operation: str,
        body: Optional[Mapping[str, Any]] = None,
    ) -> dict:
        started = time.perf_counter()
        response = None
        error = False
        error_message = None
        try:
            request = getattr(self.session, method.lower())
            kwargs = {"headers": dict(headers or {}), "params": params, "timeout": self.timeout_seconds}
            if method == "POST":
                kwargs["json"] = dict(body or {})
            response = request(self._url(path), **kwargs)
            return self._decode(response, method=method, path=path)
        except Exception as exc:
            error = True
            error_message = str(exc)
            raise
        finally:
            status = int(getattr(response, "status_code", 0)) if response is not None else None
            self.activity.record_rest(
                method=method,
                operation=operation,
                status_code=status,
                latency_ms=(time.perf_counter() - started) * 1000,
                error=error or bool(status and status >= 400),
                error_message=error_message,
            )

    def activity_snapshot(self) -> dict:
        return self.activity.snapshot()
