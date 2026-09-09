"""Small venue-independent synchronous JSON-over-HTTP transport."""

from __future__ import annotations

import time
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter

from .monitoring import ActivityMonitor


def _redact_proxy_text(value: object, proxy_url: Optional[str]) -> str:
    text = str(value)
    if not proxy_url:
        return text
    try:
        parsed = urlsplit(str(proxy_url))
        host = parsed.hostname or ""
        if parsed.port is not None:
            host = f"{host}:{parsed.port}"
        safe = urlunsplit((parsed.scheme, host, parsed.path, parsed.query, parsed.fragment))
        text = text.replace(str(proxy_url), safe)
    except Exception:
        pass
    return text


class HTTPClientError(RuntimeError):
    def __init__(self, *, method: str, path: str, status_code: int, response_text: str, headers: Optional[Mapping[str, Any]] = None) -> None:
        self.method = method.upper()
        self.path = path
        self.status_code = int(status_code)
        self.response_text = response_text
        self.headers = dict(headers or {})
        super().__init__(f"{self.method} {path} failed {status_code}: {response_text[:500]}")


class HTTPClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 15,
        session: Optional[Any] = None,
        connect_timeout_seconds: Optional[float] = None,
        proxy_url: Optional[str] = None,
        trust_env: Optional[bool] = None,
        activity_error_body_limit: Optional[int] = 500,
        pool_connections: Optional[int] = None,
        pool_maxsize: Optional[int] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        # ``timeout_seconds`` bounds every socket read; ``connect_timeout_seconds``
        # (when given) bounds the TCP/TLS connect phase separately so one
        # unreachable venue endpoint cannot hold a caller for the full read
        # timeout before the first byte.  Either way no REST call is unbounded.
        self.timeout_seconds = float(timeout_seconds)
        self.connect_timeout_seconds = (
            None if connect_timeout_seconds is None else float(connect_timeout_seconds)
        )
        created_session = session is None
        self.session = session or requests.Session()
        if created_session and (pool_connections is not None or pool_maxsize is not None):
            connections = max(1, int(pool_connections if pool_connections is not None else 10))
            maxsize = max(1, int(pool_maxsize if pool_maxsize is not None else connections))
            adapter = HTTPAdapter(
                pool_connections=connections,
                pool_maxsize=maxsize,
                pool_block=False,
            )
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)
        self.proxy_url = str(proxy_url or "").strip() or None
        if activity_error_body_limit is not None and int(activity_error_body_limit) < 0:
            raise ValueError("activity_error_body_limit must be >= 0 or None")
        self.activity_error_body_limit = activity_error_body_limit
        if trust_env is not None:
            self.session.trust_env = bool(trust_env)
        if self.proxy_url:
            self.session.trust_env = False
            proxies = getattr(self.session, "proxies", None)
            if proxies is None:
                proxies = {}
                self.session.proxies = proxies
            proxies.update({"http": self.proxy_url, "https": self.proxy_url})
        self.activity = ActivityMonitor()
        self.last_response_headers: dict[str, str] = {}

    @property
    def request_timeout(self) -> Any:
        if self.connect_timeout_seconds is None:
            return self.timeout_seconds
        return (self.connect_timeout_seconds, self.timeout_seconds)

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    @staticmethod
    def _decode(response: Any, *, method: str, path: str) -> Any:
        text = str(getattr(response, "text", "") or "")
        status_code = int(getattr(response, "status_code", 0))
        if status_code >= 400:
            raise HTTPClientError(
                method=method,
                path=path,
                status_code=status_code,
                response_text=text,
                headers=getattr(response, "headers", None),
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
        body: Any = None,
        params: Optional[Mapping[str, Any]] = None,
        operation: str = "post",
    ) -> Any:
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
        body: Any = None,
    ) -> Any:
        started = time.perf_counter()
        response = None
        error = False
        error_message = None
        try:
            request = getattr(self.session, method.lower())
            kwargs = {"headers": dict(headers or {}), "params": params, "timeout": self.request_timeout}
            if method == "POST":
                kwargs["json"] = body if body is not None else {}
            response = request(self._url(path), **kwargs)
            self.last_response_headers = {
                str(key): str(value)
                for key, value in (getattr(response, "headers", None) or {}).items()
            }
            return self._decode(response, method=method, path=path)
        except Exception as exc:
            error = True
            if isinstance(exc, HTTPClientError) and self.activity_error_body_limit is not None:
                # Some providers return an HTML challenge page for an API
                # error. Keep the structured exception (and its headers) for
                # callers, but allow sensitive/verbose response bodies to be
                # omitted from activity snapshots.
                limit = int(self.activity_error_body_limit)
                if limit == 0:
                    error_message = f"{exc.method} {exc.path} failed {exc.status_code}"
                else:
                    error_message = (
                        f"{exc.method} {exc.path} failed {exc.status_code}: "
                        f"{exc.response_text[:limit]}"
                    )
                error_message = _redact_proxy_text(error_message, self.proxy_url)
            else:
                error_message = _redact_proxy_text(exc, self.proxy_url)
            if error_message != str(exc):
                # Preserve the original exception type for transport
                # classification while preventing credentials from reaching
                # logs or API status snapshots through ``str(exc)``.
                try:
                    exc.args = (error_message,)
                except Exception:
                    pass
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

    def close(self) -> None:
        closer = getattr(self.session, "close", None)
        if callable(closer):
            closer()
