"""Small venue-independent synchronous JSON-over-HTTP transport."""

from __future__ import annotations

from typing import Any, Mapping, Optional

import requests


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
    ) -> dict:
        response = self.session.get(
            self._url(path), headers=dict(headers or {}), params=params, timeout=self.timeout_seconds
        )
        return self._decode(response, method="GET", path=path)

    def post(
        self,
        path: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        body: Optional[Mapping[str, Any]] = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> dict:
        response = self.session.post(
            self._url(path),
            headers=dict(headers or {}),
            json=dict(body or {}),
            params=params,
            timeout=self.timeout_seconds,
        )
        return self._decode(response, method="POST", path=path)

    def delete(
        self,
        path: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> dict:
        response = self.session.delete(
            self._url(path), headers=dict(headers or {}), params=params, timeout=self.timeout_seconds
        )
        return self._decode(response, method="DELETE", path=path)
