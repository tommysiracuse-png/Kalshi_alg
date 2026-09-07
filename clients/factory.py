"""Construction of venue clients from serializable configuration.

Shared runtime code imports this module instead of importing a concrete
adaptor.  Phase 1 registers Kalshi only; adding another venue later is a
registry change rather than a launcher-wide import migration.
"""

from __future__ import annotations

from dataclasses import is_dataclass
from typing import Any, Mapping

from clients.base_client import BaseClient


def _mapping_values(values: Any) -> dict[str, Any]:
    if values is None:
        return {}
    if isinstance(values, Mapping):
        return {str(key): value for key, value in values.items()}
    if is_dataclass(values):
        from dataclasses import asdict

        return asdict(values)
    return {
        str(name): getattr(values, name)
        for name in dir(values)
        if not name.startswith("_") and not callable(getattr(values, name, None))
    }


def _normalize_venue(venue: str) -> str:
    normalized = str(venue or "kalshi").strip().lower()
    if not normalized:
        normalized = "kalshi"
    return normalized


def build_client_config(venue: str, values: Any = None, **kwargs: Any) -> Any:
    """Build the serializable adaptor config for ``venue``."""

    normalized = _normalize_venue(venue)
    if normalized != "kalshi":
        raise ValueError(f"unsupported venue: {normalized}")
    from adaptors.kalshi import KalshiClientConfig

    supplied = _mapping_values(values)
    supplied.update(kwargs)
    allowed = {field.name for field in getattr(KalshiClientConfig, "__dataclass_fields__", {}).values()}
    return KalshiClientConfig(**{key: value for key, value in supplied.items() if key in allowed})


def build_client(venue: str, config: Any = None, **kwargs: Any) -> BaseClient:
    """Construct a concrete client while exposing only ``BaseClient`` here."""

    normalized = _normalize_venue(venue)
    if normalized != "kalshi":
        raise ValueError(f"unsupported venue: {normalized}")
    from adaptors.kalshi import KalshiApiClient, KalshiClientConfig

    client_config = config
    if client_config is None or not isinstance(client_config, KalshiClientConfig):
        client_config = build_client_config(normalized, config, **kwargs)
    return KalshiApiClient(client_config)


def supported_venues() -> tuple[str, ...]:
    return ("kalshi",)
