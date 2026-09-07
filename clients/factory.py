"""Construction of venue clients from serializable configuration.

Shared runtime code imports this module instead of importing a concrete
adaptor.  Concrete adaptors are imported lazily so public UI/configuration
imports do not require authenticated venue SDKs.
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
    if normalized == "kalshi":
        from adaptors.kalshi import KalshiClientConfig
        config_type = KalshiClientConfig
    elif normalized == "polymarket":
        from adaptors.polymarket import PolymarketClientConfig
        config_type = PolymarketClientConfig
    else:
        raise ValueError(f"unsupported venue: {normalized}")

    supplied = _mapping_values(values)
    supplied.update(kwargs)
    allowed = {field.name for field in getattr(config_type, "__dataclass_fields__", {}).values()}
    return config_type(**{key: value for key, value in supplied.items() if key in allowed})


def build_client(venue: str, config: Any = None, **kwargs: Any) -> BaseClient:
    """Construct a concrete client while exposing only ``BaseClient`` here."""

    normalized = _normalize_venue(venue)
    if normalized == "kalshi":
        from adaptors.kalshi import KalshiApiClient, KalshiClientConfig
        client_type = KalshiApiClient
        config_type = KalshiClientConfig
    elif normalized == "polymarket":
        from adaptors.polymarket import PolymarketClient, PolymarketClientConfig
        client_type = PolymarketClient
        config_type = PolymarketClientConfig
    else:
        raise ValueError(f"unsupported venue: {normalized}")

    client_config = config
    if client_config is None or not isinstance(client_config, config_type):
        client_config = build_client_config(normalized, config, **kwargs)
    return client_type(client_config)


def supported_venues() -> tuple[str, ...]:
    return ("kalshi", "polymarket")
