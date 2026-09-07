from __future__ import annotations

import pytest

from clients.base_client import BaseClient
from clients.factory import build_client, build_client_config, supported_venues
from core.session_config import default_session_configuration, validate_session_configuration


def test_factory_builds_kalshi_client_as_base_client():
    config = build_client_config("kalshi", public_only=True)
    client = build_client("kalshi", config)
    assert isinstance(client, BaseClient)
    assert client.venue == "kalshi"


def test_factory_rejects_unsupported_venue():
    with pytest.raises(ValueError, match="unsupported venue: polymarket"):
        build_client_config("polymarket", {})
    assert supported_venues() == ("kalshi",)


def test_session_migration_adds_kalshi_venue():
    value = default_session_configuration()
    value.pop("venue")
    migrated = validate_session_configuration(value)
    assert migrated["venue"] == "kalshi"
