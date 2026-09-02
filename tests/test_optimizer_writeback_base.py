"""Write-back onto a base session keeps every field the optimizer did not search."""

from pathlib import Path

import pytest

from optimizer import main as optimizer_main
from optimizer import writeback
from session_config import default_session_configuration
from session_store import SessionStore


def _base_configuration():
    configuration = default_session_configuration()
    # Operator-tuned fields the Tier-1 optimizer never searches.
    configuration["bot"]["orderbook_pull_side_cooldown_ms"] = 1500
    configuration["bot"]["orderbook_pull_absolute_threshold_contracts"] = 5000
    configuration["bot"]["minimum_top_level_depth_contracts"] = 5
    configuration["launcher"]["maxBots"] = 100
    configuration["launcher"]["refreshIntervalSeconds"] = 1200.0
    return configuration


def test_write_back_onto_base_session_keeps_unsearched_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("KALSHI_SESSION_STORE", str(tmp_path / "store"))
    base = _base_configuration()
    defaults = default_session_configuration()
    assert base["bot"]["orderbook_pull_side_cooldown_ms"] != defaults["bot"]["orderbook_pull_side_cooldown_ms"]

    created = writeback.write_back(
        "run-base-1", {"minimum_expected_edge_cents_to_quote": 2, "yes_order_budget_cents": 825},
        7.5, 0, 1_000, base_configuration=base, base_session_name="guards-v1",
    )
    configuration = created["configuration"]
    assert configuration["bot"]["minimum_expected_edge_cents_to_quote"] == 2
    assert configuration["launcher"]["yesBudgetCents"] == 825
    # Unsearched values come from the base session, not the schema defaults.
    assert configuration["bot"]["orderbook_pull_side_cooldown_ms"] == 1500
    assert configuration["bot"]["orderbook_pull_absolute_threshold_contracts"] == 5000
    assert configuration["bot"]["minimum_top_level_depth_contracts"] == 5
    assert configuration["launcher"]["maxBots"] == 100
    assert configuration["launcher"]["refreshIntervalSeconds"] == 1200.0
    assert "unsearched fields from session 'guards-v1'" in created["description"]
    assert created["selected"] is False
    # The caller's mapping is not mutated.
    assert "yesBudgetCents" not in base["bot"]
    assert base["bot"]["minimum_expected_edge_cents_to_quote"] == defaults["bot"]["minimum_expected_edge_cents_to_quote"]


def test_write_back_without_base_uses_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("KALSHI_SESSION_STORE", str(tmp_path / "store"))
    created = writeback.write_back("run-base-2", {"minimum_expected_edge_cents_to_quote": 2}, 1.0, 0, 1_000)
    defaults = default_session_configuration()
    assert created["configuration"]["bot"]["orderbook_pull_side_cooldown_ms"] == defaults["bot"]["orderbook_pull_side_cooldown_ms"]
    assert "unsearched fields" not in created["description"]


def test_load_base_session_configuration_by_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("KALSHI_SESSION_STORE", str(tmp_path / "store"))
    store = SessionStore(tmp_path / "store")
    store.create_session({"name": "guards-v1", "description": "", "configuration": _base_configuration()})
    loaded = optimizer_main.load_base_session_configuration("guards-v1")
    assert loaded["launcher"]["maxBots"] == 100
    with pytest.raises(SystemExit, match="no saved session named"):
        optimizer_main.load_base_session_configuration("nope")

    args = optimizer_main.build_arg_parser().parse_args(["--base-session", "guards-v1"])
    assert args.base_session == "guards-v1"
