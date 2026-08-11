from argparse import Namespace

import V1
from top_of_book_bot import BotSettings, build_settings_from_args, parse_bot_args


def test_v1_wires_existing_cli_options_into_kalshi_config(monkeypatch):
    args = Namespace(
        api_key_id="cli-key",
        private_key="cli-key.pem",
        use_demo=True,
        dry_run=True,
        subaccount=7,
    )
    settings = BotSettings(market_ticker="MKT", enable_sqlite_telemetry=False)
    captured = {}

    class FakeClient:
        def __init__(self, config):
            captured["config"] = config

    class FakeBot:
        net_position_units = 0

        def __init__(self, *, settings, api_client, market):
            captured["bot"] = (settings, api_client, market)

        async def run(self):
            captured["ran"] = True

    monkeypatch.setattr(V1, "parse_bot_args", lambda: args)
    monkeypatch.setattr(V1, "build_settings_from_args", lambda value: settings)
    monkeypatch.setattr(V1, "KalshiApiClient", FakeClient)
    monkeypatch.setattr(V1, "load_market_metadata", lambda client, market_id: "market")
    monkeypatch.setattr(V1, "TopOfBookBot", FakeBot)

    V1.main()

    config = captured["config"]
    assert config.api_key_id == "cli-key"
    assert config.private_key_path == "cli-key.pem"
    assert config.use_demo_environment is True
    assert config.dry_run is True
    assert config.subaccount_number == 7
    assert captured["bot"][0] is settings
    assert captured["ran"] is True


def test_projected_contract_limit_is_configurable_from_cli(monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        ["V1.py", "--ticker", "MKT", "--maximum-projected-contracts-per-line", "12"],
    )

    settings = build_settings_from_args(parse_bot_args())

    assert settings.maximum_projected_contracts_per_line == 12
