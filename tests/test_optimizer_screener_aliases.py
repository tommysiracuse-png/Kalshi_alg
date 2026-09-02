"""The optimizer's historical screener filter understands the session's screener section verbatim."""

from optimizer.data import normalize_screener_settings, screener_settings_from_configuration
from session_config import default_session_configuration


def test_session_screener_section_maps_every_filter_field():
    section = dict(default_session_configuration()["screener"])
    section.update({
        "minOpenInterest": 777,
        "minTimeToCloseHours": 4.5,
        "maxTimeToCloseHours": 40,
        "minVol24h": 999,
        "minSpreadCents": 6,
        "maxSpreadCents": 30,
        "minYesBidCents": 7,
        "minNoBidCents": 8,
        "excludedTickerKeywords": ["lowt", "Rain"],
        "status": "open",
        "mveFilter": "exclude",
    })
    normalized = normalize_screener_settings(section)
    assert normalized["min_oi"] == 777.0
    assert normalized["min_time_to_close_hrs"] == 4.5
    assert normalized["max_time_to_close_hrs"] == 40.0
    assert normalized["min_vol24h"] == 999.0
    assert normalized["min_spread_cents"] == 6.0
    assert normalized["max_spread_cents"] == 30.0
    assert normalized["min_yes_bid_cents"] == 7.0
    assert normalized["min_no_bid_cents"] == 8.0
    assert normalized["excluded_ticker_keywords"] == ["LOWT", "RAIN"]
    assert normalized["status"] == "open"
    assert normalized["mve_filter"] == "exclude"


def test_configuration_wrapper_uses_the_section_and_defaults_match_constants():
    configuration = default_session_configuration()
    from_section = screener_settings_from_configuration(configuration)
    assert from_section == normalize_screener_settings({})
    assert screener_settings_from_configuration({"schemaVersion": 2, "bot": {}}) is None
