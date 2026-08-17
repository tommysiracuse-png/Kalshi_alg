from kalshi_urls import canonical_market_url, is_canonical_market_url, slugify_market_title


def test_deepseek_market_uses_canonical_series_event_url():
    url = canonical_market_url("KXDEEPSHARE", "KXDEEPSHARE-DEEP", "DeepSeek market share")
    assert url == "https://kalshi.com/markets/kxdeepshare/deepseek-market-share/kxdeepshare-deep"
    assert is_canonical_market_url(url)
    assert not is_canonical_market_url("https://kalshi.com/markets/kxdeepshare-deep-26")


def test_slugify_normalizes_punctuation_and_symbols():
    assert slugify_market_title("AI & U.S. Markets!") == "ai-and-u-s-markets"
