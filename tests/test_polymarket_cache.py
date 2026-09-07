from __future__ import annotations

from dataclasses import replace

from clients.models import Market
from polymarket_cache import PolymarketBookCache, PolymarketCatalogStore


def test_catalog_store_upserts_and_reloads_markets(tmp_path):
    path = tmp_path / "catalog.sqlite3"
    market = Market(
        "condition-1", title="Question", status="active", venue="polymarket",
        close_time_ms=123, volume_24h_units=900, open_interest_units=100,
        yes_token_id="yes", no_token_id="no", native_market_id="condition-1",
        price_level_structure="decimal", fractional_trading_enabled=True,
        market_rules="direct_token_books",
    )
    store = PolymarketCatalogStore(path)

    assert store.upsert([market]) == 1
    assert store.count() == 1
    reloaded = PolymarketCatalogStore(path).list_markets(status="active")
    assert reloaded == [market]

    updated = replace(market, title="Updated")
    assert store.upsert([updated]) == 1
    assert store.list_markets()[0].title == "Updated"


def test_book_cache_tracks_top_levels_and_stale_state():
    cache = PolymarketBookCache()
    cache.update_book("asset", {
        "bids": [{"price": "0.40", "size": "2"}, {"price": "0.41", "size": "1"}],
        "asks": [{"price": "0.50", "size": "3"}, {"price": "0.49", "size": "4"}],
        "hash": "h1",
    }, timestamp_ms=100)
    assert cache.get("asset").bid_units == 4_100
    assert cache.get("asset").ask_units == 4_900
    cache.mark_stale("asset")
    assert cache.get("asset") is None
    cache.update_book("asset", {"bids": [], "asks": []}, timestamp_ms=200)
    assert cache.get("asset") is not None
    assert cache.stale_assets == 0


def test_book_cache_persists_and_reloads(tmp_path):
    path = tmp_path / "books.sqlite3"
    cache = PolymarketBookCache(path)
    cache.update_book("asset", {"bids": [{"price": "0.31", "size": "2"}], "asks": [{"price": "0.4", "size": "3"}]}, timestamp_ms=9)
    cache.persist()

    restored = PolymarketBookCache(path)
    book = restored.get("asset")
    assert book is not None
    assert book.bid_units == 3100
    assert book.ask_units == 4000
    assert book.timestamp_ms == 9


def test_price_change_updates_top_or_marks_stale():
    cache = PolymarketBookCache()
    cache.update_book("asset", {"bids": [{"price": "0.31", "size": "2"}], "asks": [{"price": "0.4", "size": "3"}]})
    assert cache.update_price_change("asset", {"best_bid": "0.32", "price": "0.32", "size": "4", "side": "BUY"})
    assert cache.get("asset").bid_units == 3200
    assert not cache.update_price_change("asset", {"price": "0.1"})
    assert cache.get("asset") is None
