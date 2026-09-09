from __future__ import annotations

import time
import json
from dataclasses import replace

import pytest
from types import SimpleNamespace

from adaptors.polymarket import PolymarketClient, PolymarketClientConfig
from clients.models import Market
from polymarket_cache import BookTop
from polymarket_mirror import MirrorConfig, MirrorNotReadyError, PolymarketMirror, PolymarketMirrorStore
from screeners.screener import _BaseClientMarketSource
from screeners.kalshi_screener import build_parser, build_settings_from_args


def _market(market_id: str) -> Market:
    return Market(
        market_id,
        title=market_id,
        status="active",
        close_time_ms=int(time.time() * 1000) + 3_600_000,
        volume_24h_units=100_000,
        open_interest_units=10_000,
        yes_token_id=f"{market_id}-yes",
        no_token_id=f"{market_id}-no",
        venue="polymarket",
        native_market_id=market_id,
        market_rules="direct_token_books",
    )


def _book(timestamp: int) -> BookTop:
    return BookTop(4_000, 100, 5_000, 100, timestamp)


def test_mirror_publishes_only_complete_fresh_generations(tmp_path):
    store = PolymarketMirrorStore(tmp_path / "mirror.sqlite3")
    store.upsert_markets([_market("m1"), _market("m2")], "generation-1")
    now = int(time.time() * 1000)
    store.upsert_books({"m1-yes": _book(now), "m1-no": _book(now)})
    status = store.publish("generation-1", catalog_count=2, max_age_seconds=60)

    assert status["complete"] is False
    with pytest.raises(MirrorNotReadyError):
        list(store.iter_payloads(status="active", limit=100, max_age_seconds=60))

    store.upsert_books({"m2-yes": _book(now), "m2-no": _book(now)})
    status = store.publish("generation-1", catalog_count=2, max_age_seconds=60)
    assert status["complete"] is True
    payloads = list(store.iter_payloads(status="active", limit=100, max_age_seconds=60))
    assert [item["ticker"] for item in payloads] == ["m1", "m2"]
    assert payloads[0]["yes_bid_dollars"] == "0.4000"


def test_partial_generation_is_screenable_and_excludes_stale_books(tmp_path):
    store = PolymarketMirrorStore(tmp_path / "mirror.sqlite3")
    store.upsert_markets([_market("fresh"), _market("stale")], "generation-1")
    now = int(time.time() * 1000)
    store.upsert_books({
        "fresh-yes": _book(now), "fresh-no": _book(now),
        "stale-yes": _book(now - 120_000), "stale-no": _book(now - 120_000),
    })
    status = store.publish("generation-1", catalog_count=2, max_age_seconds=60)

    assert status["complete"] is False
    assert status["screenable"] is True
    assert status["partial"] is True
    assert status["bookReadyMarkets"] == 1
    rows = list(store.iter_payloads(status="active", limit=100, max_age_seconds=60, allow_partial=True))
    assert [item["ticker"] for item in rows] == ["fresh"]


def test_republish_active_refreshes_readiness_and_scopes_book_age(tmp_path):
    store = PolymarketMirrorStore(tmp_path / "mirror.sqlite3")
    market = _market("active")
    now = int(time.time() * 1000)
    store.upsert_markets([market], "generation-1")
    store.upsert_books({
        "active-yes": _book(now),
        "active-no": _book(now),
        # An orphaned asset from an older catalog must not make the active
        # generation's readiness telemetry look ancient.
        "orphan": _book(now - 3_600_000),
    })
    store.publish("generation-1", catalog_count=1, max_age_seconds=60)

    with store._connect() as db:
        status = json.loads(db.execute(
            "SELECT value FROM mirror_meta WHERE key='status'"
        ).fetchone()[0])
        status["capturedAtMs"] = now - 120_000
        db.execute(
            "UPDATE mirror_meta SET value=? WHERE key='status'",
            (json.dumps(status),),
        )

    refreshed = store.republish_active(max_age_seconds=60)
    assert refreshed["screenable"] is True
    assert refreshed["capturedAtMs"] > now - 120_000
    assert refreshed["bookMaxAgeMs"] < 5_000
    assert refreshed["bookLatestAgeMs"] < 5_000


def test_book_publication_skips_unchanged_cache_rows_and_advances_revision(tmp_path):
    store = PolymarketMirrorStore(tmp_path / "mirror.sqlite3")
    market = _market("revision")
    now = int(time.time() * 1000)
    store.upsert_markets([market], "generation-1")
    books = {
        "revision-yes": _book(now),
        "revision-no": _book(now),
    }

    assert store.upsert_books(books) == 2
    first = store.publish("generation-1", catalog_count=1, max_age_seconds=60)
    assert first["bookRevision"] == 1

    # The stream cache is a full snapshot, so identical heartbeats must not
    # create another SQLite write transaction or trigger a screener refresh.
    assert store.upsert_books(books) == 0
    second = store.republish_active(max_age_seconds=60)
    assert second["bookRevision"] == 1

    changed = dict(books)
    changed["revision-yes"] = _book(now + 1)
    assert store.upsert_books(changed) == 1
    third = store.republish_active(max_age_seconds=60)
    assert third["bookRevision"] == 2


def test_child_heartbeat_does_not_hide_published_snapshot(tmp_path):
    store = PolymarketMirrorStore(tmp_path / "mirror.sqlite3")
    market = _market("heartbeat")
    now = int(time.time() * 1000)
    store.upsert_markets([market], "generation-1")
    store.upsert_books({"heartbeat-yes": _book(now), "heartbeat-no": _book(now)})
    published = store.publish("generation-1", catalog_count=1, max_age_seconds=60)
    status_path = tmp_path / "mirror-status.json"
    status_path.write_text(json.dumps({
        "running": True,
        "complete": False,
        "reason": "syncing_catalog",
    }), encoding="utf-8")

    class NoRestClient:
        mirror_store = store
        config = SimpleNamespace(
            mirror_enabled=True,
            mirror_required_complete_snapshot=True,
            mirror_snapshot_max_age_seconds=60,
            mirror_status_path=str(status_path),
        )
        normalized_venue = "polymarket"

        def list_markets(self, _query):
            raise AssertionError("published mirror should bypass REST catalog retrieval")

    source = _BaseClientMarketSource(NoRestClient())
    rows = list(source.list_markets(status="open", limit=100, max_total=100, mve_filter=None))
    assert published["complete"] is True
    assert [row["ticker"] for row in rows] == ["heartbeat"]


def test_mirror_marks_markets_missing_from_new_generation_inactive(tmp_path):
    store = PolymarketMirrorStore(tmp_path / "mirror.sqlite3")
    store.upsert_markets([_market("old"), _market("new")], "generation-1")
    now = int(time.time() * 1000)
    store.upsert_books({
        "old-yes": _book(now), "old-no": _book(now),
        "new-yes": _book(now), "new-no": _book(now),
    })
    store.publish("generation-1", catalog_count=2, max_age_seconds=60)

    store.upsert_markets([_market("new")], "generation-2")
    store.publish("generation-2", catalog_count=1, max_age_seconds=60)
    rows = list(store.iter_payloads(status="active", limit=100, max_age_seconds=60))
    assert [item["ticker"] for item in rows] == ["new"]


def test_market_source_reads_fresh_mirror_without_calling_client_rest(tmp_path):
    store = PolymarketMirrorStore(tmp_path / "mirror.sqlite3")
    market = _market("local")
    now = int(time.time() * 1000)
    store.upsert_markets([market], "generation-1")
    store.upsert_books({"local-yes": _book(now), "local-no": _book(now)})
    store.publish("generation-1", catalog_count=1, max_age_seconds=60)

    class NoRestClient:
        mirror_store = store
        config = SimpleNamespace(
            mirror_enabled=True,
            mirror_required_complete_snapshot=True,
            mirror_snapshot_max_age_seconds=60,
        )
        normalized_venue = "polymarket"

        def list_markets(self, _query):
            raise AssertionError("fresh mirror should bypass REST catalog retrieval")

    source = _BaseClientMarketSource(NoRestClient())
    rows = list(source.list_markets(status="open", limit=100, max_total=100, mve_filter=None))
    assert rows[0]["ticker"] == "local"
    assert source.bounded_results is True


def test_market_source_reads_partial_generation_without_rest_fallback(tmp_path):
    store = PolymarketMirrorStore(tmp_path / "mirror.sqlite3")
    market = _market("partial")
    now = int(time.time() * 1000)
    store.upsert_markets([market, _market("waiting")], "generation-1")
    store.upsert_books({"partial-yes": _book(now), "partial-no": _book(now)})
    status = store.publish("generation-1", catalog_count=2, max_age_seconds=60)
    assert status["screenable"] is True and status["complete"] is False

    class NoRestClient:
        mirror_store = store
        config = SimpleNamespace(
            mirror_enabled=True,
            mirror_required_complete_snapshot=True,
            mirror_snapshot_max_age_seconds=60,
        )
        normalized_venue = "polymarket"

        def list_markets(self, _query):
            raise AssertionError("partial mirror should bypass REST catalog retrieval")

    rows = list(_BaseClientMarketSource(NoRestClient()).list_markets(
        status="open", limit=100, max_total=100, mve_filter=None,
    ))
    assert [row["ticker"] for row in rows] == ["partial"]


def test_market_source_does_not_fall_back_to_rest_when_mirror_is_warming(tmp_path):
    store = PolymarketMirrorStore(tmp_path / "mirror.sqlite3")

    class NoRestClient:
        mirror_store = store
        config = SimpleNamespace(
            mirror_enabled=True,
            # Older sessions may omit this setting. Mirror mode must still
            # fail fast rather than starting a synchronous catalog scan.
            mirror_required_complete_snapshot=False,
            mirror_snapshot_max_age_seconds=60,
        )
        normalized_venue = "polymarket"

        def list_markets(self, _query):
            raise AssertionError("warming mirror must not fall back to REST")

    source = _BaseClientMarketSource(NoRestClient())
    with pytest.raises(MirrorNotReadyError, match="not ready"):
        list(source.list_markets(status="open", limit=100, max_total=100, mve_filter=None))


def test_vectorized_mirror_candidates_preserve_top_market_fields(tmp_path):
    store = PolymarketMirrorStore(tmp_path / "mirror.sqlite3")
    market = _market("vector")
    now = int(time.time() * 1000)
    store.upsert_markets([market], "generation-1")
    store.upsert_books({"vector-yes": _book(now), "vector-no": _book(now)})
    store.publish("generation-1", catalog_count=1, max_age_seconds=60)
    settings = build_settings_from_args(build_parser().parse_args([]))
    settings.update({"max_markets_to_scan": 10, "top_n": 10, "min_vol24h": 0, "min_oi": 0,
                     "min_time_to_close_hrs": 0, "max_time_to_close_hrs": 100})
    payloads, scanned = store.vectorized_rows(
        status="active", limit=10, max_age_seconds=60, settings=settings,
    )
    assert scanned == 1
    assert payloads[0]["ticker"] == "vector"
    assert payloads[0]["yes_bid"] == 40
    assert payloads[0]["open_interest_fp"] == "100.00"


def test_vectorized_mirror_missing_open_interest_is_not_reported_as_zero(tmp_path):
    store = PolymarketMirrorStore(tmp_path / "mirror.sqlite3")
    market = replace(_market("missing-oi"), open_interest_units=None)
    now = int(time.time() * 1000)
    store.upsert_markets([market], "generation-1")
    store.upsert_books({"missing-oi-yes": _book(now), "missing-oi-no": _book(now)})
    store.publish("generation-1", catalog_count=1, max_age_seconds=60)
    settings = build_settings_from_args(build_parser().parse_args([]))
    settings.update({"max_markets_to_scan": 10, "top_n": 10, "min_vol24h": 0,
                     "min_time_to_close_hrs": 0, "max_time_to_close_hrs": 100})

    payloads, scanned = store.vectorized_rows(
        status="active", limit=10, max_age_seconds=60, settings=settings,
    )
    raw_payloads = list(store.iter_payloads(
        status="active", limit=10, max_age_seconds=60, allow_partial=True,
    ))

    assert scanned == 1
    assert payloads == []
    assert raw_payloads[0]["open_interest_fp"] is None


def test_mirror_persists_hydrated_open_interest_before_publishing_page(tmp_path):
    class StubTransport:
        last_response_headers = {}

        def __init__(self, response):
            self.response = response

        def get(self, path, *, params, operation):
            return self.response

    client = PolymarketClient(PolymarketClientConfig())
    client.gamma_http = StubTransport({
        "data": [{
            "conditionId": "condition-1",
            "question": "Question",
            "active": True,
            "outcomes": ["Yes", "No"],
            "clobTokenIds": ["yes-1", "no-1"],
        }],
        "next_cursor": "",
    })
    client.data_http = StubTransport([{"market": "condition-1", "value": "42.50"}])
    client.hydrate_market_books = lambda markets, allow_rest=True: None
    client.activity_snapshot = lambda: {}

    mirror = PolymarketMirror(
        client.config,
        tmp_path / "mirror.sqlite3",
        tmp_path / "mirror-status.json",
        MirrorConfig(catalog_page_size=1),
    )
    generation, markets, status = mirror._sync_incremental(client)

    assert markets[0].open_interest_units == 4_250
    assert mirror.store.active_markets()[0].open_interest_units == 4_250
    assert status["openInterest"]["marketsResolved"] == 1
    assert status["openInterest"]["marketsMissing"] == 0
    assert status["openInterest"]["forbiddenResponses"] == 0
    assert status["openInterest"]["cloudflare403"] == 0
    assert status["openInterest"]["retries"] == 0
    assert status["openInterest"]["retryExhausted"] == 0
