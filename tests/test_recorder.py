"""Tests for the standalone ms order-book recorder (recorder/).

Run with a writable basetemp on this machine:
    .venv\\Scripts\\python -m pytest tests/test_recorder.py --basetemp=runtime/pt_rec
"""

from __future__ import annotations

import asyncio
import json
import shutil
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from recorder import RECORDER_VERSION
from recorder.config import RecorderConfig
from recorder.main import PRIVATE_CHANNELS, Recorder, message_type, subscription_messages
from recorder.market_selector import (
    build_market_set,
    filter_open_markets,
    read_fleet_tickers,
    reshard,
    sample_active_tickers,
    select_markets,
)
from recorder.writer import (
    HOUR_MS,
    HourlyGzipWriter,
    connection_file,
    hour_key,
    iter_records,
    next_hour_start_ms,
    prune_old_days,
)


@pytest.fixture()
def tmp_dir():
    # Deliberately not pytest's tmp_path: the shared pytest-of-admin temp root
    # has broken ACLs on this machine and errors before any test runs.
    path = Path(tempfile.mkdtemp(prefix="recorder-test-"))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _config(root: Path, **overrides) -> RecorderConfig:
    base = dict(
        record_dir=root / "record_data",
        runtime_dir=root / "runtime",
        launcher_status_path=root / "runtime" / "launcher_status.json",
    )
    base.update(overrides)
    return RecorderConfig(**base)


# ---------------------------------------------------------------- writer


T0_MS = 1_700_000_000_000  # 2023-11-14T22:13:20Z


def test_writer_round_trips_raw_byte_identically(tmp_dir):
    raws = [
        '{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"A","yes_dollars_fp":[["0.4500","10.00"]]}}',
        'unicode → ✓ "quoted" back\\slash',
        '{"x":"real\nnewline\ttab\r"}',
        "   leading and trailing spaces   ",
        "",
    ]
    writer = HourlyGzipWriter(tmp_dir, 0, flush_seconds=0.0)
    for offset, raw in enumerate(raws):
        writer.write(T0_MS + offset, raw)
    path = writer.path
    writer.close()

    assert path == tmp_dir / "20231114" / "22" / "conn0.jsonl.gz"
    records = list(iter_records(path))
    assert [record["raw"] for record in records] == raws
    assert [record["recv_ms"] for record in records] == [T0_MS + i for i in range(len(raws))]
    # One physical line per message even when raw contains newlines.
    import gzip

    with gzip.open(path, "rt", encoding="utf-8") as handle:
        assert len(handle.read().splitlines()) == len(raws)


def test_writer_hourly_rotation_naming_and_append(tmp_dir):
    writer = HourlyGzipWriter(tmp_dir, 2, flush_seconds=0.0)
    first_key = hour_key(T0_MS)
    same_path = writer.open_hour(first_key)
    writer.write(T0_MS, "a")
    assert writer.open_hour(first_key) == same_path and writer.rotations == 1  # no-op
    writer.write(T0_MS + 1, "b")
    second_key = hour_key(next_hour_start_ms(T0_MS))
    second_path = writer.open_hour(second_key)
    writer.write(next_hour_start_ms(T0_MS), "c")
    writer.close()

    assert same_path == tmp_dir / "20231114" / "22" / "conn2.jsonl.gz"
    assert second_path == tmp_dir / "20231114" / "23" / "conn2.jsonl.gz"
    assert [r["raw"] for r in iter_records(same_path)] == ["a", "b"]
    assert [r["raw"] for r in iter_records(second_path)] == ["c"]

    # Re-opening the same hour (process restart) appends a second gzip member.
    again = HourlyGzipWriter(tmp_dir, 2, flush_seconds=0.0)
    again.open_hour(first_key)
    again.write(T0_MS + 2, "d")
    again.close()
    assert [r["raw"] for r in iter_records(same_path)] == ["a", "b", "d"]


def test_writer_flushed_data_survives_missing_trailer(tmp_dir):
    writer = HourlyGzipWriter(tmp_dir, 0, flush_seconds=0.0)  # sync-flush after every line
    writer.write(T0_MS, "one")
    writer.write(T0_MS + 1, "two")
    path = writer.path
    # Simulate a hard kill: never close, copy the file as-is (no gzip trailer).
    raw_bytes = path.read_bytes()
    truncated = tmp_dir / "truncated.jsonl.gz"
    truncated.write_bytes(raw_bytes)
    assert [r["raw"] for r in iter_records(truncated)] == ["one", "two"]
    writer.close()


def test_prune_old_days_removes_only_old_day_dirs(tmp_dir):
    now_ms = 1_788_264_000_000  # 2026-09-01T12:00:00Z
    for name in ("20260701", "20260801", "20260805", "20260901", "notaday"):
        (tmp_dir / name / "00").mkdir(parents=True)
        (tmp_dir / name / "00" / "conn0.jsonl.gz").write_bytes(b"x")
    (tmp_dir / "README.txt").write_text("keep", encoding="utf-8")

    removed = prune_old_days(tmp_dir, 30, now_ms)

    assert removed == ["20260701", "20260801"]  # cutoff day = 20260802
    assert sorted(p.name for p in tmp_dir.iterdir()) == ["20260805", "20260901", "README.txt", "notaday"]
    assert prune_old_days(tmp_dir, 0, now_ms) == []  # disabled


# ---------------------------------------------------------------- selector


class FakeClient:
    def __init__(self, trade_pages, statuses):
        self.trade_pages = list(trade_pages)  # list of lists of tickers
        self.statuses = dict(statuses)
        self.trade_calls = []
        self.market_calls = []

    def list_public_trades(self, market_id="", *, page_size, max_results, cursor=""):
        self.trade_calls.append((market_id, page_size, max_results, cursor))
        index = int(cursor or 0)
        page = self.trade_pages[index] if index < len(self.trade_pages) else []
        next_cursor = str(index + 1) if index + 1 < len(self.trade_pages) else ""
        return [SimpleNamespace(market_id=t) for t in page], next_cursor

    def list_markets_raw(self, *, status="", venue_filters=None, max_results=1000, **_):
        tickers = (venue_filters or {}).get("tickers", "").split(",")
        self.market_calls.append(tickers)
        return [{"ticker": t, "status": self.statuses[t]} for t in tickers if t in self.statuses]


def _write_status(path: Path, tickers, generated_ms):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"generatedAt": generated_ms, "bots": [{"ticker": t, "botRunning": True} for t in tickers]}),
        encoding="utf-8",
    )


def test_read_fleet_tickers_handles_live_stale_and_missing(tmp_dir):
    path = tmp_dir / "launcher_status.json"
    now_ms = 10_000_000
    _write_status(path, ["F1", "F2", "F1"], now_ms - 5_000)
    assert read_fleet_tickers(path, now_ms=now_ms, max_age_seconds=60) == ["F1", "F2"]
    _write_status(path, ["F1"], now_ms - 3_600_000)
    assert read_fleet_tickers(path, now_ms=now_ms, max_age_seconds=60) == []
    assert read_fleet_tickers(path, now_ms=now_ms, max_age_seconds=0) == ["F1"]
    assert read_fleet_tickers(tmp_dir / "missing.json", now_ms=now_ms) == []
    path.write_text("{not json", encoding="utf-8")
    assert read_fleet_tickers(path, now_ms=now_ms) == []


def test_sample_and_filter_helpers():
    client = FakeClient([["X", "Y", "X", "Z"], ["X", "W", "Y"]], {"X": "open", "Y": "active", "Z": "closed", "W": "open"})
    ranked = sample_active_tickers(client, pages=12, page_size=1000)
    assert ranked == [("X", 3), ("Y", 2), ("W", 1), ("Z", 1)]
    assert [c[0] for c in client.trade_calls] == ["", ""]  # global feed, both pages
    assert filter_open_markets(client, ["Z", "X", "missing", "Y", "W"]) == ["X", "Y", "W"]


def test_select_markets_unions_fleet_and_active_validates_and_caps(tmp_dir):
    config = _config(tmp_dir, max_markets=4, top_n=3, rest_pause_seconds=0.0, trade_sample_pages=12)
    now_ms = 50_000_000
    _write_status(config.launcher_status_path, ["F1", "F2", "X"], now_ms - 1_000)
    client = FakeClient(
        [["X"] * 5 + ["Y"] * 4 + ["Z"] * 3 + ["W"] * 2 + ["V"]],
        {"F1": "open", "F2": "settled", "X": "open", "Y": "open", "Z": "closed", "W": "open", "V": "open"},
    )

    selected = select_markets(client, config, now_ms=now_ms)

    # fleet-open first (F2 settled -> dropped), then active ranking (Z closed -> skipped), capped at 4.
    assert selected == ["F1", "X", "Y", "W"]
    looked_up = {t for call in client.market_calls for t in call}
    assert {"F1", "F2", "X", "Y", "Z", "W"} <= looked_up


def test_build_market_set_priority_and_cap():
    assert build_market_set(["B", "A"], ["A", "C", "D"], max_markets=3) == ["B", "A", "C"]
    assert build_market_set([], ["A", "A", "B"], max_markets=10) == ["A", "B"]
    assert build_market_set(["A", "B"], ["C"], max_markets=1) == ["A"]


def test_reshard_is_stable_and_fills_gaps():
    previous = [["A", "B"], ["C", "D"], []]
    shards = reshard(previous, ["A", "C", "E", "F", "G"], per_connection=2, connection_count=3)
    assert shards == [["A", "E"], ["C", "F"], ["G"]]
    # Unchanged set -> identical shards (no reconnects).
    assert reshard(shards, ["G", "F", "E", "C", "A"], per_connection=2, connection_count=3) == shards
    # Fresh start chunks in priority order; overflow is dropped, not misplaced.
    assert reshard([], ["A", "B", "C", "D", "E"], per_connection=2, connection_count=2) == [["A", "B"], ["C", "D"]]
    assert reshard([["A"]], [], per_connection=2, connection_count=1) == [[]]


# ---------------------------------------------------------------- main loop


ACK1 = '{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":11}}'
ACK4 = '{"type":"subscribed","id":4,"msg":{"channel":"ticker","sid":12}}'
SNAP = '{"type":"orderbook_snapshot","sid":11,"seq":1,"msg":{"market_ticker":"A","yes_dollars_fp":[["0.4500","10.00"]],"no_dollars_fp":[]}}'
DELTA = '{"type":"orderbook_delta","sid":11,"seq":2,"msg":{"market_ticker":"A","price_dollars":"0.4500","delta_fp":"-3.00","side":"yes","ts":"2026-09-01T00:00:00Z"}}'
CANNED = [ACK1, ACK4, SNAP, DELTA]


class FakeWs:
    """Async-iterable of canned wire strings that then blocks until closed, like a live socket."""

    def __init__(self, canned):
        self.canned = list(canned)
        self.subscriptions = []  # (messages, headers) per connect
        self._closed = None

    async def subscribe(self, messages, *, headers=None):
        self.subscriptions.append((list(messages), dict(headers or {})))
        self._closed = asyncio.Event()

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        closed = self._closed
        for message in self.canned:
            yield message
        await closed.wait()

    async def close(self):
        if self._closed is not None:
            self._closed.set()


async def _wait_until(predicate, timeout=8.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


def test_subscription_messages_contain_only_public_sids():
    messages = subscription_messages(["A", "B", "A", ""])
    parsed = [json.loads(m) for m in messages]
    assert [p["id"] for p in parsed] == [1, 4]
    assert all(p["cmd"] == "subscribe" for p in parsed)
    assert parsed[0]["params"]["channels"] == ["orderbook_delta"]
    assert parsed[1]["params"]["channels"] == ["trade", "ticker"]
    assert all(p["params"]["market_tickers"] == ["A", "B"] for p in parsed)
    for message in messages:
        for private in ("user_orders", "fill", "market_positions"):
            assert private not in message
    assert not (PRIVATE_CHANNELS & {c for p in parsed for c in p["params"]["channels"]})
    assert message_type(ACK1) == "subscribed" and message_type(DELTA) == "orderbook_delta"
    assert message_type("garbage") == "unknown"
    with pytest.raises(ValueError):
        subscription_messages([])


def test_recorder_records_messages_and_resubscribes_on_market_change(tmp_dir):
    config = _config(
        tmp_dir, max_markets=4, markets_per_connection=2, refresh_seconds=0.05,
        heartbeat_seconds=0.05, flush_seconds=0.0,
    )
    selections = [["A", "B", "C"], ["A", "B", "D"]]

    def select():
        return selections.pop(0) if len(selections) > 1 else selections[0]

    sockets = {}

    def ws_factory(index):
        sockets[index] = FakeWs(CANNED)
        return sockets[index]

    recorder = Recorder(
        config, ws_factory=ws_factory, headers_provider=lambda: {"KALSHI-ACCESS-KEY": "k"}, select_markets=select,
    )
    start_ms = int(time.time() * 1000)

    async def scenario():
        task = asyncio.create_task(recorder.run())
        await _wait_until(lambda: 1 in sockets and len(sockets[1].subscriptions) >= 2)
        await _wait_until(lambda: sockets[1].subscriptions and recorder.connections[1].msg_count >= 8)
        await _wait_until(lambda: config.status_path.exists())
        recorder.request_stop()
        await asyncio.wait_for(task, 10)

    asyncio.run(scenario())
    end_ms = int(time.time() * 1000)

    # Shard 0 (A,B) never changed -> one subscribe; shard 1 went C -> D -> resubscribed.
    assert len(sockets[0].subscriptions) == 1
    assert len(sockets[1].subscriptions) == 2
    assert json.loads(sockets[1].subscriptions[0][0][0])["params"]["market_tickers"] == ["C"]
    assert json.loads(sockets[1].subscriptions[1][0][0])["params"]["market_tickers"] == ["D"]
    assert json.loads(sockets[0].subscriptions[0][0][0])["params"]["market_tickers"] == ["A", "B"]
    assert sockets[0].subscriptions[0][1] == {"KALSHI-ACCESS-KEY": "k"}

    # Every subscribe payload: exactly sids 1 and 4, no private channels.
    for socket in sockets.values():
        for messages, _headers in socket.subscriptions:
            parsed = [json.loads(m) for m in messages]
            assert [p["id"] for p in parsed] == [1, 4]
            channels = {c for p in parsed for c in p["params"]["channels"]}
            assert channels == {"orderbook_delta", "trade", "ticker"}
            assert not (channels & PRIVATE_CHANNELS)
            for m in messages:
                assert "user_orders" not in m and "fill" not in m and "market_positions" not in m

    # Messages landed verbatim with a receipt timestamp.
    key = hour_key(start_ms)
    records0 = list(iter_records(connection_file(config.record_dir, key, 0)))
    assert [r["raw"] for r in records0] == CANNED
    assert all(isinstance(r["recv_ms"], int) and start_ms <= r["recv_ms"] <= end_ms for r in records0)
    records1 = list(iter_records(connection_file(config.record_dir, key, 1)))
    assert [r["raw"] for r in records1] == CANNED + CANNED  # two sessions, fresh acks+snapshot each

    # Heartbeat + per-hour meta.
    status = json.loads(config.status_path.read_text(encoding="utf-8"))
    assert status["connection_count"] == 2 and status["markets"] == 3
    assert status["msgs_total"] == 12 and status["msgs_by_type"]["orderbook_snapshot"] == 3
    assert status["version"] == RECORDER_VERSION and status["state"] == "stopped"
    assert set(status) >= {"connected_connections", "msgs_per_sec", "bytes_today", "disk_free_gb", "last_error", "started_at_ms"}
    meta = json.loads((config.record_dir / key[0] / key[1] / "meta.json").read_text(encoding="utf-8"))
    assert meta["connections"] == {"conn0": ["A", "B"], "conn1": ["D"]}
    assert meta["markets"] == ["A", "B", "D"]
    assert [ack["channel"] for ack in meta["subscribe_acks"]["conn0"]] == ["orderbook_delta", "ticker"]
    assert meta["recorder_version"] == RECORDER_VERSION


def test_recorder_rotates_at_hour_boundary_with_fresh_subscribe(tmp_dir):
    config = _config(tmp_dir, max_markets=2, markets_per_connection=2, refresh_seconds=30, heartbeat_seconds=0.05, flush_seconds=0.0)
    real_now = time.time()
    boundary_ms = next_hour_start_ms(int(real_now * 1000))
    offset_s = (boundary_ms / 1000.0 - 0.3) - real_now  # fake clock starts 300 ms before the boundary
    clock = lambda: time.time() + offset_s  # noqa: E731

    sockets = {}

    def ws_factory(index):
        sockets[index] = FakeWs(CANNED)
        return sockets[index]

    recorder = Recorder(config, ws_factory=ws_factory, headers_provider=dict, select_markets=lambda: ["A"], clock=clock)

    async def scenario():
        task = asyncio.create_task(recorder.run())
        await _wait_until(lambda: 0 in sockets and len(sockets[0].subscriptions) >= 2)
        await _wait_until(lambda: recorder.connections[0].msg_count >= 8)
        recorder.request_stop()
        await asyncio.wait_for(task, 10)

    asyncio.run(scenario())

    before = connection_file(config.record_dir, hour_key(boundary_ms - 1), 0)
    after = connection_file(config.record_dir, hour_key(boundary_ms), 0)
    assert before.exists() and after.exists() and before.parent != after.parent
    assert [r["raw"] for r in iter_records(before)] == CANNED
    after_records = list(iter_records(after))
    assert [r["raw"] for r in after_records] == CANNED  # new hour file starts with acks + snapshot
    assert after_records[0]["recv_ms"] >= boundary_ms
    assert recorder.connections[0].connect_count == 2
    assert recorder.connections[0].last_close_reason == "stop"
    assert (after.parent / "meta.json").exists()


def test_recorder_reconnects_with_backoff_when_stream_ends(tmp_dir):
    config = _config(tmp_dir, max_markets=1, markets_per_connection=1, refresh_seconds=30, heartbeat_seconds=0.05,
                     flush_seconds=0.0, backoff_min_seconds=0.02, backoff_max_seconds=0.05)

    class DroppingWs(FakeWs):
        async def _iterate(self):
            for message in self.canned:
                yield message
            raise ConnectionError("socket dropped")

    sockets = {}

    def ws_factory(index):
        sockets[index] = DroppingWs(CANNED)
        return sockets[index]

    recorder = Recorder(config, ws_factory=ws_factory, headers_provider=dict, select_markets=lambda: ["A"])

    async def scenario():
        task = asyncio.create_task(recorder.run())
        await _wait_until(lambda: 0 in sockets and len(sockets[0].subscriptions) >= 3)
        recorder.request_stop()
        await asyncio.wait_for(task, 10)

    asyncio.run(scenario())
    assert recorder.last_error and "stream ended" in recorder.last_error
    assert recorder.connections[0].connect_count >= 3
    records = list(iter_records(connection_file(config.record_dir, hour_key(int(time.time() * 1000)), 0)))
    assert len(records) >= 12 and records[0]["raw"] == ACK1


def test_config_from_env_defaults_and_overrides(tmp_dir):
    config = RecorderConfig.from_env(tmp_dir, env={})
    assert config.max_markets == 150 and config.markets_per_connection == 50 and config.connection_count == 3
    assert config.top_n == 100 and config.refresh_seconds == 900 and config.retention_days == 30
    assert config.record_dir == tmp_dir / "record_data" and config.status_path == tmp_dir / "runtime" / "recorder_status.json"
    config = RecorderConfig.from_env(tmp_dir, env={"RECORD_MAX_MARKETS": "120", "MARKETS_PER_CONNECTION": "40", "RECORD_DIR": "x"})
    assert config.connection_count == 3 and config.record_dir == tmp_dir / "x"
    with pytest.raises(ValueError):
        RecorderConfig.from_env(tmp_dir, env={"RECORD_MAX_MARKETS": "0"})
