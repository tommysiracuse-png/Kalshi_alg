import json
from pathlib import Path

from markout_metrics import (
    add_observation,
    empty_markout_aggregate,
    finalize_aggregate,
    monetary_markout_units,
)
from session_store import RunMetricsAccumulator, SessionStore
from top_of_book_bot import TelemetryStore


def _record_fill(telemetry: TelemetryStore, *, key: str, side: str, price: int, size: int, fee: int | None, ts: int) -> None:
    telemetry.record_fill(
        fill_key=key, ts_ms=ts, ticker="TEST", side=side, trade_id=key, order_id=f"order-{key}",
        price_units=price, size_units=size, fee_units=fee, is_taker=False,
        inventory_before_units=0, inventory_after_units=size if side == "yes" else -size,
        fair_yes_before_units=5_000, best_yes_bid_units=4_900, best_no_bid_units=4_900,
        queue_ahead_units=0,
    )


def _record_markout(telemetry: TelemetryStore, *, key: str, side: str, fill_price: int, future_mid: int, ts: int) -> None:
    telemetry.record_markout(
        fill_key=key, ts_ms=ts, ticker="TEST", side=side, horizon_ms=30_000,
        fill_price_units=fill_price, future_mid_yes_units=future_mid,
        future_fair_yes_units=future_mid, adverse_units=0, bucket_key="bucket",
    )


def test_fixed_point_markout_and_fee_math():
    aggregate = empty_markout_aggregate(30_000)
    add_observation(aggregate, signed_price_units=300, size_units=150, fee_units=25)
    result = finalize_aggregate(aggregate, total_fill_count=2, pending_fill_count=1)

    assert monetary_markout_units(300, 150) == 450
    assert result["grossMarkoutUnits"] == 450
    assert result["netMarkoutUnits"] == 425
    assert result["averageNetMarkoutPriceUnits"] == 283
    assert result["coveredFillCount"] == 1
    assert result["pendingFillCount"] == 1
    assert result["unavailableFillCount"] == 0
    assert result["complete"] is False


def test_finish_and_backfill_use_latest_deduplicated_midpoint_markout(tmp_path: Path):
    store = SessionStore(tmp_path / "sessions")
    run = store.prepare_run()
    path = Path(run["artifactPath"]) / "markets" / "TEST" / "telemetry.sqlite3"
    path.parent.mkdir(parents=True)
    telemetry = TelemetryStore(str(path), enabled=True)
    _record_fill(telemetry, key="yes", side="yes", price=4_000, size=150, fee=25, ts=100_000)
    _record_fill(telemetry, key="no", side="no", price=4_500, size=200, fee=30, ts=100_100)
    _record_fill(telemetry, key="missing-fee", side="yes", price=4_000, size=100, fee=None, ts=100_200)
    _record_markout(telemetry, key="yes", side="yes", fill_price=4_000, future_mid=4_300, ts=130_000)
    # The later duplicate is authoritative: +4c/contract rather than +3c.
    _record_markout(telemetry, key="yes", side="yes", fill_price=4_000, future_mid=4_400, ts=130_100)
    _record_markout(telemetry, key="no", side="no", fill_price=4_500, future_mid=5_200, ts=130_100)
    _record_markout(telemetry, key="missing-fee", side="yes", fill_price=4_000, future_mid=4_500, ts=130_200)
    telemetry.flush()

    store.finish_run(run["id"], "stopped", metrics={"totalCents": 999.0})
    saved = store.get_run(run["id"], include_artifact_bytes=False)["metrics"]
    markout = saved["markoutsByHorizon"]["30000"]
    assert saved["totalCents"] == 999.0
    assert markout["grossMarkoutUnits"] == 1_200
    assert markout["feeUnits"] == 55
    assert markout["netMarkoutUnits"] == 1_145
    assert markout["coveredContractsUnits"] == 350
    assert markout["coveredFillCount"] == 2
    assert markout["unavailableFillCount"] == 1
    assert saved["markets"]["TEST"]["markoutsByHorizon"]["30000"] == markout

    first = json.dumps(saved["markoutsByHorizon"], sort_keys=True)
    report = store.backfill_markouts()
    second = store.get_run(run["id"], include_artifact_bytes=False)["metrics"]
    assert report["runsProcessed"] == 1
    assert report["runs"][0]["horizons"]["30000"] == markout
    assert json.dumps(second["markoutsByHorizon"], sort_keys=True) == first
    assert second["totalCents"] == 999.0


def test_markout_counts_change_activity_revision_without_new_fill():
    recorder = RunMetricsAccumulator(1)
    base_client = {
        "marketId": "TEST", "pid": 10, "runtime": {"startedAtMs": 2},
        "fills": {"count": 1}, "pnl": {}, "apiActivity": {"rest": {}}, "orderActivity": {},
    }
    first = recorder.observe({"clients": [{**base_client, "markouts": {}}]})
    first_revision = SessionStore._activity_revision(first)
    aggregate = empty_markout_aggregate(30_000, total_fill_count=1)
    add_observation(aggregate, signed_price_units=100, size_units=100, fee_units=0)
    aggregate = finalize_aggregate(aggregate, total_fill_count=1)
    second = recorder.observe({"clients": [{**base_client, "markouts": {"30000": aggregate}}]})

    assert SessionStore._activity_revision(second) != first_revision
    assert second["markoutsByHorizon"]["30000"]["coveredFillCount"] == 1
    assert second["markets"]["TEST"]["markoutsByHorizon"]["30000"]["netMarkoutUnits"] == 100
