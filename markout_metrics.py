"""Fixed-point post-fill markout aggregation shared by runtime and history readers."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, MutableMapping


MARKOUT_HORIZONS_MS = (1_000, 5_000, 30_000, 120_000)
COUNT_SCALE = 100
PRICE_SCALE = 10_000


def empty_markout_aggregate(horizon_ms: int, *, total_fill_count: int = 0) -> dict[str, Any]:
    return {
        "horizonMs": int(horizon_ms),
        "grossMarkoutUnits": 0,
        "feeUnits": 0,
        "netMarkoutUnits": 0,
        "averageNetMarkoutPriceUnits": None,
        "coveredFillCount": 0,
        "coveredContractsUnits": 0,
        "totalFillCount": int(total_fill_count),
        "pendingFillCount": 0,
        "unavailableFillCount": int(total_fill_count),
        "complete": total_fill_count == 0,
    }


def monetary_markout_units(signed_price_units: int, size_units: int) -> int:
    """Convert price-units × fixed-point contracts into 1/10,000-dollar units."""
    return int(round(int(signed_price_units) * int(size_units) / COUNT_SCALE))


def add_observation(
    aggregate: MutableMapping[str, Any],
    *,
    signed_price_units: int,
    size_units: int,
    fee_units: int,
) -> None:
    gross = monetary_markout_units(signed_price_units, size_units)
    aggregate["grossMarkoutUnits"] = int(aggregate.get("grossMarkoutUnits") or 0) + gross
    aggregate["feeUnits"] = int(aggregate.get("feeUnits") or 0) + int(fee_units)
    aggregate["netMarkoutUnits"] = int(aggregate.get("netMarkoutUnits") or 0) + gross - int(fee_units)
    aggregate["coveredFillCount"] = int(aggregate.get("coveredFillCount") or 0) + 1
    aggregate["coveredContractsUnits"] = int(aggregate.get("coveredContractsUnits") or 0) + int(size_units)


def finalize_aggregate(
    aggregate: MutableMapping[str, Any],
    *,
    total_fill_count: int,
    pending_fill_count: int = 0,
) -> dict[str, Any]:
    covered = int(aggregate.get("coveredFillCount") or 0)
    contracts = int(aggregate.get("coveredContractsUnits") or 0)
    pending = max(0, min(int(pending_fill_count), int(total_fill_count) - covered))
    unavailable = max(0, int(total_fill_count) - covered - pending)
    net = int(aggregate.get("netMarkoutUnits") or 0)
    aggregate["totalFillCount"] = int(total_fill_count)
    aggregate["pendingFillCount"] = pending
    aggregate["unavailableFillCount"] = unavailable
    aggregate["complete"] = unavailable == 0 and pending == 0
    aggregate["averageNetMarkoutPriceUnits"] = (
        int(round(net * COUNT_SCALE / contracts)) if contracts else None
    )
    return dict(aggregate)


def combine_markout_maps(maps: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    combined: dict[str, dict[str, Any]] = {}
    for values in maps:
        for raw_horizon, raw in values.items():
            if not isinstance(raw, Mapping):
                continue
            horizon = int(raw.get("horizonMs") or raw_horizon)
            key = str(horizon)
            target = combined.setdefault(key, empty_markout_aggregate(horizon))
            for field in (
                "grossMarkoutUnits", "feeUnits", "netMarkoutUnits", "coveredFillCount",
                "coveredContractsUnits", "totalFillCount", "pendingFillCount", "unavailableFillCount",
            ):
                target[field] = int(target.get(field) or 0) + int(raw.get(field) or 0)
    for value in combined.values():
        contracts = int(value["coveredContractsUnits"])
        value["averageNetMarkoutPriceUnits"] = (
            int(round(int(value["netMarkoutUnits"]) * COUNT_SCALE / contracts)) if contracts else None
        )
        value["complete"] = not int(value["pendingFillCount"]) and not int(value["unavailableFillCount"])
    return {key: combined[key] for key in sorted(combined, key=int)}
