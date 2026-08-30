"""API and capital admission gates for the sharded fleet."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Literal, Mapping, Optional

from fleet_models import FleetCapacity, QuoteSide


def calculate_fleet_capacity(
    *,
    api_tier: str,
    read_refill_rate: int,
    write_refill_rate: int,
    requested_markets: int,
    cash_available_units: int,
    cash_committed_units: int = 0,
    freshness_seconds: float = 20.0,
    write_utilization_limit: float = 0.85,
    cash_reserve_fraction: float = 0.20,
    normal_request_cost: int = 10,
) -> FleetCapacity:
    if requested_markets < 0:
        raise ValueError("requested_markets must be non-negative")
    if freshness_seconds <= 0 or normal_request_cost <= 0:
        raise ValueError("freshness_seconds and normal_request_cost must be positive")
    if not 0 < write_utilization_limit < 1 or not 0 <= cash_reserve_fraction < 1:
        raise ValueError("utilization and reserve fractions are invalid")

    normal_sides = math.floor(
        max(0, write_refill_rate) * write_utilization_limit * freshness_seconds / normal_request_cost
    )
    available = max(0, int(cash_available_units))
    allocatable = math.floor(available * (1.0 - cash_reserve_fraction))
    reserved = available - allocatable
    committed = max(0, int(cash_committed_units))
    api_known = bool(str(api_tier).strip()) and read_refill_rate > 0 and write_refill_rate > 0
    capacity_ok = normal_sides >= requested_markets
    cash_consistent = committed <= allocatable
    gate_open = api_known and capacity_ok and cash_consistent
    errors = []
    if not api_known:
        errors.append("API tier or token limits unavailable")
    if not capacity_ok:
        errors.append(f"write capacity admits {normal_sides} quote sides for {requested_markets} markets")
    if not cash_consistent:
        errors.append("committed capital exceeds allocatable cash")
    admitted_markets = requested_markets if gate_open else 0
    admitted_sides = min(normal_sides, requested_markets * 2) if gate_open else 0
    return FleetCapacity(
        api_tier=str(api_tier),
        read_refill_rate=max(0, int(read_refill_rate)),
        write_refill_rate=max(0, int(write_refill_rate)),
        normal_quote_side_capacity=normal_sides,
        requested_markets=requested_markets,
        admitted_markets=admitted_markets,
        admitted_quote_sides=admitted_sides,
        cash_available_units=available,
        cash_allocatable_units=allocatable,
        cash_committed_units=committed,
        reserved_cash_units=reserved,
        freshness_seconds=float(freshness_seconds),
        gate_open=gate_open,
        reduction_only=not gate_open,
        error="; ".join(errors),
    )


@dataclass(frozen=True)
class AllocationRequest:
    ticker: str
    series_id: str
    rank: int
    preferred_side: QuoteSide
    first_side_notional_units: int
    second_side_notional_units: int = 0
    reducing_sides: tuple[QuoteSide, ...] = ()


@dataclass(frozen=True)
class AllocationResult:
    gate_open: bool
    sides_by_ticker: Mapping[str, tuple[QuoteSide, ...]]
    committed_units: int
    allocatable_units: int
    reserved_units: int
    error: str = ""


class CapitalAllocator:
    def __init__(self, *, cash_reserve_fraction: float = 0.20, series_exposure_fraction: float = 0.10) -> None:
        if not 0 <= cash_reserve_fraction < 1:
            raise ValueError("cash_reserve_fraction must be in [0, 1)")
        if not 0 < series_exposure_fraction <= 1:
            raise ValueError("series_exposure_fraction must be in (0, 1]")
        self.cash_reserve_fraction = cash_reserve_fraction
        self.series_exposure_fraction = series_exposure_fraction

    def allocate(
        self,
        requests: Iterable[AllocationRequest],
        *,
        available_cash_units: int,
        existing_committed_units: int = 0,
        existing_series_units: Optional[Mapping[str, int]] = None,
        quote_side_capacity: Optional[int] = None,
    ) -> AllocationResult:
        ordered = sorted(requests, key=lambda item: (item.rank, item.ticker))
        available = max(0, int(available_cash_units))
        allocatable = math.floor(available * (1.0 - self.cash_reserve_fraction))
        reserved = available - allocatable
        committed = max(0, int(existing_committed_units))
        series_used = {key: max(0, int(value)) for key, value in (existing_series_units or {}).items()}
        series_limit = math.floor(allocatable * self.series_exposure_fraction)
        side_capacity = quote_side_capacity if quote_side_capacity is not None else len(ordered) * 2

        if side_capacity < len(ordered):
            return AllocationResult(False, {}, committed, allocatable, reserved, "API capacity cannot allocate one side per market")

        result: dict[str, list[QuoteSide]] = {item.ticker: [] for item in ordered}
        # Reducing actions consume an admission slot but never consume additional capital.
        for item in ordered:
            if item.reducing_sides:
                side = item.reducing_sides[0]
                result[item.ticker].append(side)
                side_capacity -= 1

        # Every remaining market must receive its first side or the entire admission fails.
        for item in ordered:
            if result[item.ticker]:
                continue
            notional = max(0, item.first_side_notional_units)
            series_total = series_used.get(item.series_id, 0)
            if committed + notional > allocatable:
                return AllocationResult(False, {}, committed, allocatable, reserved, "capital cannot allocate one side per market")
            if series_total + notional > series_limit:
                return AllocationResult(False, {}, committed, allocatable, reserved, f"series limit prevents admission for {item.ticker}")
            result[item.ticker].append(item.preferred_side)
            committed += notional
            series_used[item.series_id] = series_total + notional
            side_capacity -= 1

        # Second sides are opportunistic and retain screener rank order.
        for item in ordered:
            if side_capacity <= 0:
                break
            if item.second_side_notional_units <= 0:
                continue
            other: Literal["yes", "no"] = "no" if item.preferred_side == "yes" else "yes"
            if other in result[item.ticker]:
                continue
            notional = item.second_side_notional_units
            series_total = series_used.get(item.series_id, 0)
            if committed + notional > allocatable or series_total + notional > series_limit:
                continue
            result[item.ticker].append(other)
            committed += notional
            series_used[item.series_id] = series_total + notional
            side_capacity -= 1

        frozen = {ticker: tuple(sides) for ticker, sides in result.items()}
        return AllocationResult(True, frozen, committed, allocatable, reserved)
