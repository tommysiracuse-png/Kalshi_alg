"""Account-wide portfolio monitoring built only on the venue-neutral client."""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional

from clients.base_client import BaseClient
from clients.models import (
    AccountFill,
    AccountFillQuery,
    AccountLimits,
    AccountOrder,
    AccountOrderQuery,
    AccountPosition,
    AccountPositionQuery,
    Market,
    MarketQuery,
)
from portfolio_analytics import PortfolioAnalyticsStore


PRICE_SCALE = 10_000
COUNT_SCALE = 100


def _now_ms() -> int:
    return int(time.time() * 1000)


def _position_value_units(quantity_units: int, price_units: int) -> int:
    return int(round(quantity_units * price_units / COUNT_SCALE))


def _price_for_side(market: Optional[Market], side: Optional[str], field: str) -> Optional[int]:
    if market is None or side not in {"yes", "no"}:
        return None
    if field == "bid":
        direct = market.yes_bid_units if side == "yes" else market.no_bid_units
        opposite = market.no_ask_units if side == "yes" else market.yes_ask_units
    elif field == "ask":
        direct = market.yes_ask_units if side == "yes" else market.no_ask_units
        opposite = market.no_bid_units if side == "yes" else market.yes_bid_units
    else:
        direct = market.last_price_units
        if direct is not None and side == "no":
            direct = PRICE_SCALE - direct
        opposite = None
    return direct if direct is not None else (PRICE_SCALE - opposite if opposite is not None else None)


def _percent_bps(numerator: Optional[int], denominator: Optional[int]) -> Optional[int]:
    if numerator is None or denominator in (None, 0):
        return None
    return int(round(numerator * 10_000 / abs(denominator)))


@dataclass(frozen=True)
class PortfolioMonitorConfig:
    subaccount_number: int = 0
    refresh_interval_seconds: float = 15.0
    history_window_seconds: float = 86_400.0
    tier_refresh_seconds: float = 3_600.0
    analytics_path: Optional[Path] = None


class PortfolioMonitor:
    def __init__(self, client: BaseClient, config: PortfolioMonitorConfig) -> None:
        self.client = client
        self.config = config
        self._orders: Dict[str, AccountOrder] = {}
        self._fills: Dict[str, AccountFill] = {}
        self._limits: Optional[AccountLimits] = None
        self._limits_updated_at_ms: Optional[int] = None
        self._last_fill_watermark_ms: Optional[int] = None
        self._last_order_watermark_ms: Optional[int] = None
        self._latest: Optional[Dict[str, object]] = None
        self._running = False
        self._started_at_ms: Optional[int] = None
        self._completed_at_ms: Optional[int] = None
        self._last_success_at_ms: Optional[int] = None
        self._last_error: Optional[str] = None
        self._lock = asyncio.Lock()
        self._analytics: Optional[PortfolioAnalyticsStore] = None
        self._analytics_error: Optional[str] = None
        if config.analytics_path is not None:
            try:
                self._analytics = PortfolioAnalyticsStore(config.analytics_path)
            except Exception as exc:
                self._analytics_error = str(exc)

    def due(self, now_ms: Optional[int] = None) -> bool:
        current = _now_ms() if now_ms is None else now_ms
        return (
            not self._running
            and (
                self._completed_at_ms is None
                or current - self._completed_at_ms >= int(self.config.refresh_interval_seconds * 1000)
            )
        )

    async def refresh(self) -> bool:
        if self._lock.locked():
            return False
        async with self._lock:
            started = _now_ms()
            self._running = True
            self._started_at_ms = started
            try:
                result = []
                worker = threading.Thread(
                    target=lambda: result.append(self._safe_refresh_sync(started)),
                    name="portfolio-refresh",
                    daemon=True,
                )
                worker.start()
                while worker.is_alive():
                    await asyncio.sleep(0.05)
                ok, payload = result[0]
                if not ok:
                    self._last_error = str(payload)
                    return False
                snapshot, orders, fills, limits, limits_updated_at, order_watermark, fill_watermark = payload
                self._orders = orders
                self._fills = fills
                self._limits = limits
                self._limits_updated_at_ms = limits_updated_at
                self._last_order_watermark_ms = order_watermark
                self._last_fill_watermark_ms = fill_watermark
                self._latest = snapshot
                self._last_success_at_ms = _now_ms()
                self._last_error = None
                return True
            except Exception as exc:
                self._last_error = str(exc)
                return False
            finally:
                self._completed_at_ms = _now_ms()
                self._running = False

    def _safe_refresh_sync(self, started_at_ms: int):
        """Keep worker exceptions from stranding older asyncio executor implementations."""
        try:
            return True, self._refresh_sync(started_at_ms)
        except Exception as exc:
            return False, exc

    def _load_markets(self, market_ids: Iterable[str]) -> tuple[Dict[str, Market], list[str]]:
        identifiers = sorted({value for value in market_ids if value})
        markets: Dict[str, Market] = {}
        warnings: list[str] = []
        for index in range(0, len(identifiers), 100):
            chunk = identifiers[index:index + 100]
            try:
                rows = self.client.list_markets(
                    MarketQuery(
                        status="",
                        page_size=100,
                        max_results=len(chunk),
                        venue_filters={"tickers": ",".join(chunk)},
                    )
                )
                markets.update({item.market_id: item for item in rows})
            except Exception as exc:
                warnings.append(f"market data unavailable for {len(chunk)} markets: {exc}")
        return markets, warnings

    def _refresh_sync(self, now_ms: int):
        cutoff = now_ms - int(self.config.history_window_seconds * 1000)
        overlap_ms = 60_000
        balance = self.client.get_account_balance()
        positions = self.client.list_account_positions(AccountPositionQuery(nonzero_only=True))
        resting = self.client.list_account_orders(AccountOrderQuery(status="resting"))

        order_start = cutoff if self._last_order_watermark_ms is None else max(cutoff, self._last_order_watermark_ms - overlap_ms)
        fill_start = cutoff if self._last_fill_watermark_ms is None else max(cutoff, self._last_fill_watermark_ms - overlap_ms)
        recent_orders = self.client.list_account_orders(AccountOrderQuery(min_created_at_ms=order_start))
        recent_fills = self.client.list_account_fills(AccountFillQuery(min_created_at_ms=fill_start))

        orders = {
            key: value
            for key, value in self._orders.items()
            if value.created_at_ms is not None and value.created_at_ms >= cutoff
        }
        orders.update({item.order_id: item for item in recent_orders if item.order_id})
        orders.update({item.order_id: item for item in resting if item.order_id})
        resting_order_ids = {item.order_id for item in resting}
        fills = {
            key: value
            for key, value in self._fills.items()
            if (
                value.order_id in resting_order_ids
                or (value.created_at_ms is not None and value.created_at_ms >= cutoff)
            )
        }
        fills.update({item.fill_id: item for item in recent_fills if item.fill_id})

        fills_by_order: Dict[str, list[AccountFill]] = {}
        for fill in fills.values():
            fills_by_order.setdefault(fill.order_id, []).append(fill)
        for order in resting:
            if order.fill_count_units <= 0 or order.order_id in fills_by_order:
                continue
            for fill in self.client.list_account_fills(AccountFillQuery(order_id=order.order_id)):
                if fill.fill_id:
                    fills[fill.fill_id] = fill
                    fills_by_order.setdefault(order.order_id, []).append(fill)

        limits = self._limits
        limits_updated_at = self._limits_updated_at_ms
        tier_due = (
            limits is None
            or self._limits_updated_at_ms is None
            or now_ms - self._limits_updated_at_ms >= int(self.config.tier_refresh_seconds * 1000)
        )
        warnings: list[str] = []
        if tier_due:
            try:
                limits = self.client.get_account_limits()
                limits_updated_at = now_ms
            except Exception as exc:
                warnings.append(f"API tier unavailable: {exc}")

        market_ids = [item.market_id for item in positions]
        market_ids.extend(item.market_id for item in resting)
        markets, market_warnings = self._load_markets(market_ids)
        warnings.extend(market_warnings)
        if self._analytics_error:
            warnings.append(f"portfolio analytics storage unavailable: {self._analytics_error}")
        snapshot = self._build_snapshot(now_ms, balance, positions, resting, orders, fills, markets, limits, warnings)
        if self._analytics is not None:
            try:
                self._analytics.record_refresh(snapshot, orders.values(), fills.values(), resting)
                self._analytics_error = None
            except Exception as exc:
                self._analytics_error = str(exc)
                snapshot["warnings"] = list(
                    dict.fromkeys(
                        list(snapshot.get("warnings") or [])
                        + [f"portfolio analytics storage unavailable: {exc}"]
                    )
                )
        order_times = [item.created_at_ms for item in orders.values() if item.created_at_ms is not None]
        fill_times = [item.created_at_ms for item in fills.values() if item.created_at_ms is not None]
        return (
            snapshot,
            orders,
            fills,
            limits,
            limits_updated_at,
            max(order_times, default=self._last_order_watermark_ms),
            max(fill_times, default=self._last_fill_watermark_ms),
        )

    def _build_snapshot(
        self,
        now_ms: int,
        balance,
        positions: list[AccountPosition],
        resting: list[AccountOrder],
        history_orders: Dict[str, AccountOrder],
        fills: Dict[str, AccountFill],
        markets: Dict[str, Market],
        limits: Optional[AccountLimits],
        warnings: list[str],
    ) -> Dict[str, object]:
        history_cutoff_ms = now_ms - int(self.config.history_window_seconds * 1000)
        open_count_by_market: Dict[str, int] = {}
        for order in resting:
            open_count_by_market[order.market_id] = open_count_by_market.get(order.market_id, 0) + 1

        position_rows = []
        unrealized_values: list[int] = []
        liquidation_values: list[int] = []
        midpoint_values: list[int] = []
        total_open_cost = 0
        for position in positions:
            if position.position_units == 0:
                continue
            side = "yes" if position.position_units > 0 else "no"
            quantity = abs(position.position_units)
            market = markets.get(position.market_id)
            bid = _price_for_side(market, side, "bid")
            ask = _price_for_side(market, side, "ask")
            last = _price_for_side(market, side, "last")
            mid = int(round((bid + ask) / 2)) if bid is not None and ask is not None else None
            cost = abs(position.market_exposure_units) if position.market_exposure_units is not None else None
            liquidation = _position_value_units(quantity, bid) if bid is not None else None
            unrealized = liquidation - cost if liquidation is not None and cost is not None else None
            midpoint_value = _position_value_units(quantity, mid) if mid is not None else None
            market_value = _position_value_units(quantity, last) if last is not None else None
            market_unrealized = market_value - cost if market_value is not None and cost is not None else None
            realized = position.realized_pnl_units
            fees = position.fees_paid_units
            total = realized + unrealized - fees if realized is not None and unrealized is not None and fees is not None else None
            market_total = (
                realized + market_unrealized - fees
                if realized is not None and market_unrealized is not None and fees is not None
                else None
            )
            if unrealized is not None:
                unrealized_values.append(unrealized)
            else:
                warnings.append(f"{position.market_id} has no complete liquidation mark")
            if cost is not None:
                total_open_cost += cost
            if liquidation is not None:
                liquidation_values.append(liquidation)
            if midpoint_value is not None:
                midpoint_values.append(midpoint_value)
            position_rows.append(
                {
                    "marketId": position.market_id,
                    "ticker": position.market_id,
                    "title": market.title if market else "",
                    "marketUrl": market.market_url if market else None,
                    "side": side,
                    "contractsUnits": quantity,
                    "lastPriceUnits": last,
                    "bidPriceUnits": bid,
                    "askPriceUnits": ask,
                    "midPriceUnits": mid,
                    "costBasisUnits": cost,
                    "averageCostPriceUnits": int(round(cost * COUNT_SCALE / quantity)) if cost is not None and quantity else None,
                    "liquidationValueUnits": liquidation,
                    "unrealizedValueUnits": midpoint_value,
                    "realizedPnlUnits": realized,
                    "feesUnits": fees,
                    "unrealizedPnlUnits": unrealized,
                    "unrealizedReturnBps": _percent_bps(unrealized, cost),
                    "totalPnlUnits": total,
                    "totalReturnBps": _percent_bps(total, position.total_traded_units),
                    "marketUnrealizedPnlUnits": market_unrealized,
                    "marketUnrealizedReturnBps": _percent_bps(market_unrealized, cost),
                    "marketTotalPnlUnits": market_total,
                    "marketTotalReturnBps": _percent_bps(market_total, position.total_traded_units),
                    "totalTradedUnits": position.total_traded_units,
                    "openOrderCount": open_count_by_market.get(position.market_id, position.resting_order_count),
                    "updatedAtMs": position.updated_at_ms,
                }
            )

        fills_by_order: Dict[str, list[AccountFill]] = {}
        for fill in fills.values():
            fills_by_order.setdefault(fill.order_id, []).append(fill)
        order_rows = []
        open_market_value = 0
        for order in resting:
            market = markets.get(order.market_id)
            bid = _price_for_side(market, order.side, "bid")
            ask = _price_for_side(market, order.side, "ask")
            last = _price_for_side(market, order.side, "last")
            mid = int(round((bid + ask) / 2)) if bid is not None and ask is not None else None
            order_fills = sorted(
                fills_by_order.get(order.order_id, []), key=lambda item: item.created_at_ms or 0
            )
            first_fill = order_fills[0].created_at_ms if order_fills else None
            last_fill = order_fills[-1].created_at_ms if order_fills else None
            fill_time = (
                max(0, first_fill - order.created_at_ms)
                if first_fill is not None and order.created_at_ms is not None
                else None
            )
            notional = (
                _position_value_units(order.remaining_count_units, order.price_units)
                if order.price_units is not None
                else None
            )
            if notional is not None:
                open_market_value += notional
            average_fill = (
                int(round(order.fill_cost_units * COUNT_SCALE / order.fill_count_units))
                if order.fill_count_units > 0
                else None
            )
            order_rows.append(
                {
                    "orderId": order.order_id,
                    "marketId": order.market_id,
                    "ticker": order.market_id,
                    "title": market.title if market else "",
                    "marketUrl": market.market_url if market else None,
                    "side": order.side,
                    "remainingContractsUnits": order.remaining_count_units,
                    "initialContractsUnits": order.initial_count_units,
                    "filledContractsUnits": order.fill_count_units,
                    "averageFillPriceUnits": average_fill,
                    "orderPriceUnits": order.price_units,
                    "bidPriceUnits": bid,
                    "askPriceUnits": ask,
                    "midPriceUnits": mid,
                    "lastPriceUnits": last,
                    "openMarketValueUnits": notional,
                    "createdAtMs": order.created_at_ms,
                    "lastFillAtMs": last_fill,
                    "firstFillTimeMs": fill_time,
                    "status": order.status,
                }
            )

        latencies = []
        for order_id, order_fills in fills_by_order.items():
            order = history_orders.get(order_id)
            fill_times = [item.created_at_ms for item in order_fills if item.created_at_ms is not None]
            first_fill = min(fill_times) if fill_times else None
            if (
                order is not None
                and order.created_at_ms is not None
                and first_fill is not None
                and first_fill >= history_cutoff_ms
            ):
                latencies.append(max(0, first_fill - order.created_at_ms))
        order_created_times = [
            item.created_at_ms
            for item in history_orders.values()
            if item.created_at_ms is not None and item.created_at_ms >= history_cutoff_ms
        ]
        fill_created_times = [
            item.created_at_ms
            for item in fills.values()
            if item.created_at_ms is not None and item.created_at_ms >= history_cutoff_ms
        ]
        aggregate_unrealized = sum(unrealized_values) if len(unrealized_values) == len(position_rows) else None
        aggregate_liquidation = sum(liquidation_values) if len(liquidation_values) == len(position_rows) else None
        aggregate_midpoint = sum(midpoint_values) if len(midpoint_values) == len(position_rows) else None
        total_portfolio_value = (
            balance.available_cash_units + aggregate_midpoint
            if aggregate_midpoint is not None
            else None
        )
        return {
            "schemaVersion": 1,
            "available": True,
            "stale": False,
            "generatedAtMs": now_ms,
            "lastSuccessAtMs": now_ms,
            "subaccountNumber": self.config.subaccount_number,
            "historyWindowMs": int(self.config.history_window_seconds * 1000),
            "summary": {
                "availableCashUnits": balance.available_cash_units,
                "portfolioValueUnits": balance.portfolio_value_units,
                "midpointPositionValueUnits": aggregate_midpoint,
                "totalPortfolioValueUnits": total_portfolio_value,
                "positionsLiquidationValueUnits": aggregate_liquidation,
                "balanceUpdatedAtMs": balance.updated_at_ms,
                "unrealizedPnlUnits": aggregate_unrealized,
                "unrealizedReturnBps": _percent_bps(aggregate_unrealized, total_open_cost),
                "positionCount": len(position_rows),
                "apiTier": limits.usage_tier if limits else None,
                "readRateLimit": (
                    {"refillRate": limits.read.refill_rate, "bucketCapacity": limits.read.bucket_capacity}
                    if limits else None
                ),
                "writeRateLimit": (
                    {"refillRate": limits.write.refill_rate, "bucketCapacity": limits.write.bucket_capacity}
                    if limits else None
                ),
            },
            "positions": position_rows,
            "orders": {
                "summary": {
                    "openOrderCount": len(order_rows),
                    "lastOrderAtMs": max(order_created_times, default=None),
                    "lastFillAtMs": max(fill_created_times, default=None),
                    "openMarketValueUnits": open_market_value,
                    "averageFirstFillTimeMs": int(round(sum(latencies) / len(latencies))) if latencies else None,
                    "filledOrderSampleSize": len(latencies),
                },
                "items": order_rows,
            },
            "warnings": list(dict.fromkeys(warnings)),
        }

    def status_snapshot(self) -> Dict[str, object]:
        now = _now_ms()
        if self._latest is None:
            snapshot: Dict[str, object] = {
                "schemaVersion": 1,
                "available": False,
                "stale": True,
                "generatedAtMs": None,
                "lastSuccessAtMs": self._last_success_at_ms,
                "subaccountNumber": self.config.subaccount_number,
                "historyWindowMs": int(self.config.history_window_seconds * 1000),
                "summary": {},
                "positions": [],
                "orders": {"summary": {}, "items": []},
                "warnings": [],
            }
        else:
            snapshot = dict(self._latest)
            stale_after = int(max(45.0, self.config.refresh_interval_seconds * 3) * 1000)
            snapshot["stale"] = bool(
                self._last_error
                or self._last_success_at_ms is None
                or now - self._last_success_at_ms > stale_after
            )
        snapshot.update(
            {
                "running": self._running,
                "currentStartedAtMs": self._started_at_ms if self._running else None,
                "currentDurationMs": max(0, now - self._started_at_ms) if self._running and self._started_at_ms else None,
                "lastCompletedAtMs": self._completed_at_ms,
                "lastSuccessAtMs": self._last_success_at_ms,
                "lastError": self._last_error,
                "apiActivity": self.client.activity_snapshot(),
            }
        )
        warnings = list(snapshot.get("warnings") or [])
        if self._last_error:
            warnings.append(f"portfolio refresh failed: {self._last_error}")
        snapshot["warnings"] = list(dict.fromkeys(warnings))
        return snapshot
