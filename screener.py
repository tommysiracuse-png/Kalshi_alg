"""Venue-neutral market screening and desired-fleet reconciliation."""

from __future__ import annotations

import asyncio
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Mapping, Optional, Sequence, Set

import pandas as pd

from clients.base_client import BaseClient
from clients.models import Market, MarketQuery
from fleet_models import ScreenerEvent, ScreenerPick, ScreenerUpdate
from kalshi_screener import build_export_dataframe, screen_markets


def _iso_from_ms(value: Optional[int]) -> str:
    if value is None:
        return ""
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def market_to_screen_payload(market: Market) -> Dict[str, object]:
    def dollars(value: Optional[int]) -> Optional[str]:
        return None if value is None else f"{value / 10_000:.4f}"

    def count(value: Optional[int]) -> Optional[str]:
        return None if value is None else f"{value / 100:.2f}"

    return {
        "ticker": market.market_id,
        "title": market.title,
        "status": market.status,
        "series_ticker": market.series_id,
        "event_ticker": market.event_id,
        "close_time": _iso_from_ms(market.close_time_ms),
        "expected_expiration_time": _iso_from_ms(market.expected_expiration_time_ms),
        "expiration_time": _iso_from_ms(market.expiration_time_ms),
        "tick_size": max(1, market.legacy_tick_size_units // 100),
        "price_ranges": [
            {
                "start": f"{item.start_units / 10_000:.4f}",
                "end": f"{item.end_units / 10_000:.4f}",
                "step": f"{item.step_units / 10_000:.4f}",
            }
            for item in market.price_ranges
        ],
        "yes_bid_dollars": dollars(market.yes_bid_units),
        "yes_ask_dollars": dollars(market.yes_ask_units),
        "no_bid_dollars": dollars(market.no_bid_units),
        "no_ask_dollars": dollars(market.no_ask_units),
        "last_price_dollars": dollars(market.last_price_units),
        "yes_bid_size_fp": count(market.yes_bid_size_units),
        "yes_ask_size_fp": count(market.yes_ask_size_units),
        "no_bid_size_fp": count(market.no_bid_size_units),
        "no_ask_size_fp": count(market.no_ask_size_units),
        "volume_24h_fp": count(market.volume_24h_units),
        "open_interest_fp": count(market.open_interest_units),
    }


class _BaseClientMarketSource:
    """Compatibility facade for the existing pure screening algorithm."""

    def __init__(self, client: BaseClient) -> None:
        self.client = client

    def list_markets(self, *, status: str, limit: int, max_total: int, mve_filter: Optional[str]):
        query = MarketQuery(
            status=status,
            page_size=limit,
            max_results=max_total,
            venue_filters={"mve_filter": mve_filter} if mve_filter else {},
        )
        return [market_to_screen_payload(market) for market in self.client.list_markets(query)]


class Screener:
    def __init__(
        self,
        *,
        client: BaseClient,
        settings: Mapping[str, object],
        output_path: Path,
        default_yes_budget_cents: int,
        default_no_budget_cents: int,
        max_bots: int,
        minimum_carryover_value_cents: float,
        yes_budget_column: str = "",
        no_budget_column: str = "",
        disabled_market_ids: Optional[Callable[[], Set[str]]] = None,
    ) -> None:
        self.client = client
        self.settings = dict(settings)
        self.output_path = output_path
        self.default_yes_budget_cents = int(default_yes_budget_cents)
        self.default_no_budget_cents = int(default_no_budget_cents)
        self.max_bots = int(max_bots)
        self.minimum_carryover_value_cents = float(minimum_carryover_value_cents)
        self.yes_budget_column = str(yes_budget_column or "")
        self.no_budget_column = str(no_budget_column or "")
        self.disabled_market_ids = disabled_market_ids or (lambda: set())
        self.events: "asyncio.Queue[ScreenerEvent]" = asyncio.Queue()
        self._latest_update: Optional[ScreenerUpdate] = None
        self._generation = 0

    def get_latest_picks(self) -> tuple[ScreenerPick, ...]:
        return self._latest_update.picks if self._latest_update is not None else ()

    def _atomic_export(self, frame: pd.DataFrame) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w", delete=False, dir=self.output_path.parent, suffix=".csv", encoding="utf-8", newline=""
        ) as handle:
            temporary_path = Path(handle.name)
            frame.to_csv(handle, index=False)
        os.replace(temporary_path, self.output_path)

    def _screen_picks(self) -> tuple[list[ScreenerPick], pd.DataFrame]:
        screen_frame = screen_markets(_BaseClientMarketSource(self.client), self.settings)
        export_frame = build_export_dataframe(screen_frame, self.settings)
        disabled = set(self.disabled_market_ids())
        picks: list[ScreenerPick] = []
        for row in export_frame.to_dict(orient="records"):
            market_id = str(row.get("Ticker") or "").strip()
            if not market_id or market_id in disabled:
                continue
            yes_budget = (
                int(float(row[self.yes_budget_column]))
                if self.yes_budget_column and row.get(self.yes_budget_column) not in (None, "")
                else self.default_yes_budget_cents
            )
            no_budget = (
                int(float(row[self.no_budget_column]))
                if self.no_budget_column and row.get(self.no_budget_column) not in (None, "")
                else self.default_no_budget_cents
            )
            if yes_budget < 0 or no_budget < 0:
                raise ValueError(f"Ticker {market_id} has a negative budget")
            picks.append(
                ScreenerPick(
                    market_id=market_id,
                    title=str(row.get("SearchText") or ""),
                    yes_budget_cents=yes_budget,
                    no_budget_cents=no_budget,
                    ranking=row,
                    selection_reason="screen",
                )
            )
            if self.max_bots > 0 and len(picks) >= self.max_bots:
                break
        return picks, export_frame

    def _with_inventory_carryover(
        self,
        picks: Sequence[ScreenerPick],
        current_picks: Mapping[str, ScreenerPick],
    ) -> tuple[list[ScreenerPick], list[str], list[str]]:
        desired = {pick.market_id: pick for pick in picks}
        carried: list[str] = []
        unknown: list[str] = []
        for market_id, previous in current_picks.items():
            if market_id in desired or market_id in self.disabled_market_ids():
                continue
            try:
                positions = self.client.get_positions(market_id)
                position_units = positions[0].position_units if positions else 0
                if position_units == 0:
                    continue
                quote = self.client.get_market_quote(market_id)
                bid_units = quote.yes_bid_units if position_units > 0 else quote.no_bid_units
                if bid_units is None:
                    raise RuntimeError("inventory side has no bid")
                marked_value_cents = abs(position_units) * bid_units / 10_000.0
                if marked_value_cents < self.minimum_carryover_value_cents:
                    continue
                reason = "inventory"
            except Exception:
                unknown.append(market_id)
                reason = "inventory_unknown"
            desired[market_id] = ScreenerPick(
                market_id=previous.market_id,
                title=previous.title or "Inventory carryover",
                yes_budget_cents=previous.yes_budget_cents,
                no_budget_cents=previous.no_budget_cents,
                ranking=previous.ranking,
                selection_reason=reason,
            )
            carried.append(market_id)
        ordered = list(picks) + [desired[key] for key in carried if key not in {pick.market_id for pick in picks}]
        return ordered, carried, unknown

    def _refresh_sync(self, current_picks: Mapping[str, ScreenerPick], reason: str) -> ScreenerUpdate:
        screened, export_frame = self._screen_picks()
        picks, carried, unknown = self._with_inventory_carryover(screened, current_picks)
        desired = {pick.market_id: pick for pick in picks}
        current_ids = set(current_picks)
        desired_ids = set(desired)
        common = current_ids & desired_ids
        changed = sorted(
            market_id
            for market_id in common
            if current_picks[market_id].runtime_key() != desired[market_id].runtime_key()
        )
        kept = sorted(common - set(changed))
        self._generation += 1
        update = ScreenerUpdate(
            generation_id=self._generation,
            generated_at_ms=int(time.time() * 1000),
            reason=reason,
            picks=tuple(picks),
            added=tuple(sorted(desired_ids - current_ids)),
            kept=tuple(kept),
            changed=tuple(changed),
            removed=tuple(sorted(current_ids - desired_ids)),
            inventory_carried=tuple(sorted(carried)),
            inventory_unknown=tuple(sorted(unknown)),
        )
        self._atomic_export(export_frame)
        self._latest_update = update
        return update

    async def refresh(
        self,
        current_picks: Mapping[str, ScreenerPick],
        *,
        reason: str = "scheduled",
    ) -> Optional[ScreenerUpdate]:
        try:
            update = await asyncio.to_thread(self._refresh_sync, dict(current_picks), reason)
            await self.events.put(ScreenerEvent(True, reason, update.generated_at_ms, update=update))
            return update
        except Exception as exc:
            await self.events.put(
                ScreenerEvent(False, reason, int(time.time() * 1000), error=str(exc))
            )
            return None
