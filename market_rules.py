"""Venue-specific market semantics used by the venue-neutral trading bot."""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Optional

from clients.models import Market


@dataclass(frozen=True)
class MarketRules(ABC):
    """Small capability boundary between strategy logic and a venue."""

    name: str = "binary_complement"

    def uses_direct_asks(self) -> bool:
        """Whether the normalized market carries executable ask levels."""

        return False

    def yes_bid(self, market: Market) -> Optional[int]:
        return market.yes_bid_units

    def no_bid(self, market: Market) -> Optional[int]:
        return market.no_bid_units

    def yes_ask(self, market: Market) -> Optional[int]:
        return market.yes_ask_units

    def no_ask(self, market: Market) -> Optional[int]:
        return market.no_ask_units

    def yes_ask_from_books(self, opposing_bid_units: int, direct_ask_units: Optional[int]) -> int:
        """Return the executable YES ask from normalized book inputs."""
        return (
            int(direct_ask_units)
            if self.uses_direct_asks() and direct_ask_units is not None
            else 10_000 - int(opposing_bid_units)
        )

    def no_ask_from_books(self, opposing_bid_units: int, direct_ask_units: Optional[int]) -> int:
        """Return the executable NO ask from normalized book inputs."""
        return (
            int(direct_ask_units)
            if self.uses_direct_asks() and direct_ask_units is not None
            else 10_000 - int(opposing_bid_units)
        )

    def token_for_side(self, market: Market, side: str) -> str:
        if side == "yes":
            return market.yes_token_id
        if side == "no":
            return market.no_token_id
        raise ValueError(f"unknown binary side: {side!r}")

    def normalize_fill_price(self, side: str, price_units: int) -> int:
        return int(price_units)


class KalshiMarketRules(MarketRules):
    name = "binary_complement"


class PolymarketMarketRules(MarketRules):
    name = "direct_token_books"

    def uses_direct_asks(self) -> bool:
        return True


def rules_for_market(market: Market) -> MarketRules:
    return PolymarketMarketRules(name="direct_token_books") if market.venue == "polymarket" else KalshiMarketRules(name="binary_complement")
