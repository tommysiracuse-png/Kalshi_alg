"""Registry for constructing the three venue-neutral runtime components."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

from clients.base_client import BaseClient
from clients.factory import build_client, build_client_config
from portfolio.base_portfolio import BasePortfolio
from portfolio.portfolio_monitor import KalshiPortfolio, PortfolioMonitorConfig
from screeners.base_screener import BaseScreener
from screeners.screener import KalshiScreener


@dataclass(frozen=True)
class VenueRuntime:
    venue: str
    client: BaseClient
    screener: BaseScreener
    portfolio: BasePortfolio
    client_config: Any


def build_runtime(
    venue: str,
    *,
    client_config: Any = None,
    client_values: Optional[Mapping[str, object]] = None,
    screener_settings: Optional[Mapping[str, object]] = None,
    screener_kwargs: Optional[Mapping[str, object]] = None,
    portfolio_config: Optional[PortfolioMonitorConfig] = None,
) -> VenueRuntime:
    normalized = str(venue or "kalshi").strip().lower()
    if normalized != "kalshi":
        raise ValueError(f"unsupported venue: {normalized}")
    config = client_config or build_client_config(normalized, client_values or {})
    client = build_client(normalized, config)
    kwargs = dict(screener_kwargs or {})
    screener = KalshiScreener(
        client=client,
        settings=dict(screener_settings or {}),
        output_path=Path(kwargs.pop("output_path", "screener.csv")),
        default_yes_budget_cents=int(kwargs.pop("default_yes_budget_cents", 100)),
        default_no_budget_cents=int(kwargs.pop("default_no_budget_cents", 100)),
        max_bots=int(kwargs.pop("max_bots", 40)),
        minimum_carryover_value_cents=float(kwargs.pop("minimum_carryover_value_cents", 20.0)),
        **kwargs,
    )
    portfolio = KalshiPortfolio(client, portfolio_config or PortfolioMonitorConfig())
    return VenueRuntime(normalized, client, screener, portfolio, config)

