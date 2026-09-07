"""Venue-neutral portfolio monitor contracts and typed snapshots."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Any, Mapping, Optional

from clients.base_client import BaseClient
from clients.models import AccountLimits, AccountOrder, AccountPosition


@dataclass(frozen=True)
class PortfolioSnapshot:
    venue: str
    generated_at_ms: int
    available_cash_units: Optional[int]
    portfolio_value_units: Optional[int]
    midpoint_position_value_units: Optional[int]
    liquidation_value_units: Optional[int]
    positions: tuple[AccountPosition, ...] = ()
    orders: tuple[AccountOrder, ...] = ()
    limits: Optional[AccountLimits] = None
    balance_complete: bool = False
    portfolio_value_complete: bool = False
    liquidation_value_complete: bool = False
    stale: bool = False
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        """JSON-friendly typed serialization used by status and APIs."""

        result = asdict(self)
        result["positions"] = [asdict(item) for item in self.positions]
        result["orders"] = [asdict(item) for item in self.orders]
        if self.limits is not None:
            result["limits"] = asdict(self.limits)
        result["warnings"] = list(self.warnings)
        return result


class BasePortfolio(ABC):
    """Common scheduling and snapshot surface for a venue portfolio."""

    venue = "unknown"

    def __init__(self, client: BaseClient) -> None:
        self.client = client
        self._typed_latest: Optional[PortfolioSnapshot] = None

    @abstractmethod
    def due(self, now_ms: Optional[int] = None) -> bool:
        ...

    @abstractmethod
    async def refresh(self) -> bool:
        ...

    def latest_snapshot(self) -> Optional[PortfolioSnapshot]:
        return self._typed_latest

    @abstractmethod
    def status_snapshot(self) -> Mapping[str, object]:
        ...

    @property
    def client_activity(self) -> Mapping[str, object]:
        try:
            return self.client.activity_snapshot()
        except Exception:
            return {}
