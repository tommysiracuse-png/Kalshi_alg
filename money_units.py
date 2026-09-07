"""Shared fixed-point money units used by venue adaptors and the UI."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


# One dollar is represented by 10,000 units throughout the account and
# portfolio layers.  This is also the existing Kalshi price scale.
MONEY_SCALE = 10_000


def decimal_money_units(value: Any) -> int:
    """Convert a decimal dollar amount to shared fixed-point units."""

    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid monetary value: {value!r}") from exc
    if not amount.is_finite():
        raise ValueError(f"invalid monetary value: {value!r}")
    return int((amount * MONEY_SCALE).to_integral_value(rounding=ROUND_HALF_UP))


def nonnegative_base_money_units(value: Any, *, base_scale: int = 1_000_000) -> int:
    """Convert non-negative token base units to the shared money scale."""

    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid monetary value: {value!r}") from exc
    if not amount.is_finite() or amount < 0:
        raise ValueError(f"invalid non-negative monetary value: {value!r}")
    scaled = amount * MONEY_SCALE / Decimal(base_scale)
    return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))
