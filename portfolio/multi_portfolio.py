"""Strict aggregation helpers for per-venue portfolio status."""
from __future__ import annotations

from typing import Any, Mapping


def aggregate_portfolio_status(snapshots: Mapping[str, Mapping[str, Any]], enabled: tuple[str, ...] | list[str]) -> dict[str, Any]:
    names = tuple(str(name).lower() for name in enabled)
    per_venue = {name: dict(snapshots.get(name) or {}) for name in names}
    missing = [name for name in names if not per_venue[name].get("available", False)]
    summary: dict[str, Any] = {}
    for field in ("availableCashUnits", "midpointPositionValueUnits", "totalPortfolioValueUnits", "positionsLiquidationValueUnits"):
        values = []
        complete = not missing
        for name in names:
            raw = (per_venue[name].get("summary") or {}).get(field)
            if raw is None:
                complete = False
            else:
                values.append(int(raw))
        summary[field] = sum(values) if complete else None
        summary[field.replace("Units", "Complete")] = complete
    summary["balanceComplete"] = bool(summary.get("availableCashComplete"))
    summary["portfolioValueComplete"] = bool(summary.get("totalPortfolioValueComplete"))
    summary["liquidationValueComplete"] = bool(summary.get("positionsLiquidationValueComplete"))
    return {"perVenue": per_venue, "summary": summary, "includedVenues": list(names), "missingVenues": missing, "displayCurrency": "USD-equivalent"}
