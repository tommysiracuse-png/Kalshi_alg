"""Deterministic synthetic evaluator matching the replay.driver contract.

Used by the optimizer's own tests (including the cross-process Pool test via
OPTIMIZER_EVALUATOR=optimizer._synthetic:evaluate_candidate). Noise-free
quadratic objective in two bot parameters; everything else is ignored.
"""

from __future__ import annotations

import zlib
from typing import Any, Dict

INFLUENTIAL_FIELD = "minimum_expected_edge_cents_to_quote"  # bot default: 2
SECONDARY_FIELD = "default_fee_factor_for_maker_quotes"  # bot default: 0.45
INFLUENTIAL_DEFAULT = 2.0
SECONDARY_DEFAULT = 0.45
INFLUENTIAL_OPT = 6.0
SECONDARY_OPT = 1.2
FILLS_PER_MARKET = 50


def evaluate_candidate(
    history_db_path: str,
    ticker: str,
    bot_overrides: Dict[str, Any],
    start_ms: int,
    end_ms: int,
    seed: int = 0,
    fill_share_fraction: float = 0.5,
    assumed_top_depth_contracts: int = 100,
) -> Dict[str, Any]:
    a = float(bot_overrides.get(INFLUENTIAL_FIELD, INFLUENTIAL_DEFAULT))
    b = float(bot_overrides.get(SECONDARY_FIELD, SECONDARY_DEFAULT))
    score = 100.0 - 3.0 * (a - INFLUENTIAL_OPT) ** 2 - 20.0 * (b - SECONDARY_OPT) ** 2
    score *= float(fill_share_fraction) / 0.5
    # Small deterministic per-ticker term so markets differ without noise.
    score += (zlib.crc32(ticker.encode("utf-8")) % 97) / 97.0
    horizon = {1000: score / 4.0, 5000: score / 4.0, 30000: score / 4.0, 120000: score / 4.0}
    return {
        "ticker": ticker,
        "error": None,
        "decisions": 20,
        "orders_placed": 12,
        "fills": FILLS_PER_MARKET,
        "contracts_units": 60.0,
        "fees_units": 1.0,
        "net_markout_units_by_horizon": horizon,
        "total_net_units": score,
        "max_drawdown_units": 5.0,
        "settlement_net_units": score,
    }
