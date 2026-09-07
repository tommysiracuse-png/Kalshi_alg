"""Market screening interfaces and venue adapters."""

from .base_screener import BaseScreener, ScreeningResult

__all__ = ["BaseScreener", "ScreeningResult", "KalshiScreener", "PolymarketScreener", "Screener"]


def __getattr__(name: str):
    """Load the pandas-backed screener only when it is actually requested.

    Configuration and UI API processes import ``screeners.kalshi_screener_config``
    but do not run a screener.  Keeping these exports lazy avoids making pandas
    a dependency of those lightweight processes.
    """

    if name in {"KalshiScreener", "PolymarketScreener", "Screener"}:
        from .screener import KalshiScreener, PolymarketScreener, Screener

        if name == "KalshiScreener":
            return KalshiScreener
        if name == "PolymarketScreener":
            return PolymarketScreener
        return Screener
    raise AttributeError(name)
