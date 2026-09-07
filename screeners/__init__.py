"""Market screening interfaces and venue adapters."""

from .base_screener import BaseScreener, ScreeningResult

__all__ = ["BaseScreener", "ScreeningResult", "KalshiScreener", "Screener"]


def __getattr__(name: str):
    """Load the pandas-backed screener only when it is actually requested.

    Configuration and UI API processes import ``screeners.kalshi_screener_config``
    but do not run a screener.  Keeping these exports lazy avoids making pandas
    a dependency of those lightweight processes.
    """

    if name in {"KalshiScreener", "Screener"}:
        from .screener import KalshiScreener, Screener

        return KalshiScreener if name == "KalshiScreener" else Screener
    raise AttributeError(name)
