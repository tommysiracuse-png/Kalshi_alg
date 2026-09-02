"""The flatten exit never sells into a vacuum (top_of_book_bot.watchdog_exit_quote_usable)."""

from top_of_book_bot import WATCHDOG_EXIT_MAX_SPREAD_UNITS, watchdog_exit_quote_usable


def test_exit_requires_a_two_sided_book_no_wider_than_the_limit():
    assert WATCHDOG_EXIT_MAX_SPREAD_UNITS == 2_000
    # 2026-09-02 KXAAAGASDOH-26SEP03-3.885 at 17:35: YES bid 3c, NO bid 24c -> 73c wide.
    assert watchdog_exit_quote_usable(300, 2_400) == (False, "spread_too_wide", 7_300)
    # A normal book: YES 64 / NO 29 -> 7c wide.
    assert watchdog_exit_quote_usable(6_400, 2_900) == (True, "ok", 700)
    # Exactly at the limit passes; one tick beyond does not.
    assert watchdog_exit_quote_usable(5_000, 3_000)[0] is True
    assert watchdog_exit_quote_usable(5_000, 2_900) == (False, "spread_too_wide", 2_100)
    # One side missing: nothing to sell into.
    assert watchdog_exit_quote_usable(None, 2_400) == (False, "one_sided_book", None)
    assert watchdog_exit_quote_usable(6_400, 0) == (False, "one_sided_book", None)
    # A crossed/locked book is usable: the IOC fills at or better than the far side.
    assert watchdog_exit_quote_usable(7_000, 3_100) == (True, "ok", -100)
    # The limit is a parameter for tests and future tuning.
    assert watchdog_exit_quote_usable(300, 2_400, max_spread_units=8_000)[0] is True
