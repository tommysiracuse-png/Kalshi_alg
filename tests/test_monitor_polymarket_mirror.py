from __future__ import annotations

from tools.monitor_polymarket_mirror import _BookCoverageEstimator, _format_duration_seconds


def test_book_coverage_eta_uses_mirror_elapsed_time_for_first_sample():
    estimator = _BookCoverageEstimator()

    estimate = estimator.estimate(
        {
            "generationId": "generation-1",
            "catalogCount": 100,
            "bookReadyMarkets": 20,
            "running": True,
            "complete": False,
            "catalogDurationMs": 10_000,
        },
        now_ms=10_000,
    )

    assert estimate["status"] == "estimating"
    assert estimate["remainingMarkets"] == 80
    assert estimate["rateMarketsPerSecond"] == 2.0
    assert estimate["etaSeconds"] == 40.0


def test_book_coverage_eta_uses_observed_progress_and_resets_by_generation():
    estimator = _BookCoverageEstimator()
    estimator.estimate(
        {
            "generationId": "generation-1",
            "catalogCount": 100,
            "bookReadyMarkets": 20,
            "running": True,
            "complete": False,
        },
        now_ms=0,
    )
    estimate = estimator.estimate(
        {
            "generationId": "generation-1",
            "catalogCount": 100,
            "bookReadyMarkets": 40,
            "running": True,
            "complete": False,
        },
        now_ms=10_000,
    )

    assert estimate["rateSource"] == "observed_refreshes"
    assert estimate["etaSeconds"] == 30.0

    reset = estimator.estimate(
        {
            "generationId": "generation-2",
            "catalogCount": 10,
            "bookReadyMarkets": 10,
            "running": True,
            "complete": True,
        },
        now_ms=20_000,
    )
    assert reset["status"] == "complete"
    assert reset["etaSeconds"] == 0.0


def test_book_coverage_eta_reports_unknown_when_not_scanning():
    estimate = _BookCoverageEstimator().estimate(
        {
            "generationId": "generation-1",
            "catalogCount": 100,
            "bookReadyMarkets": 20,
            "running": True,
            "complete": True,
        },
        now_ms=10_000,
    )

    assert estimate["status"] == "not_scanning"
    assert estimate["etaSeconds"] is None
    assert _format_duration_seconds(125) == "2m 05s"
