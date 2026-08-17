from clients.models import AccountFill, AccountOrder
from portfolio_analytics import PortfolioAnalyticsStore


def snapshot(at_ms: int, *, cash: int = 100_000, midpoint: int = 10_000, liquidation: int = 9_000):
    return {
        "generatedAtMs": at_ms,
        "warnings": [],
        "summary": {
            "availableCashUnits": cash,
            "midpointPositionValueUnits": midpoint,
            "totalPortfolioValueUnits": cash + midpoint,
            "positionsLiquidationValueUnits": liquidation,
            "apiTier": "advanced",
            "readRateLimit": {"refillRate": 20, "bucketCapacity": 40},
            "writeRateLimit": {"refillRate": 10, "bucketCapacity": 20},
            "positionCount": 1,
        },
        "positions": [{
            "marketId": "TEST-1", "ticker": "TEST-1", "title": "Test market", "side": "yes",
            "contractsUnits": 200, "bidPriceUnits": 5_000, "askPriceUnits": 5_200,
            "midPriceUnits": 5_100, "costBasisUnits": 8_000, "averageCostPriceUnits": 4_000,
            "liquidationValueUnits": 10_000, "unrealizedValueUnits": 10_200, "openOrderCount": 2,
        }],
        "orders": {"items": [
            {"ticker": "TEST-1", "marketId": "TEST-1", "title": "Test market", "side": "yes", "midPriceUnits": 5_100, "marketUrl": None},
            {"ticker": "TEST-1", "marketId": "TEST-1", "title": "Test market", "side": "no", "midPriceUnits": 4_900, "marketUrl": None},
        ]},
    }


def test_persistence_history_fill_math_and_open_order_aggregation(tmp_path):
    path = tmp_path / "portfolio.sqlite3"
    store = PortfolioAnalyticsStore(path)
    first_at = 1_000_000
    orders = [
        AccountOrder("o1", "TEST-1", "yes", status="resting", price_units=4_000,
                     fill_count_units=100, remaining_count_units=100, initial_count_units=200,
                     created_at_ms=first_at - 4_000, updated_at_ms=first_at - 500),
        AccountOrder("o2", "TEST-1", "no", status="resting", price_units=4_500,
                     fill_count_units=100, remaining_count_units=200, initial_count_units=300,
                     created_at_ms=first_at - 3_000, updated_at_ms=first_at - 250),
    ]
    fills = [
        AccountFill("f1", "t1", "o1", "TEST-1", "yes", 100, 4_000, fee_units=100, created_at_ms=first_at - 2_500),
        AccountFill("f2", "t2", "o2", "TEST-1", "no", 100, 4_500, fee_units=50, created_at_ms=first_at - 1_500),
    ]
    store.record_refresh(snapshot(first_at), orders, fills, orders)
    store.record_refresh(snapshot(first_at + 3_600_000, cash=120_000, midpoint=12_000, liquidation=11_000), orders, fills, orders)

    restarted = PortfolioAnalyticsStore(path)
    summary = restarted.summary()
    assert summary["coverage"]["partial"] is True
    assert summary["history"]["availableCash"]["changeUnits"] == 20_000
    assert summary["history"]["availableCash"]["changeBps"] == 2_000

    positions = restarted.positions(active_run={"startedAt": first_at - 3_500})
    assert positions["items"][0]["totalFillCount"] == 2
    assert positions["items"][0]["totalOrderCount"] == 2
    assert positions["items"][0]["runningInCurrentSession"] is True

    fill_page = restarted.fills("TEST-1", limit=1)
    assert len(fill_page["items"]) == 1
    assert fill_page["nextCursor"]
    second_page = restarted.fills("TEST-1", limit=1, cursor=fill_page["nextCursor"])
    yes_fill = second_page["items"][0]
    assert yes_fill["costInPositionUnits"] == 4_100
    assert yes_fill["liquidationValueUnits"] == 5_000
    assert yes_fill["liquidationPnlUnits"] == 900
    assert yes_fill["marketPnlUnits"] == 1_000
    assert yes_fill["timeToFillMs"] == 1_500

    order_data = restarted.orders(
        active_run={"startedAt": first_at - 3_500},
        placement_attempts={"TEST-1": 17, "CLOSED-1": 4},
    )
    assert order_data["summary"]["totalOpenOrders"] == 2
    assert order_data["summary"]["ordersAttempted"] == 21
    assert order_data["summary"]["averageTimeBetweenOrdersMs"] == 1_000
    assert order_data["summary"]["averageFillTimeMs"] == 1_500
    assert order_data["summary"]["totalMarketValueUnits"] == 14_900
    assert len(order_data["items"]) == 1
    assert order_data["items"][0]["runningInCurrentSession"] is True
    assert order_data["items"][0]["ordersAttempted"] == 17
    assert order_data["items"][0]["totalFillCount"] == 2
    assert order_data["items"][0]["midPriceUnits"] is None


def test_missing_marks_stay_unavailable(tmp_path):
    store = PortfolioAnalyticsStore(tmp_path / "portfolio.sqlite3")
    payload = snapshot(1_000_000)
    payload["summary"]["totalPortfolioValueUnits"] = None
    payload["summary"]["positionsLiquidationValueUnits"] = None
    payload["positions"][0]["bidPriceUnits"] = None
    payload["positions"][0]["midPriceUnits"] = None
    store.record_refresh(payload, [], [], [])
    result = store.summary()
    assert result["history"]["totalPortfolioValue"]["currentUnits"] is None
    assert result["history"]["positionsLiquidationValue"]["changeUnits"] is None


def test_full_window_with_zero_baseline_has_no_percent_change(tmp_path):
    store = PortfolioAnalyticsStore(tmp_path / "portfolio.sqlite3")
    first_at = 1_000_000
    store.record_refresh(snapshot(first_at, cash=0, midpoint=0, liquidation=0), [], [], [])
    store.record_refresh(snapshot(first_at + 25 * 60 * 60 * 1000, cash=10_000), [], [], [])
    result = store.summary()
    cash = result["history"]["availableCash"]
    assert result["coverage"]["partial"] is False
    assert cash["changeUnits"] == 10_000
    assert cash["changeBps"] is None
