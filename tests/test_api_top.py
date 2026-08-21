from api_top.kalshi_api_top import (
    BucketUsageTracker,
    EndpointCosts,
    aggregate_operations,
    api_sources,
    category_name,
    newest_error,
)


def _rest(operation, *, successes, errors, latency, last_at, last_error=None):
    total = successes + errors
    return {
        "total": total,
        "successes": successes,
        "errors": errors,
        "requestsLast60s": total,
        "errorsLast60s": errors,
        "totalLatencyMs": latency,
        "lastActivityAtMs": last_at,
        "lastError": last_error,
        "byOperation": {operation: total},
        "operations": {
            operation: {
                "total": total,
                "successes": successes,
                "errors": errors,
                "requestsLast60s": total,
                "errorsLast60s": errors,
                "totalLatencyMs": latency,
                "averageLatencyMs": latency / total,
                "lastActivityAtMs": last_at,
                "lastError": last_error,
            }
        },
    }


def test_sources_do_not_double_count_manager_and_categories_are_aggregated():
    status = {
        "clients": [
            {"marketId": "ONE", "pid": 1, "apiActivity": {"rest": _rest("get_market", successes=2, errors=0, latency=20, last_at=100)}},
            {"marketId": "TWO", "pid": 2, "apiActivity": {"rest": _rest("list_markets", successes=1, errors=1, latency=60, last_at=200)}},
        ],
        "manager": {"apiActivity": {"rest": {"total": 999}}},
    }

    sources = api_sources(status)
    assert len(sources) == 2
    rows = aggregate_operations(sources)
    assert rows == [
        {
            "name": "pull market",
            "successes": 3,
            "errors": 1,
            "total": 4,
            "totalLatencyMs": 80.0,
            "lastActivityAtMs": 200,
            "complete": True,
            "averageLatencyMs": 20.0,
        }
    ]


def test_newest_error_and_friendly_order_names():
    older = {"atMs": 100, "operation": "create_order", "message": "older"}
    newer = {"atMs": 200, "operation": "cancel_order", "message": "newer"}
    sources = [("one", {"lastError": older}), ("two", {"lastError": newer})]

    assert newest_error({}, sources) == newer
    assert category_name("create_order") == "create order"
    assert category_name("cancel_order") == "delete order"


def test_token_bucket_usage_uses_read_write_costs_and_refill():
    costs = EndpointCosts()
    tracker = BucketUsageTracker()
    limits = {
        "read": {"refill": 300.0, "capacity": 900.0},
        "write": {"refill": 300.0, "capacity": 900.0},
    }

    def source(reads, writes, rate_limits=0):
        operations = {
            "list_markets": {"requestsLast60s": reads},
            "create_order": {"requestsLast60s": writes},
        }
        rest = {
            "byOperation": {"list_markets": reads, "create_order": writes},
            "operations": operations,
            "byStatus": {"429": rate_limits},
            "rateLimitErrorsLast60s": rate_limits,
        }
        return [("bot:one", rest)]

    initial = tracker.update(100.0, source(10, 5), costs, limits)
    assert initial["balance"] == {"read": 900.0, "write": 900.0}

    current = tracker.update(101.0, source(13, 7, 1), costs, limits)
    assert current["tokensLast60s"] == {"read": 130, "write": 70}
    assert current["balance"] == {"read": 870.0, "write": 880.0}
    assert current["rateLimitErrorsLast60s"] == 1
    assert current["rateLimitDetailed"] is True


def test_cancel_order_uses_non_default_two_token_cost():
    costs = EndpointCosts()
    assert costs.operation("list_markets") == ("read", 10)
    assert costs.operation("create_order") == ("write", 10)
    assert costs.operation("cancel_order") == ("write", 2)
