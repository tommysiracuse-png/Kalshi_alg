"""Dedicated execution-process RPC used by every market worker."""

from __future__ import annotations

import heapq
import multiprocessing as mp
import queue
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from adaptors.kalshi import KalshiApiClient, KalshiClientConfig
from clients.models import (
    AmendTargetUnavailableError,
    ClientError,
    OrderNotFoundError,
    PostOnlyCrossError,
    RateLimitError,
)
from fleet_models import IntentUrgency, QuoteIntent
from .broker import TokenBucket


_ERROR_TYPES = {
    item.__name__: item
    for item in (
        ClientError,
        RateLimitError,
        OrderNotFoundError,
        PostOnlyCrossError,
        AmendTargetUnavailableError,
    )
}


def _urgency(operation: str, payload: Mapping[str, Any]) -> IntentUrgency:
    intent = payload.get("intent")
    if isinstance(intent, QuoteIntent):
        return intent.urgency
    if operation in {"cancel_all", "cancel_market"}:
        return IntentUrgency.EMERGENCY_CANCEL
    if operation == "cancel_order":
        return IntentUrgency.EMERGENCY_CANCEL
    request = payload.get("request")
    if bool(getattr(request, "reduce_only", False)):
        return IntentUrgency.RISK_REDUCE
    if operation in {"amend_order", "decrease_order_to"}:
        return IntentUrgency.NORMAL_CANCEL_OR_AMEND
    return IntentUrgency.NORMAL


@dataclass(frozen=True)
class BrokerRequest:
    request_id: str
    response_channel: str
    operation: str
    payload: Mapping[str, Any]
    submitted_at_ms: int


class BrokerRpcClient:
    """Read-through venue client whose order writes are broker RPCs."""

    def __init__(self, delegate: KalshiApiClient, request_queue: Any, response_queue: Any, channel: str) -> None:
        self._delegate = delegate
        self._request_queue = request_queue
        self._response_queue = response_queue
        self._channel = channel
        self._pending: dict[str, "queue.Queue[Mapping[str, Any]]"] = {}
        self._pending_lock = threading.Lock()
        self._closed = threading.Event()
        self._dispatcher = threading.Thread(target=self._dispatch, name=f"broker-rpc-{channel}", daemon=True)
        self._dispatcher.start()
        self.venue_name = delegate.venue_name
        self.environment_name = delegate.environment_name
        self.dry_run = delegate.dry_run
        self.rate_limit_backoff_seconds = delegate.rate_limit_backoff_seconds

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    def _dispatch(self) -> None:
        while not self._closed.is_set():
            try:
                response = self._response_queue.get(timeout=0.25)
            except queue.Empty:
                continue
            request_id = str(response.get("request_id") or "")
            with self._pending_lock:
                waiter = self._pending.get(request_id)
            if waiter is not None:
                waiter.put(response)

    def _rpc(self, operation: str, payload: Mapping[str, Any], *, timeout: float = 30.0) -> Any:
        request_id = uuid.uuid4().hex
        waiter: "queue.Queue[Mapping[str, Any]]" = queue.Queue(maxsize=1)
        with self._pending_lock:
            self._pending[request_id] = waiter
        self._request_queue.put(BrokerRequest(request_id, self._channel, operation, dict(payload), int(time.time() * 1000)))
        try:
            response = waiter.get(timeout=timeout)
        except queue.Empty as exc:
            raise TimeoutError(f"execution broker timed out during {operation}") from exc
        finally:
            with self._pending_lock:
                self._pending.pop(request_id, None)
        if not response.get("ok"):
            error_type = _ERROR_TYPES.get(str(response.get("error_type")), ClientError)
            raise error_type(str(response.get("error") or f"broker {operation} failed"))
        return response.get("result")

    def create_order(self, request: Any) -> Any:
        now = int(time.time() * 1000)
        intent = QuoteIntent(
            request.market_id, request.side, request.price_units, request.count_units,
            now, IntentUrgency.RISK_REDUCE if request.reduce_only else IntentUrgency.NORMAL,
            now, now + 20_000, exposure_increasing=not bool(request.reduce_only),
            risk_justification="reduce_only" if request.reduce_only else "strategy_quote",
        )
        return self._rpc("quote_intent", {"action": "create_order", "request": request, "intent": intent})

    def amend_order(self, request: Any) -> Any:
        now = int(time.time() * 1000)
        intent = QuoteIntent(
            request.market_id, request.side, request.new_price_units,
            request.new_total_fillable_count_units, now, IntentUrgency.NORMAL_CANCEL_OR_AMEND,
            now, now + 20_000, risk_justification="strategy_amend",
        )
        return self._rpc("quote_intent", {"action": "amend_order", "request": request, "intent": intent})

    def decrease_order_to(self, *, order_id: str, remaining_count_units: int) -> Any:
        return self._rpc("decrease_order_to", {"order_id": order_id, "remaining_count_units": remaining_count_units})

    def cancel_order(self, *, order_id: str) -> Any:
        return self._rpc("cancel_order", {"order_id": order_id})

    def get_market(self, market_id: str) -> Any:
        return self._rpc("get_market", {"market_id": market_id})

    def list_markets(self, query: Any) -> Any:
        return self._rpc("list_markets", {"query": query})

    def get_market_quote(self, market_id: str) -> Any:
        return self._rpc("get_market_quote", {"market_id": market_id})

    def get_account_balance(self) -> Any:
        return self._rpc("balance", {})

    def get_account_limits(self) -> Any:
        return self._rpc("limits", {})

    def list_account_positions(self, query: Any = None) -> Any:
        return self._rpc("list_account_positions", {"query": query})

    def list_account_orders(self, query: Any = None) -> Any:
        return self._rpc("list_account_orders", {"query": query})

    def list_account_fills(self, query: Any = None) -> Any:
        return self._rpc("list_account_fills", {"query": query})

    def get_positions(self, market_id: str) -> Any:
        return self._rpc("get_positions", {"market_id": market_id})

    def get_resting_orders(self, market_id: str) -> Any:
        return self._rpc("get_resting_orders", {"market_id": market_id})

    def get_order_queue_position(self, order_id: str) -> Any:
        return self._rpc("get_order_queue_position", {"order_id": order_id})

    def get_series(self, series_id: str) -> Any:
        return self._rpc("get_series", {"series_id": series_id})

    def get_series_fee_changes(self, series_id: str, *, show_historical: bool = False) -> Any:
        return self._rpc("get_series_fee_changes", {"series_id": series_id, "show_historical": show_historical})

    def get_incentive_programs(self, *, status: str = "active", incentive_type: str = "all", limit: int = 10_000) -> Any:
        return self._rpc("get_incentive_programs", {"status": status, "incentive_type": incentive_type, "limit": limit})

    async def close(self) -> None:
        # The worker owns and closes the delegate after all actors stop.
        return None

    def close_dispatcher(self) -> None:
        self._closed.set()
        self._dispatcher.join(timeout=1.0)


class ExecutionBrokerProcess(mp.Process):
    BOT_ORDER_PREFIXES = ("mm:", "tob:", "wd:")

    def __init__(
        self,
        *,
        client_config: KalshiClientConfig,
        request_queue: Any,
        response_queues: Mapping[str, Any],
        status_queue: Any,
        write_utilization_limit: float = 0.85,
        refresh_limits_seconds: float = 60.0,
    ) -> None:
        super().__init__(name="execution-broker", daemon=False)
        self.client_config = client_config
        self.request_queue = request_queue
        self.response_queues = dict(response_queues)
        self.status_queue = status_queue
        self.write_utilization_limit = write_utilization_limit
        self.refresh_limits_seconds = refresh_limits_seconds

    @staticmethod
    def _owned(client_order_id: str) -> bool:
        return bool(client_order_id) and client_order_id.startswith(ExecutionBrokerProcess.BOT_ORDER_PREFIXES)

    def _respond(self, request: BrokerRequest, *, result: Any = None, error: Optional[BaseException] = None) -> None:
        target = self.response_queues.get(request.response_channel)
        if target is None:
            return
        target.put({
            "request_id": request.request_id,
            "ok": error is None,
            "result": result,
            "error": str(error) if error else None,
            "error_type": type(error).__name__ if error else None,
        })

    def _cancel_owned(self, client: KalshiApiClient, market_id: str = "") -> int:
        from clients.models import AccountOrderQuery

        canceled = 0
        orders = client.list_account_orders(AccountOrderQuery(status="resting", market_id=market_id, page_size=1_000))
        for order in orders:
            if order.order_id and self._owned(order.client_order_id):
                try:
                    client.cancel_order(order_id=order.order_id)
                except OrderNotFoundError:
                    pass
                canceled += 1
        remaining = client.list_account_orders(AccountOrderQuery(status="resting", market_id=market_id, page_size=1_000))
        if any(self._owned(item.client_order_id) for item in remaining):
            raise RuntimeError("bot-tagged orders remain after broker cancellation")
        return canceled

    def _execute(self, client: KalshiApiClient, request: BrokerRequest) -> Any:
        payload = request.payload
        if request.operation == "quote_intent":
            action = str(payload.get("action") or "")
            intent = payload.get("intent")
            if action == "create_order":
                if isinstance(intent, QuoteIntent):
                    existing = self._order_registry.get((intent.ticker, intent.side))
                    if existing and existing.order_id:
                        try:
                            client.cancel_order(order_id=existing.order_id)
                        except OrderNotFoundError:
                            pass
                result = client.create_order(payload["request"])
                if isinstance(intent, QuoteIntent):
                    self._order_registry[(intent.ticker, intent.side)] = result
                return result
            if action == "amend_order":
                result = client.amend_order(payload["request"])
                if isinstance(intent, QuoteIntent):
                    self._order_registry[(intent.ticker, intent.side)] = result
                return result
            raise ValueError(f"unsupported quote intent action: {action}")
        if request.operation == "create_order":
            return client.create_order(payload["request"])
        if request.operation == "amend_order":
            return client.amend_order(payload["request"])
        if request.operation == "decrease_order_to":
            return client.decrease_order_to(
                order_id=str(payload["order_id"]),
                remaining_count_units=int(payload["remaining_count_units"]),
            )
        if request.operation == "cancel_order":
            order_id = str(payload["order_id"])
            result = client.cancel_order(order_id=order_id)
            self._order_registry = {
                key: value for key, value in self._order_registry.items()
                if getattr(value, "order_id", "") != order_id
            }
            return result
        if request.operation == "cancel_market":
            return self._cancel_owned(client, str(payload.get("market_id") or ""))
        if request.operation == "cancel_all":
            return self._cancel_owned(client)
        if request.operation == "verify_clear":
            return self._cancel_owned(client)
        if request.operation == "limits":
            return client.get_account_limits()
        if request.operation == "balance":
            return client.get_account_balance()
        if request.operation == "get_market":
            return client.get_market(str(payload["market_id"]))
        if request.operation == "list_markets":
            return client.list_markets(payload["query"])
        if request.operation == "get_market_quote":
            return client.get_market_quote(str(payload["market_id"]))
        if request.operation == "list_account_positions":
            return client.list_account_positions(payload.get("query")) if payload.get("query") is not None else client.list_account_positions()
        if request.operation == "list_account_orders":
            return client.list_account_orders(payload.get("query")) if payload.get("query") is not None else client.list_account_orders()
        if request.operation == "list_account_fills":
            return client.list_account_fills(payload.get("query")) if payload.get("query") is not None else client.list_account_fills()
        if request.operation == "get_positions":
            return client.get_positions(str(payload["market_id"]))
        if request.operation == "get_resting_orders":
            return client.get_resting_orders(str(payload["market_id"]))
        if request.operation == "get_order_queue_position":
            return client.get_order_queue_position(str(payload["order_id"]))
        if request.operation == "get_series":
            return client.get_series(str(payload["series_id"]))
        if request.operation == "get_series_fee_changes":
            return client.get_series_fee_changes(str(payload["series_id"]), show_historical=bool(payload.get("show_historical")))
        if request.operation == "get_incentive_programs":
            return client.get_incentive_programs(
                status=str(payload.get("status") or "active"),
                incentive_type=str(payload.get("incentive_type") or "all"),
                limit=int(payload.get("limit") or 10_000),
            )
        if request.operation == "admission_snapshot":
            from clients.models import AccountOrderQuery, AccountPositionQuery
            return {
                "limits": client.get_account_limits(),
                "balance": client.get_account_balance(),
                "orders": client.list_account_orders(AccountOrderQuery(status="resting", page_size=1_000)),
                "positions": client.list_account_positions(AccountPositionQuery(nonzero_only=True, page_size=1_000)),
            }
        if request.operation == "order_update":
            event = payload.get("event")
            key = (getattr(event, "market_id", ""), getattr(event, "side", ""))
            if getattr(event, "status", "") == "resting":
                self._order_registry[key] = event
            else:
                self._order_registry.pop(key, None)
            return "reconciled"
        if request.operation == "stop":
            return "stopping"
        raise ValueError(f"unsupported broker operation: {request.operation}")

    def run(self) -> None:  # pragma: no cover - integration exercised with real multiprocessing
        config = KalshiClientConfig(
            **{
                **self.client_config.__dict__,
                "enable_shared_write_rate_limiter": False,
            }
        )
        client = KalshiApiClient(config)
        self._order_registry: dict[tuple[str, str], Any] = {}
        try:
            # Previous-release bot orders are recognized by their client tag.
            # Start from a verified clear account rather than inheriting an
            # order whose strategy generation is unknown to this broker.
            client.list_account_positions()
            self._cancel_owned(client)
        except Exception as exc:
            self.status_queue.put({"type": "startup_error", "at_ms": int(time.time() * 1000), "error": str(exc)})
            import asyncio
            asyncio.run(client.close())
            return
        normal_bucket = TokenBucket(0, 0)
        reserved_bucket = TokenBucket(0, 0)
        normal_read_bucket = TokenBucket(0, 0)
        reserved_read_bucket = TokenBucket(0, 0)
        last_limits = 0.0
        sequence = 0
        pending: list[tuple[int, int, BrokerRequest]] = []
        stopping = False
        repeated_429 = 0
        superseded: set[str] = set()
        followers: dict[str, list[BrokerRequest]] = {}
        latest_intents: dict[tuple[str, str], BrokerRequest] = {}
        try:
            while not stopping:
                now = time.monotonic()
                if now - last_limits >= self.refresh_limits_seconds:
                    try:
                        limits = client.get_account_limits()
                        write_rate = max(0, limits.write.refill_rate)
                        normal_rate = write_rate * self.write_utilization_limit
                        reserved_rate = write_rate - normal_rate
                        read_rate = max(0, limits.read.refill_rate)
                        normal_read_rate = read_rate * 0.80
                        reserved_read_rate = read_rate - normal_read_rate
                        normal_bucket.reconfigure(normal_rate, max(10.0, normal_rate))
                        reserved_bucket.reconfigure(reserved_rate, max(2.0, reserved_rate))
                        normal_read_bucket.reconfigure(normal_read_rate, max(10.0, normal_read_rate))
                        reserved_read_bucket.reconfigure(reserved_read_rate, max(10.0, reserved_read_rate))
                        self.status_queue.put({
                            "type": "capacity", "at_ms": int(time.time() * 1000), "limits": limits,
                            "normal_write_rate": normal_rate, "reserved_write_rate": reserved_rate,
                        })
                        last_limits = now
                        repeated_429 = 0
                    except Exception as exc:
                        self.status_queue.put({"type": "capacity_error", "at_ms": int(time.time() * 1000), "error": str(exc)})
                        last_limits = now

                try:
                    request = self.request_queue.get(timeout=0.05 if not pending else 0.0)
                    sequence += 1
                    if request.operation == "quote_intent":
                        intent = request.payload.get("intent")
                        if isinstance(intent, QuoteIntent) and intent.urgency == IntentUrgency.NORMAL:
                            key = (intent.ticker, intent.side)
                            prior = latest_intents.get(key)
                            if prior is not None:
                                superseded.add(prior.request_id)
                                followers.setdefault(request.request_id, []).append(prior)
                                followers[request.request_id].extend(followers.pop(prior.request_id, []))
                            latest_intents[key] = request
                    heapq.heappush(pending, (int(_urgency(request.operation, request.payload)), sequence, request))
                    while len(pending) < 1_024:
                        request = self.request_queue.get_nowait()
                        sequence += 1
                        if request.operation == "quote_intent":
                            intent = request.payload.get("intent")
                            if isinstance(intent, QuoteIntent) and intent.urgency == IntentUrgency.NORMAL:
                                key = (intent.ticker, intent.side)
                                prior = latest_intents.get(key)
                                if prior is not None:
                                    superseded.add(prior.request_id)
                                    followers.setdefault(request.request_id, []).append(prior)
                                    followers[request.request_id].extend(followers.pop(prior.request_id, []))
                                latest_intents[key] = request
                        heapq.heappush(pending, (int(_urgency(request.operation, request.payload)), sequence, request))
                except queue.Empty:
                    pass
                if not pending:
                    continue
                urgency, _seq, request = heapq.heappop(pending)
                if request.request_id in superseded:
                    continue
                if request.operation == "quote_intent":
                    intent = request.payload.get("intent")
                    if isinstance(intent, QuoteIntent):
                        key = (intent.ticker, intent.side)
                        if latest_intents.get(key) is request:
                            latest_intents.pop(key, None)
                cancel_cost = request.operation in {"cancel_order", "cancel_market", "cancel_all", "verify_clear"}
                cost = 2.0 if cancel_cost else 10.0
                write_operation = request.operation in {
                    "quote_intent", "create_order", "amend_order", "decrease_order_to", "cancel_order",
                    "cancel_market", "cancel_all", "verify_clear",
                }
                no_token_operation = request.operation in {"stop", "order_update"}
                if no_token_operation:
                    permitted = True
                elif write_operation:
                    permitted = (
                        (reserved_bucket.consume(cost) or normal_bucket.consume(cost))
                        if cancel_cost or urgency <= int(IntentUrgency.RISK_REDUCE)
                        else normal_bucket.consume(cost)
                    )
                else:
                    reserved_read = request.operation in {"limits", "balance", "admission_snapshot"}
                    permitted = (
                        (reserved_read_bucket.consume(cost) or normal_read_bucket.consume(cost))
                        if reserved_read else normal_read_bucket.consume(cost)
                    )
                if not permitted:
                    heapq.heappush(pending, (urgency, _seq, request))
                    time.sleep(0.005)
                    continue
                try:
                    result = self._execute(client, request)
                    self._respond(request, result=result)
                    for follower in followers.pop(request.request_id, []):
                        self._respond(follower, result=result)
                    repeated_429 = 0
                    stopping = request.operation == "stop"
                except RateLimitError as exc:
                    repeated_429 += 1
                    self._respond(request, error=exc)
                    for follower in followers.pop(request.request_id, []):
                        self._respond(follower, error=exc)
                    if repeated_429 >= 2:
                        last_limits = 0.0
                    if urgency >= int(IntentUrgency.NORMAL_CANCEL_OR_AMEND):
                        time.sleep(min(5.0, 0.25 * (2 ** min(repeated_429, 4))))
                except Exception as exc:
                    self._respond(request, error=exc)
                    for follower in followers.pop(request.request_id, []):
                        self._respond(follower, error=exc)
        finally:
            try:
                self._cancel_owned(client)
                self._cancel_owned(client)
            finally:
                import asyncio
                asyncio.run(client.close())
