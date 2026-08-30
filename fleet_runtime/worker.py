"""Worker process hosting up to 25 independently stateful market actors."""

from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing as mp
import os
import queue
import resource
import time
import uuid
from collections import deque
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Mapping

from adaptors.kalshi import KalshiApiClient, KalshiClientConfig
from clients.models import Fill, OrderUpdate, StreamReset
from fleet_models import MarketHealth, ScreenerPick, WorkerHeartbeat
from session_config import bot_settings_payload
from top_of_book_bot import BotSettings, MarketActor, TelemetryStore, load_market_metadata
from .execution import BrokerRequest, BrokerRpcClient
from .risk import RiskSample, evaluate_risk


def _rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value * 1024) if os.name != "darwin" else int(value)


class FleetWorkerProcess(mp.Process):
    def __init__(
        self,
        *,
        worker_id: str,
        client_config: KalshiClientConfig,
        session_configuration: Mapping[str, Any],
        artifact_root: Path,
        broker_request_queue: Any,
        broker_response_queue: Any,
        command_queue: Any,
        heartbeat_queue: Any,
    ) -> None:
        super().__init__(name=f"fleet-{worker_id}", daemon=False)
        self.worker_id = worker_id
        self.client_config = client_config
        self.session_configuration = dict(session_configuration)
        self.artifact_root = Path(artifact_root)
        self.broker_request_queue = broker_request_queue
        self.broker_response_queue = broker_response_queue
        self.command_queue = command_queue
        self.heartbeat_queue = heartbeat_queue

    def run(self) -> None:  # pragma: no cover - covered by multiprocessing integration simulations
        asyncio.run(self._run())

    async def _run(self) -> None:
        shard_dir = self.artifact_root / "shards" / self.worker_id
        shard_dir.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            shard_dir / "worker.log", maxBytes=25 * 1024 * 1024, backupCount=4, encoding="utf-8"
        )
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logging.getLogger("kalshi_top_of_book_bot").addHandler(handler)
        logging.getLogger("kalshi_top_of_book_bot").setLevel(logging.INFO)
        markets_dir = self.artifact_root / "markets"
        markets_dir.mkdir(parents=True, exist_ok=True)
        direct = KalshiApiClient(self.client_config)
        client = BrokerRpcClient(
            direct,
            self.broker_request_queue,
            self.broker_response_queue,
            self.worker_id,
        )
        actors: dict[str, MarketActor] = {}
        picks: dict[str, ScreenerPick] = {}
        risk_windows: dict[str, deque[RiskSample]] = {}
        quoting_enabled = False
        last_retention_ms = 0
        stop_requested = asyncio.Event()
        subscriptions_ready = asyncio.Event()
        stream_generation = 0
        fleet = self.session_configuration["fleetRuntime"]
        heartbeat_seconds = float(fleet["workerHeartbeatSeconds"])
        freshness_seconds = float(fleet["quoteFreshnessSeconds"])

        async def add_actor(pick: ScreenerPick) -> None:
            if pick.market_id in actors:
                picks[pick.market_id] = pick
                return
            payload = bot_settings_payload(
                self.session_configuration,
                market_ticker=pick.market_id,
                yes_budget_cents=pick.yes_budget_cents,
                no_budget_cents=pick.no_budget_cents,
                telemetry_sqlite_path=str(shard_dir / "telemetry.sqlite3"),
                pnl_tracker_path=str(shard_dir / "telemetry.sqlite3"),
            )
            settings = BotSettings(**payload)
            market_dir = markets_dir / pick.market_id.replace("/", "_").replace("\\", "_")
            market_dir.mkdir(parents=True, exist_ok=True)
            (market_dir / "settings.json").write_text(
                json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8"
            )
            metadata = await asyncio.to_thread(load_market_metadata, client, pick.market_id)
            telemetry = TelemetryStore(
                str(shard_dir / "telemetry.sqlite3"),
                enabled=settings.enable_sqlite_telemetry,
                shard_mode=True,
            )
            actor = MarketActor(
                settings,
                client,
                metadata,
                owns_api_client=False,
                quote_freshness_seconds=freshness_seconds,
                quoting_enabled=False,
                telemetry_store=telemetry,
            )
            await actor.start()
            actor.telemetry_store.record_runtime_event(
                ticker=pick.market_id, source="worker", event_type="actor_started",
                payload={"worker_id": self.worker_id},
            )
            actors[pick.market_id] = actor
            picks[pick.market_id] = pick
            risk_windows[pick.market_id] = deque(maxlen=1_000)
            actor.set_quoting_enabled(quoting_enabled)

        async def remove_actor(ticker: str) -> None:
            actor = actors.pop(ticker, None)
            picks.pop(ticker, None)
            risk_windows.pop(ticker, None)
            if actor is not None:
                await actor.stop(verify_orders=True)

        async def command_loop() -> None:
            nonlocal quoting_enabled, stream_generation
            while not stop_requested.is_set():
                command = await asyncio.to_thread(self.command_queue.get)
                action = str(command.get("action") or "")
                if action == "reconcile":
                    desired = {item.market_id: item for item in command.get("picks") or ()}
                    had_actors = bool(actors)
                    remove = sorted(set(actors) - set(desired))
                    add = sorted(set(desired) - set(actors))
                    for ticker in remove:
                        await remove_actor(ticker)
                    for ticker in add:
                        await add_actor(desired[ticker])
                    for ticker in set(desired) & set(actors):
                        picks[ticker] = desired[ticker]
                    if had_actors and (add or remove):
                        try:
                            await direct.update_market_subscriptions(add=add, remove=remove)
                        except RuntimeError:
                            # Reconnect only when the subscription acknowledgement
                            # has not arrived yet; ordinary refreshes stay dynamic.
                            await direct.close()
                    stream_generation += 1
                    if actors:
                        subscriptions_ready.set()
                    else:
                        subscriptions_ready.clear()
                elif action == "enable_quoting":
                    quoting_enabled = bool(command.get("enabled"))
                    allocations = command.get("allocations") or {}
                    for ticker, actor in actors.items():
                        actor.set_allowed_quote_sides(allocations.get(ticker, ()))
                        actor.set_quoting_enabled(quoting_enabled)
                elif action == "freeze":
                    quoting_enabled = False
                    for actor in actors.values():
                        actor.set_quoting_enabled(False)
                    await asyncio.gather(
                        *(actor.emergency_cancel_all_quotes(reason="worker_freeze") for actor in actors.values()),
                        return_exceptions=True,
                    )
                elif action == "stop":
                    stop_requested.set()
                    return

        async def stream_loop() -> None:
            nonlocal stream_generation
            observed_generation = -1
            while not stop_requested.is_set():
                await subscriptions_ready.wait()
                if stop_requested.is_set():
                    return
                observed_generation = stream_generation
                try:
                    async for event in direct.stream_events_many(tuple(actors)):
                        if stop_requested.is_set():
                            return
                        actor = actors.get(event.market_id)
                        if actor is None:
                            continue
                        actor.handle_event(event)
                        actor.telemetry_store.record_runtime_event(
                            ticker=event.market_id,
                            source="execution" if isinstance(event, (OrderUpdate, Fill)) else "adaptor",
                            event_type=type(event).__name__,
                            severity="warning" if isinstance(event, StreamReset) else "info",
                        )
                        if isinstance(event, OrderUpdate):
                            self.broker_request_queue.put(BrokerRequest(
                                uuid.uuid4().hex, "", "order_update", {"event": event}, int(time.time() * 1000)
                            ))
                        if actor.book_ready:
                            yes = max(actor.book_yes) if actor.book_yes else None
                            no = max(actor.book_no) if actor.book_no else None
                            risk_windows[event.market_id].append(RiskSample(int(time.time() * 1000), yes, no))
                            if not actor.fleet_risk_generated_at_ms:
                                decision = evaluate_risk(
                                    risk_windows[event.market_id], now_ms=int(time.time() * 1000),
                                    position_units=actor.net_position_units, has_resting_orders=False,
                                )
                                actor.set_fleet_risk(decision.mode, reason=decision.reason, generated_at_ms=decision.generated_at_ms)
                        if stream_generation != observed_generation:
                            break
                except Exception:
                    await asyncio.sleep(1.0)

        async def risk_loop() -> None:
            while not stop_requested.is_set():
                now = int(time.time() * 1000)
                ordered = sorted(
                    actors.values(),
                    key=lambda actor: (not bool(actor.net_position_units), not any(x.has_active_resting_order for x in actor.orders.values()), actor.settings.market_ticker),
                )
                for index, actor in enumerate(ordered):
                    ticker = actor.settings.market_ticker
                    try:
                        decision = evaluate_risk(
                            risk_windows[ticker], now_ms=now, position_units=actor.net_position_units,
                            has_resting_orders=any(item.has_active_resting_order for item in actor.orders.values()),
                        )
                        actor.set_fleet_risk(decision.mode, reason=decision.reason, generated_at_ms=decision.generated_at_ms)
                        actor.telemetry_store.record_runtime_event(
                            ticker=ticker, source="watchdog", event_type="risk_evaluated",
                            severity="warning" if decision.mode != "normal" else "info",
                            payload={"mode": decision.mode, "reason": decision.reason, "confidence": decision.confidence},
                        )
                        if decision.mode == "flatten_only" and actor.net_position_units:
                            await actor.emergency_cancel_all_quotes(reason="risk_flatten_only")
                            await actor.submit_watchdog_exit_order()
                    except Exception:
                        actor.set_fleet_risk("flatten_only" if actor.net_position_units else "reduction_only", reason="risk_evaluator_failed", generated_at_ms=now)
                    if index + 1 < len(ordered):
                        await asyncio.sleep(60.0 / max(1, len(ordered)))
                await asyncio.sleep(max(0.1, 60.0 / max(1, len(ordered))))

        async def heartbeat_loop() -> None:
            nonlocal last_retention_ms
            while not stop_requested.is_set():
                now = int(time.time() * 1000)
                health = {
                    ticker: MarketHealth(
                        book_available=actor.book_ready,
                        book_age_ms=(now - actor.last_orderbook_event_timestamp_ms) if actor.last_orderbook_event_timestamp_ms else None,
                        decision_age_ms=(now - actor.last_quote_decision_at_ms) if actor.last_quote_decision_at_ms else None,
                        risk_mode=actor.fleet_risk_mode,
                        risk_age_ms=(now - actor.fleet_risk_generated_at_ms) if actor.fleet_risk_generated_at_ms else None,
                        resting_order_count=sum(1 for item in actor.orders.values() if item.has_active_resting_order),
                        position_units=actor.net_position_units,
                    )
                    for ticker, actor in actors.items()
                }
                self.heartbeat_queue.put(WorkerHeartbeat(
                    self.worker_id, tuple(sorted(actors)), health, _rss_bytes(),
                    0, max((item.book_age_ms or 0 for item in health.values()), default=0), now,
                ))
                if actors:
                    telemetry = next(iter(actors.values())).telemetry_store
                    telemetry.flush()
                    if now - last_retention_ms >= 86_400_000:
                        telemetry.apply_retention(now_timestamp_ms=now, raw_days=7)
                        last_retention_ms = now
                await asyncio.sleep(heartbeat_seconds)

        tasks = [
            asyncio.create_task(command_loop()),
            asyncio.create_task(stream_loop()),
            asyncio.create_task(risk_loop()),
            asyncio.create_task(heartbeat_loop()),
        ]
        try:
            await stop_requested.wait()
        finally:
            for actor in actors.values():
                actor.set_quoting_enabled(False)
            await asyncio.gather(*(actor.stop(verify_orders=True) for actor in actors.values()), return_exceptions=True)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await direct.close()
            client.close_dispatcher()
