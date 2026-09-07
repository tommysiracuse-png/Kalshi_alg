"""Deterministic backtest driver for the production MarketActor.

``evaluate_candidate`` replays a historical window through the real
``top_of_book_bot.MarketActor`` in-process: the replay clock is monkeypatched
over ``top_of_book_bot.now_ms``, synthesized book/ticker/trade events are fed
through ``MarketActor.handle_event``, and quoting is driven synchronously
through the actor's own ``desired_quote_prices``/``ensure_side_quote`` path
(the same calls ``requote_worker`` makes) so its internal order state stays
consistent. Fills come from a conservative paper-fill simulator and re-enter
the actor as real ``Fill``/``OrderUpdate`` events.

Data sources (``data_source`` keyword of ``evaluate_candidate``):

- Tier 1 (default): ``{"tier": 1}`` -- candles + trades from ``history_db_path``
  through ``loader.load_events`` and the print-based ``FillSimulator``.
- Tier 2: ``{"tier": 2, "record_root": <record_data dir>}`` -- raw recorded
  websocket messages through ``loader.load_tier2_events`` and the
  queue-aware ``QueueFillSimulator``. Real ``OrderBookDelta`` events reach the
  actor (its own ``observe_orderbook_toxicity_from_delta`` sees real pulls),
  our own placements/cancels/fills are overlaid into the actor's book as the
  deltas the live feed would carry, and the queue-position poll the live
  ``queue_position_worker`` performs is emulated on the virtual clock so the
  queue-abandonment guard runs. ``history_db_path`` is optional at Tier 2
  (market metadata / settlement only, when the file exists).

Model learning (``model_learning`` keyword, default on): the actor's
``ToxicityModel`` and ``FillProbabilityModel`` learn inside the replay the way
they do live -- post-fill markouts are scheduled on the virtual clock at every
configured horizon and evaluated against the replay's future book through the
actor's own ``record_markout`` path, and the periodic telemetry-backed model
refresh is emulated against the candidate's temp telemetry DB (see
``replay.learning``). ``model_learning=False`` restores the old constant-prior
behaviour for A/B comparisons; ``data_source["model_learning"]`` overrides the
keyword so optimizer runs can flip it through ``OPTIMIZER_DATA_SOURCE``, and
``data_source["model_refresh_ms"]`` overrides the emulated refresh cadence
(``0`` = no periodic telemetry refresh, direct ``record_markout`` only).
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import shutil
import tempfile
from typing import Dict, List, Optional, Tuple

from clients.models import (
    Fill,
    OrderBookDelta,
    OrderBookSnapshot,
    PublicTrade,
    StreamReset,
)

from replay import loader, scoring
from replay.clock import ReplayClock
from replay.fill_sim import FillSimulator, QueueFillSimulator
from replay.learning import ReplayModelLearner
from replay.replay_client import ReplayClient

PRICE_SCALE = 10_000
COUNT_SCALE = 100
DEFAULT_QUOTE_FRESHNESS_SECONDS = 20.0
DEFAULT_QUEUE_POLL_MS = 10_000  # mirrors BotSettings.queue_position_log_interval_seconds
MIN_QUEUE_POLL_MS = 1_000

# Temp directories created by evaluate_candidate (introspection for tests).
_TEMP_DIRS_CREATED: List[str] = []


def _empty_result(ticker: str) -> dict:
    return {
        "ticker": ticker,
        "error": None,
        "decisions": 0,
        "orders_placed": 0,
        "fills": 0,
        "contracts_units": 0,
        "fees_units": 0,
        "net_markout_units_by_horizon": {h: 0 for h in scoring.HORIZONS_MS},
        "total_net_units": 0,
        "max_drawdown_units": 0,
        "settlement_net_units": None,
        "tier": 1,
        "warnings": [],
        "tier2_stats": None,
        "learning": None,
    }


def resolve_data_source(data_source: Optional[dict]) -> dict:
    """Normalize the ``data_source`` argument; Tier 1 when absent."""
    if not data_source:
        return {"tier": 1}
    tier = int(data_source.get("tier", 1))
    if tier not in (1, 2):
        raise ValueError(f"unsupported data_source tier {tier!r}")
    out = {"tier": tier}
    if data_source.get("model_learning") is not None:
        out["model_learning"] = bool(data_source["model_learning"])
    if data_source.get("model_refresh_ms") is not None:
        # Virtual-clock cadence of the telemetry-backed model refresh;
        # 0 disables it (direct record_markout learning only).
        out["model_refresh_ms"] = max(0, int(data_source["model_refresh_ms"]))
    if tier == 2:
        record_root = str(data_source.get("record_root") or "").strip()
        if not record_root:
            raise ValueError("data_source tier 2 requires 'record_root'")
        out["record_root"] = record_root
        if data_source.get("match_window_ms") is not None:
            out["match_window_ms"] = int(data_source["match_window_ms"])
        if data_source.get("queue_poll_ms") is not None:
            out["queue_poll_ms"] = int(data_source["queue_poll_ms"])
    return out


def _build_settings(bot_mod, session_config, ticker: str, bot_overrides: dict,
                    temp_dir: str):
    configuration = session_config.default_session_configuration()
    # Close the toxicity/fill-probability learning loop inside the candidate's
    # own temp DB (isolation requirement).
    configuration["bot"]["enable_sqlite_telemetry"] = True

    managed = set(session_config._MANAGED_BOT_FIELDS)
    managed_overrides = {}
    for key, value in dict(bot_overrides or {}).items():
        if key in managed:
            managed_overrides[key] = value
        else:
            # Unknown field names fail loudly inside validation -> error field.
            configuration["bot"][key] = value

    payload = session_config.bot_settings_payload(
        configuration,
        market_ticker=ticker,
        yes_budget_cents=int(managed_overrides.pop("yes_order_budget_cents", 100)),
        no_budget_cents=int(managed_overrides.pop("no_order_budget_cents", 100)),
        telemetry_sqlite_path=f"{temp_dir}/telemetry.sqlite3",
        pnl_tracker_path=f"{temp_dir}/pnl_fills.jsonl",
        watchdog_state_file="",
    )
    payload.update(managed_overrides)
    # Hard determinism / no-wall-clock requirements.
    payload["expiration_jitter_seconds"] = 0
    payload["post_only_reprice_cooldown_seconds"] = 0.0
    payload["enable_queue_position_logging"] = False

    settings = bot_mod.BotSettings(**payload)
    settings.validate()
    return settings


def _build_metadata(bot_mod, ticker: str, market_row: Optional[dict]):
    row = market_row or {}
    return bot_mod.MarketMetadata(
        ticker=ticker,
        title=str(row.get("title") or ticker),
        status=str(row.get("status") or "active"),
        series_ticker=str(row.get("series") or ""),
        event_ticker=ticker,
        close_time_ms=(int(row["close_ts_ms"]) if row.get("close_ts_ms") is not None else None),
        price_level_structure="linear_cent",
        fractional_trading_enabled=True,
        price_grid=bot_mod.PriceGrid([bot_mod.PriceRange(0, PRICE_SCALE, 100)]),
    )


async def _quote_cycle(bot_mod, actor, clock: ReplayClock) -> None:
    """One synchronous pass of the requote_worker decision body."""
    actor.fleet_risk_generated_at_ms = clock.now_ms()
    async with actor.requote_lock:
        desired_yes_units, desired_no_units = actor.desired_quote_prices()
        if "yes" not in actor.allowed_quote_sides:
            desired_yes_units = None
        if "no" not in actor.allowed_quote_sides:
            desired_no_units = None
        actor.last_quote_decision_at_ms = clock.now_ms()
        try:
            await actor.ensure_side_quote("yes", desired_yes_units)
            await actor.ensure_side_quote("no", desired_no_units)
            actor.last_requote_action_timestamp_ms = clock.now_ms()
        except Exception as exc:  # mirror requote_worker: never kill the loop
            if not bot_mod.is_post_only_cross_error(exc):
                logging.getLogger("replay").debug("requote error swallowed: %s", exc)


async def _run(bot_mod, actor, simulator: FillSimulator, clock: ReplayClock,
               events, mid_path: scoring.MidPath, minimum_requote_ms: int,
               learner: ReplayModelLearner) -> int:
    decisions = 0
    actor.load_startup_position()
    actor.cancel_owned_resting_quotes_on_startup()
    actor.set_fleet_risk("normal", reason="replay", generated_at_ms=clock.now_ms())
    last_cycle_ms: Optional[int] = None

    for ts_ms, _priority, event in events:
        # Moves the clock; due markout observations / model refreshes fire
        # on the way (no-op passthrough when learning is disabled).
        learner.advance_to(ts_ms)
        if isinstance(event, OrderBookSnapshot):
            best_bid = max(event.yes_levels) if event.yes_levels else None
            best_ask = (
                PRICE_SCALE - max(event.no_levels) if event.no_levels else None
            )
            simulator.update_book(best_bid, best_ask)
            if best_bid is not None and best_ask is not None:
                mid_path.record(ts_ms, (best_bid + best_ask) // 2)
        elif isinstance(event, PublicTrade):
            # Fills against our resting quotes happen before the actor sees
            # the public print (the venue fills the maker as the print occurs).
            for fill_event, order_event in simulator.on_trade(event):
                actor.handle_event(fill_event)
                actor.handle_event(order_event)

        actor.handle_event(event)

        if not actor.book_ready:
            continue
        if last_cycle_ms is not None and clock.now_ms() - last_cycle_ms < minimum_requote_ms:
            continue
        await _quote_cycle(bot_mod, actor, clock)
        decisions += 1
        last_cycle_ms = clock.now_ms()

    learner.finish()
    return decisions


def _feed_own_deltas(actor, simulator: QueueFillSimulator, clock: ReplayClock) -> None:
    """Overlay our own order activity into the actor's book as live deltas."""
    for side, price_units, delta_units in simulator.take_own_deltas():
        actor.handle_event(
            OrderBookDelta(
                market_id=simulator.market_id,
                sequence=actor.last_orderbook_sequence,
                side=side,  # type: ignore[arg-type]
                price_units=int(price_units),
                delta_count_units=int(delta_units),
                timestamp_ms=clock.now_ms(),
            )
        )


async def _queue_poll(actor, simulator: QueueFillSimulator) -> None:
    """One pass of the live ``queue_position_worker`` body on the sim's queue."""
    for side, state in actor.orders.items():
        if not state.has_active_resting_order or not state.order_id:
            state.consecutive_queue_ahead_breaches = 0
            continue
        try:
            response = simulator.get_order_queue_position(state.order_id)
        except Exception:
            continue
        state.last_queue_position_units = response.queue_position_units
        await actor.process_queue_position_update(side, response.queue_position_units)


def _record_mid(mid_path: scoring.MidPath, ts_ms: int, simulator: QueueFillSimulator) -> None:
    mid = simulator.book.mid_units()
    if mid is not None:
        mid_path.record(ts_ms, mid)


async def _run_tier2(bot_mod, actor, simulator: QueueFillSimulator, clock: ReplayClock,
                     events, mid_path: scoring.MidPath, minimum_requote_ms: int,
                     queue_poll_ms: int, learner: ReplayModelLearner) -> int:
    decisions = 0
    actor.load_startup_position()
    actor.cancel_owned_resting_quotes_on_startup()
    actor.set_fleet_risk("normal", reason="replay", generated_at_ms=clock.now_ms())
    last_cycle_ms: Optional[int] = None
    last_poll_ms: Optional[int] = None

    for ts_ms, _priority, event in events:
        learner.advance_to(ts_ms)
        simulator.set_clock(clock.now_ms())
        if isinstance(event, StreamReset):
            simulator.on_stream_reset()
            actor.handle_event(event)
        elif isinstance(event, OrderBookSnapshot):
            simulator.apply_snapshot(event)
            _record_mid(mid_path, ts_ms, simulator)
            actor.handle_event(simulator.overlay_snapshot(event))
        elif isinstance(event, OrderBookDelta):
            simulator.apply_delta(event)
            _record_mid(mid_path, ts_ms, simulator)
            actor.handle_event(event)
        elif isinstance(event, PublicTrade):
            # The venue fills the maker as the print occurs: fills first.
            for fill_event, order_event in simulator.on_trade(event):
                actor.handle_event(fill_event)
                actor.handle_event(order_event)
            _feed_own_deltas(actor, simulator, clock)
            actor.handle_event(event)
        else:
            actor.handle_event(event)
        _feed_own_deltas(actor, simulator, clock)

        if not actor.book_ready:
            continue
        now = clock.now_ms()
        if last_poll_ms is None or now - last_poll_ms >= queue_poll_ms:
            await _queue_poll(actor, simulator)
            _feed_own_deltas(actor, simulator, clock)
            last_poll_ms = now
        if last_cycle_ms is not None and now - last_cycle_ms < minimum_requote_ms:
            continue
        await _quote_cycle(bot_mod, actor, clock)
        _feed_own_deltas(actor, simulator, clock)
        decisions += 1
        last_cycle_ms = now

    learner.finish()
    return decisions


def _load_market_row(history_db_path: str, ticker: str) -> Tuple[Optional[dict], Optional[object]]:
    """(market_row, connection); tolerant of a missing DB at Tier 2."""
    if not history_db_path or not os.path.isfile(history_db_path):
        return None, None
    connection = loader.open_history(history_db_path)
    return loader.load_market(connection, ticker), connection


def evaluate_candidate(history_db_path: str, ticker: str, bot_overrides: dict,
                       start_ms: int, end_ms: int, seed: int = 0,
                       fill_share_fraction: float = 0.5,
                       assumed_top_depth_contracts: int = 100,
                       data_source: Optional[dict] = None,
                       model_learning: bool = True) -> dict:
    """Replay one candidate configuration over a historical window.

    ``data_source`` selects the input tier (see module docstring); omitted
    or ``{"tier": 1}`` keeps the candle/trade replay from ``history_db_path``.
    ``model_learning`` lets the actor's toxicity / fill-probability models
    learn from simulated fills on the virtual clock (default); ``False``
    keeps them at their priors (pre-learning behaviour). The result's
    ``learning`` block reports what was learned.
    Never raises: any failure is reported in the result's ``error`` field.
    Two calls with identical arguments return identical dicts.
    """
    result = _empty_result(ticker)
    connection = None
    temp_dir = None
    actor = None
    saved_now_ms = None
    saved_expiration_fn = None
    bot_mod = None
    bot_logger = logging.getLogger("kalshi_top_of_book_bot")
    saved_log_level = bot_logger.level
    try:
        from bots import top_of_book_bot as bot_mod
        from core import session_config

        source = resolve_data_source(data_source)
        tier = source["tier"]
        result["tier"] = tier
        learning_enabled = bool(source.get("model_learning", model_learning))
        random.seed(seed)
        bot_logger.setLevel(logging.WARNING)  # keep replay silent

        if tier == 2:
            market_row, connection = _load_market_row(history_db_path, ticker)
            events, warnings = loader.load_tier2_events(
                source["record_root"], ticker, int(start_ms), int(end_ms)
            )
            result["warnings"] = list(warnings)
        else:
            connection = loader.open_history(history_db_path)
            market_row = loader.load_market(connection, ticker)
            assumed_depth_units = max(1, int(assumed_top_depth_contracts)) * COUNT_SCALE
            events = loader.load_events(connection, ticker, int(start_ms), int(end_ms),
                                        assumed_depth_units)
        if not events:
            result["error"] = f"no replayable events for {ticker!r} in window"
            return result

        temp_dir = tempfile.mkdtemp(prefix="kalshi_replay_")
        _TEMP_DIRS_CREATED.append(temp_dir)

        settings = _build_settings(bot_mod, session_config, ticker,
                                   bot_overrides or {}, temp_dir)
        metadata = _build_metadata(bot_mod, ticker, market_row)

        clock = ReplayClock(events[0][0])
        saved_now_ms = bot_mod.now_ms
        saved_expiration_fn = bot_mod.utc_seconds_to_expiration_timestamp
        bot_mod.now_ms = clock.now_ms
        bot_mod.utc_seconds_to_expiration_timestamp = (
            lambda seconds: clock.now_seconds() + int(seconds)
        )

        if tier == 2:
            simulator = QueueFillSimulator(
                ticker,
                fill_share_fraction=float(fill_share_fraction),
                fee_factor=scoring.SCORING_FEE_FACTOR,
                match_window_ms=int(source.get("match_window_ms", 1500)),
            )
        else:
            simulator = FillSimulator(
                ticker,
                fill_share_fraction=float(fill_share_fraction),
                fee_factor=scoring.SCORING_FEE_FACTOR,
            )
        client = ReplayClient(simulator)
        if tier == 2:
            # Duck-typed venue surface: the sim's queue model answers polls.
            client.get_order_queue_position = simulator.get_order_queue_position
        actor = bot_mod.MarketActor(
            settings,
            client,
            metadata,
            owns_api_client=False,
            quote_freshness_seconds=DEFAULT_QUOTE_FRESHNESS_SECONDS,
            quoting_enabled=True,
            telemetry_store=None,
        )
        # The network-facing refresh is never called in replay (no workers
        # run); the wall-clock markout capture is replaced by the learner,
        # which re-schedules the same observations on the virtual clock and
        # emulates the periodic telemetry-backed model refresh. With learning
        # off it is a no-op and the models stay at their priors.
        # replay/scoring.py still computes the *score* markouts from the tape.
        actor.refresh_external_models = lambda: None
        learner = ReplayModelLearner(
            actor, clock, enabled=learning_enabled,
            refresh_interval_ms=source.get("model_refresh_ms"),
        )
        actor.schedule_fill_markouts = learner.schedule_fill_markouts

        mid_path = scoring.MidPath()
        if tier == 2:
            queue_poll_ms = int(source.get(
                "queue_poll_ms",
                max(MIN_QUEUE_POLL_MS, int(float(settings.queue_position_log_interval_seconds) * 1000)),
            ))
            decisions = asyncio.run(
                _run_tier2(
                    bot_mod, actor, simulator, clock, events, mid_path,
                    int(settings.minimum_milliseconds_between_requotes),
                    max(MIN_QUEUE_POLL_MS, queue_poll_ms),
                    learner,
                )
            )
            result["tier2_stats"] = dict(simulator.stats)
        else:
            decisions = asyncio.run(
                _run(
                    bot_mod, actor, simulator, clock, events, mid_path,
                    int(settings.minimum_milliseconds_between_requotes),
                    learner,
                )
            )

        result["learning"] = learner.snapshot()
        scored = scoring.score_fills(simulator.fills, mid_path)
        result["decisions"] = int(decisions)
        result["orders_placed"] = int(simulator.orders_placed)
        result["fills"] = len(simulator.fills)
        result["contracts_units"] = int(scored["contracts_units"])
        result["fees_units"] = int(scored["fees_units"])
        result["net_markout_units_by_horizon"] = dict(scored["net_markout_units_by_horizon"])
        result["total_net_units"] = int(scored["total_net_units"])
        result["max_drawdown_units"] = int(scored["max_drawdown_units"])
        result["settlement_net_units"] = scoring.settlement_net_units(
            simulator.fills,
            (market_row or {}).get("result"),
            (market_row or {}).get("close_ts_ms"),
            int(start_ms),
            int(end_ms),
        )
        return result
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result
    finally:
        if bot_mod is not None and saved_now_ms is not None:
            bot_mod.now_ms = saved_now_ms
        if bot_mod is not None and saved_expiration_fn is not None:
            bot_mod.utc_seconds_to_expiration_timestamp = saved_expiration_fn
        bot_logger.setLevel(saved_log_level)
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
        if actor is not None:
            try:
                store_connection = getattr(actor.telemetry_store, "_connection", None)
                if store_connection is not None:
                    store_connection.close()
            except Exception:
                pass
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)
