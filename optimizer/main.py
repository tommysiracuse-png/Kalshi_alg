"""CLI entrypoint and orchestration for the parameter-optimization swarm.

Usage (from the repo root):
    python -m optimizer.main --history history_data/history.sqlite3 \
        --candidates 150 --workers 6 --seed 1 --top-params 15 \
        --budget-minutes 20 --markets 40 [--write-back] [--class thinWide]

``--class <name>`` restricts the run to backfilled markets the market-class
classifier (``market_classes.classify_history_markets``, default
``botClasses.classifier`` thresholds) assigns to that class, and
``--write-back`` then stores the winner as that class's ``botClasses``
overrides (classification enabled) instead of the base ``bot`` section.

Tier-2 (recorded full-depth order books; unpins the depth/queue/pull group):
    python -m optimizer.main --tier 2 --record-root record_data \
        --candidates 150 --workers 6 [--last-days 3] [--write-back]

Markets then come from the recording (``replay.loader.available_tier2_windows``)
and every evaluation replays raw websocket messages through the queue-aware
fill simulator; ``--history`` is only consulted for market metadata and, with
``--class``, for classification.

Market universe: by default the candidate markets are restricted to those
that would have passed the live screener's liquidity / time filters
(``optimizer.data.screen_history_windows``) at the window start or a
walk-forward split boundary, so the run trains on the fleet's universe rather
than merely the most-traded history markets. Settings come from
``--screener-session`` / ``--base-session`` (session ``screener`` section) or
``kalshi_screener_config``; ``--no-screener-filter`` restores the old
behaviour. The settings and pass/fail record land in the run's ``args_json``.

Result record and write-back: right after the final stage the run persists
``<output-dir>/winner_<run_id>.json`` (winner candidate, params, scores, data
window, base session, market class) and completes it after the report, so the
result survives whatever happens afterwards. ``--write-back`` then creates the
session in a FRESH subprocess (``python -m optimizer.writeback --winner-file
...``) that imports the session schema from disk instead of the modules this
long-lived process loaded at start; its stdout is forwarded, so the
``write-back: created session '<name>'`` line still reaches the optimizer
chain and the dashboard. A failed write-back is logged with the re-run
command and does not fail the run (exit 0); recover with
``python -m optimizer.writeback --winner-file runtime/optimizer/winner_<run_id>.json``.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import sqlite3
import subprocess
import sys
import time
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from optimizer import data as data_mod
from optimizer import results as results_mod
from optimizer import runner, search, sensitivity, walkforward, writeback
from optimizer.space import build_space, diff_from_default

NEG_INF = float("-inf")
# The line ``optimizer.writeback`` prints for a created session; the chain
# runner and the dashboard parse the same text.
WRITEBACK_SESSION_RE = re.compile(r"write-back: created session '([^']+)'")
WRITEBACK_TIMEOUT_SECONDS = 600.0


def log(message: str) -> None:
    print(f"[optimizer] {message}", flush=True)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m optimizer.main")
    parser.add_argument("--history", default="history_data/history.sqlite3")
    parser.add_argument("--tier", type=int, choices=(1, 2), default=1,
                        help="Replay data tier: 1 = candles+trades from --history (default); "
                             "2 = raw recorded order-book messages from --record-root "
                             "(full depth, queue model; depth/queue/pull params searchable)")
    parser.add_argument("--record-root", default="record_data",
                        help="Tier-2 recording root (record_data/YYYYMMDD/HH/conn<k>.jsonl.gz)")
    parser.add_argument("--candidates", type=int, default=150)
    parser.add_argument("--workers", type=int, default=runner.default_workers())
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--top-params", type=int, default=15)
    parser.add_argument("--budget-minutes", type=float, default=20.0)
    parser.add_argument("--markets", type=int, default=40)
    parser.add_argument("--splits", type=int, default=3)
    parser.add_argument("--fill-share", type=float, default=0.5)
    parser.add_argument("--output-dir", default="runtime/optimizer",
                        help="directory for results.sqlite3 and the report")
    parser.add_argument("--write-back", action="store_true")
    parser.add_argument("--base-params-json", default="",
                        help="JSON file of bot params used as the sensitivity-screen base "
                             "config and injected as a seeded candidate (e.g. a prior winner)")
    parser.add_argument("--only-params", default="",
                        help="Comma-separated bot fields to search; every other field stays at the "
                             "seeded base (--base-params-json). The sensitivity screen and --top-params "
                             "then apply to this list only (targeted run, e.g. the toxicity group).")
    parser.add_argument("--base-session", default="",
                        help="Name of a saved session whose configuration is the starting point "
                             "for --write-back: every field the run did not search (Tier-1-pinned "
                             "guards, fleet size, refresh interval) keeps that session's value.")
    parser.add_argument("--from-date", default="",
                        help="Restrict optimization to data on/after this date "
                             "(YYYY-MM-DD or epoch ms). Optimize on a chosen period.")
    parser.add_argument("--to-date", default="",
                        help="Restrict optimization to data on/before this date "
                             "(YYYY-MM-DD or epoch ms).")
    parser.add_argument("--last-days", type=float, default=0.0,
                        help="Shorthand for --from-date = (latest data - N days). "
                             "Optimize only on the most recent N days.")
    parser.add_argument("--screener-session", default="",
                        help="Name of a saved session whose 'screener' section supplies the "
                             "historical screener filter settings (default: --base-session's "
                             "section if it has one, else the kalshi_screener_config constants).")
    parser.add_argument("--no-screener-filter", dest="screener_filter", action="store_false", default=True,
                        help="Do not restrict the history markets to those that would have passed "
                             "the live screener's liquidity/time filters (default: filter on).")
    parser.add_argument("--screener-eval-hours", type=float, default=data_mod.DEFAULT_SCREENER_EVAL_HOURS,
                        help="Re-evaluate the screener filter every N hours across the data window "
                             "in addition to the window start and the walk-forward split boundaries "
                             f"(default {data_mod.DEFAULT_SCREENER_EVAL_HOURS}; 0 = boundaries only). "
                             "A market is kept if it passes at any evaluation time.")
    from core.session_config import MARKET_CLASS_NAMES

    parser.add_argument("--class", dest="market_class", default="", choices=("", *MARKET_CLASS_NAMES),
                        metavar="{" + ",".join(MARKET_CLASS_NAMES) + "}",
                        help="Optimize only markets the classifier assigns to this market class; "
                             "with --write-back the winner becomes that class's botClasses overrides.")
    return parser


def load_base_session_configuration(name: str) -> Dict[str, Any]:
    """Configuration of the saved session called ``name`` (KALSHI_SESSION_STORE or session_data).

    Shares its lookup (archived sessions included) with the
    ``optimizer.writeback`` CLI, so the write-back subprocess resolves
    ``--base-session`` exactly the way this process did at start.
    """
    configuration = writeback.find_session_configuration(name)
    if configuration is None:
        raise SystemExit(f"--base-session: no saved session named {name!r}")
    return configuration


def classify_markets_for_class(
    history_path: str,
    market_class: str,
    from_ms: Optional[int],
    to_ms: Optional[int],
    log_fn=log,
) -> set[str]:
    """Tickers in ``history_path`` that the classifier assigns to ``market_class``."""
    from core import session_config
    from core.market_classes import ClassifierThresholds, classify_history_markets

    thresholds = ClassifierThresholds.from_configuration(session_config.default_session_configuration())
    classes = classify_history_markets(history_path, thresholds, from_ms=from_ms, to_ms=to_ms)
    counts = Counter(classes.values())
    log_fn(
        f"market classes ({thresholds}): "
        + ", ".join(f"{name}={counts.get(name, 0)}" for name in session_config.MARKET_CLASS_NAMES)
    )
    return {ticker for ticker, name in classes.items() if name == market_class}


def _size_plan(
    n_candidates: int,
    market_count: int,
    split_count: int,
    per_eval_seconds: float,
    budget_seconds: float,
    workers: int,
) -> Dict[str, int]:
    """Size the halving rungs so the run fits the soft wall-clock budget."""
    parallel = max(1, workers)

    def cost(n0: int, m0: int, m1: int) -> float:
        n1 = max(1, math.ceil(n0 / 3))
        n2 = max(1, math.ceil(n1 / 3))
        evaluations = (
            n0 * m0 * 1  # rung 0: one train split
            + n1 * m1 * 2  # rung 1: two train splits
            + (n2 + 1) * market_count * (2 * split_count)  # final + default A/B
            + 3 * market_count * split_count  # fill-share sensitivity rows
        )
        return evaluations * per_eval_seconds / parallel

    n0 = max(9, n_candidates)
    m0 = max(2, min(market_count, market_count // 4 or market_count))
    m1 = max(m0, min(market_count, market_count // 2 or market_count))
    for _ in range(64):
        if cost(n0, m0, m1) <= budget_seconds:
            break
        if n0 > 24:
            n0 = max(24, int(n0 * 0.8))
        elif m1 > max(2, market_count // 4):
            m1 = max(2, int(m1 * 0.8))
        elif m0 > 2:
            m0 = max(2, int(m0 * 0.8))
        else:
            break
    return {"n0": n0, "m0": m0, "m1": m1}


def _parse_date_ms(value: str) -> Optional[int]:
    """Parse a YYYY-MM-DD date or epoch-ms string into epoch ms (None if empty)."""
    value = (value or "").strip()
    if not value:
        return None
    if value.isdigit():
        return int(value)
    import datetime
    dt = datetime.datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def _tier2_data_source(args: argparse.Namespace) -> Optional[Dict[str, Any]]:
    tier = int(getattr(args, "tier", 1) or 1)
    if tier != 2:
        return None
    return {"tier": 2, "record_root": str(Path(getattr(args, "record_root", "record_data")))}


def _forward_output(text: Any) -> None:
    """Echo a child process's captured output line by line on this process's stdout."""
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    for line in str(text or "").splitlines():
        if line.strip():
            print(line, flush=True)


def run_write_back_subprocess(
    winner_file: Path,
    base_session: Optional[str],
    market_class: Optional[str],
    log_fn=log,
) -> Optional[str]:
    """Create the winner's session with ``python -m optimizer.writeback`` in a fresh interpreter.

    The child imports ``session_config``/``session_store`` from disk, so a
    schema that changed while this (hours-long) process was running cannot
    make the write-back fail on stale in-memory modules. The child's
    stdout/stderr lines are forwarded verbatim to this process's stdout — the
    optimizer chain and the dashboard parse ``write-back: created session
    '<name>'`` from there. Returns the created session's name, or ``None``
    after logging the failure and the re-run command; never raises, so a
    failed write-back cannot fail a run whose result is already persisted.
    """
    winner_file = Path(winner_file)
    workspace = Path(__file__).resolve().parents[1]
    # Resolve the store here so a relative KALSHI_SESSION_STORE names the same
    # directory for the child (cwd = workspace) as for this process.
    store_root = Path(os.environ.get("KALSHI_SESSION_STORE", "session_data")).resolve()
    command = [
        sys.executable, "-m", "optimizer.writeback",
        "--winner-file", str(winner_file.resolve()),
        "--store-root", str(store_root),
    ]
    if base_session:
        command += ["--base-session", base_session]
    if market_class:
        command += ["--class", market_class]
    hint = f"re-run: python -m optimizer.writeback --winner-file {winner_file}"
    env = dict(os.environ, PYTHONIOENCODING="utf-8")

    def failed(rc: Any, detail: str) -> None:
        log_fn(f"write-back FAILED (rc={rc}): {detail}; {hint}")
        try:
            writeback.update_winner_file(
                winner_file,
                write_back_error={"rc": rc, "detail": detail, "at_ms": int(time.time() * 1000)},
            )
        except (OSError, ValueError):
            pass

    try:
        completed = subprocess.run(
            command, cwd=str(workspace), env=env, capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=WRITEBACK_TIMEOUT_SECONDS, check=False,
        )
    except subprocess.TimeoutExpired as exc:
        _forward_output(exc.stdout)
        _forward_output(exc.stderr)
        failed("timeout", f"no result after {WRITEBACK_TIMEOUT_SECONDS:.0f}s")
        return None
    except OSError as exc:
        failed("spawn", str(exc))
        return None
    stdout, stderr = completed.stdout or "", completed.stderr or ""
    _forward_output(stdout)
    _forward_output(stderr)
    name = None
    for match in WRITEBACK_SESSION_RE.finditer(stdout):
        name = match.group(1)
    if completed.returncode == 0 and name:
        return name
    lines = [line.strip() for line in (stderr + "\n" + stdout).splitlines() if line.strip()]
    if lines:
        detail = lines[-1]
    elif completed.returncode:
        detail = "no output"
    else:
        detail = "exit 0 but no created-session line in the output"
    failed(completed.returncode, detail)
    return None


def run_optimization(args: argparse.Namespace) -> Dict[str, Any]:
    data_source = _tier2_data_source(args)
    saved_env = os.environ.get(runner.ENV_DATA_SOURCE)
    if data_source is not None:
        # Workers (spawned pools included) pick the replay source up from the env.
        os.environ[runner.ENV_DATA_SOURCE] = json.dumps(data_source, sort_keys=True)
    try:
        return _run_optimization(args, data_source)
    finally:
        if data_source is not None:
            if saved_env is None:
                os.environ.pop(runner.ENV_DATA_SOURCE, None)
            else:
                os.environ[runner.ENV_DATA_SOURCE] = saved_env


def resolve_screener_settings(args: argparse.Namespace, log_fn=log) -> Tuple[Dict[str, Any], str]:
    """Screener filter settings for the run and a label saying where they came from.

    Precedence: ``--screener-session``'s ``screener`` section, then
    ``--base-session``'s, then the ``kalshi_screener_config`` constants. A
    named session without a ``screener`` section (older schema) falls through
    with a log line rather than failing the run.
    """
    for option, name in (("--screener-session", getattr(args, "screener_session", "")),
                         ("--base-session", getattr(args, "base_session", ""))):
        if not name:
            continue
        configuration = load_base_session_configuration(name)
        settings = data_mod.screener_settings_from_configuration(configuration)
        if settings is not None:
            return settings, f"session {name!r} ({option})"
        log_fn(f"screener filter: session {name!r} has no 'screener' section; trying the next source")
    return data_mod.default_screener_settings(), "kalshi_screener_config constants"


def apply_screener_filter(
    args: argparse.Namespace,
    windows: List[data_mod.MarketWindow],
    log_fn=log,
) -> Tuple[List[data_mod.MarketWindow], Dict[str, Any]]:
    """Restrict ``windows`` to markets that would have passed the live screener.

    Returns the surviving windows (trade-count order preserved) and a
    JSON-friendly record of what was applied for the run's ``args_json``.
    """
    if not getattr(args, "screener_filter", True):
        log_fn("screener filter: disabled (--no-screener-filter)")
        return windows, {"enabled": False}
    if not windows:
        return windows, {"enabled": True, "total": 0, "passed": 0}
    settings, source = resolve_screener_settings(args, log_fn)
    data_from_ms, data_to_ms = data_mod.data_window(windows)
    checkpoints = data_mod.screener_checkpoints(
        data_from_ms, data_to_ms,
        splits=int(getattr(args, "splits", 3) or 3),
        interval_hours=float(getattr(args, "screener_eval_hours", data_mod.DEFAULT_SCREENER_EVAL_HOURS) or 0.0),
    )
    try:
        report = data_mod.screen_history_windows(args.history, settings, windows, checkpoints)
    except sqlite3.OperationalError as exc:
        # A history database without the book/trade columns (a bare test
        # fixture) cannot be screened; keep the unfiltered universe but say so.
        log_fn(f"screener filter: skipped, history database cannot be screened ({exc})")
        return windows, {"enabled": False, "error": str(exc), "settings": settings, "source": source}
    reasons = ", ".join(f"{reason}={count}" for reason, count in report.top_reasons())
    log_fn(
        f"screener filter: {len(report.passing)} of {report.total} history markets pass"
        f" (top reasons: {reasons or 'none'}) [settings from {source};"
        f" {len(checkpoints)} evaluation time(s)]"
    )
    record = {"enabled": True, "source": source, **report.summary()}
    kept = data_mod.passing_windows(report, windows)
    if not kept:
        raise SystemExit(
            f"screener filter: none of the {report.total} history markets would have passed the live"
            f" screener in the requested period (top reasons: {reasons or 'none'});"
            " use --no-screener-filter, another --screener-session, or a different period"
        )
    return kept, record


def _select_markets(
    args: argparse.Namespace,
    data_source: Optional[Dict[str, Any]],
    from_ms: Optional[int],
    to_ms: Optional[int],
    market_class: str,
) -> Tuple[List[data_mod.MarketWindow], Dict[str, Any]]:
    """Replayable (ticker, start, end) windows for the run's tier, screener and class filters.

    Also returns the screener-filter record (settings used, pass/fail counts)
    so the run can store it in ``args_json``.
    """
    if data_source is not None:
        record_root = data_source["record_root"]
        windows = data_mod.load_tier2_market_windows(record_root, from_ms=from_ms, to_ms=to_ms)
        source_label = f"recorded order books in {record_root}"
    else:
        windows = data_mod.load_market_windows(args.history, from_ms=from_ms, to_ms=to_ms)
        source_label = args.history
    # The live fleet only ever trades markets the screener lets through, so
    # the same universe applies before the class filter and the market cap.
    windows, screener_record = apply_screener_filter(args, windows)
    if market_class:
        # Classify first, then apply the market cap to the class members only.
        members = classify_markets_for_class(args.history, market_class, from_ms, to_ms)
        markets = [window for window in windows if window[0] in members][: max(0, int(args.markets)) or None]
        log(f"class {market_class!r}: {len(markets)} replayable member market(s) selected from {source_label}")
        if not markets:
            raise SystemExit(f"no replayable markets classified as {market_class!r} for the requested period")
        return markets, screener_record
    limit = max(0, int(args.markets)) or None
    markets = windows[:limit] if limit else windows
    if not markets:
        raise SystemExit(f"no replayable markets found in {source_label} for the requested period")
    return markets, screener_record


def _run_optimization(args: argparse.Namespace, data_source: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    started = time.time()
    run_id = time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
    output_dir = Path(args.output_dir)
    results_db = output_dir / "results.sqlite3"
    report_path = output_dir / f"report_{run_id}.md"
    tier = 2 if data_source is not None else 1

    if tier == 2:
        log(f"run {run_id}: Tier-2 markets from recorded order books in {data_source['record_root']}")
    else:
        log(f"run {run_id}: loading markets from {args.history}")
    from_ms = _parse_date_ms(args.from_date)
    to_ms = _parse_date_ms(args.to_date)
    if args.last_days and args.last_days > 0:
        if tier == 2:
            latest = data_mod.latest_tier2_ms(data_source["record_root"])
        else:
            latest = data_mod.latest_data_ms(args.history)
        if latest:
            from_ms = int(latest - args.last_days * 86_400_000)
    if from_ms or to_ms:
        log(f"period filter: from_ms={from_ms} to_ms={to_ms}")
    market_class = str(getattr(args, "market_class", "") or "")
    markets, screener_record = _select_markets(args, data_source, from_ms, to_ms, market_class)
    data_from_ms, data_to_ms = data_mod.data_window(markets)
    splits = walkforward.build_splits(data_from_ms, data_to_ms, k=args.splits)
    log(f"{len(markets)} markets, window {data_from_ms}..{data_to_ms}, {len(splits)} walk-forward splits")

    log(f"building tier-{tier} search space from session-exposed bot parameters")
    space = build_space(tier=tier)
    log(f"space: {len(space.dims)} searchable dims, {len(space.excluded)} excluded")

    store = results_mod.ResultsStore(str(results_db))
    run_args = dict(vars(args))
    run_args["data_source"] = data_source or {"tier": 1}
    run_args["screener_filter"] = screener_record
    store.create_run(run_id, data_from_ms, data_to_ms, markets, space.to_json(), run_args)

    import threading

    stop_heartbeat = threading.Event()

    def _heartbeat() -> None:
        while not stop_heartbeat.wait(60.0):
            try:
                store.heartbeat_run(run_id)
            except Exception:  # a missed heartbeat must never kill the run
                pass

    threading.Thread(target=_heartbeat, name="optimizer-heartbeat", daemon=True).start()

    def sink(row: Dict[str, Any]) -> None:
        store.record_candidate(run_id, row)

    base_params: Dict[str, Any] = {}
    if args.base_params_json:
        raw = json.loads(Path(args.base_params_json).read_text(encoding="utf-8"))
        unknown = sorted(set(raw) - set(space.dims))
        base_params = {name: value for name, value in raw.items() if name in space.dims}
        if unknown:
            log(f"base params: ignoring {len(unknown)} non-searchable field(s): {', '.join(unknown[:5])}")
        log(f"base params: sensitivity screens around {len(base_params)} seeded field(s)")

    # --- Stage 1: sensitivity screen (Morris OAT) on a subsample -------------
    # Screen on the NEWEST train window: short-lived markets cluster at the end
    # of the data range, so the oldest window has almost no trades and every
    # delta degenerates to zero (the run-1/run-2 all-zero-ranking bug).
    screen_markets = markets[: max(2, len(markets) // 5)]
    screen_window = (splits[-1].train_start, splits[-1].train_end)
    only_params = [name.strip() for name in str(args.only_params or "").split(",") if name.strip()]
    screen_space = space
    if only_params:
        from optimizer.space import restrict_space

        screen_space = restrict_space(space, only_params)
        log(f"targeted run: searching only {len(only_params)} field(s): {', '.join(only_params)}")
    log(f"sensitivity screen: {len(screen_space.dims)} fields on {len(screen_markets)} markets")
    ranking, per_eval_seconds = sensitivity.run_screen(
        screen_space, args.history, screen_markets, screen_window,
        args.workers, args.seed, args.fill_share, log=log,
        base_params=base_params or None,
    )
    store.record_sensitivity(run_id, ranking)
    keep_count = len(only_params) if only_params else max(1, args.top_params)
    kept_fields = [name for name, _ in ranking[:keep_count]]
    log(f"kept top {len(kept_fields)} params: {', '.join(kept_fields)}")

    # --- Stage 2: size the rungs to the wall-clock budget --------------------
    budget_seconds = max(60.0, args.budget_minutes * 60.0)
    elapsed = time.time() - started
    plan = _size_plan(
        args.candidates, len(markets), len(splits),
        per_eval_seconds, budget_seconds - elapsed, args.workers,
    )
    if plan["n0"] < args.candidates:
        log(f"budget: cutting candidates {args.candidates} -> {plan['n0']} (per-eval ~{per_eval_seconds:.3f}s)")
    log(f"plan: rung0 n={plan['n0']} markets={plan['m0']}; rung1 markets={plan['m1']}; final full data")

    # --- Stage 3: Latin-hypercube sampling ------------------------------------
    rng = random.Random(args.seed)
    candidates = search.latin_hypercube_candidates(space, kept_fields, plan["n0"], rng)
    if base_params:
        from optimizer.space import finalize_params
        # Every sample inherits the seed's values for the fields it does not
        # search, so candidates and seed differ only in the searched fields.
        candidates = search.seed_candidates_with_base(space, candidates, kept_fields, base_params)
        candidates.append(search.Candidate("c_seed_base", finalize_params(space, dict(base_params))))
        log("seeded prior winner into the candidate pool as c_seed_base; samples inherit its unsearched fields")

    # --- Stage 4: successive halving ------------------------------------------
    # Rungs audition on the NEWEST train windows first: short-lived markets
    # cluster at the end of the data range, and the oldest window scores every
    # candidate zero (no market overlap), making rung selection random noise.
    train = walkforward.train_windows(splits)
    rung_plans = [
        {"windows": [train[-1]], "markets": markets[: plan["m0"]]},
        {"windows": train[-2:], "markets": markets[: plan["m1"]]},
    ]
    survivors = search.successive_halving(
        args.history, candidates, rung_plans,
        args.workers, args.seed, args.fill_share, sink=sink, log=log,
    )

    # --- Stage 5: final stage — full data, all splits, OOS ranking ------------
    log(f"final stage: {len(survivors)} candidates on full data")
    finals = search.final_stage(
        args.history, survivors, splits, markets,
        args.workers, args.seed, args.fill_share, sink=sink, log=log,
    )
    winner = finals[0]

    # --- Durable result record --------------------------------------------------
    # Written before the remaining (slow) diagnostic stages and completed after
    # the report: whatever happens later — a crash, a failed write-back — the
    # winner can be recovered with
    #     python -m optimizer.writeback --winner-file <winner_file>
    winner_file = writeback.winner_file_path(output_dir, run_id)
    writeback.save_winner_file(winner_file, {
        "version": writeback.WINNER_FILE_VERSION,
        "run_id": run_id,
        "status": "final_stage",
        "candidate_id": winner.candidate_id,
        "params": winner.params,
        "oos_score": winner.oos_score,
        "oos_raw": winner.oos_raw,
        "train_score": winner.train_score,
        "fills": winner.fills,
        "data_from_ms": data_from_ms,
        "data_to_ms": data_to_ms,
        "market_class": market_class or None,
        "base_session": args.base_session or None,
        "write_back": bool(args.write_back),
        "tier": tier,
        "kept_fields": kept_fields,
        "results_db": str(results_db),
        "created_at_ms": int(time.time() * 1000),
    })
    log(f"winner file: {winner_file} (candidate {winner.candidate_id})")

    # --- Stage 6: A/B against the pure default --------------------------------
    default_finals = search.final_stage(
        args.history, [search.Candidate("__default__", {})], splits, markets,
        args.workers, args.seed, args.fill_share, sink=sink, log=log, stage="default_ab",
    )
    default_result = default_finals[0]

    # --- Stage 7: winner fill-share sensitivity (OOS windows) ------------------
    fill_share_rows = []
    for fraction in (0.25, 0.5, 1.0):
        scores = search.evaluate_candidates(
            args.history, f"fillshare_{fraction}", [search.Candidate(winner.candidate_id, winner.params)],
            walkforward.test_windows(splits), markets,
            args.workers, args.seed, fraction, sink=sink,
        )
        fill_share_rows.append((fraction, scores[winner.candidate_id].score))

    # --- Stage 8: fleet sizing — winner per-market OOS P&L curve ---------------
    # Ranks markets by the winner's own OOS P&L and reports the cumulative curve
    # plus how many markets each capital level can fund at the winner's budgets,
    # so maxBots and budget scale can be chosen from evidence.
    log("fleet sizing: winner per-market OOS P&L")
    per_market = []
    for ticker, start_ms, end_ms in markets:
        scores = search.evaluate_candidates(
            args.history, "fleet_sizing", [search.Candidate(winner.candidate_id, winner.params)],
            walkforward.test_windows(splits), [(ticker, start_ms, end_ms)],
            args.workers, args.seed, args.fill_share,
        )
        result = scores[winner.candidate_id]
        per_market.append({"ticker": ticker, "oos_units": result.score, "fills": result.fills})
    per_market.sort(key=lambda row: -row["oos_units"])
    cumulative = 0.0
    for index, row in enumerate(per_market, 1):
        cumulative += row["oos_units"]
        row["rank"] = index
        row["cumulative_units"] = cumulative
    best_n = max(per_market, key=lambda row: row["cumulative_units"])["rank"] if per_market else 0
    winner_budget_cents = (int(winner.params.get("yes_order_budget_cents", 100))
                           + int(winner.params.get("no_order_budget_cents", 100)))
    fleet_sizing = {
        "per_market": per_market,
        "best_market_count": best_n,
        "winner_budget_cents_per_market": winner_budget_cents,
    }

    # --- Report ----------------------------------------------------------------
    leaderboard = [
        {
            "candidate_id": f.candidate_id,
            "oos_score": f.oos_score,
            "oos_raw": f.oos_raw,
            "train_score": f.train_score,
            "fills": f.fills,
            "diff_params": diff_from_default(space, f.params),
        }
        for f in finals
    ]
    ctx = {
        "run_id": run_id,
        "data_from_ms": data_from_ms,
        "data_to_ms": data_to_ms,
        "market_count": len(markets),
        "split_count": len(splits),
        "seed": args.seed,
        "n_candidates": plan["n0"],
        "kept_fields": kept_fields,
        "min_fills": search.MIN_FILLS,
        "leaderboard": leaderboard,
        "winner": leaderboard[0],
        "default_ab": {
            "candidate_id": "__default__",
            "oos_score": default_result.oos_score,
            "train_score": default_result.train_score,
            "fills": default_result.fills,
        },
        "fill_share_rows": fill_share_rows,
        "sensitivity": ranking,
        "excluded": space.excluded,
        "fleet_sizing": fleet_sizing,
        "tier": tier,
        "data_source": data_source or {"tier": 1},
    }
    report_file = results_mod.write_report(str(report_path), ctx)
    stop_heartbeat.set()
    store.finish_run(run_id, "finished")

    # Complete the result record: the run is finished and reported, so the
    # winner file now carries everything a hand recovery would need.
    try:
        writeback.update_winner_file(
            winner_file,
            status="finished",
            finished_at_ms=int(time.time() * 1000),
            report_path=report_file,
            default_oos=default_result.oos_score,
            default_train_score=default_result.train_score,
            default_fills=default_result.fills,
            fill_share_rows=[[fraction, score] for fraction, score in fill_share_rows],
            fleet_sizing_best_market_count=best_n,
            elapsed_seconds=time.time() - started,
        )
    except (OSError, ValueError) as exc:
        log(f"winner file: could not update {winner_file}: {exc}")

    session_name = None
    if args.write_back:
        # Fresh interpreter: the session schema on disk, not the modules this
        # process imported hours ago. The child prints the created-session
        # line (forwarded below); a failure is logged and never fails the run.
        log(f"write-back: creating the session in a fresh process from {winner_file}")
        session_name = run_write_back_subprocess(winner_file, args.base_session or None, market_class or None)

    summary = {
        "run_id": run_id,
        "market_class": market_class or None,
        "winner_id": winner.candidate_id,
        "winner_params": winner.params,
        "winner_diff": diff_from_default(space, winner.params),
        "winner_oos": winner.oos_score,
        "default_oos": default_result.oos_score,
        "report_path": report_file,
        "winner_file": str(winner_file),
        "session_name": session_name,
        "elapsed_seconds": time.time() - started,
    }
    log(
        "summary: winner J={w} vs default J={d} | report {r}{s}".format(
            w=f"{winner.oos_score:,.2f}" if winner.oos_score > NEG_INF else "DQ",
            d=f"{default_result.oos_score:,.2f}" if default_result.oos_score > NEG_INF else "DQ",
            r=report_file,
            s=f" | session {session_name!r}" if session_name else "",
        )
    )
    return summary


def main(argv: List[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    run_optimization(args)
    return 0


if __name__ == "__main__":
    main()
