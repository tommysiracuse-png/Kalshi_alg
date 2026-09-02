"""Spawn-safe parallel evaluation runner.

The worker function is module-level so multiprocessing spawn can pickle it.
``replay.driver`` is imported lazily inside the worker so the optimizer's own
tests can stub the evaluator (module attribute for in-process runs, the
OPTIMIZER_EVALUATOR env var — "module:function" — for cross-process pools).
"""

from __future__ import annotations

import importlib
import json
import os
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

ENV_EVALUATOR = "OPTIMIZER_EVALUATOR"
# JSON replay data source (e.g. {"tier": 2, "record_root": "record_data"});
# inherited by spawned pool workers. Unset/empty -> the evaluator's default
# (Tier 1) and no ``data_source`` keyword is passed at all.
ENV_DATA_SOURCE = "OPTIMIZER_DATA_SOURCE"

# Tests may monkeypatch this with a callable matching the replay contract.
EVALUATOR_OVERRIDE: Optional[Callable[..., Dict[str, Any]]] = None

# Task tuple layout:
# (history_db, candidate_id, stage, split_id, params, jobs, seed, fill_share[, data_source])
# where jobs is a sequence of (ticker, start_ms, end_ms) and the optional
# 9th element overrides the OPTIMIZER_DATA_SOURCE environment variable.
Task = Tuple[str, str, str, str, Dict[str, Any], Sequence[Tuple[str, int, int]], int, float]


def _resolve_evaluator() -> Callable[..., Dict[str, Any]]:
    if EVALUATOR_OVERRIDE is not None:
        return EVALUATOR_OVERRIDE
    spec = os.environ.get(ENV_EVALUATOR, "").strip()
    if spec:
        module_name, _, function_name = spec.partition(":")
        module = importlib.import_module(module_name)
        return getattr(module, function_name)
    from replay.driver import evaluate_candidate  # built in parallel; lazy on purpose

    return evaluate_candidate


def resolve_data_source() -> Optional[Dict[str, Any]]:
    spec = os.environ.get(ENV_DATA_SOURCE, "").strip()
    if not spec:
        return None
    parsed = json.loads(spec)
    return dict(parsed) if isinstance(parsed, dict) and parsed else None


def evaluate_task(task: Task) -> Dict[str, Any]:
    """Evaluate one candidate over a list of (ticker, window) jobs; sums P&L."""
    history_db, candidate_id, stage, split_id, params, jobs, seed, fill_share = task[:8]
    data_source = task[8] if len(task) > 8 and task[8] else resolve_data_source()
    extra_kwargs = {"data_source": dict(data_source)} if data_source else {}
    evaluator = _resolve_evaluator()
    score = 0.0
    fills = 0
    contracts = 0.0
    max_drawdown = 0.0
    errors = 0
    evaluated = 0
    for ticker, start_ms, end_ms in jobs:
        try:
            result = evaluator(
                history_db,
                ticker,
                dict(params),
                int(start_ms),
                int(end_ms),
                seed=int(seed),
                fill_share_fraction=float(fill_share),
                **extra_kwargs,
            )
        except Exception:
            errors += 1
            continue
        if not isinstance(result, dict) or result.get("error"):
            errors += 1
            continue
        evaluated += 1
        score += float(result.get("total_net_units") or 0.0)
        fills += int(result.get("fills") or 0)
        contracts += float(result.get("contracts_units") or 0.0)
        max_drawdown = max(max_drawdown, abs(float(result.get("max_drawdown_units") or 0.0)))
    return {
        "candidate_id": candidate_id,
        "stage": stage,
        "split_id": split_id,
        "score": score,
        "fills": fills,
        "contracts": contracts,
        "max_drawdown": max_drawdown,
        "errors": errors,
        "evaluated": evaluated,
    }


def run_tasks(
    tasks: List[Task],
    workers: int,
    progress: Optional[Callable[[int, int, Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    """Run tasks serially (workers<=1) or on a spawn Pool, streaming results."""
    total = len(tasks)
    results: List[Dict[str, Any]] = []
    if workers <= 1 or total <= 1:
        for index, task in enumerate(tasks):
            result = evaluate_task(task)
            results.append(result)
            if progress:
                progress(index + 1, total, result)
        return results
    import multiprocessing as mp

    context = mp.get_context("spawn")
    with context.Pool(processes=workers) as pool:
        for index, result in enumerate(pool.imap_unordered(evaluate_task, tasks, chunksize=1)):
            results.append(result)
            if progress:
                progress(index + 1, total, result)
    return results


def default_workers() -> int:
    import multiprocessing as mp

    return max(1, mp.cpu_count() - 2)
