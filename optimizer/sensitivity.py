"""Morris one-at-a-time sensitivity screen over the search space."""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from optimizer import search
from optimizer.space import ParamDim, SearchSpace, finalize_params

MarketWindow = Tuple[str, int, int]


def perturbed_value(dim: ParamDim, base_value: Any = None) -> Any:
    """Base value perturbed by one step (~25% of the range), staying in bounds."""
    anchor = dim.default if base_value is None else base_value
    if dim.kind == "bool":
        return not anchor
    step = 0.25 * (dim.high - dim.low)
    value = float(anchor) + step
    if value > dim.high:
        value = float(anchor) - step
    value = min(dim.high, max(dim.low, value))
    if dim.kind == "int":
        anchor_int = int(round(float(anchor)))
        value = int(round(value))
        if value == anchor_int:
            candidate_up = anchor_int + 1
            value = candidate_up if candidate_up <= dim.high else anchor_int - 1
        return int(value)
    return float(value)


def build_perturbations(
    space: SearchSpace, base_params: Optional[Dict[str, Any]] = None
) -> List[Tuple[str, Dict[str, Any]]]:
    """One-field perturbations around ``base_params`` (default config if None).

    Screening around a config that actually trades produces measurable deltas;
    the silent stock default scores zero everywhere and ranks nothing.
    """
    base = dict(base_params or {})
    out = []
    for name in sorted(space.dims):
        dim = space.dims[name]
        perturbed = dict(base)
        perturbed[name] = perturbed_value(dim, base.get(name))
        out.append((name, finalize_params(space, perturbed)))
    return out


def run_screen(
    space: SearchSpace,
    history_db: str,
    markets: Sequence[MarketWindow],
    window: Tuple[int, int],
    workers: int,
    seed: int,
    fill_share: float = 0.5,
    log: Optional[Callable[[str], None]] = None,
    base_params: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Tuple[str, float]], float]:
    """Evaluate the base config vs each one-field perturbation on the subsample.

    Returns (ranking, per_market_eval_seconds): ranking is (field, |delta J|)
    sorted descending; the timing feeds the wall-clock budget model.
    """
    started = time.perf_counter()
    perturbations = build_perturbations(space, base_params)
    baseline = finalize_params(space, dict(base_params)) if base_params else {}
    candidates = [search.Candidate("__default__", baseline)]
    candidates += [search.Candidate(f"sens::{name}", params) for name, params in perturbations]
    windows = [("screen", window[0], window[1])]
    scores = search.evaluate_candidates(
        history_db, "sensitivity", candidates, windows, markets,
        workers, seed, fill_share, log=log,
    )
    elapsed = time.perf_counter() - started
    jobs = len(search.make_jobs(markets, window[0], window[1]))
    evaluations = max(1, jobs * len(candidates))
    per_eval_seconds = elapsed / evaluations

    base = scores["__default__"].raw_mean
    deltas = [
        (name, abs(scores[f"sens::{name}"].raw_mean - base))
        for name, _ in perturbations
    ]
    deltas.sort(key=lambda item: (-item[1], item[0]))
    return deltas, per_eval_seconds
