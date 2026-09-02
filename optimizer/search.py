"""Latin-hypercube sampling, successive halving, and OOS ranking."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from optimizer import runner
from optimizer.space import SearchSpace, finalize_params, is_valid_bot_overrides, sample_value

MIN_FILLS = 30  # statistical guard: fewer summed fills than this disqualifies
NEG_INF = float("-inf")

Window = Tuple[str, int, int]  # (split label, start_ms, end_ms)
MarketWindow = Tuple[str, int, int]


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    params: Dict[str, Any]


@dataclass
class StageScore:
    candidate_id: str
    score: float  # mean over split windows, -inf when disqualified
    raw_mean: float
    split_scores: Dict[str, float]
    fills: int
    contracts: float
    max_drawdown: float
    errors: int


@dataclass
class FinalScore:
    candidate_id: str
    params: Dict[str, Any]
    oos_score: float  # mean over TEST windows only, -inf when disqualified
    oos_raw: float
    train_score: float
    fills: int
    errors: int


def latin_hypercube_candidates(
    space: SearchSpace,
    kept_names: Sequence[str],
    n: int,
    rng: random.Random,
    prefix: str = "c",
) -> List[Candidate]:
    """Stratified per-dimension permutation LHS over the kept parameters."""
    kept = [space.dims[name] for name in kept_names]
    columns: Dict[str, List[float]] = {}
    for dim in kept:
        permutation = list(range(n))
        rng.shuffle(permutation)
        columns[dim.name] = [(permutation[i] + rng.random()) / n for i in range(n)]
    candidates: List[Candidate] = []
    for i in range(n):
        params = finalize_params(
            space, {dim.name: sample_value(dim, columns[dim.name][i]) for dim in kept}
        )
        if not is_valid_bot_overrides(params):
            repaired = None
            for _ in range(25):
                trial = finalize_params(
                    space, {dim.name: sample_value(dim, rng.random()) for dim in kept}
                )
                if is_valid_bot_overrides(trial):
                    repaired = trial
                    break
            params = repaired if repaired is not None else finalize_params(space, {})
        candidates.append(Candidate(f"{prefix}{i:04d}", params))
    return candidates


def seed_candidates_with_base(
    space: SearchSpace,
    candidates: Sequence[Candidate],
    kept_names: Sequence[str],
    base_params: Dict[str, Any],
) -> List[Candidate]:
    """Give every sampled candidate the base values for the fields it does not search.

    Without this a candidate only carries its sampled fields and every other
    field silently falls back to the schema default, so a run seeded from a
    tuned session compares "seed with 54 tuned fields" against "sample with
    20 sampled fields + 34 defaults" - the seed wins for the wrong reason.
    """
    kept = set(kept_names)
    background = {name: value for name, value in base_params.items() if name not in kept and name in space.dims}
    if not background:
        return list(candidates)
    seeded: List[Candidate] = []
    for candidate in candidates:
        merged = finalize_params(space, {**background, **candidate.params})
        seeded.append(Candidate(candidate.candidate_id, merged if is_valid_bot_overrides(merged) else dict(candidate.params)))
    return seeded


def make_jobs(markets: Sequence[MarketWindow], start_ms: int, end_ms: int) -> List[Tuple[str, int, int]]:
    jobs = []
    for ticker, market_start, market_end in markets:
        start = max(market_start, start_ms)
        end = min(market_end, end_ms)
        if end > start:
            jobs.append((ticker, start, end))
    return jobs


def evaluate_candidates(
    history_db: str,
    stage: str,
    candidates: Sequence[Candidate],
    windows: Sequence[Window],
    markets: Sequence[MarketWindow],
    workers: int,
    seed: int,
    fill_share: float = 0.5,
    sink: Optional[Callable[[Dict[str, Any]], None]] = None,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, StageScore]:
    """Evaluate every candidate on every window; score = mean over windows of
    the summed-over-markets total_net_units."""
    params_by_id = {c.candidate_id: c.params for c in candidates}
    tasks: List[runner.Task] = []
    for candidate in candidates:
        for label, window_start, window_end in windows:
            jobs = make_jobs(markets, window_start, window_end)
            tasks.append(
                (history_db, candidate.candidate_id, stage, label, candidate.params, jobs, seed, fill_share)
            )

    best_so_far = {"score": NEG_INF}
    partial: Dict[str, List[Dict[str, Any]]] = {}

    def progress(done: int, total: int, result: Dict[str, Any]) -> None:
        partial.setdefault(result["candidate_id"], []).append(result)
        rows = partial[result["candidate_id"]]
        if len(rows) == len(windows):
            mean_score = sum(r["score"] for r in rows) / len(rows)
            if mean_score > best_so_far["score"]:
                best_so_far["score"] = mean_score
        if log and (done % 25 == 0 or done == total):
            best = best_so_far["score"]
            best_text = f"{best:.2f}" if best > NEG_INF else "n/a"
            log(f"stage={stage} done={done}/{total} best-so-far J={best_text}")
        if sink:
            sink(
                {
                    "candidate_id": result["candidate_id"],
                    "params": params_by_id[result["candidate_id"]],
                    "stage": stage,
                    "split_id": result["split_id"],
                    "score": result["score"],
                    "fills": result["fills"],
                    "contracts": result["contracts"],
                    "max_drawdown": result["max_drawdown"],
                    "errors": result["errors"],
                }
            )

    raw = runner.run_tasks(tasks, workers, progress=progress)

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for result in raw:
        grouped.setdefault(result["candidate_id"], []).append(result)
    scores: Dict[str, StageScore] = {}
    for candidate_id in sorted(grouped):
        rows = sorted(grouped[candidate_id], key=lambda row: str(row["split_id"]))
        split_scores = {row["split_id"]: row["score"] for row in rows}
        raw_mean = sum(split_scores.values()) / len(split_scores)
        fills = sum(row["fills"] for row in rows)
        score = NEG_INF if fills < MIN_FILLS else raw_mean
        scores[candidate_id] = StageScore(
            candidate_id=candidate_id,
            score=score,
            raw_mean=raw_mean,
            split_scores=split_scores,
            fills=fills,
            contracts=sum(row["contracts"] for row in rows),
            max_drawdown=max(row["max_drawdown"] for row in rows),
            errors=sum(row["errors"] for row in rows),
        )
    return scores


def _rank(candidates: Sequence[Candidate], scores: Dict[str, StageScore]) -> List[Candidate]:
    return sorted(
        candidates,
        key=lambda c: (-scores[c.candidate_id].score, c.candidate_id),
    )


def successive_halving(
    history_db: str,
    candidates: List[Candidate],
    rung_plans: Sequence[Dict[str, Any]],
    workers: int,
    seed: int,
    fill_share: float = 0.5,
    sink: Optional[Callable[[Dict[str, Any]], None]] = None,
    log: Optional[Callable[[str], None]] = None,
    keep_fraction: float = 1.0 / 3.0,
) -> List[Candidate]:
    """Run the screening rungs; each rung keeps the top ``keep_fraction``.

    rung_plans: [{"windows": [...], "markets": [...]}, ...] in rung order.
    Returns the survivors after the last screening rung.
    """
    survivors = list(candidates)
    for rung_index, plan in enumerate(rung_plans):
        stage = f"rung{rung_index}"
        if log:
            log(f"stage={stage} candidates={len(survivors)} markets={len(plan['markets'])} windows={len(plan['windows'])}")
        scores = evaluate_candidates(
            history_db, stage, survivors, plan["windows"], plan["markets"],
            workers, seed, fill_share, sink=sink, log=log,
        )
        ordered = _rank(survivors, scores)
        keep = max(1, math.ceil(len(ordered) * keep_fraction))
        survivors = ordered[:keep]
        if log:
            top = scores[survivors[0].candidate_id].score
            top_text = f"{top:.2f}" if top > NEG_INF else "disqualified"
            log(f"stage={stage} kept={len(survivors)} best J={top_text}")
    return survivors


def final_stage(
    history_db: str,
    candidates: Sequence[Candidate],
    splits: Sequence[Any],
    markets: Sequence[MarketWindow],
    workers: int,
    seed: int,
    fill_share: float = 0.5,
    sink: Optional[Callable[[Dict[str, Any]], None]] = None,
    log: Optional[Callable[[str], None]] = None,
    stage: str = "final",
) -> List[FinalScore]:
    """Evaluate on every train and test window; rank by mean over TEST windows only."""
    windows: List[Window] = []
    for split in splits:
        windows.append((f"train{split.split_id}", split.train_start, split.train_end))
        windows.append((f"test{split.split_id}", split.test_start, split.test_end))
    scores = evaluate_candidates(
        history_db, stage, candidates, windows, markets, workers, seed, fill_share,
        sink=sink, log=log,
    )
    params_by_id = {c.candidate_id: c.params for c in candidates}
    finals: List[FinalScore] = []
    for candidate_id, stage_score in scores.items():
        test_scores = [v for k, v in stage_score.split_scores.items() if k.startswith("test")]
        train_scores = [v for k, v in stage_score.split_scores.items() if k.startswith("train")]
        oos_raw = sum(test_scores) / len(test_scores) if test_scores else NEG_INF
        train_mean = sum(train_scores) / len(train_scores) if train_scores else NEG_INF
        oos = NEG_INF if stage_score.fills < MIN_FILLS else oos_raw
        finals.append(
            FinalScore(
                candidate_id=candidate_id,
                params=params_by_id[candidate_id],
                oos_score=oos,
                oos_raw=oos_raw,
                train_score=train_mean,
                fills=stage_score.fills,
                errors=stage_score.errors,
            )
        )
    finals.sort(key=lambda f: (-f.oos_score, f.candidate_id))
    return finals
