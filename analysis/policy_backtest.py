"""Counterfactual policy backtester for the YES/NO asymmetry problem.

Reads every telemetry_*.sqlite3 in the workspace, joins fills with their
markouts, and replays a set of policies that decide per-fill kept_size.

Output: stdout summary table + analysis/policy_results.csv

A "policy" is a pure function: f(fill_features) -> kept_size_contracts in
[0, original_size]. We can ONLY scale a fill DOWN; we have no way to
counterfactually create fills that didn't happen. Policies that do not take
the fill at all return 0.

Realized P&L per fill at horizon H (model-free, marked to future market mid):

    yes_buy:  net_c = (future_mid_yes - fill_price) / 100 * kept_size - fee_pro_rata
    no_buy:   net_c = ((10000 - future_mid_yes) - fill_price) / 100 * kept_size - fee_pro_rata

Fee is pro-rated by kept_size / original_size (lower size = lower fee).
"""

from __future__ import annotations

import csv
import glob
import math
import os
import sqlite3
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

PRICE_SCALE = 10000  # 5100 -> 51.00 cents
COUNT_SCALE = 100    # 1100 -> 11 contracts
HORIZONS = (1000, 5000, 30000, 120000)


@dataclass
class FillRow:
    ticker: str
    ts_ms: int
    side: str
    fill_price_units: int
    size_units: int
    fee_units: int
    inventory_before_units: int
    fair_yes_before_units: Optional[int]
    best_yes_bid_units: Optional[int]
    best_no_bid_units: Optional[int]
    spread_units: Optional[int]            # from co-located quote
    toxicity_units: Optional[int]          # from co-located quote
    queue_ahead_units: Optional[int]       # from co-located quote
    bucket_key: Optional[str]              # from markouts (any horizon -- same)
    # markouts at each horizon: future_mid_yes_units. None if missing.
    future_mid: Dict[int, Optional[int]]

    @property
    def size_contracts(self) -> float:
        return self.size_units / COUNT_SCALE

    def markout_per_contract_c(self, horizon_ms: int) -> Optional[float]:
        """Cents per contract at horizon (signed, model-free)."""
        fm = self.future_mid.get(horizon_ms)
        if fm is None:
            return None
        if self.side == "yes":
            return (fm - self.fill_price_units) / 100.0
        else:
            return ((PRICE_SCALE - fm) - self.fill_price_units) / 100.0

    def fee_per_contract_c(self) -> float:
        if self.size_units <= 0:
            return 0.0
        return (self.fee_units / 100.0) / max(1.0, self.size_contracts)


# ---------------- Loader ----------------

def load_fills(db_path: str) -> List[FillRow]:
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2)
    except Exception:
        return []
    try:
        n_fills = con.execute("SELECT COUNT(*) FROM fills").fetchone()[0]
        if n_fills == 0:
            return []

        # Pull fills
        fill_rows = con.execute(
            """
            SELECT fill_key, ts_ms, ticker, side, price_units, size_units, fee_units,
                   inventory_before_units, fair_yes_before_units,
                   best_yes_bid_units, best_no_bid_units
            FROM fills
            """
        ).fetchall()
        if not fill_rows:
            return []

        # Pull all markouts grouped by fill_key
        mk_by_key: Dict[str, Dict[int, int]] = defaultdict(dict)
        bucket_by_key: Dict[str, str] = {}
        for fk, h, fm, bk in con.execute(
            "SELECT fill_key, horizon_ms, future_mid_yes_units, bucket_key FROM markouts"
        ):
            if fm is not None:
                mk_by_key[fk][int(h)] = int(fm)
            if bk and fk not in bucket_by_key:
                bucket_by_key[fk] = bk

        # Pull quotes within +-500ms of each fill ts on the same side as the fill
        # (cheap: index by side+ts bucket)
        quote_index: Dict[Tuple[str, int], List[Tuple[int, int, int, int]]] = defaultdict(list)
        # (side, ts_ms_bucket=second) -> list of (ts_ms, spread, toxicity, queue_ahead)
        for ts, side, sp, tox, qa in con.execute(
            "SELECT ts_ms, side, spread_units, toxicity_units, queue_ahead_units FROM quotes"
        ):
            if ts is None:
                continue
            sec = ts // 1000
            quote_index[(side, sec)].append(
                (
                    int(ts),
                    int(sp) if sp is not None else 0,
                    int(tox) if tox is not None else 0,
                    int(qa) if qa is not None else 0,
                )
            )
    finally:
        con.close()

    out: List[FillRow] = []
    for (
        fill_key, ts_ms, ticker, side, price_u, size_u, fee_u, inv_b, fair_b,
        bb_y, bb_n,
    ) in fill_rows:
        # Find nearest quote on same side within +-1.5s
        spread = toxicity = queue_ahead = None
        sec0 = ts_ms // 1000
        candidates = []
        for sec_off in (-1, 0, 1):
            candidates.extend(quote_index.get((side, sec0 + sec_off), ()))
        if candidates:
            best = min(candidates, key=lambda x: abs(x[0] - ts_ms))
            if abs(best[0] - ts_ms) <= 1500:
                _, spread, toxicity, queue_ahead = best
        out.append(
            FillRow(
                ticker=ticker,
                ts_ms=int(ts_ms),
                side=side,
                fill_price_units=int(price_u),
                size_units=int(size_u),
                fee_units=int(fee_u or 0),
                inventory_before_units=int(inv_b or 0),
                fair_yes_before_units=int(fair_b) if fair_b is not None else None,
                best_yes_bid_units=int(bb_y) if bb_y is not None else None,
                best_no_bid_units=int(bb_n) if bb_n is not None else None,
                spread_units=spread,
                toxicity_units=toxicity,
                queue_ahead_units=queue_ahead,
                bucket_key=bucket_by_key.get(fill_key),
                future_mid={h: mk_by_key.get(fill_key, {}).get(h) for h in HORIZONS},
            )
        )
    return out


def load_all_fills(workspace_dir: str) -> List[FillRow]:
    fills: List[FillRow] = []
    for db in glob.glob(os.path.join(workspace_dir, "telemetry_*.sqlite3")):
        fills.extend(load_fills(db))
    return fills


# ---------------- Policies ----------------
# Each policy: (FillRow) -> kept_size_contracts

def _depth_proxy_contracts(f: FillRow) -> int:
    """A *proxy* for resting depth at top on the side we joined.
    Best available signal in our recorded data: (queue_ahead_units / COUNT_SCALE).
    queue_ahead = contracts ahead of our resting order at our price level. When
    the bot has just posted, that's the size of any pre-existing resting line
    (i.e., depth ahead of us, which is what the depth-guard cared about).
    None means we have no evidence -> treat as 0 (worst case for thin-top check).
    """
    if f.queue_ahead_units is None:
        return 0
    return int(f.queue_ahead_units // COUNT_SCALE)


def _toxicity_c(f: FillRow) -> float:
    if f.toxicity_units is None:
        return 1.0
    return f.toxicity_units / 100.0


# Policy P0 -- pre-regression: depth>=15 both sides, cap=25 both sides.
def policy_P0(f: FillRow) -> float:
    if _depth_proxy_contracts(f) < 15:
        return 0.0
    return min(f.size_contracts, 25.0)


# Policy P1 -- regressed running config: depth>=3 both sides, cap=100 both sides.
def policy_P1(f: FillRow) -> float:
    if _depth_proxy_contracts(f) < 3:
        return 0.0
    return min(f.size_contracts, 100.0)


# Policy P2 -- naive asymmetric: depth>=15 both, YES cap 15, NO cap 100.
def policy_P2(f: FillRow) -> float:
    if _depth_proxy_contracts(f) < 15:
        return 0.0
    cap = 15.0 if f.side == "yes" else 100.0
    return min(f.size_contracts, cap)


# P3 -- toxicity-skewed reservation: only KEEP fills whose claimed edge net of
# a per-side-bucket signed-tox prior is >= threshold. This emulates raising
# the side-fair down by the per-bucket adverse drift. Cap=100.
# We approximate signed-tox as the bucket's mean *historical* markout of fills
# we've already seen on this same bucket; computed externally (see runner).
def make_policy_P3(signed_drift_per_bucket: Dict[Tuple[str, str], float]) -> Callable[[FillRow], float]:
    def policy(f: FillRow) -> float:
        if _depth_proxy_contracts(f) < 3:
            return 0.0
        bk = f.bucket_key or "unknown"
        drift_c = signed_drift_per_bucket.get((f.side, bk), 0.0)  # cents/ctr, signed
        # Recompute a model-free claimed edge at the moment of fill:
        if f.fair_yes_before_units is None:
            claimed_edge_c = 0.0
        else:
            if f.side == "yes":
                claimed_edge_c = (f.fair_yes_before_units - f.fill_price_units) / 100.0
            else:
                claimed_edge_c = ((PRICE_SCALE - f.fair_yes_before_units) - f.fill_price_units) / 100.0
        # Subtract historical signed-drift adjustment (drift<0 means side has been losing in this bucket).
        adj_edge_c = claimed_edge_c + drift_c  # drift is signed: bleeding bucket -> drift<0 -> adj_edge shrinks
        if adj_edge_c < 1.0:
            return 0.0
        return min(f.size_contracts, 100.0)
    return policy


# P4 -- depth-conditional max size: keep cap=100 ceiling, but max_size <= 0.5 * depth_ahead.
def policy_P4(f: FillRow) -> float:
    depth = _depth_proxy_contracts(f)
    if depth < 3:
        return 0.0
    return min(f.size_contracts, 100.0, 0.5 * depth)


# P5 -- asymmetric edge thresholds: YES needs >=4c claimed edge, NO needs >=2c.
def policy_P5(f: FillRow) -> float:
    if _depth_proxy_contracts(f) < 15:
        return 0.0
    if f.fair_yes_before_units is None:
        return min(f.size_contracts, 25.0)
    if f.side == "yes":
        edge_c = (f.fair_yes_before_units - f.fill_price_units) / 100.0
        thr = 4.0
    else:
        edge_c = ((PRICE_SCALE - f.fair_yes_before_units) - f.fill_price_units) / 100.0
        thr = 2.0
    if edge_c < thr:
        return 0.0
    return min(f.size_contracts, 25.0)


# P6 -- P3 + P4 combined.
def make_policy_P6(signed_drift_per_bucket: Dict[Tuple[str, str], float]) -> Callable[[FillRow], float]:
    p3 = make_policy_P3(signed_drift_per_bucket)
    def policy(f: FillRow) -> float:
        size_via_p3 = p3(f)
        if size_via_p3 == 0.0:
            return 0.0
        size_via_p4 = policy_P4(f)
        return min(size_via_p3, size_via_p4)
    return policy


# P7 -- P6 + per-bucket Kelly sizing.
# fraction = max(0, mu_c) / max(eps, var_c) per (side, bucket_key)
# We compute mu and var of per-fill markout-per-contract empirically.
def make_policy_P7(
    signed_drift_per_bucket: Dict[Tuple[str, str], float],
    var_per_bucket: Dict[Tuple[str, str], float],
) -> Callable[[FillRow], float]:
    p6 = make_policy_P6(signed_drift_per_bucket)
    def policy(f: FillRow) -> float:
        base = p6(f)
        if base == 0.0:
            return 0.0
        bk = f.bucket_key or "unknown"
        mu = signed_drift_per_bucket.get((f.side, bk), 0.0)
        var = var_per_bucket.get((f.side, bk), 4.0)  # default var = 4 c^2 (sd=2c)
        if mu <= 0:
            return 0.0
        kelly = max(0.0, mu) / max(1e-3, var)
        # Map kelly fraction (often <<1) to a cap multiplier. Clamp [0.1, 1.0].
        scale = max(0.1, min(1.0, kelly))
        return min(base, base * scale + 1.0)  # +1 to avoid pinching to 0 when scale is tiny
    return policy


# ---------------- Empirical priors ----------------

def compute_signed_drift_per_bucket(
    fills: List[FillRow], horizon_ms: int = 5000
) -> Tuple[Dict[Tuple[str, str], float], Dict[Tuple[str, str], float]]:
    """Returns (mean_per_ctr_c, variance_per_ctr_c) per (side, bucket_key)."""
    samples: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for f in fills:
        m = f.markout_per_contract_c(horizon_ms)
        if m is None:
            continue
        bk = f.bucket_key or "unknown"
        samples[(f.side, bk)].append(m)
    mean_d: Dict[Tuple[str, str], float] = {}
    var_d: Dict[Tuple[str, str], float] = {}
    for k, xs in samples.items():
        if len(xs) >= 2:
            mean_d[k] = statistics.mean(xs)
            var_d[k] = statistics.pvariance(xs)
        elif xs:
            mean_d[k] = xs[0]
            var_d[k] = 4.0
    return mean_d, var_d


# ---------------- Backtest runner ----------------

@dataclass
class PolicyResult:
    name: str
    horizon_ms: int
    n_fills_taken: int
    contracts_taken: float
    total_net_c: float
    yes_net_c: float
    no_net_c: float
    avg_net_per_ctr: float
    yes_avg_net_per_ctr: float
    no_avg_net_per_ctr: float


def run_policy(
    name: str, policy_fn: Callable[[FillRow], float], fills: List[FillRow], horizon_ms: int
) -> PolicyResult:
    n = 0
    contracts = 0.0
    total = 0.0
    yes_net = 0.0
    no_net = 0.0
    yes_ctr = 0.0
    no_ctr = 0.0
    for f in fills:
        kept = policy_fn(f)
        if kept <= 0:
            continue
        m_per_ctr = f.markout_per_contract_c(horizon_ms)
        if m_per_ctr is None:
            continue
        # Pro-rated fee
        fee_c = f.fee_per_contract_c() * kept
        net_c = m_per_ctr * kept - fee_c
        n += 1
        contracts += kept
        total += net_c
        if f.side == "yes":
            yes_net += net_c
            yes_ctr += kept
        else:
            no_net += net_c
            no_ctr += kept
    return PolicyResult(
        name=name,
        horizon_ms=horizon_ms,
        n_fills_taken=n,
        contracts_taken=contracts,
        total_net_c=total,
        yes_net_c=yes_net,
        no_net_c=no_net,
        avg_net_per_ctr=(total / contracts) if contracts > 0 else 0.0,
        yes_avg_net_per_ctr=(yes_net / yes_ctr) if yes_ctr > 0 else 0.0,
        no_avg_net_per_ctr=(no_net / no_ctr) if no_ctr > 0 else 0.0,
    )


def main() -> int:
    workspace = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    print(f"Workspace: {workspace}", file=sys.stderr)
    fills = load_all_fills(workspace)
    print(f"Loaded {len(fills)} fills.", file=sys.stderr)

    # Coverage stats for the depth proxy (queue_ahead from co-located quote)
    have_queue = sum(1 for f in fills if f.queue_ahead_units is not None)
    have_bucket = sum(1 for f in fills if f.bucket_key is not None)
    yes_n = sum(1 for f in fills if f.side == "yes")
    no_n = len(fills) - yes_n
    print(
        f"  yes={yes_n} no={no_n}  "
        f"queue_ahead_known={have_queue}/{len(fills)}  bucket_known={have_bucket}/{len(fills)}",
        file=sys.stderr,
    )

    # Empirical priors for P3/P6/P7 -- IN-SAMPLE for now (bias caveat noted in stdout).
    drift_5s, var_5s = compute_signed_drift_per_bucket(fills, horizon_ms=5000)
    drift_30s, var_30s = compute_signed_drift_per_bucket(fills, horizon_ms=30000)
    print(
        f"  drift buckets (5s): {len(drift_5s)}; (30s): {len(drift_30s)}",
        file=sys.stderr,
    )

    policies = [
        ("P0_depth15_cap25", policy_P0),
        ("P1_depth3_cap100", policy_P1),
        ("P2_asym_yes15_no100", policy_P2),
        ("P3_tox_skew_30s", make_policy_P3(drift_30s)),
        ("P3_tox_skew_5s", make_policy_P3(drift_5s)),
        ("P4_depth_proportional", policy_P4),
        ("P5_asym_edge", policy_P5),
        ("P6_P3+P4_30s", make_policy_P6(drift_30s)),
        ("P7_P6+kelly_30s", make_policy_P7(drift_30s, var_30s)),
    ]

    results: List[PolicyResult] = []
    for name, fn in policies:
        for h in HORIZONS:
            results.append(run_policy(name, fn, fills, h))

    # Print summary tables grouped by horizon.
    print("\n" + "=" * 110)
    print("POLICY BACKTEST -- realized P&L marked to model-free future_mid_yes")
    print("=" * 110)
    for h in HORIZONS:
        print(f"\n--- horizon = {h} ms ---")
        print(
            f"{'policy':28s} {'n_fills':>8s} {'ctr':>7s} "
            f"{'total_c':>10s} {'/ctr':>7s}  "
            f"{'yes_total':>11s} {'yes/ctr':>9s}  "
            f"{'no_total':>10s} {'no/ctr':>9s}"
        )
        baseline = next(r for r in results if r.name == "P0_depth15_cap25" and r.horizon_ms == h)
        for r in [x for x in results if x.horizon_ms == h]:
            delta = r.total_net_c - baseline.total_net_c
            print(
                f"{r.name:28s} {r.n_fills_taken:>8d} {r.contracts_taken:>7.0f} "
                f"{r.total_net_c:>+10.1f} {r.avg_net_per_ctr:>+7.2f}  "
                f"{r.yes_net_c:>+11.1f} {r.yes_avg_net_per_ctr:>+9.2f}  "
                f"{r.no_net_c:>+10.1f} {r.no_avg_net_per_ctr:>+9.2f}  "
                f"d={delta:+7.1f}"
            )

    # Write CSV
    out_csv = os.path.join(workspace, "analysis", "policy_results.csv")
    with open(out_csv, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(
            [
                "policy", "horizon_ms", "n_fills", "contracts",
                "total_net_c", "avg_net_per_ctr",
                "yes_total_c", "yes_avg_per_ctr",
                "no_total_c", "no_avg_per_ctr",
            ]
        )
        for r in results:
            w.writerow(
                [
                    r.name, r.horizon_ms, r.n_fills_taken, f"{r.contracts_taken:.0f}",
                    f"{r.total_net_c:.2f}", f"{r.avg_net_per_ctr:.4f}",
                    f"{r.yes_net_c:.2f}", f"{r.yes_avg_net_per_ctr:.4f}",
                    f"{r.no_net_c:.2f}", f"{r.no_avg_net_per_ctr:.4f}",
                ]
            )
    print(f"\nWrote {out_csv}", file=sys.stderr)

    # Caveat note
    print(
        "\nNOTE: P3/P6/P7 use IN-SAMPLE bucket drifts (data leak). "
        "Out-of-sample they will be weaker; treat their gains as upper bounds.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
