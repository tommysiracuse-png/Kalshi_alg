"""Stable bounded-load assignment of market actors to worker shards."""

from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from typing import Iterable, Mapping, Optional

from core.fleet_models import DEFAULT_SHARD_SIZE, MAX_WORKERS


def derive_worker_count(max_bots: int, shard_size: int = DEFAULT_SHARD_SIZE) -> int:
    if max_bots < 1:
        raise ValueError("max_bots must be positive")
    if shard_size < 1:
        raise ValueError("shard_size must be positive")
    count = math.ceil(max_bots / shard_size)
    if count > MAX_WORKERS:
        raise ValueError(f"requested capacity needs {count} workers; maximum is {MAX_WORKERS}")
    return count


def _score(ticker: str, worker_id: str) -> int:
    digest = hashlib.blake2b(f"{ticker}\0{worker_id}".encode("utf-8"), digest_size=16).digest()
    return int.from_bytes(digest, "big")


def assign_markets(
    tickers: Iterable[str],
    *,
    worker_count: int,
    shard_size: int = DEFAULT_SHARD_SIZE,
    previous: Optional[Mapping[str, str]] = None,
) -> dict[str, tuple[str, ...]]:
    """Assign tickers using rendezvous scores while retaining valid prior owners.

    Keeping valid prior assignments makes screener refreshes stable. New markets use
    rendezvous hashing and spill to their next-best shard only when a shard is full.
    """

    if not 1 <= worker_count <= MAX_WORKERS:
        raise ValueError(f"worker_count must be between 1 and {MAX_WORKERS}")
    if shard_size < 1:
        raise ValueError("shard_size must be positive")
    normalized = tuple(sorted({str(ticker).strip() for ticker in tickers if str(ticker).strip()}))
    if len(normalized) > worker_count * shard_size:
        raise ValueError("market count exceeds aggregate shard capacity")

    worker_ids = tuple(f"worker-{index:02d}" for index in range(worker_count))
    assignments: dict[str, list[str]] = defaultdict(list)
    placed: set[str] = set()
    previous = previous or {}

    # Retention is deterministic and capacity bounded even if prior state is corrupt.
    for ticker in normalized:
        worker_id = previous.get(ticker)
        if worker_id in worker_ids and len(assignments[worker_id]) < shard_size:
            assignments[worker_id].append(ticker)
            placed.add(ticker)

    for ticker in normalized:
        if ticker in placed:
            continue
        ranked = sorted(worker_ids, key=lambda item: (_score(ticker, item), item), reverse=True)
        worker_id = next(item for item in ranked if len(assignments[item]) < shard_size)
        assignments[worker_id].append(ticker)

    return {worker_id: tuple(sorted(assignments[worker_id])) for worker_id in worker_ids}
