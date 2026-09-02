"""Walk-forward train/test splits over the replayable data window."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True)
class Split:
    split_id: int
    train_start: int
    train_end: int
    test_start: int
    test_end: int


def build_splits(data_from_ms: int, data_to_ms: int, k: int = 3) -> List[Split]:
    """Partition [data_from, data_to] into k+1 sequential segments producing k
    train/test pairs where test(i) is the segment following train(i)."""
    if data_to_ms <= data_from_ms:
        raise ValueError("data window is empty")
    if k < 1:
        raise ValueError("k must be >= 1")
    segments = k + 1
    span = data_to_ms - data_from_ms
    edges = [data_from_ms + span * i // segments for i in range(segments)]
    edges.append(data_to_ms)
    return [
        Split(i, edges[i], edges[i + 1], edges[i + 1], edges[i + 2])
        for i in range(k)
    ]


def train_windows(splits: List[Split]) -> List[Tuple[str, int, int]]:
    return [(f"train{s.split_id}", s.train_start, s.train_end) for s in splits]


def test_windows(splits: List[Split]) -> List[Tuple[str, int, int]]:
    return [(f"test{s.split_id}", s.test_start, s.test_end) for s in splits]
