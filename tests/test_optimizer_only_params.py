"""Targeted runs (--only-params) and seed inheritance for sampled candidates."""

import random

import pytest

from optimizer import main as optimizer_main
from optimizer import search
from optimizer.space import FAIR_MID, FAIR_TICKER, FAIR_TRADE, build_space, restrict_space

TOX_FIELDS = [
    "default_toxicity_cents",
    "bucket_pessimism_enabled",
    "bucket_pessimism_max_cents",
    "minimum_expected_edge_cents_to_quote",
]


def test_restrict_space_keeps_only_the_named_fields_in_order():
    space = build_space(tier=1)
    restricted = restrict_space(space, TOX_FIELDS)
    assert list(restricted.dims) == TOX_FIELDS
    assert restricted.defaults is space.defaults
    assert restricted.tier == space.tier
    assert restricted.excluded["minimum_milliseconds_between_requotes"] == "not in --only-params"
    assert "default_toxicity_cents" not in restricted.excluded


def test_restrict_space_rejects_pinned_or_unknown_fields():
    space = build_space(tier=1)
    with pytest.raises(ValueError, match="not searchable"):
        restrict_space(space, ["default_toxicity_cents", "orderbook_pull_side_cooldown_ms"])
    with pytest.raises(ValueError, match="no_such_field"):
        restrict_space(space, ["no_such_field"])


def test_only_params_argument_parses_to_a_list_in_main():
    args = optimizer_main.build_arg_parser().parse_args(["--only-params", "a, b,c"])
    assert [name.strip() for name in args.only_params.split(",") if name.strip()] == ["a", "b", "c"]
    assert optimizer_main.build_arg_parser().parse_args([]).only_params == ""


def test_sampled_candidates_inherit_unsearched_fields_from_the_seed():
    space = build_space(tier=1)
    kept = ["default_toxicity_cents", "minimum_expected_edge_cents_to_quote"]
    base = {
        "default_toxicity_cents": 3,
        "minimum_expected_edge_cents_to_quote": 5,
        "minimum_milliseconds_between_requotes": 516,
        "trade_history_window_seconds": 38,
        FAIR_MID: 0.38,
        FAIR_TICKER: 0.08,
        FAIR_TRADE: 0.54,
        "not_a_field": 1,
    }
    sampled = search.latin_hypercube_candidates(space, kept, 6, random.Random(3))
    assert all(set(candidate.params) <= set(kept) | {FAIR_TRADE} for candidate in sampled)

    seeded = search.seed_candidates_with_base(space, sampled, kept, base)

    assert [c.candidate_id for c in seeded] == [c.candidate_id for c in sampled]
    for before, after in zip(sampled, seeded):
        # searched fields keep their sampled values...
        for name in kept:
            assert after.params[name] == before.params[name]
        # ...unsearched fields come from the seed, unknown keys are dropped.
        assert after.params["minimum_milliseconds_between_requotes"] == 516
        assert after.params["trade_history_window_seconds"] == 38
        assert "not_a_field" not in after.params
        assert abs(after.params[FAIR_MID] + after.params[FAIR_TICKER] + after.params[FAIR_TRADE] - 1.0) < 1e-9


def test_seeding_without_base_is_a_no_op():
    space = build_space(tier=1)
    sampled = search.latin_hypercube_candidates(space, ["default_toxicity_cents"], 3, random.Random(1))
    assert search.seed_candidates_with_base(space, sampled, ["default_toxicity_cents"], {}) == sampled
