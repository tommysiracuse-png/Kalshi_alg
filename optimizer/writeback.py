"""Save the winning configuration as a NEW session via the existing SessionStore.

Two entry points:

* ``write_back(...)`` — the in-process function (tests, the dashboard and
  other callers use it directly; its signature is stable).
* ``python -m optimizer.writeback --winner-file <path>`` — the CLI.
  ``optimizer.main --write-back`` runs it in a FRESH subprocess so the
  session is always validated with the ``session_config`` schema on disk: a
  long run keeps the modules it imported at start in memory, and when the
  schema gained a field while the run was in flight (2026-09-02 incident,
  ``fleetRuntime.allocationOversubscription``) the in-process write-back
  could not read any saved session and crashed the process after the run had
  already finished.

Manual recovery — the run finished (report written, ``opt_runs.status`` is
``finished``) but no session was created; the result is in the winner file
``optimizer.main`` persists right after the final stage::

    .venv\\Scripts\\python.exe -m optimizer.writeback --winner-file runtime/optimizer/winner_<run_id>.json

The winner file already records the base session, market class and scores,
so no other argument is needed. ``--base-session NAME``, ``--class NAME`` and
``--store-root PATH`` override the recorded values (``--base-session ""``
drops the base session and starts from the schema defaults); the default
store is ``$KALSHI_SESSION_STORE`` or ``session_data``. On success the CLI
prints ``write-back: created session '<name>'`` (the line the optimizer
chain and the dashboard parse) and records the session under
``write_back_result`` in the winner file; a missing winner file or base
session exits 2, any other failure exits 1 with the traceback on stderr.
Re-running the CLI after a success creates another session (names carry a
minute stamp and fall back to a run-id suffix on conflict).
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

NEG_INF = float("-inf")
WINNER_FILE_VERSION = 1
WINNER_FILE_REQUIRED = ("run_id", "params", "data_from_ms", "data_to_ms")


def winner_file_path(output_dir: str | Path, run_id: str) -> Path:
    """``<output_dir>/winner_<run_id>.json`` — the durable record of a run's result."""
    return Path(output_dir) / f"winner_{run_id}.json"


def _json_safe(value: Any) -> Any:
    """Copy ``value`` with non-finite floats (``-inf`` = disqualified) replaced by ``None``.

    Standard JSON has no infinity; ``load_winner_file`` maps ``None`` scores
    back to ``-inf`` so callers keep the optimizer's convention.
    """
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def save_winner_file(path: str | Path, record: Mapping[str, Any]) -> Path:
    """Write ``record`` as JSON to ``path`` atomically (temp file + replace)."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(_json_safe(record))
    payload.setdefault("version", WINNER_FILE_VERSION)
    temp = target.with_name(target.name + ".tmp")
    temp.write_text(json.dumps(payload, indent=1, sort_keys=True, default=str), encoding="utf-8")
    os.replace(temp, target)
    return target


def update_winner_file(path: str | Path, **fields: Any) -> Dict[str, Any]:
    """Merge ``fields`` into the winner file at ``path`` and return the new record."""
    target = Path(path)
    record = json.loads(target.read_text(encoding="utf-8"))
    record.update(fields)
    save_winner_file(target, record)
    return record


def load_winner_file(path: str | Path) -> Dict[str, Any]:
    """Read and sanity-check a winner file; ``None`` scores come back as ``-inf``."""
    target = Path(path)
    record = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(record, dict):
        raise ValueError("winner file must hold a JSON object")
    missing = [key for key in WINNER_FILE_REQUIRED if key not in record]
    if missing:
        raise ValueError(f"winner file is missing field(s): {', '.join(missing)}")
    if not isinstance(record["params"], dict):
        raise ValueError("winner file field 'params' must be an object")
    for key in ("oos_score", "train_score", "oos_raw", "default_oos"):
        if key in record and record[key] is None:
            record[key] = NEG_INF
    record.setdefault("oos_score", NEG_INF)
    return record


def find_session_configuration(name: str, store_root: Optional[str | Path] = None) -> Optional[Dict[str, Any]]:
    """Configuration of the saved session called ``name`` (archived included), or ``None``.

    The store is ``store_root``, else ``$KALSHI_SESSION_STORE``, else
    ``session_data`` — the same lookup ``optimizer.main`` uses for
    ``--base-session``.
    """
    from session_store import SessionStore

    store = SessionStore(Path(store_root or os.environ.get("KALSHI_SESSION_STORE", "session_data")))
    for session in store.list_sessions(include_archived=True):
        if session["name"] == name:
            return store.get_session(session["id"])["configuration"]
    return None


def write_back(
    run_id: str,
    winner_params: Dict[str, Any],
    oos_score: float,
    data_from_ms: int,
    data_to_ms: int,
    store_root: Optional[str] = None,
    market_class: Optional[str] = None,
    base_configuration: Optional[Mapping[str, Any]] = None,
    base_session_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new session holding the winner config. Never touches selection.

    With ``market_class`` the winner is stored as that class's ``botClasses``
    overrides (only fields that differ from the base ``bot`` defaults) with
    classification enabled, so the base section keeps its defaults and the
    tuned values apply exclusively to markets classified into that class.

    ``base_configuration`` (a saved session's configuration) is the starting
    point instead of the schema defaults: every field the optimizer did not
    search — order-book pull guards and other Tier-1-pinned settings, fleet
    size, refresh interval — keeps the operator's value, so the new session is
    usable as-is rather than needing a hand merge.
    """
    import session_config
    from session_store import SessionConflictError, SessionStore

    root = Path(store_root or os.environ.get("KALSHI_SESSION_STORE", "session_data"))
    store = SessionStore(root)

    # Per-side budgets are launcher-managed (injected into each bot at launch),
    # so they never appear in configuration["bot"]. Without this mapping the
    # winner's sizing — often the highest-impact thing a run discovers — would
    # be silently dropped from the saved session.
    MANAGED_TO_LAUNCHER = {
        "yes_order_budget_cents": "yesBudgetCents",
        "no_order_budget_cents": "noBudgetCents",
    }

    if market_class is not None and market_class not in session_config.MARKET_CLASS_NAMES:
        raise ValueError(f"unknown market class: {market_class}")

    if base_configuration is not None:
        configuration = session_config.validate_session_configuration(copy.deepcopy(dict(base_configuration)))
    else:
        configuration = session_config.default_session_configuration()
    unmapped: list[str] = []
    overrides: list[Dict[str, Any]] = []
    for key, value in winner_params.items():
        if key in configuration["bot"]:
            if market_class is None:
                configuration["bot"][key] = value
            elif value != configuration["bot"][key]:
                overrides.append({"field": key, "value": value})
        elif key in MANAGED_TO_LAUNCHER:
            # Budgets cannot be overridden per class; they stay fleet-wide.
            configuration["launcher"][MANAGED_TO_LAUNCHER[key]] = int(value)
        else:
            unmapped.append(key)
    if unmapped:
        print(f"[writeback] warning: {len(unmapped)} winner param(s) had no home "
              f"in the session schema and were dropped: {', '.join(sorted(unmapped))}")
    if market_class is not None:
        configuration["botClasses"]["enabled"] = True
        configuration["botClasses"][market_class]["overrides"] = overrides
        if any(key in MANAGED_TO_LAUNCHER for key in winner_params):
            print(f"[writeback] note: budget params apply to the whole fleet, not only class {market_class}")

    stamp = time.strftime('%Y%m%d-%H%M')
    name = f"optimized-{market_class}-{stamp}" if market_class else f"optimized-{stamp}"
    description = (
        f"optimizer run {run_id}, window {data_from_ms}..{data_to_ms} ms, "
        f"OOS J={oos_score:.2f}"
        + (f", class {market_class} ({len(overrides)} override(s))" if market_class else "")
        + (f", unsearched fields from session '{base_session_name}'" if base_session_name else "")
    )[:500]
    payload = {"name": name, "description": description, "configuration": configuration}
    try:
        return store.create_session(payload)
    except SessionConflictError:
        payload["name"] = f"{name}-{run_id[:8]}"
        return store.create_session(payload)


# --- CLI: python -m optimizer.writeback --winner-file <path> -------------------

def created_session_line(name: str, market_class: Optional[str] = None) -> str:
    """The stdout line the optimizer chain and the dashboard parse for the new session."""
    return f"write-back: created session '{name}'" + (f" (class {market_class})" if market_class else "")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m optimizer.writeback",
        description="Create the session for a finished optimizer run from its winner file "
                    "(runtime/optimizer/winner_<run_id>.json). Runs in its own process so the "
                    "session schema on disk is the one used; also the manual recovery when "
                    "optimizer.main's --write-back step failed.",
    )
    parser.add_argument("--winner-file", required=True,
                        help="winner_<run_id>.json written by optimizer.main after the final stage")
    parser.add_argument("--base-session", default=None,
                        help="saved session whose configuration supplies every unsearched field "
                             "(default: the winner file's base_session; \"\" = none, schema defaults)")
    parser.add_argument("--class", dest="market_class", default=None,
                        help="store the winner as this market class's botClasses overrides "
                             "(default: the winner file's market_class; \"\" = none)")
    parser.add_argument("--store-root", default=None,
                        help="session store directory (default: $KALSHI_SESSION_STORE or session_data)")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    path = Path(args.winner_file)
    try:
        record = load_winner_file(path)
    except (OSError, ValueError) as exc:
        print(f"write-back: cannot load winner file {path}: {exc}", file=sys.stderr, flush=True)
        return 2

    base_session = record.get("base_session") if args.base_session is None else (args.base_session or None)
    market_class = record.get("market_class") if args.market_class is None else (args.market_class or None)
    store_root = args.store_root or os.environ.get("KALSHI_SESSION_STORE") or "session_data"

    base_configuration = None
    if base_session:
        base_configuration = find_session_configuration(base_session, store_root)
        if base_configuration is None:
            print(f"write-back: no saved session named {base_session!r} in store {store_root}",
                  file=sys.stderr, flush=True)
            return 2
        print(f"write-back: unsearched fields taken from session {base_session!r}", flush=True)

    session = write_back(
        str(record["run_id"]), dict(record["params"]), float(record["oos_score"]),
        int(record["data_from_ms"]), int(record["data_to_ms"]),
        store_root=str(store_root), market_class=market_class,
        base_configuration=base_configuration, base_session_name=base_session,
    )
    name = str(session["name"])
    try:
        update_winner_file(path, write_back_result={
            "session_name": name, "session_id": session.get("id"),
            "store_root": str(Path(store_root).resolve()), "created_at_ms": int(time.time() * 1000),
        })
    except (OSError, ValueError) as exc:
        print(f"[writeback] warning: session {name!r} created but the winner file was not updated: {exc}",
              file=sys.stderr, flush=True)
    print(created_session_line(name, market_class), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
