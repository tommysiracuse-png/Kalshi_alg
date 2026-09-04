#!/usr/bin/env python3
"""Independent systemd post-stop cleanup for a sharded fleet."""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

from adaptors.kalshi import KalshiApiClient, KalshiClientConfig
from fleet_runtime.execution import cancel_and_verify_owned_orders
from session_store import SessionStore


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _write_receipt(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main() -> int:
    started_at_ms = int(time.time() * 1000)
    workspace = Path(__file__).resolve().parent
    runtime_dir = Path(os.getenv("KALSHI_RUNTIME_DIR", str(workspace / "runtime"))).expanduser()
    receipt_path = runtime_dir / "shutdown_cleanup.json"
    receipt: dict[str, object] = {
        "state": "running", "source": "systemd-post-stop", "startedAtMs": started_at_ms,
        "completedAtMs": None, "canceledOrders": 0, "ordersVerifiedAbsent": False,
        "remainingBotOrderIds": [], "workersStopped": True, "brokerStopped": True,
        "error": None,
    }
    _write_receipt(receipt_path, receipt)
    client: KalshiApiClient | None = None
    exit_code = 0
    try:
        api_key_id = (os.getenv("KALSHI_API_KEY_ID") or os.getenv("API_KEY_ID") or "").strip()
        private_key_path = (
            os.getenv("KALSHI_PRIVATE_KEY_PATH") or os.getenv("PRIVATE_KEY_PATH") or ""
        ).strip()
        if not api_key_id or not private_key_path:
            raise RuntimeError("Kalshi credentials are unavailable to the post-stop cleanup")
        client = KalshiApiClient(KalshiClientConfig(
            api_key_id=api_key_id,
            private_key_path=private_key_path,
            use_demo_environment=_truthy(os.getenv("KALSHI_USE_DEMO", "0")),
            subaccount_number=int(os.getenv("KALSHI_SUBACCOUNT", "0") or 0),
            enable_shared_write_rate_limiter=False,
        ))
        receipt["canceledOrders"] = cancel_and_verify_owned_orders(
            client, attempts=8, delay_seconds=0.5,
        )
        receipt["ordersVerifiedAbsent"] = True
        receipt["state"] = "verified"
    except Exception as exc:
        receipt["state"] = "failed"
        receipt["error"] = str(exc)
        receipt["remainingBotOrderIds"] = list(getattr(exc, "remaining_order_ids", ()))
        exit_code = 1
    finally:
        if client is not None:
            try:
                asyncio.run(client.close())
            except Exception:
                pass
        receipt["completedAtMs"] = int(time.time() * 1000)
        _write_receipt(receipt_path, receipt)

    try:
        session_root = Path(os.getenv("KALSHI_SESSION_STORE", str(workspace / "session_data")))
        sessions = SessionStore(session_root)
        active = sessions.active_run()
        if active:
            status = "stopped" if receipt["state"] == "verified" else "shutdown_failed"
            sessions.finish_run(
                str(active["id"]), status,
                error=None if status == "stopped" else str(receipt.get("error") or "post-stop cleanup failed"),
            )
    except Exception as exc:
        receipt["sessionFinalizationError"] = str(exc)
        _write_receipt(receipt_path, receipt)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
