#!/usr/bin/env python3
"""Opt-in live connectivity check for Polymarket's configured transports.

The command is intentionally public-data only.  It uses the same
``PolymarketClient`` construction as the launcher, so a successful run proves
that Gamma, CLOB bulk books, and the market WebSocket use the configured
Polymarket proxy.  Credentials are never accepted on the command line.

Example::

    POLYMARKET_PROXY_URL=http://127.0.0.1:18080 \
      .venv/bin/python tools/test_polymarket_proxy.py
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

# Running ``python tools/test_polymarket_proxy.py`` puts ``tools`` (rather
# than the repository root) on sys.path. Match the root CLI entrypoints so the
# validator works without an extra PYTHONPATH export.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from adaptors.polymarket import PolymarketClient, PolymarketClientConfig
from clients.models import MarketQuery


def _redact_url(value: str) -> str:
    if not value:
        return "direct"
    parsed = urlsplit(value)
    host = parsed.hostname or ""
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def _redact_text(value: object, proxy: str) -> str:
    text = str(value)
    if proxy:
        text = text.replace(proxy, _redact_url(proxy))
    return text


async def _run(args: argparse.Namespace) -> int:
    proxy = args.proxy if args.proxy is not None else os.getenv("POLYMARKET_PROXY_URL", "")
    config = PolymarketClientConfig(
        proxy_url=proxy,
        public_only=True,
        rest_timeout_seconds=args.timeout,
        rest_connect_timeout_seconds=min(args.timeout, 10.0),
        book_bootstrap_timeout_seconds=args.timeout,
        catalog_path="",
        book_cache_path="",
    )
    client = PolymarketClient(config)
    print(f"Proxy: {_redact_url(client.proxy_url or '')}")
    print(f"Gamma: {config.gamma_base_url} (trust_env=False)")
    print(f"CLOB: {config.clob_base_url} (trust_env=False)")
    print(f"Market WebSocket: {config.websocket_url}")
    try:
        markets = client.list_markets(MarketQuery(status="open", page_size=1, max_results=1))
        if not markets:
            print("FAIL Gamma returned no binary YES/NO market", file=sys.stderr)
            return 1
        market = markets[0]
        print(f"PASS Gamma REST market={market.market_id}")

        books = client.hydrate_market_books([market], allow_rest=True)
        if market.market_id not in books:
            print("FAIL CLOB REST did not return both token books", file=sys.stderr)
            return 1
        print(f"PASS CLOB REST market={market.market_id}")

        bootstrapped = await client.bootstrap_market_books(
            [market], timeout_seconds=args.timeout
        )
        expected = sum(1 for token in (market.yes_token_id, market.no_token_id) if token)
        ready = sum(1 for token in (market.yes_token_id, market.no_token_id) if token in bootstrapped)
        if ready < expected:
            print(f"FAIL market WebSocket snapshots ready={ready}/{expected}", file=sys.stderr)
            return 1
        print(f"PASS market WebSocket snapshots ready={ready}/{expected}")
        return 0
    except Exception as exc:
        print(f"FAIL {_redact_text(exc, client.proxy_url or '')}", file=sys.stderr)
        return 1
    finally:
        await client.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--proxy",
        default=None,
        help="Polymarket-only HTTP(S)/SOCKS proxy; defaults to POLYMARKET_PROXY_URL. Use --proxy '' for direct mode.",
    )
    parser.add_argument("--timeout", type=float, default=15.0, help="Per-stage timeout in seconds.")
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
