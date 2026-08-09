#!/usr/bin/env python3
"""Compatible command-line entrypoint for the top-of-book bot."""

import asyncio
import os
from pathlib import Path

from adaptors.kalshi import KalshiApiClient, KalshiClientConfig
from runtime_control import BotControlServer
from top_of_book_bot import (
    TopOfBookBot,
    build_settings_from_args,
    format_count_fp,
    load_market_metadata,
    log_event,
    parse_bot_args,
)


def main() -> None:
    bot_args = parse_bot_args()
    settings = build_settings_from_args(bot_args)
    client_config = KalshiClientConfig(
        api_key_id=(bot_args.api_key_id or os.getenv("KALSHI_API_KEY_ID") or os.getenv("API_KEY_ID") or "").strip(),
        private_key_path=(bot_args.private_key or os.getenv("KALSHI_PRIVATE_KEY_PATH") or os.getenv("PRIVATE_KEY_PATH") or "./privkey.txt").strip(),
        use_demo_environment=bool(bot_args.use_demo),
        dry_run=bool(bot_args.dry_run),
        subaccount_number=int(bot_args.subaccount or 0),
        post_only_quotes=settings.post_only_quotes,
        cancel_quotes_if_exchange_pauses=settings.cancel_quotes_if_exchange_pauses,
    )
    api_client = KalshiApiClient(client_config)
    market = load_market_metadata(api_client, settings.market_ticker)
    bot = TopOfBookBot(settings=settings, api_client=api_client, market=market)

    async def run_bot() -> None:
        control_server = None
        control_socket = str(getattr(bot_args, "control_socket", "") or "")
        if control_socket:
            control_server = BotControlServer(
                Path(control_socket).expanduser().resolve(),
                bot.status_snapshot,
                lambda: bot.request_shutdown(0),
            )
            await control_server.start()
        try:
            await bot.run()
        finally:
            if control_server is not None:
                await control_server.stop()

    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        log_event(
            "GRACEFUL_SHUTDOWN",
            ticker=settings.market_ticker,
            net_position_contracts=format_count_fp(bot.net_position_units),
            reason="SIGINT",
        )
        try:
            bot.cancel_owned_resting_quotes_on_startup()
        except Exception as exc:
            log_event("SHUTDOWN_CANCEL_ERROR", error=str(exc))


if __name__ == "__main__":
    main()
