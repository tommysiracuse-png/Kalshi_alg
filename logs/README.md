# Sample Logs

These two files are **examples only** to show the format the bot and the
watchdog write at runtime. They are not a full session and not curated for
performance — just representative output.

- `SAMPLE_bot.log` — output from `V1.py` for a single market (quotes, fills,
  EV decisions, gate rejections, cancels/places).
- `SAMPLE_watchdog.log` — output from `market_watchdog_runner.py` for the
  same market (periodic risk re-profiling, mode transitions).

The full `logs/` directory at runtime contains one `<TICKER>.log` and one
`<TICKER>.watchdog.log` per active market. Those are git-ignored.
