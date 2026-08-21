# API error monitor

Follow new Kalshi API errors across every market log in the active UI run:

```bash
./monitor_api_errors.sh
```

Check the last 200 lines of every log first, then continue following:

```bash
./monitor_api_errors.sh 200
```

Press `Ctrl-C` to stop. The optional argument is the number of existing lines
to inspect per log; it defaults to `0`, so an invocation without an argument
only prints newly received errors.

The monitor reads the current artifact directory from
`runtime/launcher_status.json`. Set `KALSHI_LAUNCHER_STATUS` if that file is in
a different location.
