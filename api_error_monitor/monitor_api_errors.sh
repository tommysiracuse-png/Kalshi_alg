#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATUS_FILE="${KALSHI_LAUNCHER_STATUS:-$ROOT/runtime/launcher_status.json}"
HISTORY_LINES="${1:-0}"

if [[ "$HISTORY_LINES" == "-h" || "$HISTORY_LINES" == "--help" ]]; then
  echo "Usage: $0 [HISTORY_LINES_PER_LOG]"
  echo "Example: $0 200"
  exit 0
fi
if [[ ! "$HISTORY_LINES" =~ ^[0-9]+$ ]]; then
  echo "Error: history must be a non-negative number of lines." >&2
  exit 2
fi

RUN_DIR="$(python3 - "$STATUS_FILE" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    status = json.load(handle)
print((status.get("run") or {}).get("artifactPath") or "")
PY
)"

if [[ -z "$RUN_DIR" || ! -d "$RUN_DIR" ]]; then
  echo "Error: no active run artifact directory was found in $STATUS_FILE" >&2
  exit 1
fi

mapfile -d '' LOG_FILES < <(find "$RUN_DIR" -type f -name '*.log' -print0)
if (( ${#LOG_FILES[@]} == 0 )); then
  echo "Error: no log files were found under $RUN_DIR" >&2
  exit 1
fi

echo "Monitoring ${#LOG_FILES[@]} logs in the active run (Ctrl-C to stop)..."
tail -n "$HISTORY_LINES" --follow=name --retry "${LOG_FILES[@]}" 2>/dev/null \
  | awk '
      /^==> .* <==$/ {
        source = $0
        sub(/^==> /, "", source)
        sub(/ <==$/, "", source)
        count = split(source, parts, "/")
        source = parts[count]
        next
      }
      {
        line = tolower($0)
        if (line ~ /(get|post|put|patch|delete) \/trade-api\/.*failed [45][0-9][0-9]/ ||
            line ~ /httpclienterror|too[_ ]many[_ ]requests|rate[_ -]?limit(ed)?|ws_error/ ||
            line ~ /header_timestamp|invalid_content_type|unauthorized|forbidden/) {
          printf "[%s] %s\n", source, $0
          fflush()
        }
      }
    '
