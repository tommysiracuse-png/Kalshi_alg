#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"

if [ -x /usr/bin/npm ]; then
  NPM_BIN=/usr/bin/npm
elif command -v npm >/dev/null 2>&1; then
  NPM_BIN=$(command -v npm)
else
  echo "npm is required to install/build the operations UI" >&2
  exit 127
fi

if [ -x /usr/bin/node ]; then
  NODE_BIN=/usr/bin/node
elif command -v node >/dev/null 2>&1; then
  NODE_BIN=$(command -v node)
else
  echo "node is required to run the operations UI" >&2
  exit 127
fi

# A fresh checkout (or a deployment that pruned node_modules) has no `next`
# executable. Install from the lockfile before asking npm to run the build;
# otherwise npm reports the misleading `next: not found` and systemd enters a
# restart loop. Include development dependencies because Next's production
# build compiles the TypeScript application.
if [ ! -x "$ROOT/node_modules/.bin/next" ]; then
  echo "UI dependencies are missing; running npm ci" >&2
  "$NPM_BIN" ci --include=dev --no-audit --no-fund
fi

# The systemd unit runs the standalone server directly.  A source checkout or
# an interrupted deployment may not have a standalone artifact yet, so make
# the first start self-healing instead of entering a restart loop with
# MODULE_NOT_FOUND.
if [ ! -s "$ROOT/.next/standalone/server.js" ]; then
  # A build killed during a deploy can leave Next's advisory lock behind.
  # There is no other build owned by this service while this script is
  # running, so clear that stale marker before retrying.
  rm -f "$ROOT/.next/lock"
  "$NPM_BIN" run build
fi

if [ ! -s "$ROOT/.next/standalone/server.js" ]; then
  echo "production build did not create .next/standalone/server.js" >&2
  exit 1
fi

# Next's standalone output does not include these static assets. Keep the
# standalone tree synchronized with the build that produced server.js; without
# this, the HTML references valid hashes but the server returns text/plain 404s
# for every CSS and browser chunk.
rm -rf "$ROOT/.next/standalone/.next/static"
cp -R "$ROOT/.next/static" "$ROOT/.next/standalone/.next/static"
if [ -d "$ROOT/public" ]; then
  rm -rf "$ROOT/.next/standalone/public"
  cp -R "$ROOT/public" "$ROOT/.next/standalone/public"
fi

exec "$NODE_BIN" "$ROOT/.next/standalone/server.js"
