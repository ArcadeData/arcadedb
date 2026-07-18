#!/usr/bin/env bash
#
# Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
# SPDX-License-Identifier: Apache-2.0
#
set -euo pipefail

# Usage: smoke.sh <path-to-server-executable> [extra server args...]
#
# Boots an ArcadeDB server (the JVM server.sh or, later, the native-image binary),
# waits for HTTP readiness, then asserts SQL, Cypher, Studio and Postgres-wire
# round-trips. Reused as-is for local runs, the CI matrix, and container smoke
# tests: any argument beyond $1 is forwarded verbatim to the server process, so
# JVM-style "-Dprop=value" overrides and plugin activation flags pass through.
#
# Requires ARCADEDB_HOME to point at the distribution root (config/, lib/, bin/).

EXE="${1:?path to server executable required}"
shift || true

HOST=127.0.0.1
HTTP=2480
PG=5432
DB_USER=root
PASS="${ARCADEDB_ROOT_PASSWORD:-PlayWithData123!}"

WORK="$(mktemp -d)"
SRV_PID=""

cleanup() {
  if [ -n "$SRV_PID" ] && kill -0 "$SRV_PID" 2>/dev/null; then
    kill "$SRV_PID" 2>/dev/null || true
    wait "$SRV_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

echo "[smoke] launching: $EXE $*"
# Root every server-generated path (security config, groups, logs, databases) under the
# disposable $WORK directory instead of the caller's current directory: the server otherwise
# auto-detects its root path from cwd (see ServerPathUtils.setRootPath) and would happily create
# and reuse a "config" directory wherever this script is invoked from.
ARCADEDB_ROOT_PASSWORD="$PASS" \
  "$EXE" -Darcadedb.server.rootPassword="$PASS" \
  -Darcadedb.server.rootPath="$WORK" \
  -Darcadedb.server.logsDirectory="$WORK/log" \
  -Darcadedb.server.databaseDirectory="$WORK/databases" "$@" \
  >"$WORK/server.log" 2>&1 &
SRV_PID=$!

echo "[smoke] waiting for HTTP ready on :$HTTP"
READY=0
for i in $(seq 1 60); do
  if curl -fsS "http://$HOST:$HTTP/api/v1/ready" >/dev/null 2>&1; then
    READY=1
    break
  fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "[smoke] FAIL: server exited early"
    tail -50 "$WORK/server.log"
    exit 1
  fi
  sleep 2
done
if [ "$READY" -ne 1 ]; then
  echo "[smoke] FAIL: HTTP never ready"
  tail -50 "$WORK/server.log"
  exit 1
fi

req() { curl -fsS -u "$DB_USER:$PASS" -H 'Content-Type: application/json' "$@"; }

echo "[smoke] Studio index"
OUT="$(req "http://$HOST:$HTTP/")"
grep -qi "arcadedb" <<<"$OUT" || {
  echo "[smoke] FAIL: Studio index"
  exit 1
}

echo "[smoke] create DB"
req -X POST "http://$HOST:$HTTP/api/v1/server" -d '{"command":"create database smoke"}' >/dev/null

echo "[smoke] SQL round-trip"
req -X POST "http://$HOST:$HTTP/api/v1/command/smoke" \
  -d '{"language":"sql","command":"CREATE DOCUMENT TYPE T"}' >/dev/null
req -X POST "http://$HOST:$HTTP/api/v1/command/smoke" \
  -d '{"language":"sql","command":"INSERT INTO T SET n = 42"}' >/dev/null
OUT="$(req -X POST "http://$HOST:$HTTP/api/v1/query/smoke" -d '{"language":"sql","command":"SELECT n FROM T"}')"
grep -q '42' <<<"$OUT" || {
  echo "[smoke] FAIL: SQL, got $OUT"
  exit 1
}

echo "[smoke] Cypher round-trip"
OUT="$(req -X POST "http://$HOST:$HTTP/api/v1/command/smoke" \
  -d '{"language":"cypher","command":"CREATE (a:Person {name:\"Ada\"}) RETURN a.name AS n"}')"
grep -q 'Ada' <<<"$OUT" || {
  echo "[smoke] FAIL: Cypher, got $OUT"
  exit 1
}

echo "[smoke] Postgres-wire round-trip"
if command -v psql >/dev/null 2>&1; then
  OUT="$(PGPASSWORD="$PASS" psql -h "$HOST" -p "$PG" -U "$DB_USER" -d smoke -tAc 'SELECT 1')"
  grep -q '1' <<<"$OUT" || {
    echo "[smoke] FAIL: Postgres wire, got $OUT"
    exit 1
  }
else
  echo "[smoke] WARN: psql not installed, skipping Postgres-wire assertion"
fi

echo "[smoke] PASS"
