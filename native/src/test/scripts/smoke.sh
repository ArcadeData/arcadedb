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

# The actual HTTP-readiness wait plus Studio/SQL/Cypher/Postgres-wire (and, opportunistically,
# Redis/Bolt/Mongo/gRPC) assertions live in exercise.sh so trace.sh can replay the identical
# checks against a server it starts itself under the GraalVM native-image tracing agent.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST="$HOST" HTTP="$HTTP" PG="$PG" DB_USER="$DB_USER" PASS="$PASS" \
  SRV_PID="$SRV_PID" SERVER_LOG="$WORK/server.log" \
  "$SCRIPT_DIR/exercise.sh"

echo "[smoke] PASS"
