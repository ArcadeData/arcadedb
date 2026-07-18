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

# Usage: exercise.sh
#
# Waits for HTTP readiness, then drives Studio, SQL, Cypher and every wire protocol against an
# ALREADY-RUNNING ArcadeDB server. Extracted out of smoke.sh so the exact same assertions can be
# replayed against a server launched under the GraalVM native-image tracing agent (trace.sh):
# the agent only records what the instrumented JVM actually does, so smoke.sh and trace.sh must
# drive identical HTTP/SQL/Cypher/wire-protocol traffic against the process they each start.
#
# Every wire-protocol check below is best-effort: if the corresponding plugin was not enabled on
# this server run (the port never opens) or the local machine lacks the client tool, the check
# prints a WARN and is skipped rather than failing the script. This keeps smoke.sh's assertions
# (Studio/SQL/Cypher/Postgres) unchanged while letting trace.sh, which enables every wire plugin,
# exercise the extra protocols opportunistically.
#
# Env overrides (all optional, defaults match smoke.sh / GlobalConfiguration defaults):
#   HOST, HTTP, PG, REDIS_PORT, MONGO_PORT, BOLT_PORT, GRPC_PORT, DB_USER, PASS, DB
#   SRV_PID    - if set and the process dies while waiting for HTTP readiness, fail fast
#   SERVER_LOG - if set, tailed on failure

HOST="${HOST:-127.0.0.1}"
HTTP="${HTTP:-2480}"
PG="${PG:-5432}"
REDIS_PORT="${REDIS_PORT:-6379}"
MONGO_PORT="${MONGO_PORT:-27017}"
BOLT_PORT="${BOLT_PORT:-7687}"
GRPC_PORT="${GRPC_PORT:-50051}"
DB_USER="${DB_USER:-root}"
PASS="${PASS:-PlayWithData123!}"
DB="${DB:-smoke}"

EX_WORK="$(mktemp -d)"
trap 'rm -rf "$EX_WORK"' EXIT

echo "[exercise] waiting for HTTP ready on :$HTTP"
READY=0
for i in $(seq 1 60); do
  if curl -fsS "http://$HOST:$HTTP/api/v1/ready" >/dev/null 2>&1; then
    READY=1
    break
  fi
  if [ -n "${SRV_PID:-}" ] && ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "[exercise] FAIL: server exited early"
    [ -n "${SERVER_LOG:-}" ] && tail -50 "$SERVER_LOG"
    exit 1
  fi
  sleep 2
done
if [ "$READY" -ne 1 ]; then
  echo "[exercise] FAIL: HTTP never ready"
  [ -n "${SERVER_LOG:-}" ] && tail -50 "$SERVER_LOG"
  exit 1
fi

req() { curl -fsS -u "$DB_USER:$PASS" -H 'Content-Type: application/json' "$@"; }

echo "[exercise] Studio index"
OUT="$(req "http://$HOST:$HTTP/")" || true
grep -qi "arcadedb" <<<"$OUT" || {
  echo "[exercise] FAIL: Studio index"
  exit 1
}

echo "[exercise] create DB"
req -X POST "http://$HOST:$HTTP/api/v1/server" -d "{\"command\":\"create database $DB\"}" >/dev/null

echo "[exercise] SQL round-trip"
req -X POST "http://$HOST:$HTTP/api/v1/command/$DB" \
  -d '{"language":"sql","command":"CREATE DOCUMENT TYPE T"}' >/dev/null
req -X POST "http://$HOST:$HTTP/api/v1/command/$DB" \
  -d '{"language":"sql","command":"INSERT INTO T SET n = 42"}' >/dev/null
OUT="$(req -X POST "http://$HOST:$HTTP/api/v1/query/$DB" -d '{"language":"sql","command":"SELECT n FROM T"}')" || true
grep -q '42' <<<"$OUT" || {
  echo "[exercise] FAIL: SQL, got $OUT"
  exit 1
}

echo "[exercise] Cypher round-trip"
OUT="$(req -X POST "http://$HOST:$HTTP/api/v1/command/$DB" \
  -d '{"language":"cypher","command":"CREATE (a:Person {name:\"Ada\"}) RETURN a.name AS n"}')" || true
grep -q 'Ada' <<<"$OUT" || {
  echo "[exercise] FAIL: Cypher, got $OUT"
  exit 1
}

# GraalJS ships embedded in the native image (see docs/native-image.md); this is a hard assertion,
# not a best-effort WARN-skip, so a regression that drops JS from the image (or from the JVM
# build) fails the build loudly rather than silently degrading.
echo "[exercise] JS round-trip"
OUT="$(req -X POST "http://$HOST:$HTTP/api/v1/command/$DB" \
  -d '{"language":"js","command":"40 + 2"}')" || true
grep -q '"value":42' <<<"$OUT" || {
  echo "[exercise] FAIL: JS, got $OUT"
  exit 1
}

echo "[exercise] Postgres-wire round-trip"
if command -v psql >/dev/null 2>&1; then
  OUT="$(PGPASSWORD="$PASS" psql -h "$HOST" -p "$PG" -U "$DB_USER" -d "$DB" -tAc 'SELECT 1')" || true
  grep -q '1' <<<"$OUT" || {
    echo "[exercise] FAIL: Postgres wire, got $OUT"
    exit 1
  }
else
  echo "[exercise] WARN: psql not installed, skipping Postgres-wire assertion"
fi

# Opens fd $1 to $2:$3 for raw read/write. Returns non-zero (without aborting under `set -e`,
# since callers only ever invoke this as an `if`/`while` condition) when the port is closed -
# e.g. because the corresponding wire plugin was not enabled on this server run.
tcp_connect() {
  eval "exec $1<>/dev/tcp/$2/$3" 2>/dev/null
}

# Reads whatever arrives on fd $1 into file $2, for at most ~5s, then kills the reader. Deliberately
# uses `cat` rather than `head -c <n>`: `head -c` only writes its output once it has read exactly
# <n> bytes (or hit EOF), so killing it mid-wait for a short reply (e.g. Redis's 7-byte "+PONG\r\n"
# against a generous byte budget) discards everything it had buffered - `cat` copies each read()
# straight through, so a SIGTERM after the deadline still leaves whatever arrived on disk. Avoids a
# hard dependency on GNU `timeout`, which stock macOS does not ship.
read_fd_with_deadline() {
  local fd="$1" outfile="$2"
  ( cat <&"$fd" >"$outfile" 2>/dev/null ) &
  local pid=$!
  for _ in 1 2 3 4 5; do
    kill -0 "$pid" 2>/dev/null || return 0
    sleep 1
  done
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

echo "[exercise] Redis PING (port $REDIS_PORT)"
if tcp_connect 3 "$HOST" "$REDIS_PORT"; then
  # ArcadeDB's Redis wrapper only accepts RESP-encoded commands (no inline-command support), so
  # PING must be sent as the RESP array *1\r\n$4\r\nPING\r\n rather than a bare "PING\r\n".
  printf '*1\r\n$4\r\nPING\r\n' >&3
  RESP_FILE="$EX_WORK/redis.resp"
  read_fd_with_deadline 3 "$RESP_FILE"
  exec 3<&- 3>&- 2>/dev/null || true
  if grep -qi 'PONG' "$RESP_FILE" 2>/dev/null; then
    echo "[exercise] Redis PING -> PONG"
  else
    echo "[exercise] WARN: Redis PING did not return PONG (got: $(cat "$RESP_FILE" 2>/dev/null))"
  fi
else
  echo "[exercise] WARN: Redis port $REDIS_PORT not reachable (plugin not enabled?), skipping"
fi

echo "[exercise] Bolt handshake (port $BOLT_PORT)"
if tcp_connect 4 "$HOST" "$BOLT_PORT"; then
  # Magic bytes + up to 4 proposed versions (big-endian int32: [unused(8)][range(8)][minor(8)][major(8)]):
  # 5.4, 4.4, 3.0, then zero padding. See BoltNetworkExecutor.SUPPORTED_VERSIONS.
  printf '\x60\x60\xb0\x17\x00\x00\x04\x05\x00\x00\x04\x04\x00\x00\x00\x03\x00\x00\x00\x00' >&4
  RESP_FILE="$EX_WORK/bolt.resp"
  read_fd_with_deadline 4 "$RESP_FILE"
  exec 4<&- 4>&- 2>/dev/null || true
  if command -v xxd >/dev/null 2>&1; then
    NEGOTIATED="$(xxd -p "$RESP_FILE" 2>/dev/null | tr -d '\n')" || true
    if [ -n "$NEGOTIATED" ] && [ "$NEGOTIATED" != "00000000" ]; then
      echo "[exercise] Bolt negotiated version bytes: $NEGOTIATED"
    else
      echo "[exercise] WARN: Bolt handshake did not negotiate a version (got: ${NEGOTIATED:-<empty>})"
    fi
  else
    echo "[exercise] WARN: xxd not installed, skipping Bolt negotiation decode"
  fi
else
  echo "[exercise] WARN: Bolt port $BOLT_PORT not reachable (plugin not enabled?), skipping"
fi

echo "[exercise] Mongo hello (port $MONGO_PORT)"
if command -v python3 >/dev/null 2>&1 && tcp_connect 5 "$HOST" "$MONGO_PORT"; then
  exec 5<&- 5>&- 2>/dev/null || true
  # python3's socket module builds the OP_MSG {hello:1} handshake by hand (no bson/pymongo
  # dependency available on this machine) so this stays a plain stdlib call.
  if python3 "$(dirname "${BASH_SOURCE[0]}")/mongo_hello.py" "$HOST" "$MONGO_PORT"; then
    echo "[exercise] Mongo hello -> reply received"
  else
    echo "[exercise] WARN: Mongo hello did not return a reply"
  fi
else
  echo "[exercise] WARN: Mongo port $MONGO_PORT not reachable or python3 missing, skipping"
fi

echo "[exercise] gRPC reflection list (port $GRPC_PORT)"
if command -v grpcurl >/dev/null 2>&1; then
  if GRPC_OUT="$(grpcurl -plaintext -connect-timeout 3 -max-time 5 "$HOST:$GRPC_PORT" list 2>&1)"; then
    echo "[exercise] gRPC services: $(tr '\n' ' ' <<<"$GRPC_OUT")"
  else
    echo "[exercise] WARN: gRPC reflection list failed (plugin not enabled?): $GRPC_OUT"
  fi
else
  echo "[exercise] WARN: grpcurl not installed, skipping gRPC reflection check"
fi

echo "[exercise] PASS"
