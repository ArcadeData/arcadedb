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

# Usage: ha-smoke.sh <path-to-server-executable> [extra server args...]
#
# Boots a NODES-node (default 3) Raft HA cluster out of the SAME executable on loopback, waits for a
# leader, writes through the leader and reads the row back from EVERY node. This is the one check
# that proves arcadedb-ha-raft works inside the native image: Ratis builds its gRPC transport, its
# state machine and its log storage reflectively, so a binary that boots fine as a single node can
# still fail the moment two peers talk to each other.
#
# Extra arguments are forwarded verbatim to every node (JVM-style -Dprop=value overrides).
#
# Env overrides (all optional):
#   HOST       - bind/loopback address, default 127.0.0.1
#   HTTP_BASE  - HTTP port of node 0; node i uses HTTP_BASE+i, default 2480
#   RAFT_BASE  - Raft gRPC port of node 0; node i uses RAFT_BASE+i, default 2434
#   NODES      - cluster size, default 3
#   PASS       - root password, default PlayWithData123!
#   DB         - database to create and replicate, default hasmoke
#   KEEP_WORK  - "1" to keep the per-node directories and logs after the run instead of deleting them
# Both port bases must be free on this machine (a locally running ArcadeDB on 2480 / 2434 is the
# usual reason for an override), the same caveat as smoke.sh. The defaults suit CI's fixed runners;
# the script checks them up front and fails by name if one is taken.

EXE="${1:?path to server executable required}"
shift || true

HOST="${HOST:-127.0.0.1}"
HTTP_BASE="${HTTP_BASE:-2480}"
RAFT_BASE="${RAFT_BASE:-2434}"
NODES="${NODES:-3}"
DB_USER=root
PASS="${PASS:-PlayWithData123!}"
DB="${DB:-hasmoke}"
CLUSTER_TOKEN="ha-smoke-cluster-token"

WORK="$(mktemp -d)"
PIDS=()

cleanup() {
  local rc=$?
  # Logs first: after the nodes are stopped the tail is mostly shutdown noise, and a node stuck
  # shutting down must not be able to keep the failure output from ever being printed.
  if [ "$rc" -ne 0 ]; then
    for i in $(seq 0 $((NODES - 1))); do
      echo "[ha-smoke] ---- node $i log (tail) ----"
      tail -40 "$WORK/node$i.log" 2>/dev/null || true
    done
  fi
  for pid in "${PIDS[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  # Bounded: a node that ignores SIGTERM is killed after 30s instead of hanging the job until its timeout.
  for pid in "${PIDS[@]:-}"; do
    [ -n "$pid" ] || continue
    for _ in $(seq 1 30); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  if [ "${KEEP_WORK:-0}" = "1" ]; then
    echo "[ha-smoke] node directories and logs kept in $WORK"
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

http() { echo "http://$HOST:$((HTTP_BASE + $1))"; }
req() { curl -fsS -u "$DB_USER:$PASS" -H 'Content-Type: application/json' "$@"; }

# name@host:raftPort:httpPort per node. The explicit name is what lets every node find its own entry
# in the list (RaftPeerAddressResolver.findLocalPeerId), so no node-index naming convention is needed.
SERVER_LIST=""
for i in $(seq 0 $((NODES - 1))); do
  SERVER_LIST="${SERVER_LIST:+$SERVER_LIST,}n$i@$HOST:$((RAFT_BASE + i)):$((HTTP_BASE + i))"
done

# Fail fast and by name if something already listens on one of our ports: a stranger on an HTTP port
# answers this script's requests in place of the node it was meant for, which reads as an
# authentication or leader-election failure rather than as a port conflict.
for i in $(seq 0 $((NODES - 1))); do
  for port in $((HTTP_BASE + i)) $((RAFT_BASE + i)); do
    if (exec 3<>"/dev/tcp/$HOST/$port") 2>/dev/null; then
      echo "[ha-smoke] FAIL: port $port is already in use; set HTTP_BASE/RAFT_BASE to free ports"
      exit 1
    fi
  done
done

for i in $(seq 0 $((NODES - 1))); do
  mkdir -p "$WORK/node$i"
  echo "[ha-smoke] starting node $i (http $((HTTP_BASE + i)), raft $((RAFT_BASE + i)))"
  ARCADEDB_ROOT_PASSWORD="$PASS" \
    "$EXE" -Darcadedb.server.rootPassword="$PASS" \
    -Darcadedb.server.name="n$i" \
    -Darcadedb.server.rootPath="$WORK/node$i" \
    -Darcadedb.server.logsDirectory="$WORK/node$i/log" \
    -Darcadedb.server.databaseDirectory="$WORK/node$i/databases" \
    -Darcadedb.server.httpIncomingPort="$((HTTP_BASE + i))" \
    -Darcadedb.ha.enabled=true \
    -Darcadedb.ha.raftPort="$((RAFT_BASE + i))" \
    -Darcadedb.ha.serverList="$SERVER_LIST" \
    -Darcadedb.ha.clusterToken="$CLUSTER_TOKEN" \
    -Dorg.jline.terminal.dumb=true "$@" \
    >"$WORK/node$i.log" 2>&1 &
  PIDS+=("$!")
done

echo "[ha-smoke] waiting for every node to answer /ready"
for i in $(seq 0 $((NODES - 1))); do
  READY=0
  for _ in $(seq 1 90); do
    if curl -fsS "$(http "$i")/api/v1/ready" >/dev/null 2>&1; then
      READY=1
      break
    fi
    # Any node, not just the one being waited on: a node that died while an earlier one was still
    # starting would otherwise be noticed only after that earlier wait finished or timed out.
    for j in $(seq 0 $((NODES - 1))); do
      if ! kill -0 "${PIDS[$j]}" 2>/dev/null; then
        echo "[ha-smoke] FAIL: node $j exited early"
        exit 1
      fi
    done
    sleep 2
  done
  [ "$READY" -eq 1 ] || { echo "[ha-smoke] FAIL: node $i never became ready"; exit 1; }
done

# /api/v1/cluster exists only when RaftHAPlugin loaded, so a 404 here is the "module missing from the
# image" signature, reported as such instead of as a generic timeout.
echo "[ha-smoke] waiting for a ready leader"
LEADER=-1
for _ in $(seq 1 60); do
  for i in $(seq 0 $((NODES - 1))); do
    OUT="$(req "$(http "$i")/api/v1/cluster" 2>/dev/null)" || OUT=""
    if grep -q '"isLeader":true' <<<"$OUT" && grep -q '"leaderReady":true' <<<"$OUT"; then
      LEADER=$i
      break 2
    fi
  done
  sleep 2
done
if [ "$LEADER" -lt 0 ]; then
  echo "[ha-smoke] FAIL: no ready Raft leader (is arcadedb-ha-raft in the image? /api/v1/cluster answers 404 if not)"
  exit 1
fi
echo "[ha-smoke] leader is node $LEADER"

echo "[ha-smoke] create database $DB and write through the leader"
req -X POST "$(http "$LEADER")/api/v1/server" -d "{\"command\":\"create database $DB\"}" >/dev/null
req -X POST "$(http "$LEADER")/api/v1/command/$DB" \
  -d '{"language":"sql","command":"CREATE DOCUMENT TYPE HaSmoke"}' >/dev/null
req -X POST "$(http "$LEADER")/api/v1/command/$DB" \
  -d '{"language":"sql","command":"INSERT INTO HaSmoke SET n = 4242"}' >/dev/null

echo "[ha-smoke] reading the row back from every node"
for i in $(seq 0 $((NODES - 1))); do
  FOUND=0
  for _ in $(seq 1 30); do
    OUT="$(req -X POST "$(http "$i")/api/v1/query/$DB" \
      -d '{"language":"sql","command":"SELECT n FROM HaSmoke"}' 2>/dev/null)" || OUT=""
    if grep -q '4242' <<<"$OUT"; then
      FOUND=1
      break
    fi
    sleep 1
  done
  [ "$FOUND" -eq 1 ] || { echo "[ha-smoke] FAIL: node $i never saw the replicated row (got: ${OUT:-<none>})"; exit 1; }
  echo "[ha-smoke] node $i has the replicated row"
done

echo "[ha-smoke] PASS"
