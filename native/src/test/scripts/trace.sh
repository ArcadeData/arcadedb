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

# Usage: trace.sh
#
# Launches the assembled JVM distribution under the GraalVM native-image tracing agent while
# exercise.sh drives SQL, Cypher, Studio and every included wire protocol against it, so the
# agent records the reflection/resource/JNI/serialization/proxy config native-image needs
# (Task 4 consumes it; the reachability-metadata repository enabled in native/pom.xml supplies
# the well-known library configs - Netty, Lucene, gRPC/protobuf, graphql-java - so only the
# ArcadeDB-specific delta needs to be captured here).
#
# Every wire plugin ArcadeDB ships is enabled at launch so its startService()/init reflection
# is captured even without a matching client driver on this machine; exercise.sh additionally
# drives a real request against Redis/Bolt/Mongo/gRPC when possible (see its header comment).
#
# Requires: GraalVM's java (JAVA_HOME, or already first on PATH) so -agentlib:native-image-agent
# resolves, and a built JVM distribution under package/target/arcadedb-*.dir/ (run
# `mvn clean install -DskipTests` at the repo root first if it is missing).
#
# A pre-existing `brew services` ArcadeDB install binds :2480/:5432/:9998 on this machine and
# must be stopped first (`brew services stop arcadedb`) - see task instructions.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

OUT="$REPO_ROOT/native/src/main/resources/META-INF/native-image/com.arcadedb/arcadedb-native"
mkdir -p "$OUT"

# The "-headless"/"-base"/"-minimal" distribution variants under package/target do not carry
# every wire-protocol jar; pick the full distribution (bare "arcadedb-<version>.dir").
DIST_ROOT="$(ls -d "$REPO_ROOT"/package/target/arcadedb-*.dir 2>/dev/null \
  | grep -Ev -- '-(base|headless|minimal)\.dir$' | head -1)" || true
if [ -z "$DIST_ROOT" ]; then
  echo "[trace] FAIL: no full distribution found under package/target/arcadedb-*.dir" \
    "(run 'mvn clean install -DskipTests' first)"
  exit 1
fi
DIST="$(ls -d "$DIST_ROOT"/arcadedb-*/ 2>/dev/null | head -1)" || true
DIST="${DIST%/}"
if [ -z "$DIST" ] || [ ! -d "$DIST/lib" ]; then
  echo "[trace] FAIL: distribution at $DIST_ROOT has no lib/ directory"
  exit 1
fi

JAVA_BIN="java"
[ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ] && JAVA_BIN="$JAVA_HOME/bin/java"
"$JAVA_BIN" -version 2>&1 | grep -qi graalvm || {
  echo "[trace] WARN: '$JAVA_BIN' does not look like GraalVM;" \
    "-agentlib:native-image-agent requires GraalVM's java (point JAVA_HOME at it)"
}

# Overridable for a machine that already has a server on 2480, same as smoke.sh: exercise.sh would
# otherwise be answered by that other server while the instrumented one moves up to 2481.
export HTTP="${HTTP:-2480}"

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

# Every wire plugin ArcadeDB ships, so the tracing agent observes each plugin's startService()
# and request-handling reflection even when no matching client driver is available to drive it.
PLUGINS="Postgres:com.arcadedb.postgres.PostgresProtocolPlugin"
PLUGINS="$PLUGINS,Redis:com.arcadedb.redis.RedisProtocolPlugin"
PLUGINS="$PLUGINS,MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin"
PLUGINS="$PLUGINS,Bolt:com.arcadedb.bolt.BoltProtocolPlugin"
PLUGINS="$PLUGINS,Grpc:com.arcadedb.server.grpc.GrpcServerPlugin"
PLUGINS="$PLUGINS,Prometheus:com.arcadedb.metrics.prometheus.PrometheusMetricsPlugin"

# JVM flags shared by both phases: server.sh's --add-opens/--add-modules, so the instrumented run exercises
# the same code paths as a normal server.sh launch (see package/src/main/scripts/server.sh JAVA_OPTS_SCRIPT).
JVM_FLAGS=(
  --add-exports java.management/sun.management=ALL-UNNAMED
  --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED
  --add-opens java.base/java.nio.channels.spi=ALL-UNNAMED
  --add-opens java.base/java.lang=ALL-UNNAMED
  --add-modules jdk.incubator.vector
  --enable-native-access=ALL-UNNAMED
  -Djava.awt.headless=true -Dfile.encoding=UTF8
)

# ---- Phase 1: one server, every wire protocol and the observability plugins ----
echo "[trace] phase 1: launching instrumented server from $DIST"
# Root every server-generated path under $WORK, matching smoke.sh.
"$JAVA_BIN" \
  -agentlib:native-image-agent=config-merge-dir="$OUT",config-write-period-secs=5 \
  "${JVM_FLAGS[@]}" \
  -Darcadedb.server.rootPassword=PlayWithData123! \
  -Darcadedb.server.rootPath="$WORK" \
  -Darcadedb.server.logsDirectory="$WORK/log" \
  -Darcadedb.server.databaseDirectory="$WORK/databases" \
  -Darcadedb.server.httpIncomingPort="$HTTP" \
  -Darcadedb.server.plugins="$PLUGINS" \
  -Darcadedb.serverMetrics.otlp.enabled=true \
  -Darcadedb.serverMetrics.tracing.enabled=true \
  -Darcadedb.serverMetrics.tracing.samplingRate=1.0 \
  -cp "$DIST/lib/*" com.arcadedb.server.ArcadeDBServer \
  >"$WORK/server.log" 2>&1 &
SRV_PID=$!

ARCADEDB_ROOT_PASSWORD="PlayWithData123!" \
  SRV_PID="$SRV_PID" SERVER_LOG="$WORK/server.log" PASS="PlayWithData123!" \
  "$SCRIPT_DIR/exercise.sh"

echo "[trace] shutting down instrumented server (pid $SRV_PID)"
kill "$SRV_PID" 2>/dev/null || true
wait "$SRV_PID" 2>/dev/null || true
SRV_PID=""

# ---- Phase 2: a three-node Raft cluster ----
# One server cannot reach most of Ratis: a leader only builds its log appenders (loaded reflectively by
# class name, see LogAppender's static initializer) once it has followers, and the follower-side apply
# path only runs on a follower. ha-smoke.sh drives exactly that scenario, so it is reused here with a
# launcher that starts each node under the agent instead of the native binary.
#
# The agent takes an exclusive lock on its output directory for the life of the JVM, so three nodes
# cannot share $OUT. Each node writes to its own directory instead (the launcher makes a fresh one per
# start) and native-image-configure folds them into $OUT afterwards. The launcher puts ha-smoke.sh's
# -D arguments BEFORE -cp: after the main class a JVM would take them for program arguments.
NODE_DIRS="$WORK/agent-nodes"
mkdir -p "$NODE_DIRS"
LAUNCHER="$WORK/traced-node.sh"
{
  echo '#!/usr/bin/env bash'
  echo "NODE_OUT=\"\$(mktemp -d \"$NODE_DIRS/node.XXXXXX\")\""
  printf 'exec %q "-agentlib:native-image-agent=config-output-dir=$NODE_OUT"' "$JAVA_BIN"
  printf ' %q' "${JVM_FLAGS[@]}"
  printf ' "$@" -cp %q com.arcadedb.server.ArcadeDBServer\n' "$DIST/lib/*"
} >"$LAUNCHER"
chmod +x "$LAUNCHER"

echo "[trace] phase 2: three-node Raft cluster under the agent"
HTTP_BASE="$HTTP" "$SCRIPT_DIR/ha-smoke.sh" "$LAUNCHER"

echo "[trace] merging the three node traces into $OUT"
MERGE_ARGS=(--input-dir="$OUT")
for d in "$NODE_DIRS"/node.*; do
  MERGE_ARGS+=(--input-dir="$d")
done
CONFIGURE_BIN="$(dirname "$JAVA_BIN")/native-image-configure"
[ -x "$CONFIGURE_BIN" ] || CONFIGURE_BIN="native-image-configure"
"$CONFIGURE_BIN" generate "${MERGE_ARGS[@]}" --output-dir="$WORK/agent-merged"
cp "$WORK/agent-merged/reachability-metadata.json" "$OUT/reachability-metadata.json"

echo "[trace] metadata written to $OUT"
ls -la "$OUT"
