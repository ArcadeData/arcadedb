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

# Key/value benchmark: real Redis versus ArcadeDB's Redis wire protocol, both driven by the same redis-benchmark binary.
#
# Usage: ARCADEDB_HOME=<unpacked full distribution> benchmark-vs-redis.sh
#
# Needs a native redis-server, redis-benchmark and redis-cli (macOS: `brew install redis`) and a FULL ArcadeDB distribution
# (the `base` one has no Redis plugin). Both servers run as native processes on loopback: do not run Redis in Docker for
# this, the Docker Desktop VM would charge its network hop to one side only.
#
# What is compared, in three pairs. A pair is only fair because both sides do the same amount of work for durability:
#
#   ram      Redis `--save "" --appendonly no`                 vs ArcadeDB SET / GET / INCR. These are RAM-only in ArcadeDB
#                                                              (database global variables), nothing is written to disk.
#   relaxed  Redis `--appendonly yes --appendfsync everysec`   vs ArcadeDB persistent HSET / HGET, txWalFlush=0 (WAL written
#                                                              to the OS, no fsync). A crash of the process loses nothing in
#                                                              either; a power cut can lose up to ~1s (Redis) / the OS cache.
#   scale    Redis without persistence, as shipped and with REDIS_IO_THREADS io-threads, vs ArcadeDB SET / GET in RAM, under
#            LOADGENS load generators at once. Redis executes commands on ONE thread; ArcadeDB on one per connection.
#            The table reports the summed rate and how many cores each server used on average (CPU time / wall time).
#            Generators and server share the machine: run the generators on another host to see the servers' ceiling.
#   strict   Redis `--appendonly yes --appendfsync always`     vs ArcadeDB persistent HSET / HGET, txWalFlush=2 (fsync at every
#                                                              commit). Every acknowledged write survives a power cut.
#
# The persistent ArcadeDB side is a document type KV with a UNIQUE index on `id`: HSET inserts a record, HGET looks it up by
# the index. That is NOT the same data model as a Redis string (a record versus a blob), so read it as "the durable key/value
# use case on ArcadeDB", not as a like-for-like command comparison.
#
# Every measurement is repeated REPEAT times and the summary reports the median. Runs are sized so that a fast configuration still
# lasts seconds: redis-benchmark computes the rate from a millisecond clock, and a run under a second reports round artefacts
# (80000, 2000000) rather than a rate.
#
# Without pipelining, 50 connections on loopback top out around 200k requests/s on a laptop whatever the server: measured on an
# M5 Pro, Redis itself answered 206k with one load-generator thread, 182k with 4 and 93k with 8, and two load-generator processes
# together 228k. Those rows show where the kernel saturates, not which server is faster; the pipelined rows separate them.
#
# Before/after comparisons: run once, then rerun with SERVERS=arcadedb ARCADE_LABEL=<name> BASELINE_RESULTS=<first results.tsv>
# and the summary puts every label side by side, each with its ratio to Redis.
#
# Tunables (environment): REQUESTS (P=1), REQUESTS_PIPELINED (P>1), REQUESTS_WRITE, REQUESTS_STRICT (writes of the strict pair),
# REPEAT, CLIENTS, PIPELINES, DATA_SIZE, KEYSPACE, KEYSPACE_STRICT, WARMUP_REQUESTS, THREADS, ARCADE_HEAP, ARCADE_JAVA_OPTS,
# ARCADE_LABEL, SERVERS, BASELINE_RESULTS, ROOT_PASSWORD, REDIS_PORT, ARCADE_REDIS_PORT, ARCADE_HTTP_PORT, OUT_DIR, PAIRS,
# REDIS_SERVER_BIN, REDIS_BENCHMARK_BIN, REDIS_CLI_BIN, BENCH_HOST.

ARCADEDB_HOME=${ARCADEDB_HOME:?set ARCADEDB_HOME to an unpacked FULL ArcadeDB distribution (package/target/arcadedb-<version>.dir/arcadedb-<version>)}
REDIS_SERVER_BIN=${REDIS_SERVER_BIN:-redis-server}
REDIS_BENCHMARK_BIN=${REDIS_BENCHMARK_BIN:-redis-benchmark}
REDIS_CLI_BIN=${REDIS_CLI_BIN:-redis-cli}
BENCH_HOST=${BENCH_HOST:-127.0.0.1}
REDIS_PORT=${REDIS_PORT:-7379}
ARCADE_REDIS_PORT=${ARCADE_REDIS_PORT:-7380}
ARCADE_HTTP_PORT=${ARCADE_HTTP_PORT:-7381}
ROOT_PASSWORD=${ROOT_PASSWORD:-benchmarkpw1}
REQUESTS=${REQUESTS:-1000000}
REQUESTS_PIPELINED=${REQUESTS_PIPELINED:-5000000}
REQUESTS_WRITE=${REQUESTS_WRITE:-500000}
REQUESTS_STRICT=${REQUESTS_STRICT:-10000}
REPEAT=${REPEAT:-3}
WARMUP_REQUESTS=${WARMUP_REQUESTS:-200000}
CLIENTS=${CLIENTS:-"1 50"}
PIPELINES=${PIPELINES:-"1 16"}
DATA_SIZE=${DATA_SIZE:-100}
KEYSPACE=${KEYSPACE:-100000}
# the strict preload pays one fsync per key on both servers (~3.5ms on a laptop SSD): a smaller set keeps it under a minute
KEYSPACE_STRICT=${KEYSPACE_STRICT:-10000}
THREADS=${THREADS:-4}
ARCADE_HEAP=${ARCADE_HEAP:-4g}
ARCADE_JAVA_OPTS=${ARCADE_JAVA_OPTS:-}
ARCADE_LABEL=${ARCADE_LABEL:-arcadedb}
SERVERS=${SERVERS:-"redis arcadedb"}
BASELINE_RESULTS=${BASELINE_RESULTS:-}
# scale pair: LOADGENS redis-benchmark processes at once, each with SCALE_CLIENTS connections and SCALE_THREADS threads
LOADGENS=${LOADGENS:-4}
SCALE_CLIENTS=${SCALE_CLIENTS:-50}
SCALE_THREADS=${SCALE_THREADS:-4}
SCALE_PIPELINE=${SCALE_PIPELINE:-32}
SCALE_REQUESTS=${SCALE_REQUESTS:-5000000}
SCALE_SIZES=${SCALE_SIZES:-"100 4096"}
REDIS_IO_THREADS=${REDIS_IO_THREADS:-4}
PAIRS=${PAIRS:-"ram relaxed strict"}
OUT_DIR=${OUT_DIR:-$PWD/redis-benchmark-results-$(date +%Y%m%d-%H%M%S)}
# Write workloads insert NEW keys: ArcadeDB's UNIQUE index refuses a repeated one, and redis-benchmark aborts on the first error
# reply. -r is parsed as an int (a larger value wraps negative and silently turns random keys OFF, so every key repeats), and
# every __rand_int__ in an argument is drawn separately, so two of them widen the range. That alone is not enough: see the
# single-thread rule in bench(). The "w:" prefix keeps these keys apart from the preloaded "key:" ones the read workload uses.
WRITE_KEYSPACE=2147483647
WRITE_KEY='w:__rand_int__:__rand_int__'

for bin in "$REDIS_SERVER_BIN" "$REDIS_BENCHMARK_BIN" "$REDIS_CLI_BIN"; do
  command -v "$bin" >/dev/null || { echo "missing '$bin' (macOS: brew install redis)" >&2; exit 1; }
done
[ -x "$ARCADEDB_HOME/bin/server.sh" ] || { echo "no bin/server.sh in ARCADEDB_HOME=$ARCADEDB_HOME" >&2; exit 1; }
ls "$ARCADEDB_HOME"/lib/arcadedb-redisw-*.jar >/dev/null 2>&1 || { echo "ARCADEDB_HOME has no arcadedb-redisw jar: use the FULL distribution" >&2; exit 1; }

mkdir -p "$OUT_DIR"
RESULTS="$OUT_DIR/results.tsv"
: >"$RESULTS"
PAYLOAD=$(LC_ALL=C head -c "$DATA_SIZE" /dev/zero | tr '\0' 'x')
SERVER_PID=""

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

stop_server() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    # a graceful shutdown flushes the database: wait for it so the next server never starts on top of a dying one
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  SERVER_PID=""
}
trap stop_server EXIT

# Ready means "answers a RESP PING" (ArcadeDB answers NOAUTH before AUTH, which is still an answer): an open port is not enough
wait_for_resp() {
  local port=$1 i reply
  for i in $(seq 1 300); do
    reply=$( (exec 3<>"/dev/tcp/127.0.0.1/$port"; printf '*1\r\n$4\r\nPING\r\n' >&3; read -t 2 -r line <&3; echo "$line") 2>/dev/null || true)
    [ -n "$reply" ] && return 0
    sleep 0.2
  done
  echo "nothing answering RESP on port $port after 60s" >&2
  return 1
}

wait_for_http() {
  local i
  for i in $(seq 1 300); do
    curl -sf "http://127.0.0.1:$1/api/v1/ready" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "ArcadeDB HTTP not ready on port $1 after 60s" >&2
  return 1
}

start_redis() { # <name> <appendonly yes|no> <appendfsync everysec|always|no> [extra redis-server args...]
  local dir="$OUT_DIR/redis-$1" name=$1 appendonly=$2 appendfsync=$3
  shift 3
  rm -rf "$dir"; mkdir -p "$dir"
  log "starting redis ($name: appendonly=$appendonly appendfsync=$appendfsync $*)"
  "$REDIS_SERVER_BIN" --port "$REDIS_PORT" --bind 127.0.0.1 --save "" --dir "$dir" --appendonly "$appendonly" --appendfsync "$appendfsync" "$@" \
    >"$dir/redis.log" 2>&1 &
  SERVER_PID=$!
  wait_for_resp "$REDIS_PORT"
}

start_arcade() { # <name> <txWalFlush 0|2>
  local dir="$OUT_DIR/arcadedb-$1"
  rm -rf "$dir"; mkdir -p "$dir"
  log "starting arcadedb ($1: txWalFlush=$2, heap=$ARCADE_HEAP)"
  # ARCADEDB_JMX is blanked: server.sh would otherwise open the JMX ports 9999/9998 and clash with a second server on the machine
  # ARCADE_JAVA_OPTS reaches the JVM through server.sh's JAVA_OPTS, e.g. a -XX:StartFlightRecording for a profile
  JAVA_OPTS="$ARCADE_JAVA_OPTS" ARCADEDB_JMX=" " ARCADEDB_OPTS_MEMORY="-Xms$ARCADE_HEAP -Xmx$ARCADE_HEAP" "$ARCADEDB_HOME/bin/server.sh" \
    -Darcadedb.server.rootPath="$dir" \
    -Darcadedb.server.rootPassword="$ROOT_PASSWORD" \
    -Darcadedb.server.plugins=RedisProtocolPlugin \
    -Darcadedb.server.httpIncomingPort="$ARCADE_HTTP_PORT" \
    -Darcadedb.redis.host=127.0.0.1 \
    -Darcadedb.redis.port="$ARCADE_REDIS_PORT" \
    -Darcadedb.redis.defaultDatabase=bench \
    -Darcadedb.txWalFlush="$2" \
    >"$dir/server.log" 2>&1 &
  SERVER_PID=$!
  wait_for_resp "$ARCADE_REDIS_PORT"
  wait_for_http "$ARCADE_HTTP_PORT"
  # created after the start, not through arcadedb.server.defaultDatabases (which insists on a credentials block): the Redis
  # connections only look arcadedb.redis.defaultDatabase up when they open, and the benchmark opens them later
  curl -sf -u "root:$ROOT_PASSWORD" -H 'Content-Type: application/json' -d '{"command":"create database bench"}' \
    "http://127.0.0.1:$ARCADE_HTTP_PORT/api/v1/server" >/dev/null \
    || { echo "create database failed (see $dir/server.log)" >&2; exit 1; }
  local sql
  for sql in "CREATE DOCUMENT TYPE KV" "CREATE PROPERTY KV.id STRING" "CREATE INDEX ON KV (id) UNIQUE"; do
    curl -sf -u "root:$ROOT_PASSWORD" -H 'Content-Type: application/json' \
      -d "{\"language\":\"sql\",\"command\":\"$sql\"}" "http://127.0.0.1:$ARCADE_HTTP_PORT/api/v1/command/bench" >/dev/null \
      || { echo "schema command failed: $sql (see $dir/server.log)" >&2; exit 1; }
  done
}

# bench <server> <pair> <workload> <clients> <pipeline> <requests> <port> <keyspace-or-0> -- <redis-benchmark args...>
# --seed: redis-benchmark seeds its generator with time(NULL)^pid, so two invocations in the same second (a warm-up and the run
# after it) can draw the SAME key sequence, and the second one then inserts keys the first already did: a "Duplicated key"
# error on ArcadeDB's unique index, which aborts redis-benchmark.
# The redis-benchmark CSV line is parsed from its END: the label column holds the command, which for HSET carries commas.
bench() {
  local server=$1 pair=$2 workload=$3 clients=$4 pipeline=$5 requests=$6 port=$7 keyspace=$8
  shift 9
  local auth=() threads=$((clients < THREADS ? clients : THREADS)) rkeys=() label=$server
  [ "$server" = arcadedb ] && auth=(--user root -a "$ROOT_PASSWORD") && label=$ARCADE_LABEL
  # Without pipelining one load-generator thread drives loopback harder than several (see the header): more threads only add
  # contention in redis-benchmark itself
  [ "$pipeline" = 1 ] && threads=1
  [ "$keyspace" != 0 ] && rkeys=(-r "$keyspace")
  # With more than one thread redis-benchmark's generator repeats keys (measured: 0.2-0.7% of 500k SETs, none with one thread,
  # for the same seeds), and a repeated insert is a "Duplicated key" error on ArcadeDB's UNIQUE index that aborts the run.
  # The write workloads need new keys every time, so they load from one thread; the others (SET/GET/INCR over a small keyspace,
  # reads) are indifferent to repeats and keep THREADS.
  case "$workload" in write|warmup-write) threads=1 ;; esac
  local csv
  csv=$("$REDIS_BENCHMARK_BIN" -h "$BENCH_HOST" -p "$port" ${auth[@]+"${auth[@]}"} -c "$clients" -P "$pipeline" -n "$requests" \
    --threads "$threads" --seed "$((RANDOM * 32768 + RANDOM))" ${rkeys[@]+"${rkeys[@]}"} --csv "$@" 2>>"$OUT_DIR/redis-benchmark.err") \
    || { echo "redis-benchmark failed for $server/$pair/$workload (see $OUT_DIR/redis-benchmark.err)" >&2; return 1; }
  local row name rows=0 expected=1
  # a -t set,get,incr run reports one row per test; a single command one row
  case "$workload" in -|warmup) expected=3 ;; esac
  while IFS= read -r row; do
    rows=$((rows + 1))
    name=$workload
    # a redis-benchmark -t run reports one row per test (SET, GET, INCR): the label column names the workload
    [ "$workload" = - ] && name=$(printf '%s' "$row" | sed -E 's/^"([^"]*)".*/\1/')
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$pair" "$name" "$clients" "$pipeline" "$label" \
      "$(printf '%s' "$row" | sed -E 's/.*"([0-9.]+)","([0-9.]+)","([0-9.]+)","([0-9.]+)","([0-9.]+)","([0-9.]+)","([0-9.]+)"$/\1 \2 \4 \5 \6 \7/' | tr ' ' '\t')" \
      >>"$RESULTS"
  done < <(printf '%s\n' "$csv" | grep -E '"[0-9.]+","[0-9.]+","[0-9.]+","[0-9.]+","[0-9.]+","[0-9.]+","[0-9.]+"$')
  if [ "$rows" -ne "$expected" ]; then
    echo "redis-benchmark returned $rows of $expected result rows for $server/$pair/$workload (see $OUT_DIR/redis-benchmark.err)" >&2
    return 1
  fi
}

# the warm-up runs go through bench() like everything else; their rows are dropped here
drop_warmup() {
  grep -v '^warmup' "$RESULTS" >"$RESULTS.tmp" || true
  mv "$RESULTS.tmp" "$RESULTS"
}

# Sequential keys key:000000000000 .. key:<n-1> loaded through ONE connection, so the read workload hits only keys that exist.
# redis-benchmark cannot generate a sequence (7.x), and `redis-cli --pipe` needs ECHO, which ArcadeDB does not implement:
# plain redis-cli reading one command per line from stdin is the loader both servers understand. One round trip per key, so
# keep KEYSPACE modest; its elapsed time is not a benchmark figure.
preload() { # <server> <port> <count>
  local server=$1 port=$2 count=$3 auth=() start end status=0
  [ "$server" = arcadedb ] && auth=(--user root -a "$ROOT_PASSWORD")
  log "$server: preloading $count keys"
  start=$(date +%s)
  awk -v n="$count" -v server="$server" -v payload="$PAYLOAD" 'BEGIN {
    for (i = 0; i < n; i++) {
      key = sprintf("key:%012d", i)
      if (server == "arcadedb")
        printf "HSET bench KV \047{\"id\":\"%s\",\"v\":\"%s\"}\047\n", key, payload
      else
        printf "SET %s %s\n", key, payload
    }
  }' | "$REDIS_CLI_BIN" -h "$BENCH_HOST" -p "$port" ${auth[@]+"${auth[@]}"} >"$OUT_DIR/preload-$server.log" 2>&1 || status=$?
  # redis-cli exits 0 on an error REPLY, and a refused connection prints no "error": both checks are needed
  if [ "$status" -ne 0 ] || grep -qiE 'err|error|not found|could not connect' "$OUT_DIR/preload-$server.log"; then
    echo "preload failed, see $OUT_DIR/preload-$server.log" >&2
    return 1
  fi
  end=$(date +%s)
  log "$server: preload took $((end - start))s"
}

# A benchmark that reads nothing is still green: redis-benchmark counts a nil reply as a served request. So check, from a NEW
# connection, that what the workload wrote is really there. This is what catches a connection-local SET (arcadedb.redis.
# defaultDatabase not applied) or a lookup that silently misses.
verify() { # <server> <port> <expected substring> <command...>
  local server=$1 port=$2 expected=$3 auth=() out
  shift 3
  [ "$server" = arcadedb ] && auth=(--user root -a "$ROOT_PASSWORD")
  out=$("$REDIS_CLI_BIN" -h "$BENCH_HOST" -p "$port" ${auth[@]+"${auth[@]}"} "$@" 2>/dev/null) || out=""
  case "$out" in
    *"$expected"*) ;;
    *) echo "VERIFY FAILED on $server: '$*' answered '${out:0:120}', expected it to contain '$expected': the numbers would measure misses" >&2; exit 1 ;;
  esac
}

run_ram() { # <server> <port>
  local server=$1 port=$2 c p
  log "$server: warm-up"
  bench "$server" warmup warmup 50 16 "$WARMUP_REQUESTS" "$port" "$KEYSPACE" -- -d "$DATA_SIZE" -t set,get,incr || true
  drop_warmup
  # written on one connection, read on another: a connection-local SET would not survive the first one
  verify "$server" "$port" OK SET verify:probe probe-value
  verify "$server" "$port" probe-value GET verify:probe
  local r
  for r in $(seq 1 "$REPEAT"); do
    for c in $CLIENTS; do
      for p in $PIPELINES; do
        log "$server: ram c=$c P=$p (run $r/$REPEAT)"
        # one invocation runs SET then GET then INCR, so the GETs hit keys the SETs just wrote (KEYSPACE << REQUESTS)
        bench "$server" ram - "$c" "$p" "$(requests_for "$p")" "$port" "$KEYSPACE" -- -d "$DATA_SIZE" -t set,get,incr
      done
    done
  done
}

requests_for() { # <pipeline>
  if [ "$1" = 1 ]; then echo "$REQUESTS"; else echo "$REQUESTS_PIPELINED"; fi
}

run_durable() { # <server> <port> <pair> <write requests> <keyspace>
  local server=$1 port=$2 pair=$3 requests=$4 keyspace=$5 c p r
  local write_cmd read_cmd
  if [ "$server" = arcadedb ]; then
    write_cmd=(HSET bench KV "{\"id\":\"$WRITE_KEY\",\"v\":\"$PAYLOAD\"}")
    read_cmd=(HGET "bench.KV[id]" "key:__rand_int__")
  else
    write_cmd=(SET "$WRITE_KEY" "$PAYLOAD")
    read_cmd=(GET "key:__rand_int__")
  fi
  log "$server: $pair warm-up (writes)"
  bench "$server" warmup warmup-write 50 16 "$((requests < WARMUP_REQUESTS ? requests : WARMUP_REQUESTS))" "$port" "$WRITE_KEYSPACE" -- \
    "${write_cmd[@]}" || true
  drop_warmup
  for r in $(seq 1 "$REPEAT"); do
    for c in $CLIENTS; do
      for p in $PIPELINES; do
        log "$server: $pair write c=$c P=$p (run $r/$REPEAT)"
        bench "$server" "$pair" write "$c" "$p" "$requests" "$port" "$WRITE_KEYSPACE" -- "${write_cmd[@]}"
      done
    done
  done
  preload "$server" "$port" "$keyspace"
  if [ "$server" = arcadedb ]; then
    verify arcadedb "$port" '"id":"key:000000000001"' HGET "bench.KV[id]" key:000000000001
  else
    verify redis "$port" xxxx GET key:000000000001
  fi
  # reads are not touched by the durability setting, so they run as long as the ram ones
  for r in $(seq 1 "$REPEAT"); do
    for c in $CLIENTS; do
      for p in $PIPELINES; do
        log "$server: $pair read c=$c P=$p (run $r/$REPEAT)"
        bench "$server" "$pair" read "$c" "$p" "$(requests_for "$p")" "$port" "$keyspace" -- "${read_cmd[@]}"
      done
    done
  done
}

now() {
  perl -MTime::HiRes=time -e 'printf "%.3f\n", time'
}

# CPU time (user + system) a process has used so far, in seconds, from ps's [[dd-]hh:]mm:ss.ss
cpu_seconds() { # <pid>
  ps -o time= -p "$1" | awk '{ n = split($1, a, /[-:]/); s = 0; for (i = 1; i <= n; i++) s = s * 60 + a[i]; if (n == 4) s = a[1] * 86400 + a[2] * 3600 + a[3] * 60 + a[4]; print s }'
}

# Runs LOADGENS generators at once against one server and appends one row per test: the summed rate, the worst p99 of the
# generators and the cores the server used on average while they ran. SCALE_REQUESTS is split between the generators.
run_scale() { # <server kind> <label> <port>
  local server=$1 label=$2 port=$3 size g auth=() pids start end cpu0 cpu1
  [ "$server" = arcadedb ] && auth=(--user root -a "$ROOT_PASSWORD")
  log "$label: scale warm-up"
  "$REDIS_BENCHMARK_BIN" -h "$BENCH_HOST" -p "$port" ${auth[@]+"${auth[@]}"} -c 50 -P 16 --threads 4 -n "$WARMUP_REQUESTS" \
    -r "$KEYSPACE" -d 100 -t set,get --csv >/dev/null 2>>"$OUT_DIR/redis-benchmark.err" || true
  for r in $(seq 1 "$REPEAT"); do
    for size in $SCALE_SIZES; do
      log "$label: scale ${size}B x$LOADGENS generators (run $r/$REPEAT)"
      pids=()
      # a previous run that failed half way must not leave its generators' rows to be summed with this one's
      rm -f "$OUT_DIR"/scale.*.csv
      cpu0=$(cpu_seconds "$SERVER_PID"); start=$(now)
      for g in $(seq 1 "$LOADGENS"); do
        "$REDIS_BENCHMARK_BIN" -h "$BENCH_HOST" -p "$port" ${auth[@]+"${auth[@]}"} -c "$SCALE_CLIENTS" -P "$SCALE_PIPELINE" \
          --threads "$SCALE_THREADS" --seed "$((RANDOM * 32768 + RANDOM))" -n "$((SCALE_REQUESTS / LOADGENS))" -r "$KEYSPACE" \
          -d "$size" -t set,get --csv >"$OUT_DIR/scale.$g.csv" 2>>"$OUT_DIR/redis-benchmark.err" &
        pids+=($!)
      done
      for g in "${pids[@]}"; do wait "$g" || { echo "a scale generator failed (see $OUT_DIR/redis-benchmark.err)" >&2; return 1; }; done
      end=$(now); cpu1=$(cpu_seconds "$SERVER_PID")
      # one row per test (SET, GET): rates summed over the generators, p99 the worst of them; cores = server CPU over wall time,
      # for the whole SET + GET window
      cat "$OUT_DIR"/scale.*.csv | grep -E '"[0-9.]+","[0-9.]+","[0-9.]+","[0-9.]+","[0-9.]+","[0-9.]+","[0-9.]+"$' \
        | sed -E 's/^"([^"]*)",.*"([0-9.]+)","([0-9.]+)","([0-9.]+)","([0-9.]+)","([0-9.]+)","([0-9.]+)","([0-9.]+)"$/\1 \2 \7/' \
        | awk -v label="$label" -v size="$size" -v clients="$((LOADGENS * SCALE_CLIENTS))" -v p="$SCALE_PIPELINE" \
            -v cores="$(awk -v a="$cpu0" -v b="$cpu1" -v s="$start" -v e="$end" 'BEGIN { printf "%.1f", (b - a) / (e - s) }')" \
            '{ rps[$1] += $2; if ($3 > p99[$1]) p99[$1] = $3 }
             END { for (t in rps) printf "scale\t%s %sB\t%s\t%s\t%s\t%.0f\t0\t0\t0\t%s\t%s\n", t, size, clients, p, label, rps[t], p99[t], cores }' \
        >>"$RESULTS"
      rm -f "$OUT_DIR"/scale.*.csv
    done
  done
}

runs() { # <server kind>
  case " $SERVERS " in *" $1 "*) return 0 ;; *) return 1 ;; esac
}

{
  echo "date:       $(date)"
  echo "machine:    $(uname -srm)"
  echo "cpu:        $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo 2>/dev/null || echo unknown)"
  echo "redis:      $("$REDIS_SERVER_BIN" --version)"
  echo "benchmark:  $("$REDIS_BENCHMARK_BIN" --version 2>&1 | head -1)"
  echo "arcadedb:   $ARCADEDB_HOME"
  echo "java:       $(java -version 2>&1 | head -1)"
  echo "arcade jvm: ${ARCADE_JAVA_OPTS:-(defaults)} label=$ARCADE_LABEL"
  echo "power:      $(pmset -g batt 2>/dev/null | head -1 || echo n/a)"
  echo "params:     servers=[$SERVERS] repeat=$REPEAT requests=$REQUESTS pipelined=$REQUESTS_PIPELINED write=$REQUESTS_WRITE strict=$REQUESTS_STRICT clients=[$CLIENTS] pipelines=[$PIPELINES] value=${DATA_SIZE}B keyspace=$KEYSPACE/$KEYSPACE_STRICT heap=$ARCADE_HEAP"
} | tee "$OUT_DIR/environment.txt" >&2

for pair in $PAIRS; do
  case "$pair" in
    ram)
      if runs redis; then start_redis ram no no; run_ram redis "$REDIS_PORT"; stop_server; fi
      if runs arcadedb; then start_arcade ram 0; run_ram arcadedb "$ARCADE_REDIS_PORT"; stop_server; fi ;;
    relaxed)
      if runs redis; then start_redis relaxed yes everysec; run_durable redis "$REDIS_PORT" relaxed "$REQUESTS_WRITE" "$KEYSPACE"; stop_server; fi
      if runs arcadedb; then start_arcade relaxed 0; run_durable arcadedb "$ARCADE_REDIS_PORT" relaxed "$REQUESTS_WRITE" "$KEYSPACE"; stop_server; fi ;;
    strict)
      if runs redis; then start_redis strict yes always; run_durable redis "$REDIS_PORT" strict "$REQUESTS_STRICT" "$KEYSPACE_STRICT"; stop_server; fi
      if runs arcadedb; then start_arcade strict 2; run_durable arcadedb "$ARCADE_REDIS_PORT" strict "$REQUESTS_STRICT" "$KEYSPACE_STRICT"; stop_server; fi ;;
    scale)
      if runs redis; then
        start_redis scale no no; run_scale redis redis "$REDIS_PORT"; stop_server
        start_redis scale-io no no --io-threads "$REDIS_IO_THREADS"; run_scale redis "redis-io$REDIS_IO_THREADS" "$REDIS_PORT"; stop_server
      fi
      if runs arcadedb; then start_arcade scale 0; run_scale arcadedb "$ARCADE_LABEL" "$ARCADE_REDIS_PORT"; stop_server; fi ;;
    *) echo "unknown pair '$pair' (ram, relaxed, strict, scale)" >&2; exit 1 ;;
  esac
done

# columns of the results (tab separated): pair workload clients pipeline server rps avg_ms p50_ms p95_ms p99_ms max_ms; a scale
# row has no avg/p50/p95 (0) and carries in place of max_ms the cores the server used
# One column per server label, in order of first appearance; each figure is the median of its REPEAT runs, and every label but
# redis gets its ratio to redis. BASELINE_RESULTS files are read first, so a baseline's labels come before this run's.
# shellcheck disable=SC2086
awk -F'\t' '
  function median(list,    v, n, i, j, t) {
    n = split(list, v, " ")
    for (i = 2; i <= n; i++) { t = v[i] + 0; for (j = i - 1; j >= 1 && v[j] + 0 > t; j--) v[j + 1] = v[j]; v[j + 1] = t }
    return n % 2 ? v[(n + 1) / 2] : (v[n / 2] + v[n / 2 + 1]) / 2
  }
  { key = $1 SUBSEP $2 SUBSEP $3 SUBSEP $4
    if (!(key in seen)) { seen[key] = 1; order[++n] = key }
    if (!($5 in hasLabel)) { hasLabel[$5] = 1; labels[++m] = $5 }
    rps[key, $5] = rps[key, $5] " " $6; p99[key, $5] = p99[key, $5] " " $10
    if ($1 == "scale") cores[key, $5] = cores[key, $5] " " $11 }
  END {
    printf "%-8s %-9s %7s %4s", "pair", "workload", "clients", "P"
    for (l = 1; l <= m; l++) printf " | %14s", labels[l] (labels[l] == "redis" ? "" : " (ratio)")
    for (l = 1; l <= m; l++) printf " | %10s", "p99 " substr(labels[l], 1, 6)
    printf "\n"
    for (i = 1; i <= n; i++) { split(order[i], k, SUBSEP); key = order[i]
      printf "%-8s %-9s %7s %4s", k[1], k[2], k[3], k[4]
      base = ((key, "redis") in rps) ? median(rps[key, "redis"]) : 0
      for (l = 1; l <= m; l++) {
        if (!((key, labels[l]) in rps)) { printf " | %14s", "-"; continue }
        v = median(rps[key, labels[l]])
        if (labels[l] == "redis" || base == 0) printf " | %14.0f", v
        else printf " | %8.0f %4.2fx", v, v / base
      }
      for (l = 1; l <= m; l++) printf " | %8sms", ((key, labels[l]) in p99) ? median(p99[key, labels[l]]) : "-"
      if (k[1] == "scale") for (l = 1; l <= m; l++) if ((key, labels[l]) in cores) printf " | %s %s cores", labels[l], median(cores[key, labels[l]])
      printf "\n" } }' \
  $BASELINE_RESULTS "$RESULTS" | tee "$OUT_DIR/summary.txt"
log "raw rows: $RESULTS; logs and data directories under $OUT_DIR"
