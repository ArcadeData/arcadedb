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
# Runs one columnar benchmark (plan 7 measurement protocol, #10662) and collects its JSON reports with the state of the machine.
#
# usage: columnar-bench.sh <label> <rounds> <target>
#   target: ingest | count | facets | tuning:<method> | load-single | load-three
#   rounds: the measured rounds of an engine benchmark (-Dcolumnar.bench.rounds); the number of runs of a load target
# env:
#   BENCH_DIR    where the results go, under <label>/ (default <repo>/target/columnar-bench;
#                a relative path is taken from the current directory)
#   BENCH_OPTS   extra -D settings for Maven, split on blanks (e.g. "-DfacetBench.records=200000")
#   BENCH_IMAGE  the Docker image of the load targets (required for them)
#
# Writes <BENCH_DIR>/<label>/machine-<label>-<ts>.json and the Maven log, and moves the benchmark JSON reports there. The load
# ITs write fixed file names, so each load run's files are renamed <name>-<label>-<ts>-run<N>.json (columnar_bench_compare.py
# collect, which never overwrites). Outside CI.
#
# Profiling (async-profiler 4.6): -DargLine does NOT reach the surefire fork (the POM's <argLine>@{argLine} ...</argLine> late-binds
# the empty POM property, not the user property), so attach the agent through JAVA_TOOL_OPTIONS with one JFR per JVM (%p = pid),
# then keep the file of the surefire fork (the one whose stacks contain ForkedBooter):
#   P=target/columnar-profile; mkdir -p $P
#   JAVA_TOOL_OPTIONS="-agentpath:/opt/homebrew/lib/libasyncProfiler.dylib=start,event=cpu,interval=1ms,alloc=512k,lock=1ms,jfr,file=$PWD/$P/%p.jfr" \
#     caffeinate -dims ./mvnw -o --batch-mode -pl engine -am test -Dgroups=benchmark -DexcludedGroups= \
#     -Dsurefire.failIfNoSpecifiedTests=false -Dtest=ColumnarIngestOverheadBenchmark -Dcolumnar.bench.variants=B -Dcolumnar.bench.rounds=3
#   jfrconv --cpu --threads $P/<pid>.jfr $P/ingest-B-cpu.html            (also --alloc --threads, --lock)
#   jfrconv --cpu --threads -o collapsed $P/<pid>.jfr $P/ingest-B-cpu.collapsed
#   python3 -I engine/src/test/scripts/columnar_bench_compare.py rank $P/ingest-B-cpu.collapsed $P/categories.tsv
# In collapsed stacks the frames use '/' separators and _[j]/_[i] suffixes; rank takes the first matching categories.tsv line.

set -euo pipefail

if [ $# -ne 3 ]; then
  sed -n '20,/^set -euo pipefail/p' "$0" | grep '^#' >&2
  [ "${1:-}" = "--help" ] && exit 0
  exit 2
fi
LABEL=$1
ROUNDS=$2
TARGET=$3
case "$LABEL" in
  *[!A-Za-z0-9._-]*|"") echo "label must match [A-Za-z0-9._-]+: $LABEL" >&2; exit 2 ;;
esac
case "$ROUNDS" in
  ''|*[!0-9]*) echo "rounds must be a positive integer: $ROUNDS" >&2; exit 2 ;;
esac
# before anything is written: the target names the log file, so a tuning method is a Java name and never a path
case "$TARGET" in
  ingest|count|facets|load-single|load-three) ;;
  tuning:*)
    case "${TARGET#tuning:}" in
      ''|*[!A-Za-z0-9_]*) echo "tuning:<method> takes a Java method name: $TARGET" >&2; exit 2 ;;
    esac ;;
  *) echo "unknown target: $TARGET (ingest | count | facets | tuning:<method> | load-single | load-three)" >&2; exit 2 ;;
esac

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
if [ -z "${BENCH_DIR:-}" ]; then
  BENCH_DIR="$ROOT/target/columnar-bench"
elif [ "${BENCH_DIR#/}" = "$BENCH_DIR" ]; then
  BENCH_DIR="$PWD/$BENCH_DIR"
fi
OPTS=()
if [ -n "${BENCH_OPTS:-}" ]; then
  read -r -a OPTS <<< "$BENCH_OPTS"
fi
TS=$(date +%Y%m%d-%H%M%S)
OUT_DIR="$BENCH_DIR/$LABEL"
mkdir -p "$OUT_DIR"
LOG="$OUT_DIR/${TARGET//:/-}-$LABEL-$TS.log"

# ---- machine state, before the run
REV=$(git -C "$ROOT" rev-parse HEAD)
DIRTY=$(git -C "$ROOT" status --porcelain | wc -l | tr -d ' ')
export M_LABEL="$LABEL" M_TS="$TS" M_TARGET="$TARGET" M_ROUNDS="$ROUNDS" M_OPTS="${BENCH_OPTS:-}" M_REV="$REV" \
  M_DIRTY="$DIRTY" M_IMAGE="${BENCH_IMAGE:-}"
M_UPTIME=$(uptime)
M_SYSCTL=$(sysctl -n hw.ncpu hw.memsize machdep.cpu.brand_string 2>/dev/null || true)
M_BATTERY=$(pmset -g batt 2>/dev/null | head -n 1 || true)
M_THERM=$(pmset -g therm 2>/dev/null || true)
M_JAVA_PROCESSES=$(pgrep -x java | wc -l | tr -d ' ' || true)
M_DOCKER_PROCESSES=$(pgrep -f 'docker run' | wc -l | tr -d ' ' || true)
M_BUILD_PROCESSES=$(pgrep -f 'surefire|maven' | wc -l | tr -d ' ' || true)
M_JAVA_VERSION=$(java -version 2>&1 || true)
export M_UPTIME M_SYSCTL M_BATTERY M_THERM M_JAVA_PROCESSES M_DOCKER_PROCESSES M_BUILD_PROCESSES M_JAVA_VERSION
python3 -I -c '
import json, os, sys
e = os.environ
sysctl = e["M_SYSCTL"].splitlines() + ["", "", ""]
json.dump({
    "label": e["M_LABEL"], "timestamp": e["M_TS"], "target": e["M_TARGET"], "rounds": int(e["M_ROUNDS"]),
    "benchOpts": e["M_OPTS"], "image": e["M_IMAGE"],
    "git": {"rev": e["M_REV"], "dirty": int(e["M_DIRTY"])},
    "uptime": e["M_UPTIME"].strip(),
    "hw": {"ncpu": sysctl[0], "memsize": sysctl[1], "cpu": sysctl[2]},
    "battery": e["M_BATTERY"].strip(), "therm": e["M_THERM"].strip(),
    "processes": {"java": int(e["M_JAVA_PROCESSES"] or 0), "dockerRun": int(e["M_DOCKER_PROCESSES"] or 0),
                  "surefireOrMaven": int(e["M_BUILD_PROCESSES"] or 0)},
    "javaVersion": e["M_JAVA_VERSION"].strip(),
}, open(sys.argv[1], "w", encoding="utf-8"), indent=2)
' "$OUT_DIR/machine-$LABEL-$TS.json"
echo "machine state: $OUT_DIR/machine-$LABEL-$TS.json (load: $M_UPTIME)"

# ---- the run
MVN=("$ROOT/mvnw" -o -f "$ROOT/pom.xml" --batch-mode)
STATUS=0
collect() { # <source dir> <marker file> [suffix]: moves the JSON files newer than the marker into OUT_DIR, renamed with the suffix
  python3 -I "$SCRIPT_DIR/columnar_bench_compare.py" collect "$1" "$OUT_DIR" "$2" ${3:+--suffix "$3"}
}

engine_run() { # <surefire test selector>
  local run_out="$ROOT/engine/target/columnar-bench/$LABEL-$TS"
  mkdir -p "$run_out"
  local marker="$run_out/.started"
  touch "$marker"
  "${MVN[@]}" -pl engine -am test -Dtest="$1" -Dgroups=benchmark -DexcludedGroups= -Dsurefire.failIfNoSpecifiedTests=false \
    "-Dcolumnar.bench.label=$LABEL" "-Dcolumnar.bench.rounds=$ROUNDS" "-Dcolumnar.bench.out=$run_out" \
    "-Dcolumnar.bench.rev=$REV" "-Dcolumnar.bench.dirty=$DIRTY" ${OPTS[@]+"${OPTS[@]}"} > "$LOG" 2>&1 || STATUS=$?
  collect "$run_out" "$marker" || { echo "no JSON report was written, see $LOG" >&2; [ "$STATUS" -ne 0 ] || STATUS=1; }
}

load_run() { # <IT class>
  if [ -z "${BENCH_IMAGE:-}" ]; then
    echo "BENCH_IMAGE is required for $TARGET" >&2
    exit 2
  fi
  local marker="$OUT_DIR/.started-$TS"
  touch "$marker"
  for ((run = 1; run <= ROUNDS; run++)); do
    "${MVN[@]}" verify -Pintegration -pl load-tests -am "-Dit.test=$1" -Dfailsafe.failIfNoSpecifiedTests=false \
      -Dtest=ContainersTestTemplateTest -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false \
      "-Darcadedb.test.image=$BENCH_IMAGE" ${OPTS[@]+"${OPTS[@]}"} >> "$LOG" 2>&1 || STATUS=$?
    collect "$ROOT/load-tests/target/facet-load" "$marker" "$LABEL-$TS-run$run" || { echo "run $run wrote no JSON report, see $LOG" >&2; [ "$STATUS" -ne 0 ] || STATUS=1; }
    touch "$marker"
  done
  rm -f "$marker"
}

case "$TARGET" in
  ingest) engine_run ColumnarIngestOverheadBenchmark ;;
  count) engine_run ColumnarCountBenchmark ;;
  facets) engine_run FacetsVsGroupByBenchmark ;;
  tuning:?*) engine_run "FacetTuningBenchmark#${TARGET#tuning:}" ;;
  load-single) load_run SingleServerFacetLoadTestIT ;;
  load-three) load_run ThreeNodesFacetLoadTestIT ;;
  *) echo "unknown target: $TARGET (ingest | count | facets | tuning:<method> | load-single | load-three)" >&2; exit 2 ;;
esac

echo "maven log: $LOG"
[ "$STATUS" -eq 0 ] || echo "FAILED with status $STATUS" >&2
exit "$STATUS"
