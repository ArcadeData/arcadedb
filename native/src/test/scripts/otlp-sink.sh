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
# Sourced, not executed, by smoke.sh and trace.sh (issue #9425): starts otlp_sink.py, the stand-in OTLP collector, so
# exercise.sh can assert that the metrics and tracing plugins really EXPORT, not only that they started. The sink must
# be up before the server, because the endpoints are server settings read at plugin start.
#
# start_otlp_sink <work-dir> sets:
#   OTLP_SINK_DIR  where the sink records what it received (pass it to exercise.sh); empty when no sink is running
#   OTLP_SINK_PID  the sink's pid, for the caller's cleanup; empty when no sink is running
#   OTLP_ARGS      the server arguments pointing both exporters at the sink, with a 2 s metrics push interval so the
#                  check does not wait for Micrometer's one-minute default. Empty when no sink is running: expand it as
#                  ${OTLP_ARGS[@]+"${OTLP_ARGS[@]}"} so bash 3.2 (stock macOS) does not take the empty array for unset.
# Never fails the caller: without python3, or when the sink does not come up, it says so and leaves all three empty, and
# exercise.sh turns the missing sink into a WARN (or a FAIL under WIRE_STRICT=1).

OTLP_SINK_DIR=""
OTLP_SINK_PID=""
OTLP_ARGS=()

start_otlp_sink() {
  local work="$1"
  local here
  here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

  if ! command -v python3 >/dev/null 2>&1; then
    echo "[otlp-sink] python3 not installed, the OTLP export checks will be skipped"
    return 0
  fi

  local dir="$work/otlp-sink"
  mkdir -p "$dir"
  python3 "$here/otlp_sink.py" "$dir" >"$work/otlp-sink.log" 2>&1 &
  local pid=$!

  for _ in $(seq 1 50); do
    [ -s "$dir/metrics.port" ] && [ -s "$dir/traces.port" ] && break
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.2
  done
  if [ ! -s "$dir/metrics.port" ] || [ ! -s "$dir/traces.port" ]; then
    echo "[otlp-sink] the sink did not come up, the OTLP export checks will be skipped: $(cat "$work/otlp-sink.log" 2>/dev/null)"
    kill "$pid" 2>/dev/null || true
    return 0
  fi

  OTLP_SINK_DIR="$dir"
  OTLP_SINK_PID="$pid"
  OTLP_ARGS=(
    "-Darcadedb.serverMetrics.otlp.endpoint=http://127.0.0.1:$(cat "$dir/metrics.port")/v1/metrics"
    "-Darcadedb.serverMetrics.otlp.step=2000"
    "-Darcadedb.serverMetrics.tracing.endpoint=http://127.0.0.1:$(cat "$dir/traces.port")"
  )
  echo "[otlp-sink] listening: metrics on :$(cat "$dir/metrics.port"), traces on :$(cat "$dir/traces.port")"
}

stop_otlp_sink() {
  if [ -n "$OTLP_SINK_PID" ] && kill -0 "$OTLP_SINK_PID" 2>/dev/null; then
    kill "$OTLP_SINK_PID" 2>/dev/null || true
    wait "$OTLP_SINK_PID" 2>/dev/null || true
  fi
  OTLP_SINK_PID=""
}
