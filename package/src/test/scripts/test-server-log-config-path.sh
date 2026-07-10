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
# Regression test for issue #5182 (Problem 1): the server launch script must pass a
# java.util.logging.config.file path that resolves correctly no matter which working
# directory the user invoked the script from. Historically server.sh used a bare
# relative path ("config/arcadedb-log.properties") which only resolved when the
# current directory happened to be ARCADEDB_HOME, so starting the server as
# "cd bin && ./server.sh" (or from any other directory) silently lost the logging
# configuration.
#
# The test lays out a throwaway distribution, replaces "java" with a stub that just
# records the JVM arguments, launches server.sh from a foreign working directory and
# asserts the logging-config path the stub received points to an existing file.

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# package/src/test/scripts -> package/src/main/scripts
SERVER_SH="$SCRIPT_DIR/../../main/scripts/server.sh"
LOG_PROPS="$SCRIPT_DIR/../../main/config/arcadedb-log.properties"

if [ ! -f "$SERVER_SH" ]; then
  echo "FAIL: cannot find server.sh at $SERVER_SH"
  exit 1
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

# Build a minimal distribution layout: bin/ config/ lib/.
# The home path deliberately contains a space to prove the argument survives word-splitting.
HOME_DIR="$WORK_DIR/arcade db"
mkdir -p "$HOME_DIR/bin" "$HOME_DIR/config" "$HOME_DIR/lib"
cp "$SERVER_SH" "$HOME_DIR/bin/server.sh"
chmod +x "$HOME_DIR/bin/server.sh"
cp "$LOG_PROPS" "$HOME_DIR/config/arcadedb-log.properties"

# Stub "java": record the arguments it is launched with, then exit without doing anything.
STUB_BIN="$WORK_DIR/stubbin"
mkdir -p "$STUB_BIN"
ARG_DUMP="$WORK_DIR/java-args.txt"
cat >"$STUB_BIN/java" <<STUB
#!/usr/bin/env sh
for a in "\$@"; do
  echo "\$a" >>"$ARG_DUMP"
done
exit 0
STUB
chmod +x "$STUB_BIN/java"

# Launch server.sh from a given working directory (using the stub java) and assert the
# logging-config path it hands to the JVM points to an existing file.
assert_log_config_resolves_from() {
  launch_cwd="$1"
  : >"$ARG_DUMP"

  # Unset JAVA_HOME so the script falls back to "java" resolved from PATH (our stub).
  (
    cd "$launch_cwd"
    PATH="$STUB_BIN:$PATH" JAVA_HOME="" ARCADEDB_PID="$WORK_DIR/arcadedb.pid" \
      sh "$HOME_DIR/bin/server.sh" >/dev/null 2>&1
  )

  if [ ! -s "$ARG_DUMP" ]; then
    echo "FAIL: stub java was never invoked; server.sh did not launch the JVM (CWD=$launch_cwd)"
    exit 1
  fi

  log_cfg_arg="$(grep -E '^-Djava\.util\.logging\.config\.file=' "$ARG_DUMP" | head -1 || true)"
  if [ -z "$log_cfg_arg" ]; then
    echo "FAIL: server.sh did not pass -Djava.util.logging.config.file (CWD=$launch_cwd)"
    exit 1
  fi

  log_cfg_path="${log_cfg_arg#-Djava.util.logging.config.file=}"
  echo "Resolved logging config path (CWD=$launch_cwd): $log_cfg_path"

  # The path may be relative; resolve it against the working directory the JVM was
  # launched from, exactly as the JVM would.
  if [ "${log_cfg_path#/}" = "$log_cfg_path" ]; then
    resolved="$launch_cwd/$log_cfg_path"
  else
    resolved="$log_cfg_path"
  fi

  if [ ! -f "$resolved" ]; then
    echo "FAIL: logging config path does not resolve to an existing file (CWD=$launch_cwd)."
    echo "      Expected an existing file, got: $resolved"
    exit 1
  fi
}

# 1) Foreign working directory (the reporter's scenario, e.g. cd bin && ./server.sh).
assert_log_config_resolves_from "$WORK_DIR"
# 2) Launched from the distribution home directory (backward-compatibility check).
assert_log_config_resolves_from "$HOME_DIR"

echo "PASS: logging config path resolves correctly regardless of the working directory."
exit 0
