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
# Regression test for issue #4537: server.sh adds -XX:+UseCompactObjectHeaders only when the JVM accepts it.
# The flag is a product option from Java 25; Java 21 rejects it and does not start, and Java 24 needs
# -XX:+UnlockExperimentalVMOptions first. So the script probes the JVM instead of adding the flag unconditionally.
#
# A stub "java" stands in for each of those JVMs: it answers the probe ("-XX:+UseCompactObjectHeaders -version")
# the way that version does, and records the arguments of the real launch. The test then checks what server.sh
# handed the JVM for each version, for an explicit opt-out, and for an explicit override.

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

HOME_DIR="$WORK_DIR/arcadedb"
mkdir -p "$HOME_DIR/bin" "$HOME_DIR/config" "$HOME_DIR/lib"
cp "$SERVER_SH" "$HOME_DIR/bin/server.sh"
chmod +x "$HOME_DIR/bin/server.sh"
cp "$LOG_PROPS" "$HOME_DIR/config/arcadedb-log.properties"

# Stub JDK. STUB_JAVA_FEATURE selects which JVM its "java" imitates when probed; any other invocation is the server
# launch, whose arguments are recorded one per line. Every probe is counted, so an override can be shown to skip it.
# It is reached through JAVA_HOME rather than PATH: server.sh tests "${JAVA_HOME}/bin/java" first, and with JAVA_HOME
# empty that is /bin/java, which is a real JDK on many machines and would silently replace the stub.
STUB_JDK="$WORK_DIR/stubjdk"
STUB_BIN="$STUB_JDK/bin"
mkdir -p "$STUB_BIN"
ARG_DUMP="$WORK_DIR/java-args.txt"
PROBE_LOG="$WORK_DIR/probes.txt"
cat >"$STUB_BIN/java" <<STUB
#!/usr/bin/env sh
if [ "\$#" -eq 2 ] && [ "\$1" = "-XX:+UseCompactObjectHeaders" ] && [ "\$2" = "-version" ]; then
  echo probe >>"$PROBE_LOG"
  case "\$STUB_JAVA_FEATURE" in
    21) echo "Unrecognized VM option 'UseCompactObjectHeaders'" >&2; exit 1 ;;
    24) echo "Error: VM option 'UseCompactObjectHeaders' is experimental and must be enabled via -XX:+UnlockExperimentalVMOptions." >&2; exit 1 ;;
    *) exit 0 ;;
  esac
fi
for a in "\$@"; do
  echo "\$a" >>"$ARG_DUMP"
done
exit 0
STUB
chmod +x "$STUB_BIN/java"

# launch <java feature> [VAR=value ...]: run server.sh against the stub, with the extra environment given.
launch() {
  feature="$1"
  shift
  : >"$ARG_DUMP"
  : >"$PROBE_LOG"
  (
    cd "$HOME_DIR"
    # The caller's own overrides must not leak in: an exported ARCADEDB_OPTS_HEADERS skips the probe, and a
    # JAVA_OPTS carrying the flag changes the count. Each case sets only what it names.
    unset ARCADEDB_OPTS_HEADERS JAVA_OPTS
    env JAVA_HOME="$STUB_JDK" ARCADEDB_PID="$WORK_DIR/arcadedb.pid" STUB_JAVA_FEATURE="$feature" "$@" \
      sh "$HOME_DIR/bin/server.sh" >/dev/null 2>&1
  )
  if [ ! -s "$ARG_DUMP" ]; then
    echo "FAIL: the stub java was never launched as the server (Java $feature)"
    exit 1
  fi
}

flag_count() {
  grep -c -x -e '-XX:+UseCompactObjectHeaders' "$ARG_DUMP" || true
}

expect_flag() {
  expected="$1"
  label="$2"
  got="$(flag_count)"
  if [ "$got" != "$expected" ]; then
    echo "FAIL: $label: expected -XX:+UseCompactObjectHeaders $expected time(s) in the JVM arguments, got $got"
    exit 1
  fi
  echo "ok: $label"
}

# 1) Java 21 rejects the flag: the server must start without it.
launch 21
expect_flag 0 "Java 21 starts without the flag"

# 2) Java 24 rejects it without the unlock option: same.
launch 24
expect_flag 0 "Java 24 starts without the flag"

# 3) Java 25 accepts it: it is added exactly once.
launch 25
expect_flag 1 "Java 25 starts with the flag"

# 4) An explicitly empty ARCADEDB_OPTS_HEADERS opts out, even where the JVM would accept it.
launch 25 ARCADEDB_OPTS_HEADERS=
expect_flag 0 "an empty ARCADEDB_OPTS_HEADERS opts out on Java 25"

# 5) An explicit value is used as given and the probe is skipped.
launch 25 ARCADEDB_OPTS_HEADERS=-XX:+UseCompactObjectHeaders
expect_flag 1 "an explicit ARCADEDB_OPTS_HEADERS is passed through"
if [ -s "$PROBE_LOG" ]; then
  echo "FAIL: server.sh probed the JVM although ARCADEDB_OPTS_HEADERS was set"
  exit 1
fi
echo "ok: an explicit ARCADEDB_OPTS_HEADERS skips the probe"

echo "PASS: server.sh adds -XX:+UseCompactObjectHeaders only when the JVM accepts it."
exit 0
