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
# Regression test: the launchers and the Docker image open no remote JMX port by default. They used to start every server
# with a JMX agent on ports 9999/9998 with authentication and SSL off, reachable from any host, which hands code execution
# inside the server to whoever reaches the port. Remote JMX is now opt-in through ARCADEDB_JMX.
#
# A stub "java" records the arguments server.sh launches the server with. server.bat and the Dockerfile cannot be run
# here, so their text is checked for an unauthenticated JMX setting instead.
#
# @author Luca Garulli (l.garulli@arcadedata.com)

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# package/src/test/scripts -> package/src/main
MAIN_DIR="$SCRIPT_DIR/../../main"
SERVER_SH="$MAIN_DIR/scripts/server.sh"
SERVER_BAT="$MAIN_DIR/scripts/server.bat"
DOCKERFILE="$MAIN_DIR/docker/Dockerfile"
LOG_PROPS="$MAIN_DIR/config/arcadedb-log.properties"

for f in "$SERVER_SH" "$SERVER_BAT" "$DOCKERFILE" "$LOG_PROPS"; do
  if [ ! -f "$f" ]; then
    echo "FAIL: cannot find $f"
    exit 1
  fi
done

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

HOME_DIR="$WORK_DIR/arcadedb"
mkdir -p "$HOME_DIR/bin" "$HOME_DIR/config" "$HOME_DIR/lib"
cp "$SERVER_SH" "$HOME_DIR/bin/server.sh"
chmod +x "$HOME_DIR/bin/server.sh"
cp "$LOG_PROPS" "$HOME_DIR/config/arcadedb-log.properties"

# Stub JDK, reached through JAVA_HOME (server.sh tests "${JAVA_HOME}/bin/java" first). It accepts the compact headers
# probe and records the arguments of the server launch, one per line.
STUB_JDK="$WORK_DIR/stubjdk"
mkdir -p "$STUB_JDK/bin"
ARG_DUMP="$WORK_DIR/java-args.txt"
cat >"$STUB_JDK/bin/java" <<STUB
#!/usr/bin/env sh
if [ "\$#" -eq 2 ] && [ "\$2" = "-version" ]; then
  exit 0
fi
for a in "\$@"; do
  echo "\$a" >>"$ARG_DUMP"
done
exit 0
STUB
chmod +x "$STUB_JDK/bin/java"

# launch [VAR=value ...]: run server.sh against the stub with only the extra environment given.
launch() {
  : >"$ARG_DUMP"
  (
    cd "$HOME_DIR"
    unset ARCADEDB_JMX JAVA_OPTS
    env JAVA_HOME="$STUB_JDK" ARCADEDB_PID="$WORK_DIR/arcadedb.pid" "$@" sh "$HOME_DIR/bin/server.sh" >/dev/null 2>&1
  )
  if [ ! -s "$ARG_DUMP" ]; then
    echo "FAIL: the stub java was never launched as the server"
    exit 1
  fi
  # The scan below must have reached the real launch: the main class is the proof.
  if ! grep -q -x -e 'com.arcadedb.server.ArcadeDBServer' "$ARG_DUMP"; then
    echo "FAIL: the recorded launch does not start com.arcadedb.server.ArcadeDBServer"
    exit 1
  fi
}

management_flags() {
  grep -c -e '^-Dcom\.sun\.management\.' "$ARG_DUMP" || true
}

# 1) By default server.sh passes no JMX setting: the JVM opens no remote JMX port.
launch
got="$(management_flags)"
if [ "$got" != "0" ]; then
  echo "FAIL: server.sh passed $got com.sun.management setting(s) by default:"
  grep -e '^-Dcom\.sun\.management\.' "$ARG_DUMP"
  exit 1
fi
echo "ok: server.sh opens no remote JMX port by default"

# 2) ARCADEDB_JMX is still the way to open it, passed through as given.
launch ARCADEDB_JMX="-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=true"
if ! grep -q -x -e '-Dcom.sun.management.jmxremote.port=9999' "$ARG_DUMP" ||
  ! grep -q -x -e '-Dcom.sun.management.jmxremote.authenticate=true' "$ARG_DUMP"; then
  echo "FAIL: server.sh did not pass ARCADEDB_JMX to the JVM"
  exit 1
fi
echo "ok: ARCADEDB_JMX is passed to the JVM"

# 3) No launcher and no image turns JMX authentication off, or opens the agent to remote hosts, on its own. Comment
#    lines (rem / #) are skipped: they may document the settings.
for f in "$SERVER_SH" "$SERVER_BAT" "$DOCKERFILE"; do
  offenders="$(grep -v -i -E '^[[:space:]]*(#|rem[[:space:]]|rem$)' "$f" | grep -i -E 'jmxremote' || true)"
  if [ -n "$offenders" ]; then
    echo "FAIL: $(basename "$f") sets JMX options outside a comment:"
    echo "$offenders"
    exit 1
  fi
  echo "ok: $(basename "$f") sets no JMX option"
done

if grep -q -E '^[[:space:]]*EXPOSE[[:space:]]+(9999|9998)' "$DOCKERFILE"; then
  echo "FAIL: the Dockerfile still exposes the JMX ports 9999/9998"
  exit 1
fi
echo "ok: the Dockerfile exposes no JMX port"

echo "PASS: remote JMX is off by default and opt-in through ARCADEDB_JMX."
exit 0
