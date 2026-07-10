#!/usr/bin/env sh
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

echo ""
echo " █████╗ ██████╗  ██████╗ █████╗ ██████╗ ███████╗██████╗ ██████╗"
echo "██╔══██╗██╔══██╗██╔════╝██╔══██╗██╔══██╗██╔════╝██╔══██╗██╔══██╗"
echo "███████║██████╔╝██║     ███████║██║  ██║█████╗  ██║  ██║██████╔╝"
echo "██╔══██║██╔══██╗██║     ██╔══██║██║  ██║██╔══╝  ██║  ██║██╔══██╗"
echo "██║  ██║██║  ██║╚██████╗██║  ██║██████╔╝███████╗██████╔╝██████╔╝"
echo "╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚═════╝ ╚══════╝╚═════╝ ╚═════╝"
echo "PLAY WITH DATA                                    arcadedb.com\n"

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ]; do
  ls=$(ls -ld "$PRG")
  link=$(expr "$ls" : '.*-> \(.*\)$')
  if expr "$link" : '/.*' >/dev/null; then
    PRG="$link"
  else
    PRG=$(dirname "$PRG")/"$link"
  fi
done

# Get standard environment variables
PRGDIR=$(dirname "$PRG")

# Only set ARCADEDB_HOME if not already set
[ -f "$ARCADEDB_HOME"/bin/server.sh ] || ARCADEDB_HOME=$(
  cd "$PRGDIR/.."
  pwd
)

# Raspberry Pi check (Java VM does not run with -server argument on ARMv6)
if [ $(uname -m) != "armv6l" ]; then
  JAVA_OPTS="$JAVA_OPTS -server "
fi

# Set JavaHome if it exists
if [ -f "${JAVA_HOME}/bin/java" ]; then
  JAVA=${JAVA_HOME}/bin/java
else
  JAVA=java
fi

if [ -z "$ARCADEDB_PID" ]; then
  ARCADEDB_PID=$ARCADEDB_HOME/bin/arcadedb.pid
fi

if [ -f "$ARCADEDB_PID" ]; then
  echo "removing old pid file $ARCADEDB_PID"
  rm "$ARCADEDB_PID"
fi

# ARCADEDB memory options, default uses the available RAM. To set it to a specific value, like 2GB of heap, use "-Xms2G -Xmx2G".
#
# Tip for low-footprint setups (e.g. arcadedb.profile=low-ram): prefer a small initial heap
# such as "-Xms64M -Xmx256M". Pinning Xms=Xmx to a large value (e.g. -Xms512M -Xmx512M) tells
# the JVM "you have 512MB, fill it" and G1GC has no pressure to collect young-gen garbage, so
# the heap-used gauge in Studio can sit at hundreds of MB on an idle server even though the
# live working set is only ~30-50MB. A smaller initial heap makes the JVM collect early and
# Studio reports the actual live heap.
if [ -z "$ARCADEDB_OPTS_MEMORY" ]; then
  ARCADEDB_OPTS_MEMORY=""
fi

if [ -z "$JAVA_OPTS_SCRIPT" ]; then
  JAVA_OPTS_SCRIPT="-XX:+HeapDumpOnOutOfMemoryError \
        --add-exports java.management/sun.management=ALL-UNNAMED \
        --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED \
        --add-opens java.base/java.nio.channels.spi=ALL-UNNAMED \
        --add-opens java.base/java.lang=ALL-UNNAMED \
        --add-modules jdk.incubator.vector \
        -Dpolyglot.engine.WarnInterpreterOnly=false \
        -Djava.awt.headless=true -Dfile.encoding=UTF8 \
        -Dsun.net.inetaddr.ttl=30 -Dsun.net.inetaddr.negative.ttl=0 \
        --enable-native-access=ALL-UNNAMED"
fi

if [ -z "$ARCADEDB_JMX" ]; then
  ARCADEDB_JMX="-Dcom.sun.management.jmxremote=true \
        -Dcom.sun.management.jmxremote.local.only=false \
        -Dcom.sun.management.jmxremote.authenticate=false \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Dcom.sun.management.jmxremote.port=9999 \
        -Dcom.sun.management.jmxremote.rmi.port=9998"
fi

echo $$ >$ARCADEDB_PID

# Optional: relocate log files to a writable directory (e.g. on read-only root filesystems).
# ARCADEDB_LOG_DIR maps to the arcadedb.server.logsDirectory configuration referenced by
# config/arcadedb-log.properties. When unset, no argument is added and logs default to ./log.
# Passed as a quoted, conditional argument so a path containing spaces stays a single argument.
# The logging configuration path is anchored to ARCADEDB_HOME (not the current directory) so the
# server picks up config/arcadedb-log.properties regardless of the directory the script is launched
# from. It is passed as its own quoted argument so a home path containing spaces stays intact.
exec "$JAVA" $JAVA_OPTS \
  $ARCADEDB_OPTS_MEMORY \
  $JAVA_OPTS_SCRIPT \
  "-Djava.util.logging.config.file=$ARCADEDB_HOME/config/arcadedb-log.properties" \
  ${ARCADEDB_LOG_DIR:+"-Darcadedb.server.logsDirectory=$ARCADEDB_LOG_DIR"} \
  $ARCADEDB_JMX \
  $ARCADEDB_SETTINGS \
  -cp "$ARCADEDB_HOME/lib/*" \
  $ARGS "$@" com.arcadedb.server.ArcadeDBServer
