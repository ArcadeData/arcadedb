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

# Java 25 (LTS) for the runtime only: the jars are still compiled for release 21, so embedders keep running them on
# Java 21. 25 is the first release where bin/server.sh can switch on compact object headers (JEP 519), which Java 21
# rejects outright (issue #9602).
FROM eclipse-temurin:25-alpine@sha256:3fd2d245c4e0eba615fe366a71b8bd25f5db7104f53e4026b24bf508b880bd2a

LABEL maintainer="Arcade Data LTD (info@arcadedb.com)"
LABEL io.modelcontextprotocol.server.name="com.arcadedb/mcp-server"

# GC selection lives in its own variable, NOT in JAVA_OPTS: quickstarts and orchestrators routinely
# set JAVA_OPTS to pass extra flags, and Docker replaces an environment variable instead of appending
# to it, so a GC choice parked there would silently vanish exactly when the user customises the image.
# Pass database settings with ARCADEDB_SETTINGS and extra JVM flags with JAVA_OPTS: bin/server.sh
# puts all of them on the java command line alongside this one. ZGC is generational-only from Java 24, so
# -XX:+ZGenerational is obsolete and would only print a warning at every start.
ENV ARCADEDB_OPTS_GC="-XX:+UseZGC"

# Size the heap as a percentage of the container memory limit rather than pinning it: a fixed -Xmx
# larger than the cgroup limit kills the container at startup, and a fixed -Xmx smaller than the
# limit wastes whatever the operator granted. The JVM reads the cgroup limit, so this one value
# works from a 512Mi sandbox up to a multi-GB production node.
ENV ARCADEDB_OPTS_MEMORY="-XX:MaxRAMPercentage=75"

ENV ARCADEDB_JMX="-Dcom.sun.management.jmxremote=true \
    -Dcom.sun.management.jmxremote.local.only=false \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.port=9999 \
    -Dcom.sun.management.jmxremote.rmi.port=9998"

RUN apk -U upgrade && rm -rf /var/cache/apk/*

RUN adduser -D -h /home/arcadedb -s /bin/sh arcadedb

WORKDIR /home/arcadedb

USER arcadedb

COPY --chown=arcadedb:arcadedb ./arcadedb-* ./

RUN chmod +x ./bin/*.sh

# Volumes available
VOLUME [ "/home/arcadedb/config"]

VOLUME [ "/home/arcadedb/databases"]

VOLUME [ "/home/arcadedb/replication"]

VOLUME [ "/home/arcadedb/backups"]

VOLUME [ "/home/arcadedb/log"]



# ArcadeDB HTTP API & STUDIO
EXPOSE 2480

# Raft gRPC (replication)
EXPOSE 2434

# Gremlin Server (Apache TinkerPop)
EXPOSE 8182

# Postgres protocol
EXPOSE 5432

# Redis protocol
EXPOSE 6379

# MongoDB Protocol
EXPOSE 27017

# gRPC service
EXPOSE 50051

# JMX for monitoring
EXPOSE 9999
EXPOSE 9998

CMD ["./bin/server.sh"]
