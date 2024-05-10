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


FROM maven:3.8.3-eclipse-temurin-11 as build
COPY . ./
RUN mvn clean install -DskipTests

FROM eclipse-temurin:11
LABEL maintainer="Arcade Data LTD (info@arcadedb.com)"

ENV JAVA_OPTS="-Darcadedb.server.rootPassword=playwithdata"

ENV JAVA_OPTS_SCRIPT="--add-opens java.base/java.io=ALL-UNNAMED -Dpolyglot.engine.WarnInterpreterOnly=false -Djna.nosys=true -XX:+HeapDumpOnOutOfMemoryError -Djava.awt.headless=true -Dfile.encoding=UTF8"

ENV ARCADEDB_OPTS_MEMORY="-Xms2G -Xmx2G"

ENV ARCADEDB_JMX="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.rmi.port=9998"

RUN useradd -ms /bin/bash arcadedb -u 1234
RUN groupadd arcadedb -f -g 1234
RUN usermod -a -G arcadedb arcadedb

WORKDIR /home/arcadedb

USER arcadedb

COPY --from=build --chown=arcadedb:arcadedb ./package/target/arcadedb-*/arcadedb-*  ./

RUN chmod +x ./bin/*.sh

# Volumes available
VOLUME [ "/home/arcadedb/databases"]

VOLUME [ "/home/arcadedb/log"]

VOLUME [ "/home/arcadedb/config"]

# ArcadeDB HTTP API & STUDIO
EXPOSE 2480

# ArcadeDB Binary Protocol (replication)
EXPOSE 2424

# Gremlin Server (Apache TinkerPop)
# Commented out for now, the gremlin server bypasses most of the data access enforcements that have been added
# EXPOSE 8182

# Postgres protocol
EXPOSE 5432

# Redis protocol
EXPOSE 6379

# MongoDB Protocol
EXPOSE 27017

# JMX for monitoring
EXPOSE 9999
EXPOSE 9998

# Remote debug port
EXPOSE 8000

CMD ["./bin/server.sh"]
