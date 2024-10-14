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


FROM eclipse-temurin:11

LABEL maintainer="Arcade Data LTD (info@arcadedb.com)"

ENV JAVA_OPTS=" "

ENV ARCADEDB_OPTS_MEMORY="-Xms2G -Xmx2G"

ENV ARCADEDB_JMX="-Dcom.sun.management.jmxremote=true \
    -Dcom.sun.management.jmxremote.local.only=false \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.port=9999 \
    -Dcom.sun.management.jmxremote.rmi.port=9998"

RUN useradd -ms /bin/bash arcadedb

WORKDIR /home/arcadedb

USER arcadedb

COPY --chown=arcadedb:arcadedb ./arcadedb-* ./

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
EXPOSE 8182

# Postgres protocol
EXPOSE 5432

# Redis protocol
EXPOSE 6379

# MongoDB Protocol
EXPOSE 27017

# JMX for monitoring
EXPOSE 9999
EXPOSE 9998

CMD ["./bin/server.sh"]
