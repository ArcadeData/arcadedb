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

FROM adoptopenjdk/openjdk11:alpine-slim

LABEL maintainer="Arcade Data LTD (info@arcadedb.com)"

ENV JAVA_OPTS=" -XX:+UnlockExperimentalVMOptions -XX:+UseZGC "

ENV JAVA_OPTS_SCRIPT="-Djna.nosys=true -XX:+HeapDumpOnOutOfMemoryError -Djava.awt.headless=true -Dfile.encoding=UTF8"

ENV ARCADEDB_OPTS_MEMORY="-Xms2G -Xmx2G"

ENV ARCADEDB_JMX="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.rmi.port=9998"

RUN adduser -D -s /bin/sh arcadedb

WORKDIR /home/arcadedb

COPY arcadedb-*.tar.gz arcadedb.tar.gz

RUN tar -xvzf arcadedb.tar.gz -C ./ --strip-components=1\
      && rm arcadedb.tar.gz \
      && rm -rf ./databases/*

ENV PATH ./bin:$PATH

VOLUME [ "/home/arcadedb/databases"]

EXPOSE 2424

EXPOSE 2480

#JMX
EXPOSE 9999

EXPOSE 9998

CMD ["server.sh"]
