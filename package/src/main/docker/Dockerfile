FROM adoptopenjdk/openjdk11:alpine-slim

MAINTAINER ArcateData LTD (info@arcadedata.com)

ENV JAVA_OPTS=" -XX:+UnlockExperimentalVMOptions -XX:+UseZGC "

ENV JAVA_OPTS_SCRIPT="-Djna.nosys=true -XX:+HeapDumpOnOutOfMemoryError -Djava.awt.headless=true -Dfile.encoding=UTF8 -Drhino.opt.level=9"

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
