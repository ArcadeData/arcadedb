
PWD: /Users/ocohen/git/Verdance/ArcadeDB-GRPC/arcadedb-25.8.1-SNAPSHOT


./bin/server.sh
-Darcadedb.server.rootPassword=root1234
-Darcadedb.server.name=Arcade_GRPC_Test
-Darcadedb.dumpConfigAtStartup=true
-Darcadedb.server.mode=development
-Darcadedb.server.rootPath=../var/arcadedb
-Darcadedb.server.plugins=GRPC:com.arcadedb.server.grpc.GrpcServerPlugin
-Xms512M -Xmx4096M -XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0
-Darcadedb.server.httpIncomingPort=2489
-Darcadedb.grpc.enabled=true
-Darcadedb.grpc.port=50059
-Darcadedb.grpc.mode=standard
-Darcadedb.grpc.reflection.enabled=true
-Darcadedb.grpc.health.enabled=true


Ports:

HTTP: 2489
GTPC: 50059

root
root1234

Logging:

d exec -it arcadedb1-vulcan sh


vi /home/arcadedb/config/arcadedb-log.properties


handlers = java.util.logging.ConsoleHandler, java.util.logging.FileHandler

.level = INFO
com.arcadedb.level = INFO
com.arcadedb.server.grpc.level = FINE

java.util.logging.ConsoleHandler.level = INFO
java.util.logging.ConsoleHandler.formatter = com.arcadedb.utility.AnsiLogFormatter

java.util.logging.FileHandler.level = FINE
java.util.logging.FileHandler.pattern=./log/arcadedb.log
java.util.logging.FileHandler.formatter = com.arcadedb.log.LogFormatter
java.util.logging.FileHandler.limit=100000000
java.util.logging.FileHandler.count=10




tail -f /home/arcadedb/log/arcadedb.log.0




d exec -it arcadedb1-vulcan sh
