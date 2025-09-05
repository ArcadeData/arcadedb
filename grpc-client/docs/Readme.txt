
PWD: /Users/ocohen/git/Verdance/ArcadeDB-GRPC/arcadedb-25.8.1-SNAPSHOT


./bin/server.sh -Darcadedb.server.rootPassword=root1234 -Darcadedb.server.name=Arcade_GRPC_Test -Darcadedb.dumpConfigAtStartup=true -Darcadedb.server.mode=development -Darcadedb.server.rootPath=../var/arcadedb -Darcadedb.server.plugins=GRPC:com.arcadedb.server.grpc.GrpcServerPlugin -Xms512M -Xmx4096M -XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0 -Darcadedb.server.httpIncomingPort=2489 -Darcadedb.grpc.enabled=true -Darcadedb.grpc.port=50059 -Darcadedb.grpc.mode=standard -Darcadedb.grpc.reflection.enabled=true -Darcadedb.grpc.health.enabled=true


Ports: 

HTTP: 2489
GTPC: 50059

root
root1234

Logging:

d exec -it arcadedb1-vulcan sh


/home/arcadedb/config/arcadedb-log.properties


#
# Copyright 2021-present Arcade Data Ltd
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Specify the handlers to create in the root logger
# (all loggers are children of the root logger)
# The following creates two handlers
handlers = java.util.logging.ConsoleHandler, java.util.logging.FileHandler

# Set the default logging level for the root logger
.level = INFO
com.arcadedb.level = INFO
com.arcadedb.server.grpc.level = FINE

# Set the default logging level for new ConsoleHandler instances
java.util.logging.ConsoleHandler.level = INFO
# Set the default formatter for new ConsoleHandler instances
java.util.logging.ConsoleHandler.formatter = com.arcadedb.utility.AnsiLogFormatter

# Set the default logging level for new FileHandler instances
java.util.logging.FileHandler.level = FINE
# Naming style for the output file
java.util.logging.FileHandler.pattern=./log/arcadedb.log
# Set the default formatter for new FileHandler instances
java.util.logging.FileHandler.formatter = com.arcadedb.log.LogFormatter
# Limiting size of output file in bytes:
java.util.logging.FileHandler.limit=100000000
# Number of output files to cycle through, by appending an
# integer to the base file name:
java.util.logging.FileHandler.count=10