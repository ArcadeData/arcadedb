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

mkdir -p adb1

cd adb1

export JAVA_OPTS="-Darcadedb.ha.enabled=true \
-Darcadedb.server.rootPath=./ \
-Darcadedb.typeDefaultBuckets=5 \
-Darcadedb.server.rootPassword=playwithdata \
-Darcadedb.ha.raftPort=2434 \
-Darcadedb.ha.clusterToken=123456789 \
-Darcadedb.ha.implementation=raft \
-Darcadedb.ha.clusterName=mycluster \
-Darcadedb.ha.serverList=localhost:2434:2481:0,localhost:2435:2482:0,localhost:2436:2483:0 \
-Darcadedb.server.httpIncomingPort=2481 \
-Darcadedb.ha.quorum=majority \
-Darcadedb.server.plugins=RaftHAPlugin \
-Darcadedb.server.name=node_0" && \
../package/target/arcadedb-26.4.1-SNAPSHOT.dir/arcadedb-26.4.1-SNAPSHOT/bin/server.sh \
-Dcom.sun.management.jmxremote=false -Dcom.sun.management.jmxremote.port=8888 -Dcom.sun.management.jmxremote.rmi.port=8887
