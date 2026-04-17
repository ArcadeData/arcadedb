# HA-Raft Test Porting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port 13 HA integration tests from `server/src/test/java/com/arcadedb/server/ha/` to `ha-raft/src/test/java/com/arcadedb/server/ha/raft/`, covering HTTP, write forwarding, schema/views, index operations, database utilities, config validation, and chaos scenarios.

**Architecture:** All tests extend `BaseRaftHATest` (which extends `BaseGraphServerTest`). Old HA API calls (`getServer(i).getHA()`, `ReplicationServerIT`, `HAServer.SERVER_ROLE`) are replaced with Raft equivalents (`getRaftPlugin(i)`, direct `BaseRaftHATest` methods). No new base classes are added.

**Tech Stack:** JUnit 5, AssertJ, Awaitility, ArcadeDB test infrastructure (`BaseRaftHATest`, `BaseGraphServerTest`), Java 21

---

## File Map

**Create** (all in `ha-raft/src/test/java/com/arcadedb/server/ha/raft/`):
1. `RaftHTTP2ServersIT.java`
2. `RaftHTTP2ServersCreateReplicatedDatabaseIT.java`
3. `RaftHTTPGraphConcurrentIT.java`
4. `RaftReplicationWriteAgainstReplicaIT.java`
5. `RaftReplicationChangeSchemaIT.java`
6. `RaftReplicationMaterializedViewIT.java`
7. `RaftIndexCompactionReplicationIT.java`
8. `RaftIndexOperations3ServersIT.java`
9. `RaftServerDatabaseBackupIT.java`
10. `RaftServerDatabaseSqlScriptIT.java`
11. `RaftServerDatabaseAlignIT.java`
12. `RaftHAConfigurationIT.java`
13. `RaftHARandomCrashIT.java`

**Modify:**
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java` — add localhost-mixing peer address validation

---

## Key Adaptation Rules (apply in every task)

| Old pattern | Raft equivalent |
|---|---|
| `extends BaseGraphServerTest` | `extends BaseRaftHATest` |
| `extends ReplicationServerIT` | `extends BaseRaftHATest` (no equivalent base) |
| `getServer(i).getHA().getServerName()` | `getServer(i).getServerName()` |
| `getServer(i).getHA().getLeaderName()` | find via `getRaftPlugin(i).isLeader()` |
| `getServer(i).getHA()` (any method) | `getRaftPlugin(i).getRaftHAServer()` |
| `HAServer.SERVER_ROLE.ANY` | not needed — Raft always uses ANY |
| `areAllReplicasAreConnected()` | check `getRaftPlugin(i) != null && getRaftPlugin(i).isLeader()` for any i |
| `waitForReplicationIsCompleted(i)` | already overridden in `BaseRaftHATest`, call as-is |
| `hAConfiguration()` using `RemoteDatabase` | use cluster HTTP endpoint instead |
| Package declaration | `com.arcadedb.server.ha.raft` |

---

## Task 1: Group 1 — HTTP Layer Tests

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHTTP2ServersIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHTTP2ServersCreateReplicatedDatabaseIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHTTPGraphConcurrentIT.java`

- [ ] **Step 1: Create RaftHTTP2ServersIT.java**

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

import static com.arcadedb.schema.Property.RID_PROPERTY;
import static org.assertj.core.api.Assertions.*;

class RaftHTTP2ServersIT extends BaseRaftHATest {

  @Test
  void serverInfo() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server?mode=cluster").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      try {
        connection.connect();
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: %s", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void propagationOfSchema() throws Exception {
    testEachServer((serverIndex) -> {
      final String response = command(serverIndex, "create vertex type RaftVertexType" + serverIndex);
      assertThat(response).contains("RaftVertexType" + serverIndex)
          .withFailMessage("Type RaftVertexType" + serverIndex + " not found on server " + serverIndex);
    });

    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            try {
              command(i, "select from RaftVertexType" + i);
            } catch (final Exception e) {
              return false;
            }
          }
          return true;
        });
  }

  @Test
  void checkQuery() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%20from%20V1%20limit%201").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();
      try {
        final String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(response.contains("V1")).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void checkDeleteGraphElements() throws Exception {
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer((serverIndex) -> {
      final String v1 = new JSONObject(
          command(serverIndex, "create vertex V1 content {\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}"))
          .getJSONArray("result").getJSONObject(0).getString(RID_PROPERTY);

      waitForReplicationIsCompleted(serverIndex);

      testEachServer((checkServer) ->
          assertThat(new JSONObject(command(checkServer, "select from " + v1)).getJSONArray("result")).isNotEmpty());

      final String v2 = new JSONObject(
          command(serverIndex, "create vertex V1 content {\"name\":\"John\",\"surname\":\"Red\",\"age\":50}"))
          .getJSONArray("result").getJSONObject(0).getString(RID_PROPERTY);

      waitForReplicationIsCompleted(serverIndex);

      testEachServer((checkServer) ->
          assertThat(new JSONObject(command(checkServer, "select from " + v2)).getJSONArray("result")).isNotEmpty());

      final String e1 = new JSONObject(command(serverIndex, "create edge E1 from " + v1 + " to " + v2))
          .getJSONArray("result").getJSONObject(0).getString(RID_PROPERTY);

      waitForReplicationIsCompleted(serverIndex);

      testEachServer((checkServer) ->
          assertThat(new JSONObject(command(checkServer, "select from " + e1)).getJSONArray("result")).isNotEmpty());

      command(serverIndex, "delete from " + v1);
      waitForReplicationIsCompleted(serverIndex);
      for (int i = 0; i < getServerCount(); i++)
        if (i != serverIndex)
          waitForReplicationIsCompleted(i);

      testEachServer((checkServer) -> {
        try {
          final JSONObject jsonResponse = new JSONObject(command(checkServer, "select from " + v1));
          assertThat(jsonResponse.getJSONArray("result").length()).isEqualTo(0);
        } catch (final IOException e) {
          // HTTP error means record not found — acceptable
        }
        try {
          final JSONObject jsonResponse = new JSONObject(command(checkServer, "select from " + e1));
          assertThat(jsonResponse.getJSONArray("result").length()).isEqualTo(0);
        } catch (final IOException e) {
          // HTTP error means edge not found — acceptable
        }
      });
    });
  }

  @Test
  void hAConfiguration() throws Exception {
    // Verify the cluster endpoint reports exactly one leader across both nodes
    int leaderCount = 0;
    for (int i = 0; i < getServerCount(); i++) {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + i + "/api/v1/cluster").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      try {
        connection.connect();
        final String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(response).contains("\"implementation\":\"raft\"");
        final JSONObject json = new JSONObject(response);
        if (json.getBoolean("isLeader"))
          leaderCount++;
      } finally {
        connection.disconnect();
      }
    }
    assertThat(leaderCount).isEqualTo(1);
  }
}
```

- [ ] **Step 2: Create RaftHTTP2ServersCreateReplicatedDatabaseIT.java**

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

import static com.arcadedb.schema.Property.RID_PROPERTY;
import static org.assertj.core.api.Assertions.*;

class RaftHTTP2ServersCreateReplicatedDatabaseIT extends BaseRaftHATest {

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void createReplicatedDatabase() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:2480/api/v1/server").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("command", "create database " + getDatabaseName()));
      connection.connect();
      final String response = readResponse(connection);
      LogManager.instance().log(this, Level.FINE, "Response: %s", null, response);
      assertThat(connection.getResponseCode()).isEqualTo(200);
      assertThat(connection.getResponseMessage()).isEqualTo("OK");
    } finally {
      connection.disconnect();
    }

    testEachServer((serverIndex) -> {
      final String response = command(serverIndex, "create vertex type RaftCreateVertex" + serverIndex);
      assertThat(response).contains("RaftCreateVertex" + serverIndex);
    });

    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            try {
              command(i, "select from RaftCreateVertex" + i);
            } catch (final Exception e) {
              return false;
            }
          }
          return true;
        });

    testEachServer((serverIndex) -> {
      for (int i = 0; i < 100; i++) {
        final String v1 = new JSONObject(
            command(serverIndex, "create vertex RaftCreateVertex" + serverIndex
                + " content {\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}"))
            .getJSONArray("result").getJSONObject(0).getString(RID_PROPERTY);

        testEachServer((checkServer) ->
            assertThat(new JSONObject(command(checkServer, "select from " + v1)).getJSONArray("result")).isNotEmpty()
                .withFailMessage("executed on server " + serverIndex + " checking on server " + checkServer));
      }
    });
  }
}
```

- [ ] **Step 3: Create RaftHTTPGraphConcurrentIT.java**

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.*;

class RaftHTTPGraphConcurrentIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void oneEdgePerTxMultiThreads() throws Exception {
    testEachServer((serverIndex) -> {
      executeCommand(serverIndex, "sqlscript",
          "create vertex type RaftPhotos" + serverIndex + ";"
              + "create vertex type RaftUsers" + serverIndex + ";"
              + "create edge type RaftHasUploaded" + serverIndex + ";");

      waitForReplicationIsCompleted(serverIndex);

      executeCommand(serverIndex, "sql", "create vertex RaftUsers" + serverIndex + " set id = 'u1111'");
      waitForReplicationIsCompleted(serverIndex);

      final int THREADS = 4;
      final int SCRIPTS = 100;
      final AtomicInteger atomic = new AtomicInteger();

      final ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
      final List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < THREADS; i++) {
        final Future<?> future = executorService.submit(() -> {
          for (int j = 0; j < SCRIPTS; j++) {
            try {
              final JSONObject responseAsJson = executeCommand(serverIndex, "sqlscript",
                  "BEGIN ISOLATION REPEATABLE_READ;"
                      + "LET photo = CREATE vertex RaftPhotos" + serverIndex + " SET id = uuid(), name = \"downloadX.jpg\";"
                      + "LET user = SELECT FROM RaftUsers" + serverIndex + " WHERE id = \"u1111\";"
                      + "LET userEdge = Create edge RaftHasUploaded" + serverIndex
                      + " FROM $user to $photo set type = \"User_Photos\";"
                      + "commit retry 100;return $photo;");

              atomic.incrementAndGet();
              assertThat(responseAsJson).isNotNull();
              assertThat(responseAsJson.getJSONObject("result").getJSONArray("records")).isNotNull();
            } catch (final Exception e) {
              fail(e);
            }
          }
        });
        futures.add(future);
      }

      for (final Future<?> future : futures)
        future.get(60, TimeUnit.SECONDS);

      executorService.shutdown();
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS))
        executorService.shutdownNow();

      assertThat(atomic.get()).isEqualTo(THREADS * SCRIPTS);

      final JSONObject select = executeCommand(serverIndex, "sql",
          "SELECT id FROM ( SELECT expand( outE('RaftHasUploaded" + serverIndex + "') ) FROM RaftUsers" + serverIndex
              + " WHERE id = \"u1111\" )");

      assertThat(select.getJSONObject("result").getJSONArray("records").length())
          .isEqualTo(THREADS * SCRIPTS)
          .withFailMessage("Some edges were missing when executing from server " + serverIndex);
    });
  }
}
```

- [ ] **Step 4: Compile task 1 tests**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign/ha-raft
mvn test-compile -q
```

Expected: BUILD SUCCESS with no errors. Fix any compilation errors before proceeding.

- [ ] **Step 5: Run task 1 tests individually**

```bash
mvn test -Dtest="RaftHTTP2ServersIT" -DskipITs=false 2>&1 | tail -30
mvn test -Dtest="RaftHTTP2ServersCreateReplicatedDatabaseIT" -DskipITs=false 2>&1 | tail -30
mvn test -Dtest="RaftHTTPGraphConcurrentIT" -DskipITs=false 2>&1 | tail -30
```

Expected: Tests pass. If a test fails, read the error, fix the test, and rerun before moving to the next step.

- [ ] **Step 6: Commit task 1**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHTTP2ServersIT.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHTTP2ServersCreateReplicatedDatabaseIT.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHTTPGraphConcurrentIT.java
git commit -m "test(ha-raft): port HTTP layer IT tests from server/ha"
```

---

## Task 2: Group 2 — Write Forwarding + Schema/Views

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationWriteAgainstReplicaIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationChangeSchemaIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationMaterializedViewIT.java`

- [ ] **Step 1: Create RaftReplicationWriteAgainstReplicaIT.java**

This test finds the follower node (not the leader) and writes records to it via the server-side database. The `RaftReplicatedDatabase.commit()` on a follower forwards the transaction to the leader and replicates to all nodes.

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;

import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.*;

class RaftReplicationWriteAgainstReplicaIT extends BaseRaftHATest {

  private static final int TXS              = 100;
  private static final int VERTICES_PER_TX  = 100;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void writesForwardedFromReplicaToLeader() {
    // Find a follower (non-leader) server index
    int followerIndex = -1;
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin != null && !plugin.isLeader()) {
        followerIndex = i;
        break;
      }
    }
    assertThat(followerIndex).as("Expected to find a follower node").isGreaterThanOrEqualTo(0);
    LogManager.instance().log(this, Level.INFO, "Writing against follower node %d", followerIndex);

    final Database db = getServerDatabase(followerIndex, getDatabaseName());
    db.begin();

    long counter = 0;
    for (int tx = 0; tx < TXS; tx++) {
      final long lastGoodCounter = counter;
      for (int retry = 0; retry < 30; retry++) {
        try {
          for (int i = 0; i < VERTICES_PER_TX; i++) {
            final MutableVertex v = db.newVertex(VERTEX1_TYPE_NAME);
            v.set("id", ++counter);
            v.set("name", "replica-write-test");
            v.save();
          }
          db.commit();
          break;
        } catch (final TransactionException | NeedRetryException e) {
          LogManager.instance().log(this, Level.FINE, "Retry %d/%d: %s", retry, 30, e.toString());
          if (retry >= 29)
            throw e;
          counter = lastGoodCounter;
        } finally {
          db.begin();
        }
      }
    }
    db.commit();

    LogManager.instance().log(this, Level.INFO, "Wrote %d vertices via follower, waiting for replication", counter);

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Verify all nodes have the data
    final long expectedCount = 1L + TXS * VERTICES_PER_TX; // 1 from populateDatabase() + written
    for (int i = 0; i < getServerCount(); i++) {
      final Database serverDb = getServerDatabase(i, getDatabaseName());
      final long[] count = {0};
      serverDb.transaction(() -> count[0] = serverDb.countType(VERTEX1_TYPE_NAME, true));
      assertThat(count[0]).as("Server %d should have %d vertices", i, expectedCount).isEqualTo(expectedCount);
    }
  }
}
```

- [ ] **Step 2: Create RaftReplicationChangeSchemaIT.java**

This test validates the full schema lifecycle: create/drop types, properties, buckets, indexes. Verifies that schema changes replicate to all nodes and that replicas reject direct schema modifications.

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.Index;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class RaftReplicationChangeSchemaIT extends BaseRaftHATest {

  private Database[]          databases;
  private Map<String, String> schemaFiles;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void schemaChangesReplicate() throws Exception {
    databases = new Database[getServerCount()];
    schemaFiles = new LinkedHashMap<>(getServerCount());

    for (int i = 0; i < getServerCount(); i++) {
      databases[i] = getServer(i).getDatabase(getDatabaseName());
      if (databases[i].isTransactionActive())
        databases[i].commit();
    }

    // CREATE NEW TYPE
    final VertexType type1 = databases[0].getSchema().createVertexType("RaftRuntimeVertex0");
    for (int i = 0; i < getServerCount(); i++) {
      databases[i] = getServer(i).getDatabase(getDatabaseName());
      if (databases[i].isTransactionActive())
        databases[i].commit();
    }
    testOnAllServers((database) -> isInSchemaFile(database, "RaftRuntimeVertex0"));

    // CREATE NEW PROPERTY
    type1.createProperty("nameNotFoundInDictionary", Type.STRING);
    testOnAllServers((database) -> isInSchemaFile(database, "nameNotFoundInDictionary"));

    // CREATE NEW BUCKET
    final Bucket newBucket = databases[0].getSchema().createBucket("raftNewBucket");
    for (final Database database : databases)
      assertThat(database.getSchema().existsBucket("raftNewBucket")).isTrue();

    type1.addBucket(newBucket);
    testOnAllServers((database) -> isInSchemaFile(database, "raftNewBucket"));

    // CHANGE SCHEMA FROM A REPLICA (ERROR EXPECTED)
    assertThatThrownBy(() -> databases[1].getSchema().createVertexType("RaftRuntimeVertex1"))
        .isInstanceOf(ServerIsNotTheLeaderException.class);
    testOnAllServers((database) -> isNotInSchemaFile(database, "RaftRuntimeVertex1"));

    // DROP PROPERTY
    type1.dropProperty("nameNotFoundInDictionary");
    testOnAllServers((database) -> isNotInSchemaFile(database, "nameNotFoundInDictionary"));

    // REMOVE BUCKET FROM TYPE THEN DROP BUCKET
    databases[0].getSchema().getType("RaftRuntimeVertex0").removeBucket(
        databases[0].getSchema().getBucketByName("raftNewBucket"));
    for (final Database database : databases)
      assertThat(database.getSchema().getType("RaftRuntimeVertex0").hasBucket("raftNewBucket")).isFalse();

    databases[0].getSchema().dropBucket("raftNewBucket");
    testOnAllServers((database) -> isNotInSchemaFile(database, "raftNewBucket"));

    // DROP TYPE
    databases[0].getSchema().dropType("RaftRuntimeVertex0");
    testOnAllServers((database) -> isNotInSchemaFile(database, "RaftRuntimeVertex0"));

    // CREATE INDEXED TYPE
    final VertexType indexedType = databases[0].getSchema().createVertexType("RaftIndexedVertex0");
    testOnAllServers((database) -> isInSchemaFile(database, "RaftIndexedVertex0"));

    final Property indexedProperty = indexedType.createProperty("propertyIndexed", Type.INTEGER);
    testOnAllServers((database) -> isInSchemaFile(database, "propertyIndexed"));

    final Index idx = indexedProperty.createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    testOnAllServers((database) -> isInSchemaFile(database, "\"RaftIndexedVertex0\""));
    testOnAllServers((database) -> isInSchemaFile(database, "\"indexes\":{\"RaftIndexedVertex0_"));

    databases[0].transaction(() -> {
      for (int i = 0; i < 10; i++)
        databases[0].newVertex("RaftIndexedVertex0").set("propertyIndexed", i).save();
    });

    assertThatThrownBy(() -> databases[1].transaction(() -> {
      for (int i = 0; i < 10; i++)
        databases[1].newVertex("RaftIndexedVertex0").set("propertyIndexed", i).save();
    })).isInstanceOf(TransactionException.class);

    databases[0].getSchema().dropIndex(idx.getName());
    testOnAllServers((database) -> isNotInSchemaFile(database, idx.getName()));

    // CREATE NEW TYPE IN TRANSACTION
    databases[0].transaction(() ->
        assertThatCode(() -> databases[0].getSchema().createVertexType("RaftRuntimeVertexTx0")).doesNotThrowAnyException());
    testOnAllServers((database) -> isInSchemaFile(database, "RaftRuntimeVertexTx0"));
  }

  private void testOnAllServers(final Callable<String, Database> callback) {
    schemaFiles.clear();
    for (final Database database : databases) {
      try {
        final String result = callback.call(database);
        schemaFiles.put(database.getDatabasePath(), result);
      } catch (final Exception e) {
        fail("", e);
      }
    }
    checkSchemaFilesAreTheSameOnAllServers();
  }

  private String isInSchemaFile(final Database database, final String match) {
    try {
      final String content = FileUtils.readFileAsString(database.getSchema().getEmbedded().getConfigurationFile());
      assertThat(content.contains(match)).isTrue();
      return content;
    } catch (final IOException e) {
      fail("", e);
      return null;
    }
  }

  private String isNotInSchemaFile(final Database database, final String match) {
    try {
      final String content = FileUtils.readFileAsString(database.getSchema().getEmbedded().getConfigurationFile());
      assertThat(content.contains(match)).isFalse();
      return content;
    } catch (final IOException e) {
      fail("", e);
      return null;
    }
  }

  private void checkSchemaFilesAreTheSameOnAllServers() {
    assertThat(schemaFiles.size()).isEqualTo(getServerCount());
    String first = null;
    for (final Map.Entry<String, String> entry : schemaFiles.entrySet()) {
      if (first == null)
        first = entry.getValue();
      else
        assertThat(entry.getValue())
            .withFailMessage("Server %s has different schema:\nFIRST:\n%s\n%s:\n%s",
                entry.getKey(), first, entry.getKey(), entry.getValue())
            .isEqualTo(first);
    }
  }
}
```

- [ ] **Step 3: Create RaftReplicationMaterializedViewIT.java**

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.*;

class RaftReplicationMaterializedViewIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void materializedViewReplicates() throws Exception {
    final Database[] databases = new Database[getServerCount()];
    for (int i = 0; i < getServerCount(); i++) {
      databases[i] = getServer(i).getDatabase(getDatabaseName());
      if (databases[i].isTransactionActive())
        databases[i].commit();
    }

    // Create source type and insert data on leader
    databases[0].getSchema().createDocumentType("RaftMetric");
    for (final Database db : databases)
      assertThat(db.getSchema().existsType("RaftMetric")).isTrue();

    databases[0].transaction(() -> {
      databases[0].newDocument("RaftMetric").set("name", "cpu").set("value", 80).save();
      databases[0].newDocument("RaftMetric").set("name", "mem").set("value", 60).save();
    });

    // Create materialized view on leader
    databases[0].getSchema().buildMaterializedView()
        .withName("RaftHighMetrics")
        .withQuery("SELECT name, value FROM RaftMetric WHERE value > 70")
        .create();

    // Verify view exists on all servers
    for (final Database db : databases)
      assertThat(db.getSchema().existsMaterializedView("RaftHighMetrics")).isTrue();

    // Verify schema file contains the view definition
    for (final Database db : databases) {
      final String content = readSchemaFile(db);
      assertThat(content).contains("RaftHighMetrics");
      assertThat(content).contains("materializedViews");
    }

    // Query view on a replica
    try (final var rs = databases[1].query("sql", "SELECT FROM RaftHighMetrics")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }

    // Drop the view on leader
    databases[0].getSchema().dropMaterializedView("RaftHighMetrics");

    // Verify view is gone on all servers
    for (final Database db : databases)
      assertThat(db.getSchema().existsMaterializedView("RaftHighMetrics")).isFalse();

    for (final Database db : databases)
      assertThat(readSchemaFile(db)).doesNotContain("RaftHighMetrics");
  }

  private String readSchemaFile(final Database database) {
    try {
      return FileUtils.readFileAsString(database.getSchema().getEmbedded().getConfigurationFile());
    } catch (final IOException e) {
      fail("Cannot read schema file for " + database.getDatabasePath(), e);
      return null;
    }
  }
}
```

- [ ] **Step 4: Compile task 2 tests**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign/ha-raft
mvn test-compile -q
```

Expected: BUILD SUCCESS. Fix any compilation errors.

- [ ] **Step 5: Run task 2 tests individually**

```bash
mvn test -Dtest="RaftReplicationWriteAgainstReplicaIT" -DskipITs=false 2>&1 | tail -30
mvn test -Dtest="RaftReplicationChangeSchemaIT" -DskipITs=false 2>&1 | tail -30
mvn test -Dtest="RaftReplicationMaterializedViewIT" -DskipITs=false 2>&1 | tail -30
```

Expected: Tests pass. Fix any failures before moving to the next step.

- [ ] **Step 6: Commit task 2**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationWriteAgainstReplicaIT.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationChangeSchemaIT.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationMaterializedViewIT.java
git commit -m "test(ha-raft): port write forwarding and schema/view IT tests from server/ha"
```

---

## Task 3: Group 3 — Index Operations

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftIndexCompactionReplicationIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftIndexOperations3ServersIT.java`

- [ ] **Step 1: Create RaftIndexCompactionReplicationIT.java**

Adaptation: replaces `Thread.sleep()` calls with `waitForReplicationIsCompleted()` from `BaseRaftHATest` for reliable replication waiting.

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.*;

class RaftIndexCompactionReplicationIT extends BaseRaftHATest {

  private static final int TOTAL_RECORDS = 5_000;
  private static final int TX_CHUNK      = 500;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 30_000L);
  }

  @Override
  protected void populateDatabase() {
  }

  @Test
  void lsmTreeCompactionReplication() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    final VertexType v = database.getSchema().buildVertexType().withName("RaftPerson").withTotalBuckets(3).create();
    v.createProperty("id", Long.class);
    v.createProperty("uuid", String.class);

    final String indexName = "RaftPerson[id]";
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "id");

    database.transaction(() -> insertPersonRecords(database));

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    index.compact();

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer((serverIndex) -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      final Index serverIdx = serverDb.getSchema().getIndexByName(indexName);
      assertThat(serverIdx.countEntries())
          .as("Index on server %d should have %d entries", serverIndex, TOTAL_RECORDS)
          .isEqualTo(TOTAL_RECORDS);

      for (int i = 0; i < 10; i++) {
        final long value = i * 100L;
        assertThat(serverIdx.get(new Object[]{value}).hasNext() || value >= TOTAL_RECORDS)
            .as("Index on server %d should be queryable", serverIndex).isTrue();
      }
    });
  }

  @Test
  void lsmVectorReplication() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    final VertexType v = database.getSchema().buildVertexType().withName("RaftEmbedding").withTotalBuckets(1).create();
    v.createProperty("vector", float[].class);

    final TypeIndex vectorIndex = database.getSchema().buildTypeIndex("RaftEmbedding", new String[]{"vector"})
        .withLSMVectorType()
        .withDimensions(10)
        .create();

    assertThat(vectorIndex).isNotNull();

    database.transaction(() -> {
      for (int i = 0; i < TOTAL_RECORDS; i++) {
        final float[] vector = new float[10];
        for (int j = 0; j < vector.length; j++)
          vector[j] = (i + j) % 100f;
        database.newVertex("RaftEmbedding").set("vector", vector).save();
        if (i % TX_CHUNK == 0) {
          database.commit();
          database.begin();
        }
      }
    });

    final long entriesOnLeader = vectorIndex.countEntries();
    assertThat(entriesOnLeader).isGreaterThan(0);

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final String actualIndexName = vectorIndex.getName();
    testEachServer((serverIndex) -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      final Index serverVectorIndex = serverDb.getSchema().getIndexByName(actualIndexName);
      assertThat(serverVectorIndex).as("Vector index should be replicated to server %d", serverIndex).isNotNull();
      assertThat(serverVectorIndex.countEntries()).isEqualTo(entriesOnLeader);
    });
  }

  @Test
  void lsmVectorCompactionReplication() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    final VertexType v = database.getSchema().buildVertexType().withName("RaftEmbedding").withTotalBuckets(1).create();
    v.createProperty("vector", float[].class);

    final TypeIndex vectorIndex = database.getSchema().buildTypeIndex("RaftEmbedding", new String[]{"vector"})
        .withLSMVectorType()
        .withDimensions(10)
        .create();

    database.transaction(() -> {
      for (int i = 0; i < TOTAL_RECORDS; i++) {
        final float[] vector = new float[10];
        for (int j = 0; j < vector.length; j++)
          vector[j] = (i + j) % 100f;
        database.newVertex("RaftEmbedding").set("vector", vector).save();
        if (i % TX_CHUNK == 0) {
          database.commit();
          database.begin();
        }
      }
    });

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(vectorIndex.getName());
    index.scheduleCompaction();
    index.compact();

    final long entriesOnLeader = vectorIndex.countEntries();
    assertThat(entriesOnLeader).isGreaterThan(0);

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final String actualIndexName = vectorIndex.getName();
    testEachServer((serverIndex) -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      final Index serverVectorIndex = serverDb.getSchema().getIndexByName(actualIndexName);
      assertThat(serverVectorIndex).as("Vector index should be replicated to server %d", serverIndex).isNotNull();
    });
  }

  @Test
  void compactionReplicationWithConcurrentWrites() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    final VertexType v = database.getSchema().buildVertexType().withName("RaftItem").withTotalBuckets(3).create();
    v.createProperty("itemId", Long.class);
    v.createProperty("value", String.class);

    final String indexName = "RaftItem[itemId]";
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftItem", "itemId");

    database.transaction(() -> {
      for (int i = 0; i < 1000; i++)
        database.newVertex("RaftItem").set("itemId", (long) i, "value", "initial-" + i).save();
    });

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    index.compact();

    database.transaction(() -> {
      for (int i = 1000; i < 2000; i++)
        database.newVertex("RaftItem").set("itemId", (long) i, "value", "post-compact-" + i).save();
    });

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer((serverIndex) -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      final Index serverIdx = serverDb.getSchema().getIndexByName(indexName);
      assertThat(serverIdx.countEntries()).as("Server %d index should have 2000 entries", serverIndex).isEqualTo(2000);
    });
  }

  private void insertPersonRecords(final Database database) {
    for (int i = 0; i < TOTAL_RECORDS; i++) {
      database.newVertex("RaftPerson").set("id", (long) i, "uuid", UUID.randomUUID().toString()).save();
      if (i % TX_CHUNK == 0) {
        database.commit();
        database.begin();
      }
    }
  }
}
```

- [ ] **Step 2: Create RaftIndexOperations3ServersIT.java**

Adaptation: `getServer(serverIndex).getHA().getServerName()` → `getServer(serverIndex).getServerName()`.

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.TestServerHelper;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.*;

class RaftIndexOperations3ServersIT extends BaseRaftHATest {

  private static final int TOTAL_RECORDS = 10_000;
  private static final int TX_CHUNK      = 1_000;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @Test
  void rebuildIndex() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    final VertexType v = database.getSchema().buildVertexType().withName("RaftPerson").withTotalBuckets(3).create();
    v.createProperty("id", Long.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "id");
    v.createProperty("uuid", String.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "uuid");

    database.transaction(() -> insertRecords(database));

    testEachServer((serverIndex) -> {
      LogManager.instance().log(this, Level.FINE, "Rebuild RaftPerson[id] on server %s",
          getServer(serverIndex).getServerName());
      final String response1 = command(serverIndex, "rebuild index `RaftPerson[id]`");
      assertThat(new JSONObject(response1).getJSONArray("result").getJSONObject(0).getLong("totalIndexed"))
          .isEqualTo(TOTAL_RECORDS);

      LogManager.instance().log(this, Level.FINE, "Rebuild RaftPerson[uuid] on server %s",
          getServer(serverIndex).getServerName());
      final String response2 = command(serverIndex, "rebuild index `RaftPerson[uuid]`");
      assertThat(new JSONObject(response2).getJSONArray("result").getJSONObject(0).getLong("totalIndexed"))
          .isEqualTo(TOTAL_RECORDS);

      LogManager.instance().log(this, Level.FINE, "Rebuild * on server %s",
          getServer(serverIndex).getServerName());
      final String response3 = command(serverIndex, "rebuild index *");
      assertThat(new JSONObject(response3).getJSONArray("result").getJSONObject(0).getLong("totalIndexed"))
          .isEqualTo((long) TOTAL_RECORDS * 2);
    });
  }

  @Test
  void createIndexLater() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    final VertexType v = database.getSchema().buildVertexType().withName("RaftPerson").withTotalBuckets(3).create();

    database.transaction(() -> insertRecords(database));

    v.createProperty("id", Long.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "id");
    v.createProperty("uuid", String.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "uuid");

    testEachServer((serverIndex) -> {
      final String response1 = command(serverIndex, "rebuild index `RaftPerson[id]`");
      assertThat(new JSONObject(response1).getJSONArray("result").getJSONObject(0).getLong("totalIndexed"))
          .isEqualTo(TOTAL_RECORDS);

      final String response2 = command(serverIndex, "rebuild index `RaftPerson[uuid]`");
      assertThat(new JSONObject(response2).getJSONArray("result").getJSONObject(0).getLong("totalIndexed"))
          .isEqualTo(TOTAL_RECORDS);

      final String response3 = command(serverIndex, "rebuild index *");
      assertThat(new JSONObject(response3).getJSONArray("result").getJSONObject(0).getLong("totalIndexed"))
          .isEqualTo((long) TOTAL_RECORDS * 2);
    });
  }

  @Test
  void createIndexLaterDistributed() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    final VertexType v = database.getSchema().buildVertexType().withName("RaftPerson").withTotalBuckets(3).create();

    testEachServer((serverIndex) -> {
      database.transaction(() -> insertRecords(database));

      v.createProperty("id", Long.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "id");
      v.createProperty("uuid", String.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "uuid");

      TestServerHelper.expectException(
          () -> database.newVertex("RaftPerson").set("id", 0, "uuid", UUID.randomUUID().toString()).save(),
          DuplicatedKeyException.class);

      TestServerHelper.expectException(
          () -> database.getSchema().getType("RaftPerson").dropProperty("id"),
          SchemaException.class);

      database.getSchema().dropIndex("RaftPerson[id]");
      database.getSchema().getType("RaftPerson").dropProperty("id");

      TestServerHelper.expectException(
          () -> database.getSchema().getType("RaftPerson").dropProperty("uuid"),
          SchemaException.class);

      database.getSchema().dropIndex("RaftPerson[uuid]");
      database.getSchema().getType("RaftPerson").dropProperty("uuid");
      database.command("sql", "delete from RaftPerson");
    });
  }

  @Test
  void createIndexErrorDistributed() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    final VertexType v = database.getSchema().buildVertexType().withName("RaftPerson").withTotalBuckets(3).create();

    testEachServer((serverIndex) -> {
      database.transaction(() -> {
        insertRecords(database);
        insertRecords(database);
      });

      v.createProperty("id", Long.class);

      TestServerHelper.expectException(
          () -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "id"),
          IndexException.class);

      TestServerHelper.expectException(
          () -> database.getSchema().getIndexByName("RaftPerson[id]"),
          SchemaException.class);

      v.createProperty("uuid", String.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "uuid");

      database.getSchema().getType("RaftPerson").dropProperty("id");
      database.getSchema().dropIndex("RaftPerson[uuid]");
      database.getSchema().getType("RaftPerson").dropProperty("uuid");
      database.command("sql", "delete from RaftPerson");
    });
  }

  private void insertRecords(final Database database) {
    for (int i = 0; i < TOTAL_RECORDS; i++) {
      database.newVertex("RaftPerson").set("id", i, "uuid", UUID.randomUUID().toString()).save();
      if (i % TX_CHUNK == 0) {
        database.commit();
        database.begin();
      }
    }
  }
}
```

- [ ] **Step 3: Compile task 3 tests**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign/ha-raft
mvn test-compile -q
```

Expected: BUILD SUCCESS. Fix any compilation errors.

- [ ] **Step 4: Run task 3 tests individually**

```bash
mvn test -Dtest="RaftIndexCompactionReplicationIT" -DskipITs=false 2>&1 | tail -40
mvn test -Dtest="RaftIndexOperations3ServersIT" -DskipITs=false 2>&1 | tail -40
```

Expected: Tests pass. Fix any failures.

- [ ] **Step 5: Commit task 3**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftIndexCompactionReplicationIT.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftIndexOperations3ServersIT.java
git commit -m "test(ha-raft): port index compaction and operations IT tests from server/ha"
```

---

## Task 4: Group 4 — Database Utilities, Config Validation, Chaos

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftServerDatabaseBackupIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftServerDatabaseSqlScriptIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftServerDatabaseAlignIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAConfigurationIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHARandomCrashIT.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

- [ ] **Step 1: Create RaftServerDatabaseBackupIT.java**

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.*;

class RaftServerDatabaseBackupIT extends BaseRaftHATest {

  RaftServerDatabaseBackupIT() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
  }

  @Test
  void sqlBackup() {
    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServer(i).getDatabase(getDatabaseName());
      final ResultSet result = database.command("sql", "backup database");
      assertThat(result.hasNext()).isTrue();
      final Result response = result.next();
      final String backupFile = response.getProperty("backupFile");
      assertThat(backupFile).isNotNull();
      final File file = new File("target/backups/graph/" + backupFile);
      assertThat(file.exists()).isTrue();
      file.delete();
    }
  }

  @Test
  void sqlScriptBackup() {
    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServer(i).getDatabase(getDatabaseName());
      final ResultSet result = database.command("sqlscript", "backup database");
      assertThat(result.hasNext()).isTrue();
      final Result response = result.next();
      final String backupFile = response.getProperty("backupFile");
      assertThat(backupFile).isNotNull();
      final File file = new File("target/backups/graph/" + backupFile);
      assertThat(file.exists()).isTrue();
      file.delete();
    }
  }
}
```

- [ ] **Step 2: Create RaftServerDatabaseSqlScriptIT.java**

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.*;

class RaftServerDatabaseSqlScriptIT extends BaseRaftHATest {

  RaftServerDatabaseSqlScriptIT() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
  }

  @Test
  void executeSqlScript() {
    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServer(i).getDatabase(getDatabaseName());
      database.command("sql", "create vertex type RaftPhotos if not exists");
      database.command("sql", "create edge type RaftConnected if not exists");

      database.transaction(() -> {
        final ResultSet result = database.command("sqlscript",
            """
            LET photo1 = CREATE vertex RaftPhotos SET id = "3778f235a52d", name = "beach.jpg", status = "";
            LET photo2 = CREATE vertex RaftPhotos SET id = "23kfkd23223", name = "luca.jpg", status = "";
            LET connected = Create edge RaftConnected FROM $photo1 to $photo2 set type = "User_Photos";return $photo1;\
            """);
        assertThat(result.hasNext()).isTrue();
        final Result response = result.next();
        assertThat(response.<String>getProperty("name")).isEqualTo("beach.jpg");
      });
    }
  }
}
```

- [ ] **Step 3: Create RaftServerDatabaseAlignIT.java**

Note: `align database` uses the leader's view of the cluster to compare page checksums across all replicas. The `getEmbedded().getEmbedded()` chain bypasses `RaftReplicatedDatabase` to write directly to the storage layer without replication, simulating a divergence.

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class RaftServerDatabaseAlignIT extends BaseRaftHATest {

  RaftServerDatabaseAlignIT() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
  }

  @Test
  void alignNotNecessary() throws Exception {
    final Database database = getServer(0).getDatabase(getDatabaseName());
    database.transaction(() -> {
      final Record edge = database.iterateType(EDGE2_TYPE_NAME, true).next();
      database.deleteRecord(edge);
    });

    try (final ResultSet resultset = getServer(0).getDatabase(getDatabaseName()).command("sql", "align database")) {
      assertThat(resultset.hasNext()).isTrue();
      final Result result = resultset.next();
      assertThat(result.hasProperty("ArcadeDB_0")).isFalse();
      assertThat(result.hasProperty("ArcadeDB_1")).isTrue();
      assertThat(result.<List<int[]>>getProperty("ArcadeDB_1")).hasSize(0);
      assertThat(result.hasProperty("ArcadeDB_2")).isTrue();
      assertThat(result.<List<int[]>>getProperty("ArcadeDB_2")).hasSize(0);
    }
  }

  @Test
  void alignNecessary() throws Exception {
    // Bypass RaftReplicatedDatabase to write directly to storage without replication
    final DatabaseInternal database = ((DatabaseInternal) getServer(0).getDatabase(getDatabaseName()))
        .getEmbedded().getEmbedded();

    database.begin();
    final Record edge = database.iterateType(EDGE1_TYPE_NAME, true).next();
    edge.delete();
    database.commit();

    assertThatThrownBy(this::checkDatabasesAreIdentical)
        .isInstanceOf(DatabaseComparator.DatabaseAreNotIdentical.class);

    try (final ResultSet resultset = getServer(0).getDatabase(getDatabaseName()).command("sql", "align database")) {
      assertThat(resultset.hasNext()).isTrue();
      final Result result = resultset.next();
      assertThat(result.hasProperty("ArcadeDB_0")).isFalse();
      assertThat(result.hasProperty("ArcadeDB_1")).isTrue();
      assertThat(result.<List<int[]>>getProperty("ArcadeDB_1")).hasSize(3);
      assertThat(result.hasProperty("ArcadeDB_2")).isTrue();
      assertThat(result.<List<int[]>>getProperty("ArcadeDB_2")).hasSize(3);
    }
  }
}
```

- [ ] **Step 4: Add localhost peer-address validation to RaftHAServer**

First, read `RaftHAServer.java` to find where peer addresses are parsed during startup:

```bash
grep -n "peer\|serverList\|addresses\|localhost\|127\.0\.0\.1\|ParsedPeerList\|start()" \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java | head -60
```

Find the method that parses the peer address list (likely `start()` or a private initialization method that constructs `ParsedPeerList`). Add the following validation immediately after the peer addresses are parsed into a list of `host:port` strings:

```java
// Validate: mixing localhost/127.0.0.1 with non-localhost addresses is a misconfiguration
boolean hasLocalhost = false;
boolean hasNonLocalhost = false;
for (final String peer : <peerAddressList>) {
  final String host = peer.split(":")[0].trim();
  if (host.equals("localhost") || host.equals("127.0.0.1"))
    hasLocalhost = true;
  else
    hasNonLocalhost = true;
}
if (hasLocalhost && hasNonLocalhost)
  throw new ServerException(
      "Found a localhost (127.0.0.1) in the server list among non-localhost servers. "
          + "Please fix the server list configuration.");
```

Replace `<peerAddressList>` with the actual variable name holding the split peer addresses once you have read the file and found the correct location.

- [ ] **Step 5: Create RaftHAConfigurationIT.java**

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class RaftHAConfigurationIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected String getServerAddresses() {
    // Mix non-localhost IPs with localhost — this should be rejected at startup
    return "192.168.0.1:2434,192.168.0.1:2435,localhost:2434";
  }

  @Test
  void invalidPeerAddressRejected() {
    try {
      super.beginTest();
      fail("Expected ServerException for mixed localhost/non-localhost peers");
    } catch (final ServerException e) {
      assertThat(e.getMessage()).contains("Found a localhost");
    }
  }
}
```

- [ ] **Step 6: Create RaftHARandomCrashIT.java**

Adaptation notes vs `HARandomCrashIT`:
- Extends `BaseRaftHATest` directly (no `ReplicationServerIT` parent)
- `persistentRaftStorage()` returns `true` because servers are stopped and restarted
- Replaces `areAllReplicasAreConnected()` with checking any RaftPlugin is leader
- Replaces `getLeaderServer().getHA().printClusterConfiguration()` (removed)
- Replaces `HAServer.SERVER_ROLE.ANY` (not needed — Raft always uses ANY)
- Writes via `RemoteDatabase` to the HTTP layer (unchanged)
- Uses `assertClusterConsistency()` for final verification

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.HostUtil;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.CodeUtils;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.*;

class RaftHARandomCrashIT extends BaseRaftHATest {

  private static final int TXS             = 1_500;
  private static final int VERTICES_PER_TX = 10;
  private static final int MAX_RETRY       = 30;

  private volatile int  restarts = 0;
  private volatile long delay    = 0;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Test
  void replicationWithRandomCrashes() {
    final Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        // Only crash when the cluster has a leader
        boolean hasLeader = false;
        for (int i = 0; i < getServerCount(); i++) {
          final RaftHAPlugin plugin = getRaftPlugin(i);
          if (plugin != null && plugin.isLeader()) {
            hasLeader = true;
            break;
          }
        }
        if (!hasLeader)
          return;

        if (restarts >= getServerCount()) {
          delay = 0;
          return;
        }

        final int serverId = ThreadLocalRandom.current().nextInt(getServerCount());

        for (int i = 0; i < getServerCount(); ++i) {
          if (getServer(i).isStarted()) {
            final Database db = getServer(i).getDatabase(getDatabaseName());
            db.begin();
            try {
              final long count = db.countType(VERTEX1_TYPE_NAME, true);
              if (count > (long) TXS * VERTICES_PER_TX * 9 / 10) {
                LogManager.instance().log(this, Level.INFO,
                    "TEST: Skipping crash — near end of test (%d/%d)", count, TXS * VERTICES_PER_TX);
                return;
              }
            } catch (final Exception e) {
              LogManager.instance().log(this, Level.SEVERE, "TEST: Skipping crash — error counting vertices", e);
              continue;
            } finally {
              db.rollback();
            }

            delay = 100;
            LogManager.instance().log(this, Level.INFO, "TEST: Stopping server %d", serverId);
            getServer(serverId).stop();

            while (getServer(serverId).getStatus() == ArcadeDBServer.STATUS.SHUTTING_DOWN)
              CodeUtils.sleep(300);

            restarts++;
            LogManager.instance().log(this, Level.INFO, "TEST: Restarting server %d", serverId);

            for (int attempt = 0; attempt < 3; attempt++) {
              try {
                getServer(serverId).start();
                break;
              } catch (final Throwable e) {
                LogManager.instance().log(this, Level.INFO, "TEST: Restart attempt %d/3 failed", attempt + 1, e);
              }
            }

            LogManager.instance().log(this, Level.INFO, "TEST: Server %d restarted", serverId);

            new Timer().schedule(new TimerTask() {
              @Override
              public void run() {
                delay = 0;
              }
            }, 10_000);

            return;
          }
        }
      }
    }, 15_000, 10_000);

    final String server0Address = getServer(0).getHttpServer().getListeningAddress();
    final String[] addressParts = HostUtil.parseHostAddress(server0Address, HostUtil.CLIENT_DEFAULT_PORT);
    final RemoteDatabase db = new RemoteDatabase(addressParts[0], Integer.parseInt(addressParts[1]),
        getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    long counter = 0;

    for (int tx = 0; tx < TXS; ++tx) {
      final long lastGoodCounter = counter;

      for (int retry = 0; retry < MAX_RETRY; ++retry) {
        try {
          for (int i = 0; i < VERTICES_PER_TX; ++i) {
            final ResultSet resultSet = db.command("SQL", "CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?",
                ++counter, "distributed-test");
            final Result result = resultSet.next();
            final Set<String> props = result.getPropertyNames();
            assertThat(props).hasSize(2);
            assertThat(result.<Long>getProperty("id")).isEqualTo(counter);
            assertThat(result.<String>getProperty("name")).isEqualTo("distributed-test");
          }
          CodeUtils.sleep(100);
          break;
        } catch (final TransactionException | NeedRetryException | RemoteException | TimeoutException e) {
          LogManager.instance().log(this, Level.INFO, "TEST: Error (retry %d/%d): %s", retry, MAX_RETRY, e);
          if (retry >= MAX_RETRY - 1)
            throw e;
          counter = lastGoodCounter;
          CodeUtils.sleep(1_000);
        } catch (final DuplicatedKeyException e) {
          // Entry inserted before crash — this is expected
          LogManager.instance().log(this, Level.INFO, "TEST: DuplicatedKey (expected after crash): %s", e);
          break;
        }
      }
    }

    timer.cancel();

    LogManager.instance().log(this, Level.INFO, "TEST: Done. Restarts: %d", restarts);

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    assertClusterConsistency();
    assertThat(restarts).as("Expected at least %d restarts", getServerCount()).isGreaterThanOrEqualTo(getServerCount());
  }
}
```

- [ ] **Step 7: Compile all task 4 tests and the production change**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign/ha-raft
mvn test-compile -q
```

Expected: BUILD SUCCESS. Fix any compilation errors.

- [ ] **Step 8: Run task 4 tests (utilities and config)**

Run the fast tests first:

```bash
mvn test -Dtest="RaftServerDatabaseBackupIT" -DskipITs=false 2>&1 | tail -30
mvn test -Dtest="RaftServerDatabaseSqlScriptIT" -DskipITs=false 2>&1 | tail -30
mvn test -Dtest="RaftServerDatabaseAlignIT" -DskipITs=false 2>&1 | tail -30
mvn test -Dtest="RaftHAConfigurationIT" -DskipITs=false 2>&1 | tail -30
```

Expected: All pass. Note: `RaftServerDatabaseAlignIT.alignNecessary()` may fail if `getEmbedded().getEmbedded()` does not bypass `RaftReplicatedDatabase` in the Raft implementation. If it does, investigate the database wrapper chain for the Raft implementation and adjust the `.getEmbedded()` call depth.

- [ ] **Step 9: Run the chaos test**

This test runs for several minutes (1500 txs with random crashes every 10 seconds):

```bash
mvn test -Dtest="RaftHARandomCrashIT" -DskipITs=false 2>&1 | tail -50
```

Expected: Test passes with at least 3 server restarts. If the test times out or fails due to a legitimate Raft behavior difference (e.g., longer leader re-election time), increase timeouts or reduce `TXS` in the test.

- [ ] **Step 10: Commit task 4**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftServerDatabaseBackupIT.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftServerDatabaseSqlScriptIT.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftServerDatabaseAlignIT.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAConfigurationIT.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHARandomCrashIT.java \
        ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java
git commit -m "test(ha-raft): port database utilities, config validation, and chaos IT tests from server/ha

Also adds localhost peer-address validation to RaftHAServer required by RaftHAConfigurationIT."
```

---

## Task 5: Final Verification

- [ ] **Step 1: Compile the full ha-raft module**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign/ha-raft
mvn clean test-compile -q
```

Expected: BUILD SUCCESS.

- [ ] **Step 2: Run all existing ha-raft tests to verify no regressions**

Run the full existing test suite (excluding the chaos test, which is slow):

```bash
mvn test -Dtest="RaftReplication2NodesIT,RaftReplication3NodesIT,RaftLeaderFailoverIT,RaftQuorumLostIT,RaftReplicaFailureIT,RaftSchemaReplicationIT,RaftLeaderCrashAndRecoverIT,RaftReplicaCrashAndRecoverIT,RaftSplitBrain3NodesIT,GetClusterHandlerIT" -DskipITs=false 2>&1 | tail -20
```

Expected: All pass.

- [ ] **Step 3: Run all new task 1-3 tests together**

```bash
mvn test -Dtest="RaftHTTP2ServersIT,RaftHTTP2ServersCreateReplicatedDatabaseIT,RaftHTTPGraphConcurrentIT,RaftReplicationWriteAgainstReplicaIT,RaftReplicationChangeSchemaIT,RaftReplicationMaterializedViewIT,RaftIndexCompactionReplicationIT,RaftIndexOperations3ServersIT" -DskipITs=false 2>&1 | tail -30
```

Expected: All pass.
