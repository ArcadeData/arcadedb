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
package com.arcadedb.e2e;

import com.arcadedb.serializer.json.JSONObject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests snapshot-based catch-up across multiple databases. A follower is isolated
 * while data is written to both the default database (testdb) and a dynamically
 * created database (extradb). With aggressive snapshot and log purge settings,
 * the follower must recover via snapshot installation and converge on both databases.
 *
 * <p>Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HAMultiDatabaseSnapshotE2ETest}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class HAMultiDatabaseSnapshotE2ETest extends ArcadeHAContainerTemplate {

  private static final String EXTRA_DB = "extradb";

  private static final String SNAPSHOT_OPTS =
      "-Darcadedb.ha.snapshotThreshold=10"
          + " -Darcadedb.ha.logPurgeGap=1"
          + " -Darcadedb.ha.logPurgeUptoSnapshot=true"
          + " -Darcadedb.ha.logSegmentSize=64KB"
          + " -Darcadedb.ha.quorumTimeout=30000";

  @BeforeEach
  void setUp() {
    startCluster(3, SNAPSHOT_OPTS);
  }

  @AfterEach
  void tearDown() {
    for (final GenericContainer<?> c : containers)
      try { reconnectToNetwork(c); } catch (final Exception ignored) {}
    stopCluster();
  }

  @Test
  void testMultiDatabaseSnapshotRecovery() throws Exception {
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    // 1. Create schema and seed data in the default database (testdb)
    httpCommand(leader, "SQL", "CREATE VERTEX TYPE Sensor IF NOT EXISTS");
    for (int i = 0; i < 15; i++)
      httpCommand(leader, "SQL", "INSERT INTO Sensor CONTENT {\"name\":\"sensor-" + i + "\",\"value\":" + (i * 1.1) + ",\"phase\":\"seed\"}");

    // 2. Create the extra database on the leader via server command
    httpServerCommand(leader, "create database " + EXTRA_DB);

    // Wait for the extra database to become available on all nodes
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
      for (final GenericContainer<?> c : containers) {
        if (httpCountOnDb(c, EXTRA_DB, "V") == -1) {
          // Try a trivial query to check availability; -1 means the DB or type does not exist yet
          try {
            httpCommandOnDb(c, EXTRA_DB, "SQL", "SELECT 1");
          } catch (final Exception e) {
            return false;
          }
        }
      }
      return true;
    });

    // 3. Create schema and seed data in extradb
    httpCommandOnDb(leader, EXTRA_DB, "SQL", "CREATE VERTEX TYPE LogEntry IF NOT EXISTS");
    for (int i = 0; i < 15; i++)
      httpCommandOnDb(leader, EXTRA_DB, "SQL", "INSERT INTO LogEntry CONTENT {\"msg\":\"log-" + i + "\",\"level\":\"INFO\",\"phase\":\"seed\"}");

    // Wait for seed data replication on both databases
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        assertThat(httpCount(c, "Sensor")).isEqualTo(15);
        assertThat(httpCountOnDb(c, EXTRA_DB, "LogEntry")).isEqualTo(15);
      }
    });

    // 4. Isolate a follower via network disconnect
    GenericContainer<?> isolatedFollower = null;
    for (final GenericContainer<?> c : containers) {
      try {
        if (!getClusterInfo(c).getBoolean("isLeader")) {
          isolatedFollower = c;
          break;
        }
      } catch (final Exception ignored) {}
    }
    assertThat(isolatedFollower).as("Should find a follower to isolate").isNotNull();
    disconnectFromNetwork(isolatedFollower);

    // 5. Write enough data to both databases to trigger snapshot + log purge
    final GenericContainer<?> currentLeader = findLeader();
    assertThat(currentLeader).as("Majority should still have a leader").isNotNull();

    for (int i = 0; i < 150; i++)
      httpCommand(currentLeader, "SQL", "INSERT INTO Sensor CONTENT {\"name\":\"post-partition-" + i + "\",\"value\":" + (i * 2.0) + ",\"phase\":\"during-partition\"}");

    for (int i = 0; i < 150; i++)
      httpCommandOnDb(currentLeader, EXTRA_DB, "SQL",
          "INSERT INTO LogEntry CONTENT {\"msg\":\"partition-log-" + i + "\",\"level\":\"WARN\",\"phase\":\"during-partition\"}");

    final long expectedSensors = 165;   // 15 seed + 150 during partition
    final long expectedLogEntries = 165; // 15 seed + 150 during partition

    // Verify the majority has all data before reconnecting
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(httpCount(currentLeader, "Sensor")).isEqualTo(expectedSensors);
      assertThat(httpCountOnDb(currentLeader, EXTRA_DB, "LogEntry")).isEqualTo(expectedLogEntries);
    });

    // 6. Reconnect the isolated follower - it must catch up via snapshot
    reconnectToNetwork(isolatedFollower);

    // 7. Wait for the follower to catch up on BOTH databases
    final GenericContainer<?> reconnected = isolatedFollower;
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(() -> {
          assertThat(httpCount(reconnected, "Sensor")).isEqualTo(expectedSensors);
          assertThat(httpCountOnDb(reconnected, EXTRA_DB, "LogEntry")).isEqualTo(expectedLogEntries);
        });

    // 8. Final verification: all nodes converge on both databases
    for (final GenericContainer<?> c : containers) {
      assertThat(httpCount(c, "Sensor")).isEqualTo(expectedSensors);
      assertThat(httpCountOnDb(c, EXTRA_DB, "LogEntry")).isEqualTo(expectedLogEntries);
    }
  }

  /**
   * Executes a server-level command (e.g., create database) via HTTP POST to /api/v1/server.
   */
  private JSONObject httpServerCommand(final GenericContainer<?> container, final String command) throws Exception {
    final String url = "http://" + container.getHost() + ":" + container.getMappedPort(HTTP_PORT) + "/api/v1/server";

    final JSONObject body = new JSONObject();
    body.put("command", command);

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Authorization", basicAuth())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
        .build();

    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200)
      throw new RuntimeException("HTTP " + response.statusCode() + ": " + response.body());

    return new JSONObject(response.body());
  }

  /**
   * Executes a SQL command on a specific database via direct HTTP POST.
   */
  private JSONObject httpCommandOnDb(final GenericContainer<?> container, final String dbName,
      final String language, final String command) throws Exception {
    final String url = "http://" + container.getHost() + ":" + container.getMappedPort(HTTP_PORT)
        + "/api/v1/command/" + dbName;

    final JSONObject body = new JSONObject();
    body.put("language", language);
    body.put("command", command);

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Authorization", basicAuth())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
        .build();

    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200)
      throw new RuntimeException("HTTP " + response.statusCode() + ": " + response.body());

    return new JSONObject(response.body());
  }

  /**
   * Counts records of a given type on a specific database and container.
   * Returns -1 if the type or database doesn't exist yet.
   */
  private long httpCountOnDb(final GenericContainer<?> container, final String dbName, final String typeName) {
    try {
      final JSONObject result = httpCommandOnDb(container, dbName, "SQL", "SELECT count(*) as cnt FROM " + typeName);
      return result.getJSONArray("result").getJSONObject(0).getLong("cnt");
    } catch (final Exception e) {
      return -1;
    }
  }
}
