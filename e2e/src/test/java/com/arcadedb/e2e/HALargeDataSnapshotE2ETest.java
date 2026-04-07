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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests snapshot-based catch-up with large records. Each record contains multiple
 * properties including large string values (500+ bytes), ensuring that the snapshot
 * transfer and replay handles non-trivial record sizes correctly.
 *
 * <p>A follower is isolated while large records are written. With aggressive snapshot
 * and log purge settings, the follower must recover via snapshot installation. After
 * reconnecting, the test verifies that all records, including their full content,
 * are present on every node.
 *
 * <p>Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HALargeDataSnapshotE2ETest}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class HALargeDataSnapshotE2ETest extends ArcadeHAContainerTemplate {

  private static final String SNAPSHOT_OPTS =
      "-Darcadedb.ha.snapshotThreshold=10"
          + " -Darcadedb.ha.logPurgeGap=1"
          + " -Darcadedb.ha.logPurgeUptoSnapshot=true"
          + " -Darcadedb.ha.logSegmentSize=64KB"
          + " -Darcadedb.ha.quorumTimeout=30000";

  // Padding string to make each record's description field 500+ bytes
  private static final String PADDING = "A".repeat(500);

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
  void testLargeRecordSnapshotRecovery() throws Exception {
    // 1. Create schema
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    httpCommand(leader, "SQL", "CREATE VERTEX TYPE TelemetryRecord IF NOT EXISTS");
    httpCommand(leader, "SQL", "CREATE PROPERTY TelemetryRecord.sensor STRING");
    httpCommand(leader, "SQL", "CREATE PROPERTY TelemetryRecord.value DOUBLE");
    httpCommand(leader, "SQL", "CREATE PROPERTY TelemetryRecord.description STRING");
    httpCommand(leader, "SQL", "CREATE PROPERTY TelemetryRecord.tags STRING");
    httpCommand(leader, "SQL", "CREATE PROPERTY TelemetryRecord.metadata STRING");

    // 2. Seed with large records
    for (int i = 0; i < 20; i++)
      insertLargeRecord(leader, i, "seed");

    // Wait for seed data to replicate
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "TelemetryRecord")).isEqualTo(20);
    });

    // 3. Isolate a follower
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

    // 4. Write many large records to trigger snapshot + log purge
    final GenericContainer<?> currentLeader = findLeader();
    assertThat(currentLeader).as("Majority should still have a leader").isNotNull();

    for (int i = 20; i < 150; i++)
      insertLargeRecord(currentLeader, i, "during-partition");

    final long expectedTotal = 150; // 20 seed + 130 during partition

    // Verify the majority has all data
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
        assertThat(httpCount(currentLeader, "TelemetryRecord")).isEqualTo(expectedTotal));

    // 5. Reconnect the isolated follower
    reconnectToNetwork(isolatedFollower);

    // 6. Wait for the follower to catch up via snapshot
    final GenericContainer<?> reconnected = isolatedFollower;
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(httpCount(reconnected, "TelemetryRecord")).isEqualTo(expectedTotal));

    // 7. Final count verification on all nodes
    for (final GenericContainer<?> c : containers)
      assertThat(httpCount(c, "TelemetryRecord")).isEqualTo(expectedTotal);

    // 8. Verify content integrity: check specific records on each node
    for (final GenericContainer<?> c : containers) {
      // Check a seed record
      final JSONObject seedResult = httpCommand(c, "SQL", "SELECT FROM TelemetryRecord WHERE sensor = 'sensor-5'");
      final JSONArray seedRecords = seedResult.getJSONArray("result");
      assertThat(seedRecords.length()).as("Should find seed record sensor-5").isEqualTo(1);
      final JSONObject seedRecord = seedRecords.getJSONObject(0);
      assertThat(seedRecord.getString("description")).startsWith("Telemetry reading #5 from phase seed");
      assertThat(seedRecord.getString("description").length()).as("Description should be 500+ bytes").isGreaterThanOrEqualTo(500);
      assertThat(seedRecord.getDouble("value")).isEqualTo(5 * 1.23);

      // Check a partition record
      final JSONObject partResult = httpCommand(c, "SQL", "SELECT FROM TelemetryRecord WHERE sensor = 'sensor-100'");
      final JSONArray partRecords = partResult.getJSONArray("result");
      assertThat(partRecords.length()).as("Should find partition record sensor-100").isEqualTo(1);
      final JSONObject partRecord = partRecords.getJSONObject(0);
      assertThat(partRecord.getString("description")).startsWith("Telemetry reading #100 from phase during-partition");
      assertThat(partRecord.getString("description").length()).as("Description should be 500+ bytes").isGreaterThanOrEqualTo(500);
      assertThat(partRecord.getString("tags")).isEqualTo("env:production,region:us-east,tier:premium,index:100");
      assertThat(partRecord.getString("metadata")).startsWith("source=telemetry-ingester,version=2.0,batch=100");
    }
  }

  /**
   * Inserts a single large record with multiple properties including a 500+ byte description.
   */
  private void insertLargeRecord(final GenericContainer<?> container, final int index, final String phase) throws Exception {
    final String description = "Telemetry reading #" + index + " from phase " + phase + ". " + PADDING;
    final String tags = "env:production,region:us-east,tier:premium,index:" + index;
    final String metadata = "source=telemetry-ingester,version=2.0,batch=" + index + ",timestamp=" + System.currentTimeMillis() + "," + PADDING;

    // Escape any quotes in string values for safe SQL embedding
    final String sql = "INSERT INTO TelemetryRecord CONTENT {"
        + "\"sensor\":\"sensor-" + index + "\","
        + "\"value\":" + (index * 1.23) + ","
        + "\"description\":\"" + escapeForJson(description) + "\","
        + "\"tags\":\"" + escapeForJson(tags) + "\","
        + "\"metadata\":\"" + escapeForJson(metadata) + "\""
        + "}";

    httpCommand(container, "SQL", sql);
  }

  /**
   * Escapes characters that would break JSON string embedding.
   */
  private static String escapeForJson(final String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
