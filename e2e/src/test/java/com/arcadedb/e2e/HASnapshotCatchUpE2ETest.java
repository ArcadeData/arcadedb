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

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the full snapshot-based catch-up cycle: a follower that falls so far behind
 * that Ratis has purged the log entries it needs must recover via snapshot installation
 * (HTTP download from the leader) rather than Raft log replay.
 *
 * <p>The test configures aggressive snapshot and log purge thresholds so that a short
 * network partition is enough to force the snapshot path.
 *
 * <p>Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HASnapshotCatchUpE2ETest}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public class HASnapshotCatchUpE2ETest extends ArcadeHAContainerTemplate {

  // Aggressive settings to force snapshot-based catch-up:
  // - snapshot every 10 Raft entries
  // - purge logs up to the snapshot index with a gap of 1
  // - small log segments (1KB) so purging can actually remove them
  private static final String SNAPSHOT_OPTS =
      "-Darcadedb.ha.snapshotThreshold=10"
          + " -Darcadedb.ha.logPurgeGap=1"
          + " -Darcadedb.ha.logPurgeUptoSnapshot=true"
          + " -Darcadedb.ha.logSegmentSize=1KB";

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
  void testFollowerCatchesUpViaSnapshot() throws Exception {
    // 1. Create schema and seed data
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    try (final RemoteDatabase db = createRemoteDatabase(leader)) {
      db.command("SQL", "CREATE VERTEX TYPE Measurement IF NOT EXISTS");
      for (int i = 0; i < 20; i++)
        db.command("SQL", "INSERT INTO Measurement SET sensor = ?, value = ?, phase = ?",
            "sensor-" + i, i * 1.5, "seed");
    }

    // Wait for all nodes to replicate the seed data
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          assertThat(countMeasurements(db)).isEqualTo(20);
        }
      }
    });

    // 2. Isolate a follower via Docker network disconnect
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

    // 3. Write enough data to the majority to trigger multiple snapshots + log purge.
    //    With snapshotThreshold=10 and purgeGap=1, after ~50 writes the Raft log entries
    //    from before the partition will be purged, forcing snapshot installation on rejoin.
    final GenericContainer<?> currentLeader = findLeader();
    assertThat(currentLeader).as("Majority should still have a leader").isNotNull();

    try (final RemoteDatabase db = createRemoteDatabase(currentLeader)) {
      for (int i = 0; i < 200; i++)
        db.command("SQL", "INSERT INTO Measurement SET sensor = ?, value = ?, phase = ?",
            "post-partition-" + i, i * 2.0, "during-partition");
    }

    final long expectedTotal = 220; // 20 seed + 200 during partition

    // Verify the majority has all data before reconnecting
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      try (final RemoteDatabase db = createRemoteDatabase(currentLeader)) {
        assertThat(countMeasurements(db)).isEqualTo(expectedTotal);
      }
    });

    // 4. Reconnect the isolated follower - it must catch up via snapshot
    reconnectToNetwork(isolatedFollower);

    // 5. Wait for the follower to catch up (snapshot download + replay)
    final GenericContainer<?> reconnected = isolatedFollower;
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(() -> {
          try (final RemoteDatabase db = createRemoteDatabase(reconnected)) {
            assertThat(countMeasurements(db)).isEqualTo(expectedTotal);
          }
        });

    // 6. Final verification: all nodes converge
    for (final GenericContainer<?> c : containers) {
      try (final RemoteDatabase db = createRemoteDatabase(c)) {
        assertThat(countMeasurements(db)).isEqualTo(expectedTotal);
      }
    }
  }

  private void disconnectFromNetwork(final GenericContainer<?> container) {
    final var dockerClient = DockerClientFactory.instance().client();
    dockerClient.disconnectFromNetworkCmd()
        .withNetworkId(network.getId())
        .withContainerId(container.getContainerId())
        .withForce(true)
        .exec();
  }

  private void reconnectToNetwork(final GenericContainer<?> container) {
    try {
      final var dockerClient = DockerClientFactory.instance().client();
      dockerClient.connectToNetworkCmd()
          .withNetworkId(network.getId())
          .withContainerId(container.getContainerId())
          .exec();
    } catch (final Exception ignored) {}
  }

  private long countMeasurements(final RemoteDatabase db) {
    final ResultSet rs = db.query("SQL", "SELECT count(*) as cnt FROM Measurement");
    return rs.hasNext() ? ((Number) rs.next().getProperty("cnt")).longValue() : -1;
  }
}
