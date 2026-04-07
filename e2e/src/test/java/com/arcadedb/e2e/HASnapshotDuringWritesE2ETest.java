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

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a follower reconnecting during active writes can catch up correctly.
 * A background thread writes records continuously while the follower is reconnected,
 * exercising the snapshot installation path under concurrent write load.
 *
 * <p>The test uses aggressive snapshot and log purge settings so that the follower
 * falls behind far enough to require snapshot-based recovery, not just log replay.
 *
 * <p>Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HASnapshotDuringWritesE2ETest}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class HASnapshotDuringWritesE2ETest extends ArcadeHAContainerTemplate {

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
  void testFollowerReconnectsDuringActiveWrites() throws Exception {
    // 1. Create schema and seed data
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    httpCommand(leader, "SQL", "CREATE VERTEX TYPE Metric IF NOT EXISTS");
    for (int i = 0; i < 20; i++)
      httpCommand(leader, "SQL", "INSERT INTO Metric CONTENT {\"name\":\"metric-" + i + "\",\"value\":" + (i * 0.5) + ",\"phase\":\"seed\"}");

    // Wait for all nodes to replicate the seed data
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Metric")).isEqualTo(20);
    });

    // 2. Isolate a follower
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

    // 3. Write data while follower is isolated to trigger snapshot + log purge
    final GenericContainer<?> currentLeader = findLeader();
    assertThat(currentLeader).as("Majority should still have a leader").isNotNull();

    for (int i = 0; i < 100; i++)
      httpCommand(currentLeader, "SQL", "INSERT INTO Metric CONTENT {\"name\":\"pre-reconnect-" + i + "\",\"value\":" + (i * 1.0) + ",\"phase\":\"pre-reconnect\"}");

    // 4. Start concurrent background writes
    final AtomicInteger backgroundCount = new AtomicInteger(0);
    final AtomicBoolean writeError = new AtomicBoolean(false);
    final CountDownLatch writesStarted = new CountDownLatch(1);
    final CountDownLatch writesDone = new CountDownLatch(1);

    final Thread writerThread = new Thread(() -> {
      writesStarted.countDown();
      for (int i = 0; i < 100; i++) {
        try {
          httpCommand(currentLeader, "SQL",
              "INSERT INTO Metric CONTENT {\"name\":\"concurrent-" + i + "\",\"value\":" + (i * 3.0) + ",\"phase\":\"concurrent\"}");
          backgroundCount.incrementAndGet();
          // Small delay to spread writes over time so the reconnect happens mid-stream
          Thread.sleep(50);
        } catch (final Exception e) {
          writeError.set(true);
          break;
        }
      }
      writesDone.countDown();
    });
    writerThread.setDaemon(true);
    writerThread.start();

    // Wait for background writes to start
    writesStarted.await(5, TimeUnit.SECONDS);

    // Let some writes accumulate before reconnecting
    Thread.sleep(500);

    // 5. Reconnect the follower while writes are still happening
    reconnectToNetwork(isolatedFollower);

    // 6. Wait for background writes to complete
    writesDone.await(30, TimeUnit.SECONDS);
    assertThat(writeError.get()).as("Background writer should not encounter errors").isFalse();

    final long expectedTotal = 20 + 100 + backgroundCount.get(); // seed + pre-reconnect + concurrent

    // 7. Wait for the follower to catch up (snapshot download + replay of concurrent writes)
    final GenericContainer<?> reconnected = isolatedFollower;
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(httpCount(reconnected, "Metric")).isEqualTo(expectedTotal));

    // 8. Final verification: all nodes converge
    for (final GenericContainer<?> c : containers)
      assertThat(httpCount(c, "Metric")).isEqualTo(expectedTotal);
  }
}
