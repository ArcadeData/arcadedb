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

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests leader network partition: the leader is isolated from the cluster,
 * the majority elects a new leader and continues accepting writes,
 * then the old leader reconnects, steps down, and catches up.
 *
 * <p>Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HALeaderPartitionE2ETest}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class HALeaderPartitionE2ETest extends ArcadeHAContainerTemplate {

  @BeforeEach
  void setUp() {
    startCluster(3);
  }

  @AfterEach
  void tearDown() {
    for (final GenericContainer<?> c : containers)
      try { reconnectToNetwork(c); } catch (final Exception ignored) {}
    stopCluster();
  }

  @Test
  void testLeaderPartitionAndRecovery() throws Exception {
    // 1. Create schema and seed data
    final GenericContainer<?> originalLeader = findLeader();
    assertThat(originalLeader).isNotNull();

    httpCommand(originalLeader, "SQL", "CREATE VERTEX TYPE Order IF NOT EXISTS");
    for (int i = 0; i < 15; i++)
      httpCommand(originalLeader, "SQL", "INSERT INTO Order CONTENT {\"item\":\"item-" + i + "\",\"phase\":\"before-partition\"}");

    // Wait for all nodes to replicate
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Order")).isEqualTo(15);
    });

    // 2. Isolate the LEADER from the network
    disconnectFromNetwork(originalLeader);

    // 3. Wait for the remaining 2 nodes to elect a new leader
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> {
          for (final GenericContainer<?> c : containers) {
            if (c == originalLeader || !c.isRunning()) continue;
            try {
              if (getClusterInfo(c).getBoolean("isLeader"))
                return true;
            } catch (final Exception ignored) {}
          }
          return false;
        });

    // 4. Write to the new leader while the old leader is isolated
    GenericContainer<?> newLeader = null;
    for (final GenericContainer<?> c : containers) {
      if (c == originalLeader) continue;
      try {
        if (getClusterInfo(c).getBoolean("isLeader")) {
          newLeader = c;
          break;
        }
      } catch (final Exception ignored) {}
    }
    assertThat(newLeader).as("New leader should be elected from the majority").isNotNull();
    assertThat(newLeader).isNotSameAs(originalLeader);

    for (int i = 0; i < 20; i++)
      httpCommand(newLeader, "SQL", "INSERT INTO Order CONTENT {\"item\":\"during-partition-" + i + "\",\"phase\":\"during-partition\"}");

    final long expectedTotal = 35; // 15 seed + 20 during partition

    // Verify the majority has all data
    final GenericContainer<?> verifyLeader = newLeader;
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
        assertThat(httpCount(verifyLeader, "Order")).isEqualTo(expectedTotal));

    // 5. Reconnect the old leader - it should step down and catch up
    reconnectToNetwork(originalLeader);

    // gRPC channels need time to recover after network partition + Ratis needs to replicate the log
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(httpCount(originalLeader, "Order")).isEqualTo(expectedTotal));

    // 6. Verify exactly one leader in the cluster
    int leaderCount = 0;
    for (final GenericContainer<?> c : containers) {
      try {
        if (getClusterInfo(c).getBoolean("isLeader"))
          leaderCount++;
      } catch (final Exception ignored) {}
    }
    assertThat(leaderCount).as("Cluster must have exactly one leader").isEqualTo(1);

    // 7. Final verification: all nodes converge
    for (final GenericContainer<?> c : containers)
      assertThat(httpCount(c, "Order")).isEqualTo(expectedTotal);
  }
}
