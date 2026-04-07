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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Docker-based rolling restart tests verifying zero-downtime maintenance scenarios.
 * Each node is network-isolated, writes continue on the majority, then the node
 * is reconnected and verified to catch up.
 * Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HARollingRestartE2ETest}
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public class HARollingRestartE2ETest extends ArcadeHAContainerTemplate {

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
  void testRollingRestartWithContinuousWrites() throws Exception {
    // Setup: create schema and initial data
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    httpCommand(leader, "SQL", "CREATE VERTEX TYPE Product IF NOT EXISTS");
    for (int i = 0; i < 10; i++)
      httpCommand(leader, "SQL", "INSERT INTO Product CONTENT {\"name\":\"initial-" + i + "\",\"batch\":\"phase0\"}");

    // Wait for initial replication
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Product")).isEqualTo(10);
    });

    // Rolling restart: isolate each node, write to survivors, reconnect
    final AtomicInteger totalWrites = new AtomicInteger(10);
    for (int nodeIdx = 0; nodeIdx < 3; nodeIdx++) {
      final GenericContainer<?> nodeToRestart = containers.get(nodeIdx);

      // Isolate this node from the network
      disconnectFromNetwork(nodeToRestart);

      // Wait for leader on surviving nodes
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
        for (final GenericContainer<?> c : containers) {
          if (c == nodeToRestart) continue;
          try {
            if (getClusterInfo(c).getBoolean("isLeader"))
              return true;
          } catch (final Exception ignored) {}
        }
        return false;
      });

      // Write to a surviving leader
      final GenericContainer<?> survivor = findLeader();
      assertThat(survivor).isNotNull();

      for (int i = 0; i < 5; i++) {
        httpCommand(survivor, "SQL", "INSERT INTO Product CONTENT {\"name\":\"restart-" + nodeIdx + "-" + i
            + "\",\"batch\":\"phase" + (nodeIdx + 1) + "\"}");
        totalWrites.incrementAndGet();
      }

      // Reconnect the isolated node
      reconnectToNetwork(nodeToRestart);

      // Wait for the node to catch up via Raft log replay
      final int expected = totalWrites.get();
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).untilAsserted(() ->
          assertThat(httpCount(nodeToRestart, "Product")).isEqualTo(expected));
    }

    // Final verification: all nodes have all data
    final int expectedTotal = totalWrites.get();
    for (final GenericContainer<?> c : containers)
      assertThat(httpCount(c, "Product")).isEqualTo(expectedTotal);
  }
}
