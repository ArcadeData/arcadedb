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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests quorum loss and recovery: network-isolate 2 of 3 nodes so no majority exists,
 * verify that writes fail, then reconnect both nodes and verify the cluster
 * recovers and accepts writes again.
 *
 * <p>Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HAQuorumLossRecoveryE2ETest}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class HAQuorumLossRecoveryE2ETest extends ArcadeHAContainerTemplate {

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
  void testQuorumLossAndRecovery() throws Exception {
    // 1. Create schema and seed data
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    httpCommand(leader, "SQL", "CREATE VERTEX TYPE Task IF NOT EXISTS");
    for (int i = 0; i < 20; i++)
      httpCommand(leader, "SQL", "INSERT INTO Task CONTENT {\"title\":\"task-" + i + "\",\"status\":\"open\"}");

    // Wait for full replication
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Task")).isEqualTo(20);
    });

    // 2. Keep the leader alive, isolate both followers
    final GenericContainer<?> survivingNode = findLeader();
    assertThat(survivingNode).isNotNull();

    final GenericContainer<?> isolatedNode1 = containers.stream()
        .filter(c -> c != survivingNode).findFirst().orElseThrow();
    final GenericContainer<?> isolatedNode2 = containers.stream()
        .filter(c -> c != survivingNode && c != isolatedNode1).findFirst().orElseThrow();

    // 3. Disconnect 2 of 3 nodes from the network - quorum is lost
    disconnectFromNetwork(isolatedNode1);
    disconnectFromNetwork(isolatedNode2);

    // Wait briefly for the leader to detect the partition
    Thread.sleep(5000);

    // 4. Verify reads still work on the surviving node (local reads don't need quorum)
    assertThat(httpCount(survivingNode, "Task")).isEqualTo(20);

    // 5. Verify writes FAIL on the surviving node (no majority for consensus)
    assertThatThrownBy(() ->
        httpCommand(survivingNode, "SQL", "INSERT INTO Task CONTENT {\"title\":\"should-fail\",\"status\":\"blocked\"}")
    ).as("Writes should fail without quorum");

    // 6. Reconnect both isolated nodes
    reconnectToNetwork(isolatedNode1);
    reconnectToNetwork(isolatedNode2);

    // 7. Wait for leader election and cluster recovery
    waitForLeader();

    // 8. Verify the cluster is functional again - writes should succeed
    final GenericContainer<?> recoveredLeader = findLeader();
    assertThat(recoveredLeader).isNotNull();

    for (int i = 20; i < 30; i++)
      httpCommand(recoveredLeader, "SQL", "INSERT INTO Task CONTENT {\"title\":\"task-" + i + "\",\"status\":\"recovered\"}");

    // 9. Verify all nodes converge with original + new data
    final long expectedTotal = 30; // 20 original + 10 after recovery
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Task")).isEqualTo(expectedTotal);
    });
  }
}
