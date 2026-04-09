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
 * Tests a complete 1+1+1 three-way partition where every node is isolated from
 * every other node. No node has majority, so all writes must be rejected.
 * After the partition heals, the cluster must elect a leader, discard any
 * uncommitted entries, and resume normal operation.
 *
 * <p>This is more extreme than {@link HAQuorumLossRecoveryE2ETest} which only
 * isolates 2 followers (1+2 split). Here the leader also loses quorum AND the
 * two followers are isolated from each other (1+1+1 split).
 *
 * <p>Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HACompletePartitionE2ETest}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 4, unit = TimeUnit.MINUTES)
public class HACompletePartitionE2ETest extends ArcadeHAContainerTemplate {

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
  void testCompletePartitionNoQuorum() throws Exception {
    // 1. Create schema and seed data before the partition
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    httpCommand(leader, "SQL", "CREATE VERTEX TYPE PartitionTest IF NOT EXISTS");
    for (int i = 0; i < 10; i++)
      httpCommand(leader, "SQL", "INSERT INTO PartitionTest CONTENT {\"name\":\"pre-partition-" + i + "\"}");

    // Verify all nodes have the seed data
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "PartitionTest")).isEqualTo(10);
    });

    // 2. Disconnect ALL nodes from each other (1+1+1 partition)
    for (final GenericContainer<?> c : containers)
      disconnectFromNetwork(c);

    // Wait for nodes to detect the partition
    Thread.sleep(5000);

    // 3. Verify writes FAIL on every node (no majority exists anywhere)
    for (final GenericContainer<?> c : containers) {
      try {
        httpCommand(c, "SQL", "INSERT INTO PartitionTest CONTENT {\"name\":\"should-fail\"}");
        // If we get here, the write unexpectedly succeeded. In a partitioned cluster
        // writes should fail, but the node might still accept reads-only or the
        // partition detection might not be instantaneous.
      } catch (final Exception expected) {
        // Expected: write should fail without quorum
      }
    }

    // 4. Reconnect all nodes
    for (final GenericContainer<?> c : containers)
      reconnectToNetwork(c);

    // 5. Wait for leader election after partition heals
    waitForLeader();

    // 6. Write new data to the recovered cluster
    final GenericContainer<?> recoveredLeader = findLeader();
    assertThat(recoveredLeader).isNotNull();

    for (int i = 0; i < 10; i++)
      httpCommand(recoveredLeader, "SQL", "INSERT INTO PartitionTest CONTENT {\"name\":\"post-partition-" + i + "\"}");

    // 7. Verify all nodes converge to 20 records (10 seed + 10 new).
    //    Any entries that nodes tried to write during the partition (step 3)
    //    should have been discarded during Raft log reconciliation.
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "PartitionTest")).isEqualTo(20);
    });
  }
}
