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
 * Tests that repeated network partition/heal cycles do not leave stale state
 * that prevents future catch-up. Each cycle disconnects a follower (breaking
 * gRPC connections), writes to the majority, then reconnects and verifies
 * convergence.
 *
 * <p>This is different from {@link HARollingRestartE2ETest} which uses Docker
 * restart (process freeze). Network disconnect kills gRPC connections and causes
 * different Ratis recovery behavior (channel reconnection, log replay catch-up).
 *
 * <p>Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HARepeatedPartitionCyclesE2ETest}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 5, unit = TimeUnit.MINUTES)
public class HARepeatedPartitionCyclesE2ETest extends ArcadeHAContainerTemplate {

  private static final int PARTITION_CYCLES     = 3;
  private static final int RECORDS_PER_CYCLE    = 5;
  private static final int SEED_RECORDS         = 10;

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
  void testMultiplePartitionCycles() throws Exception {
    // Seed: create schema and initial data
    final GenericContainer<?> initialLeader = findLeader();
    assertThat(initialLeader).isNotNull();

    httpCommand(initialLeader, "SQL", "CREATE VERTEX TYPE CycleTest IF NOT EXISTS");
    for (int i = 0; i < SEED_RECORDS; i++)
      httpCommand(initialLeader, "SQL", "INSERT INTO CycleTest CONTENT {\"cycle\":0,\"seq\":" + i + "}");

    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "CycleTest")).isEqualTo(SEED_RECORDS);
    });

    int totalRecords = SEED_RECORDS;

    // Run partition/heal cycles
    for (int cycle = 1; cycle <= PARTITION_CYCLES; cycle++) {
      // Pick a follower to isolate (any non-leader node)
      final GenericContainer<?> leader = findLeader();
      assertThat(leader).as("Leader must exist at start of cycle %d", cycle).isNotNull();

      final GenericContainer<?> follower = containers.stream()
          .filter(c -> c != leader).findFirst().orElseThrow();

      // Isolate the follower
      disconnectFromNetwork(follower);
      Thread.sleep(2000);

      // Write to the majority (leader + remaining follower)
      for (int i = 0; i < RECORDS_PER_CYCLE; i++)
        httpCommand(leader, "SQL",
            "INSERT INTO CycleTest CONTENT {\"cycle\":" + cycle + ",\"seq\":" + i + "}");
      totalRecords += RECORDS_PER_CYCLE;

      // Reconnect the follower
      reconnectToNetwork(follower);

      // Wait for the leader to be available and all nodes to converge
      waitForLeader();

      final int expected = totalRecords;
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(2, TimeUnit.SECONDS)
          .untilAsserted(() -> {
            for (final GenericContainer<?> c : containers)
              assertThat(httpCount(c, "CycleTest"))
                  .as("All nodes should have %d records", expected)
                  .isEqualTo(expected);
          });
    }

    // Final verification: total should be seed + (cycles * records_per_cycle)
    final int expectedFinal = SEED_RECORDS + PARTITION_CYCLES * RECORDS_PER_CYCLE;
    assertThat(totalRecords).isEqualTo(expectedFinal);

    for (final GenericContainer<?> c : containers)
      assertThat(httpCount(c, "CycleTest")).isEqualTo(expectedFinal);
  }
}
