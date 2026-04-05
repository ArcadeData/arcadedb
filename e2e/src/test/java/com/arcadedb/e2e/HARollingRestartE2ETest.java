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
import org.testcontainers.containers.GenericContainer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Docker-based rolling restart tests verifying zero-downtime maintenance scenarios.
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
    stopCluster();
  }

  @Test
  void testRollingRestartWithContinuousWrites() throws Exception {
    // Setup: create schema and initial data
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    try (final RemoteDatabase db = createRemoteDatabase(leader)) {
      db.command("SQL", "CREATE VERTEX TYPE Product IF NOT EXISTS");
      for (int i = 0; i < 10; i++)
        db.command("SQL", "INSERT INTO Product SET name = ?, batch = ?", "initial-" + i, "phase0");
    }

    // Wait for initial replication
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        if (!c.isRunning()) continue;
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          assertThat(countProducts(db)).isEqualTo(10);
        }
      }
    });

    // Rolling restart: stop each node one at a time, write to survivors, restart
    final AtomicInteger totalWrites = new AtomicInteger(10);
    for (int nodeIdx = 0; nodeIdx < 3; nodeIdx++) {
      final GenericContainer<?> nodeToRestart = containers.get(nodeIdx);

      // Stop this node
      nodeToRestart.stop();

      // Wait for leader on surviving nodes
      waitForLeader();

      // Write to a surviving node
      final GenericContainer<?> survivor = findLeader();
      assertThat(survivor).isNotNull();

      try (final RemoteDatabase db = createRemoteDatabase(survivor)) {
        for (int i = 0; i < 5; i++) {
          db.command("SQL", "INSERT INTO Product SET name = ?, batch = ?",
              "restart-" + nodeIdx + "-" + i, "phase" + (nodeIdx + 1));
          totalWrites.incrementAndGet();
        }
      }

      // Restart the stopped node
      nodeToRestart.start();

      // Wait for the restarted node to rejoin and catch up
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).untilAsserted(() -> {
        try (final RemoteDatabase db = createRemoteDatabase(nodeToRestart)) {
          assertThat(countProducts(db)).isEqualTo(totalWrites.get());
        }
      });
    }

    // Final verification: all nodes have all data
    final int expectedTotal = totalWrites.get();
    for (final GenericContainer<?> c : containers) {
      if (!c.isRunning()) continue;
      try (final RemoteDatabase db = createRemoteDatabase(c)) {
        assertThat(countProducts(db)).isEqualTo(expectedTotal);
      }
    }
  }

  private long countProducts(final RemoteDatabase db) {
    final ResultSet rs = db.query("SQL", "SELECT count(*) as cnt FROM Product");
    return rs.hasNext() ? ((Number) rs.next().getProperty("cnt")).longValue() : -1;
  }
}
