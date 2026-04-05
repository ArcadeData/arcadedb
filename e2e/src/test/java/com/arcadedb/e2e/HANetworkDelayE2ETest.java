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
import eu.rekawek.toxiproxy.model.ToxicDirection;
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
 * Tests HA cluster behavior under network latency and jitter injected via Toxiproxy.
 * Verifies that Ratis consensus handles delayed Raft RPCs gracefully.
 * <p>
 * Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HANetworkDelayE2ETest}
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public class HANetworkDelayE2ETest extends ArcadeHAToxiproxyTemplate {

  @BeforeEach
  void setUp() throws Exception {
    startClusterWithToxiproxy(3);
  }

  @AfterEach
  void tearDown() {
    stopAll();
  }

  @Test
  void testSymmetricLatency() throws Exception {
    // Setup schema
    try (final RemoteDatabase db = createRemoteDatabase(containers.get(0))) {
      db.command("SQL", "CREATE VERTEX TYPE Metric IF NOT EXISTS");
    }

    // Add 200ms latency on ALL Raft connections
    addLatencyAll(200, 0);

    // Write 20 records - should succeed despite 200ms latency on each Raft RPC
    try (final RemoteDatabase db = createRemoteDatabase(containers.get(0))) {
      for (int i = 0; i < 20; i++)
        db.command("SQL", "INSERT INTO Metric SET name = ?, value = ?", "sym-" + i, i);
    }

    // Remove latency and verify replication
    removeAllToxics();

    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          assertThat(count(db, "Metric")).isEqualTo(20);
        }
      }
    });
  }

  @Test
  void testHighLatencyWithJitter() throws Exception {
    try (final RemoteDatabase db = createRemoteDatabase(containers.get(0))) {
      db.command("SQL", "CREATE VERTEX TYPE Metric IF NOT EXISTS");
    }

    // Add 300ms latency with 150ms jitter (150ms-450ms range)
    addLatencyAll(300, 150);

    // Write records - Raft should handle variable latency
    try (final RemoteDatabase db = createRemoteDatabase(containers.get(0))) {
      for (int i = 0; i < 10; i++)
        db.command("SQL", "INSERT INTO Metric SET name = ?, value = ?", "jitter-" + i, i);
    }

    removeAllToxics();

    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          assertThat(count(db, "Metric")).isEqualTo(10);
        }
      }
    });
  }

  @Test
  void testAsymmetricLeaderDelay() throws Exception {
    try (final RemoteDatabase db = createRemoteDatabase(containers.get(0))) {
      db.command("SQL", "CREATE VERTEX TYPE Metric IF NOT EXISTS");
    }

    // Add 500ms latency only on proxies FROM node 0 (likely leader after startup)
    for (final var proxy : proxies)
      if (proxy.getName().startsWith("raft-0-"))
        addLatency(proxy, ToxicDirection.DOWNSTREAM, 500, 0);

    // Write from node 1 (follower) - proxied to leader, which has slow outgoing Raft RPCs
    try (final RemoteDatabase db = createRemoteDatabase(containers.get(1))) {
      for (int i = 0; i < 5; i++)
        db.command("SQL", "INSERT INTO Metric SET name = ?, value = ?", "asym-" + i, i);
    }

    removeAllToxics();

    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          assertThat(count(db, "Metric")).isEqualTo(5);
        }
      }
    });
  }

  @Test
  void testExtremeLatency() throws Exception {
    try (final RemoteDatabase db = createRemoteDatabase(containers.get(0))) {
      db.command("SQL", "CREATE VERTEX TYPE Metric IF NOT EXISTS");
    }

    // Add 2000ms latency - some writes may timeout
    addLatencyAll(2000, 0);

    int successes = 0;
    try (final RemoteDatabase db = createRemoteDatabase(containers.get(0))) {
      for (int i = 0; i < 3; i++)
        try {
          db.command("SQL", "INSERT INTO Metric SET name = ?, value = ?", "extreme-" + i, i);
          successes++;
        } catch (final Exception ignored) {}
    }

    removeAllToxics();

    // With 2s latency, at least some writes should succeed (Ratis has configurable timeouts)
    // The exact count depends on Raft timeout settings
    final int finalSuccesses = successes;
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          assertThat(count(db, "Metric")).isEqualTo(finalSuccesses);
        }
      }
    });
  }

  private long count(final RemoteDatabase db, final String type) {
    final ResultSet rs = db.query("SQL", "SELECT count(*) as cnt FROM " + type);
    return rs.hasNext() ? ((Number) rs.next().getProperty("cnt")).longValue() : -1;
  }
}
