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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end HA tests using Docker containers.
 * Tests real multi-node cluster behavior including replication, leader failover,
 * and network partition recovery.
 * <p>
 * Requires Docker. Skipped in normal CI builds; run with: {@code mvn test -pl e2e -Dtest=HAReplicationE2ETest}
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 5, unit = TimeUnit.MINUTES)
public class HAReplicationE2ETest extends ArcadeHAContainerTemplate {

  @BeforeEach
  void setUp() {
    startCluster(3);
  }

  @AfterEach
  void tearDown() {
    stopCluster();
  }

  @Test
  void testBasicReplication() {
    // Write on the leader
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    try (final RemoteDatabase db = createRemoteDatabase(leader)) {
      db.command("SQL", "CREATE VERTEX TYPE Person IF NOT EXISTS");
      for (int i = 0; i < 10; i++)
        db.command("SQL", "INSERT INTO Person SET name = ?, age = ?", "person-" + i, i * 10);
    }

    // Verify replication on all nodes
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> container : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(container)) {
          final ResultSet rs = db.query("SQL", "SELECT count(*) as cnt FROM Person");
          assertThat(rs.hasNext()).isTrue();
          assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(10L);
        }
      }
    });
  }

  @Test
  void testLeaderFailover() {
    // Setup: create schema and write initial data on the leader
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    try (final RemoteDatabase db = createRemoteDatabase(leader)) {
      db.command("SQL", "CREATE VERTEX TYPE Order IF NOT EXISTS");
      for (int i = 0; i < 5; i++)
        db.command("SQL", "INSERT INTO Order SET id = ?, status = ?", i, "created");
    }

    // Wait for replication
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        if (!c.isRunning()) continue;
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          final ResultSet rs = db.query("SQL", "SELECT count(*) as cnt FROM Order");
          assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(5L);
        }
      }
    });

    // Kill the leader
    leader.stop();

    // Wait for new leader election
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
      for (final GenericContainer<?> c : containers) {
        if (!c.isRunning()) continue;
        try {
          final var info = getClusterInfo(c);
          if (info.getBoolean("isLeader"))
            return true;
        } catch (final Exception ignored) {}
      }
      return false;
    });

    // Write to the new leader
    final GenericContainer<?> newLeader = findLeader();
    assertThat(newLeader).isNotNull();
    assertThat(newLeader).isNotSameAs(leader);

    try (final RemoteDatabase db = createRemoteDatabase(newLeader)) {
      for (int i = 5; i < 10; i++)
        db.command("SQL", "INSERT INTO Order SET id = ?, status = ?", i, "after-failover");
    }

    // Verify data on surviving nodes
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        if (!c.isRunning()) continue;
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          final ResultSet rs = db.query("SQL", "SELECT count(*) as cnt FROM Order");
          assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(10L);
        }
      }
    });
  }

  @Test
  void testWriteToFollowerProxy() {
    // Write through a follower (should be proxied to leader transparently)
    GenericContainer<?> follower = null;
    for (final GenericContainer<?> c : containers) {
      try {
        final var info = getClusterInfo(c);
        if (!info.getBoolean("isLeader")) {
          follower = c;
          break;
        }
      } catch (final Exception ignored) {}
    }
    assertThat(follower).isNotNull();

    try (final RemoteDatabase db = createRemoteDatabase(follower)) {
      db.command("SQL", "CREATE VERTEX TYPE Item IF NOT EXISTS");
      for (int i = 0; i < 20; i++)
        db.command("SQL", "INSERT INTO Item SET name = ?, value = ?", "item-" + i, i * 100);
    }

    // Verify on all nodes
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          final ResultSet rs = db.query("SQL", "SELECT count(*) as cnt FROM Item");
          assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(20L);
        }
      }
    });
  }
}
