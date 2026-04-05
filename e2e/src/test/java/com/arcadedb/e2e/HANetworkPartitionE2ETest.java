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
 * Docker-based network partition tests using Docker network disconnect/reconnect.
 * Tests real network isolation scenarios (not just stop/start).
 * Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HANetworkPartitionE2ETest}
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public class HANetworkPartitionE2ETest extends ArcadeHAContainerTemplate {

  @BeforeEach
  void setUp() {
    startCluster(3);
  }

  @AfterEach
  void tearDown() {
    // Reconnect any isolated containers before stopping
    for (final GenericContainer<?> c : containers)
      try { reconnectToNetwork(c); } catch (final Exception ignored) {}
    stopCluster();
  }

  @Test
  void testFollowerPartitionAndRecovery() throws Exception {
    // Setup: create schema and data
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    try (final RemoteDatabase db = createRemoteDatabase(leader)) {
      db.command("SQL", "CREATE VERTEX TYPE Event IF NOT EXISTS");
      for (int i = 0; i < 10; i++)
        db.command("SQL", "INSERT INTO Event SET name = ?, phase = ?", "event-" + i, "before-partition");
    }

    // Wait for replication
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          assertThat(countEvents(db)).isEqualTo(10);
        }
      }
    });

    // Find a follower and isolate it via Docker network disconnect
    GenericContainer<?> isolatedFollower = null;
    for (final GenericContainer<?> c : containers) {
      try {
        if (!getClusterInfo(c).getBoolean("isLeader")) {
          isolatedFollower = c;
          break;
        }
      } catch (final Exception ignored) {}
    }
    assertThat(isolatedFollower).isNotNull();
    disconnectFromNetwork(isolatedFollower);

    // Write to the majority (leader + remaining follower)
    try (final RemoteDatabase db = createRemoteDatabase(leader)) {
      for (int i = 0; i < 10; i++)
        db.command("SQL", "INSERT INTO Event SET name = ?, phase = ?", "during-partition-" + i, "during-partition");
    }

    // Reconnect the isolated follower
    reconnectToNetwork(isolatedFollower);

    // Wait for the follower to catch up via Raft log replay
    final GenericContainer<?> reconnectedFollower = isolatedFollower;
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).untilAsserted(() -> {
      try (final RemoteDatabase db = createRemoteDatabase(reconnectedFollower)) {
        assertThat(countEvents(db)).isEqualTo(20);
      }
    });

    // Verify all nodes converge
    for (final GenericContainer<?> c : containers) {
      try (final RemoteDatabase db = createRemoteDatabase(c)) {
        assertThat(countEvents(db)).isEqualTo(20);
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

  private long countEvents(final RemoteDatabase db) {
    final ResultSet rs = db.query("SQL", "SELECT count(*) as cnt FROM Event");
    return rs.hasNext() ? ((Number) rs.next().getProperty("cnt")).longValue() : -1;
  }
}
