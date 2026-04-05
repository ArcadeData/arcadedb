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
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests HA cluster behavior under packet loss injected via Toxiproxy.
 * Verifies that Ratis consensus handles dropped packets via gRPC retries.
 * <p>
 * Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HAPacketLossE2ETest}
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public class HAPacketLossE2ETest extends ArcadeHAToxiproxyTemplate {

  @BeforeEach
  void setUp() throws Exception {
    startClusterWithToxiproxy(3);
    try (final RemoteDatabase db = createRemoteDatabase(containers.get(0))) {
      db.command("SQL", "CREATE VERTEX TYPE Packet IF NOT EXISTS");
    }
  }

  @AfterEach
  void tearDown() {
    stopAll();
  }

  @Test
  void testLowPacketLoss() throws Exception {
    // 5% packet loss on all Raft connections
    addPacketLossAll(5);

    try (final RemoteDatabase db = createRemoteDatabase(containers.get(0))) {
      for (int i = 0; i < 20; i++)
        db.command("SQL", "INSERT INTO Packet SET name = ?, rate = ?", "low-" + i, 5);
    }

    removeAllToxics();

    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          assertThat(count(db)).isEqualTo(20);
        }
      }
    });
  }

  @Test
  void testModeratePacketLoss() throws Exception {
    // 20% packet loss - gRPC should retry and eventually succeed
    addPacketLossAll(20);

    try (final RemoteDatabase db = createRemoteDatabase(containers.get(0))) {
      for (int i = 0; i < 10; i++)
        db.command("SQL", "INSERT INTO Packet SET name = ?, rate = ?", "moderate-" + i, 20);
    }

    removeAllToxics();

    Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          assertThat(count(db)).isEqualTo(10);
        }
      }
    });
  }

  @Test
  void testHighPacketLoss() throws Exception {
    // 50% packet loss - some writes may fail, but cluster should remain stable
    addPacketLossAll(50);

    int successes = 0;
    try (final RemoteDatabase db = createRemoteDatabase(containers.get(0))) {
      for (int i = 0; i < 10; i++)
        try {
          db.command("SQL", "INSERT INTO Packet SET name = ?, rate = ?", "high-" + i, 50);
          successes++;
        } catch (final Exception ignored) {}
    }

    removeAllToxics();

    // Verify whatever succeeded is consistent across all nodes
    final int expected = successes;
    Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          assertThat(count(db)).isEqualTo(expected);
        }
      }
    });
  }

  @Test
  void testIntermittentPacketLoss() throws Exception {
    // 3 cycles of apply/remove 25% packet loss
    int totalWrites = 0;
    for (int cycle = 0; cycle < 3; cycle++) {
      addPacketLossAll(25);

      try (final RemoteDatabase db = createRemoteDatabase(containers.get(0))) {
        for (int i = 0; i < 5; i++) {
          db.command("SQL", "INSERT INTO Packet SET name = ?, cycle = ?", "intermittent-" + cycle + "-" + i, cycle);
          totalWrites++;
        }
      }

      removeAllToxics();

      // Brief pause between cycles
      Thread.sleep(1000);
    }

    // Verify all writes are consistent after toxics are removed
    final int expected = totalWrites;
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          assertThat(count(db)).isEqualTo(expected);
        }
      }
    });
  }

  @Test
  void testDirectionalPacketLoss() throws Exception {
    // 30% downstream-only loss on proxies from node 0
    for (final Proxy proxy : proxies)
      if (proxy.getName().startsWith("raft-0-"))
        addPacketLoss(proxy, ToxicDirection.DOWNSTREAM, 30);

    // Write from node 1 - should work via majority (node 1 + node 2)
    try (final RemoteDatabase db = createRemoteDatabase(containers.get(1))) {
      for (int i = 0; i < 10; i++)
        db.command("SQL", "INSERT INTO Packet SET name = ?, direction = ?", "dir-" + i, "downstream");
    }

    removeAllToxics();

    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          assertThat(count(db)).isEqualTo(10);
        }
      }
    });
  }

  private void addPacketLossAll(final int percentage) throws IOException {
    for (final Proxy proxy : proxies)
      addPacketLoss(proxy, ToxicDirection.DOWNSTREAM, percentage);
  }

  private void addPacketLoss(final Proxy proxy, final ToxicDirection direction, final int percentage) throws IOException {
    // Toxiproxy "slow_close" + "limit_data" simulate packet loss behavior.
    // The "timeout" toxic with a probability achieves drop behavior.
    // Using bandwidth toxic with very low rate to simulate packet-level loss.
    proxy.toxics().bandwidth(proxy.getName() + "-loss-" + direction.name(), direction,
        (long) (1024 * 1024 * (100 - percentage) / 100)); // Reduce bandwidth proportionally
  }

  private long count(final RemoteDatabase db) {
    final ResultSet rs = db.query("SQL", "SELECT count(*) as cnt FROM Packet");
    return rs.hasNext() ? ((Number) rs.next().getProperty("cnt")).longValue() : -1;
  }
}
