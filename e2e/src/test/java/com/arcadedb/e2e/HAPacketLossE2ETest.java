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

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
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
 * Uses direct HTTP instead of RemoteDatabase to avoid cluster redirect issues
 * with internal Docker addresses.
 * <p>
 * Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HAPacketLossE2ETest}
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("e2e-ha")
@Disabled("Toxiproxy TCP proxy does not support Ratis gRPC bidirectional streaming - leader election never completes")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public class HAPacketLossE2ETest extends ArcadeHAToxiproxyTemplate {

  @BeforeEach
  void setUp() throws Exception {
    startClusterWithToxiproxy(3);
    // Create schema - retry until a leader is elected and DDL succeeds
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
      final GenericContainer<?> leader = findLeader();
      if (leader == null) return false;
      try {
        httpCommand(leader, "SQL", "CREATE VERTEX TYPE Packet IF NOT EXISTS");
        return true;
      } catch (final Exception e) {
        return false;
      }
    });
  }

  @AfterEach
  void tearDown() {
    stopAll();
  }

  @Test
  void testLowPacketLoss() throws Exception {
    addPacketLossAll(5);

    for (int i = 0; i < 20; i++)
      httpCommand(findLeader(), "SQL", "INSERT INTO Packet SET name = 'low-" + i + "', rate = 5");

    removeAllToxics();

    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Packet")).isEqualTo(20);
    });
  }

  @Test
  void testModeratePacketLoss() throws Exception {
    addPacketLossAll(20);

    for (int i = 0; i < 10; i++)
      httpCommand(findLeader(), "SQL", "INSERT INTO Packet SET name = 'moderate-" + i + "', rate = 20");

    removeAllToxics();

    Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Packet")).isEqualTo(10);
    });
  }

  @Test
  void testHighPacketLoss() throws Exception {
    addPacketLossAll(50);

    int successes = 0;
    for (int i = 0; i < 10; i++)
      try {
        httpCommand(findLeader(), "SQL", "INSERT INTO Packet SET name = 'high-" + i + "', rate = 50");
        successes++;
      } catch (final Exception ignored) {}

    removeAllToxics();

    final int expected = successes;
    Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Packet")).isEqualTo(expected);
    });
  }

  @Test
  void testIntermittentPacketLoss() throws Exception {
    int totalWrites = 0;
    for (int cycle = 0; cycle < 3; cycle++) {
      addPacketLossAll(25);

      for (int i = 0; i < 5; i++) {
        httpCommand(findLeader(), "SQL",
            "INSERT INTO Packet SET name = 'intermittent-" + cycle + "-" + i + "', cycle = " + cycle);
        totalWrites++;
      }

      removeAllToxics();
      Thread.sleep(1000);
    }

    final int expected = totalWrites;
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Packet")).isEqualTo(expected);
    });
  }

  @Test
  void testDirectionalPacketLoss() throws Exception {
    for (final Proxy proxy : proxies)
      if (proxy.getName().startsWith("raft-0-"))
        addPacketLoss(proxy, ToxicDirection.DOWNSTREAM, 30);

    for (int i = 0; i < 10; i++)
      httpCommand(findLeader(), "SQL",
          "INSERT INTO Packet SET name = 'dir-" + i + "', direction = 'downstream'");

    removeAllToxics();

    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Packet")).isEqualTo(10);
    });
  }

  private void addPacketLossAll(final int percentage) throws IOException {
    for (final Proxy proxy : proxies)
      addPacketLoss(proxy, ToxicDirection.DOWNSTREAM, percentage);
  }

  private void addPacketLoss(final Proxy proxy, final ToxicDirection direction, final int percentage) throws IOException {
    proxy.toxics().bandwidth(proxy.getName() + "-loss-" + direction.name(), direction,
        (long) (1024 * 1024 * (100 - percentage) / 100));
  }
}
