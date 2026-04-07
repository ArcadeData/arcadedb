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

import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
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
 * Uses direct HTTP instead of RemoteDatabase to avoid cluster redirect issues
 * with internal Docker addresses.
 * <p>
 * Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HANetworkDelayE2ETest}
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("e2e-ha")
@Disabled("Toxiproxy TCP proxy does not support Ratis gRPC bidirectional streaming - leader election never completes")
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
    httpCommand(findLeader(), "SQL", "CREATE VERTEX TYPE Metric IF NOT EXISTS");

    addLatencyAll(200, 0);

    for (int i = 0; i < 20; i++)
      httpCommand(findLeader(), "SQL", "INSERT INTO Metric SET name = 'sym-" + i + "', value = " + i);

    removeAllToxics();

    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Metric")).isEqualTo(20);
    });
  }

  @Test
  void testHighLatencyWithJitter() throws Exception {
    httpCommand(findLeader(), "SQL", "CREATE VERTEX TYPE Metric IF NOT EXISTS");

    addLatencyAll(300, 150);

    for (int i = 0; i < 10; i++)
      httpCommand(findLeader(), "SQL", "INSERT INTO Metric SET name = 'jitter-" + i + "', value = " + i);

    removeAllToxics();

    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Metric")).isEqualTo(10);
    });
  }

  @Test
  void testAsymmetricLeaderDelay() throws Exception {
    httpCommand(findLeader(), "SQL", "CREATE VERTEX TYPE Metric IF NOT EXISTS");

    for (final var proxy : proxies)
      if (proxy.getName().startsWith("raft-0-"))
        addLatency(proxy, ToxicDirection.DOWNSTREAM, 500, 0);

    for (int i = 0; i < 5; i++)
      httpCommand(findLeader(), "SQL", "INSERT INTO Metric SET name = 'asym-" + i + "', value = " + i);

    removeAllToxics();

    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Metric")).isEqualTo(5);
    });
  }

  @Test
  void testExtremeLatency() throws Exception {
    httpCommand(findLeader(), "SQL", "CREATE VERTEX TYPE Metric IF NOT EXISTS");

    addLatencyAll(2000, 0);

    int successes = 0;
    for (int i = 0; i < 3; i++)
      try {
        httpCommand(findLeader(), "SQL", "INSERT INTO Metric SET name = 'extreme-" + i + "', value = " + i);
        successes++;
      } catch (final Exception ignored) {}

    removeAllToxics();

    final int expected = successes;
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Metric")).isEqualTo(expected);
    });
  }
}
