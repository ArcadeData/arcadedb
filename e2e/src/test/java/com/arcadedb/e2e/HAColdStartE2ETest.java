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

import com.github.dockerjava.api.DockerClient;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests full cluster cold start: all nodes are restarted simultaneously.
 * Verifies that Ratis recovers its persisted state (term, vote, log segments) from disk
 * and that the database data survives the full outage.
 *
 * <p>Uses docker restart (atomic stop+start) to preserve the container filesystem.
 * All nodes are restarted at once before waiting, so Ratis can form a quorum immediately.
 *
 * <p>Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HAColdStartE2ETest}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class HAColdStartE2ETest extends ArcadeHAContainerTemplate {

  @BeforeEach
  void setUp() {
    startCluster(3);
  }

  @AfterEach
  void tearDown() {
    stopCluster();
  }

  @Test
  void testColdStartRecovery() throws Exception {
    // 1. Create schema and write data
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    httpCommand(leader, "SQL", "CREATE VERTEX TYPE Invoice IF NOT EXISTS");
    httpCommand(leader, "SQL", "CREATE PROPERTY Invoice.number STRING");
    httpCommand(leader, "SQL", "CREATE INDEX ON Invoice (number) UNIQUE");

    for (int i = 0; i < 50; i++)
      httpCommand(leader, "SQL", "INSERT INTO Invoice CONTENT {\"number\":\"INV-" + String.format("%04d", i)
          + "\",\"amount\":" + (i * 99.5) + ",\"phase\":\"before-shutdown\"}");

    // Wait for all nodes to replicate
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Invoice")).isEqualTo(50);
    });

    // 2. Restart ALL containers in parallel (fire all restart commands concurrently)
    final DockerClient dockerClient = DockerClientFactory.instance().client();
    final var restartThreads = containers.stream()
        .map(c -> new Thread(() -> dockerClient.restartContainerCmd(c.getContainerId()).withTimeout(10).exec()))
        .toList();
    restartThreads.forEach(Thread::start);
    for (final var t : restartThreads)
      t.join(30_000);

    // 3. Wait for ALL nodes to become healthy
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          int readyCount = 0;
          for (final GenericContainer<?> c : containers) {
            try {
              // Check container is actually running at Docker level
              final var inspect = dockerClient.inspectContainerCmd(c.getContainerId()).exec();
              if (!Boolean.TRUE.equals(inspect.getState().getRunning()))
                continue;

              final var response = httpClient.send(
                  HttpRequest.newBuilder()
                      .uri(URI.create("http://" + c.getHost() + ":" + c.getMappedPort(HTTP_PORT) + "/api/v1/ready"))
                      .GET().build(),
                  HttpResponse.BodyHandlers.ofString());
              if (response.statusCode() == 204)
                readyCount++;
            } catch (final Exception ignored) {}
          }
          return readyCount == containers.size();
        });

    // 4. Wait for leader election after cold start
    waitForLeader();

    // 5. Verify all data survived the full cluster restart
    for (final GenericContainer<?> c : containers)
      assertThat(httpCount(c, "Invoice")).as("Node should have all 50 invoices after cold start").isEqualTo(50);

    // 6. Verify the cluster is fully functional - write more data
    final GenericContainer<?> newLeader = findLeader();
    assertThat(newLeader).isNotNull();

    for (int i = 50; i < 60; i++)
      httpCommand(newLeader, "SQL", "INSERT INTO Invoice CONTENT {\"number\":\"INV-" + String.format("%04d", i)
          + "\",\"amount\":" + (i * 99.5) + ",\"phase\":\"after-restart\"}");

    // Verify new data replicates to all nodes
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Invoice")).isEqualTo(60);
    });

    // 7. Verify the unique index survived - duplicate should be rejected
    try {
      httpCommand(newLeader, "SQL", "INSERT INTO Invoice CONTENT {\"number\":\"INV-0001\",\"amount\":0.0}");
      assertThat(false).as("Duplicate index key should have been rejected").isTrue();
    } catch (final Exception e) {
      // Expected: unique index violation
      assertThat(e.getMessage().toLowerCase()).contains("duplicate");
    }
  }
}
