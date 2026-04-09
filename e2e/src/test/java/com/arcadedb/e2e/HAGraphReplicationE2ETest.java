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
 * Tests replication of graph data (vertices + edges) written from all 3 nodes.
 * Unlike other HA tests that only write flat vertex inserts from the leader,
 * this test creates a graph structure from every node and verifies both vertex
 * and edge counts converge across the cluster.
 *
 * <p>Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HAGraphReplicationE2ETest}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class HAGraphReplicationE2ETest extends ArcadeHAContainerTemplate {

  private static final int USERS_PER_NODE  = 5;
  private static final int PHOTOS_PER_USER = 3;

  @BeforeEach
  void setUp() {
    startCluster(3);
  }

  @AfterEach
  void tearDown() {
    stopCluster();
  }

  @Test
  void testGraphReplicationFromAllNodes() throws Exception {
    // Create schema on the leader
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    httpCommand(leader, "SQL", "CREATE VERTEX TYPE User IF NOT EXISTS");
    httpCommand(leader, "SQL", "CREATE VERTEX TYPE Photo IF NOT EXISTS");
    httpCommand(leader, "SQL", "CREATE EDGE TYPE HasPhoto IF NOT EXISTS");

    // Wait for schema to replicate
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        assertThat(httpCount(c, "User")).isGreaterThanOrEqualTo(0);
        assertThat(httpCount(c, "Photo")).isGreaterThanOrEqualTo(0);
      }
    });

    // Write from each node: create users with photos
    for (int nodeIdx = 0; nodeIdx < containers.size(); nodeIdx++) {
      final GenericContainer<?> node = containers.get(nodeIdx);
      for (int u = 0; u < USERS_PER_NODE; u++) {
        final String userName = "user_n" + nodeIdx + "_u" + u;

        // Create user vertex
        httpCommand(node, "SQL", "INSERT INTO User CONTENT {\"name\":\"" + userName + "\",\"node\":" + nodeIdx + "}");

        // Create photos and edges
        for (int p = 0; p < PHOTOS_PER_USER; p++) {
          final String photoName = userName + "_photo" + p;
          httpCommand(node, "SQL",
              "INSERT INTO Photo CONTENT {\"name\":\"" + photoName + "\",\"owner\":\"" + userName + "\"}");
          httpCommand(node, "SQL",
              "CREATE EDGE HasPhoto FROM (SELECT FROM User WHERE name = '" + userName
                  + "') TO (SELECT FROM Photo WHERE name = '" + photoName + "')");
        }
      }
    }

    // Expected counts
    final long expectedUsers = (long) containers.size() * USERS_PER_NODE;
    final long expectedPhotos = expectedUsers * PHOTOS_PER_USER;
    final long expectedEdges = expectedPhotos; // 1 edge per photo

    // Verify all nodes converge on both vertex and edge counts
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers) {
        assertThat(httpCount(c, "User"))
            .as("User count on %s", c.getNetworkAliases())
            .isEqualTo(expectedUsers);
        assertThat(httpCount(c, "Photo"))
            .as("Photo count on %s", c.getNetworkAliases())
            .isEqualTo(expectedPhotos);
        assertThat(httpCount(c, "HasPhoto"))
            .as("HasPhoto count on %s", c.getNetworkAliases())
            .isEqualTo(expectedEdges);
      }
    });
  }
}
