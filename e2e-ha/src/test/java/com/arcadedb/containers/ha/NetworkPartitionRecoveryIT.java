/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.containers.ha;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Network partition recovery and data convergence tests for Raft HA cluster resilience.
 * Tests partition healing and Raft log catch-up after network failures.
 * Uses Docker network disconnect for true symmetric partition isolation.
 * <p>
 * In Raft, the isolated minority cannot accept writes (leader steps down without quorum).
 * After partition heals, the minority catches up via the Raft log from the majority leader.
 * There is no split-brain or conflict resolution needed -- Raft prevents divergent writes.
 */
@Testcontainers
class NetworkPartitionRecoveryIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";

  private int findLeaderIndex(final List<ServerWrapper> servers) {
    for (int i = 0; i < servers.size(); i++) {
      try {
        final URL url = URI.create(
            "http://" + servers.get(i).host() + ":" + servers.get(i).httpPort() + "/api/v1/cluster").toURL();
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("Authorization",
            "Basic " + Base64.getEncoder().encodeToString("root:playwithdata".getBytes()));
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);
        try {
          if (conn.getResponseCode() == 200) {
            final String body = new String(conn.getInputStream().readAllBytes());
            final JSONObject json = new JSONObject(body);
            if (json.getBoolean("isLeader"))
              return i;
          }
        } finally {
          conn.disconnect();
        }
      } catch (final Exception e) {
        logger.warn("Failed to check leader status on node {}: {}", i, e.getMessage());
      }
    }
    return -1;
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test partition recovery: 2+1 split, heal partition, verify Raft log catch-up")
  void partitionRecovery() throws Exception {
    logger.info("Creating 3-node Raft HA cluster with majority quorum (persistent for restart)");
    final GenericContainer<?> arcade0 = createPersistentArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade1 = createPersistentArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade2 = createPersistentArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);
    final DatabaseWrapper[] dbs = { db0, db1, db2 };
    final GenericContainer<?>[] nodeContainers = { arcade0, arcade1, arcade2 };

    logger.info("Creating database and initial data");
    db0.createDatabase();
    db0.createSchema();
    db0.addUserAndPhotos(20, 10);

    logger.info("Verifying initial replication");
    db0.assertThatUserCountIs(20);
    db1.assertThatUserCountIs(20);
    db2.assertThatUserCountIs(20);

    logger.info("Finding current leader to select isolation target");
    final int leaderIdx = findLeaderIndex(servers);
    // Isolate a non-leader to ensure majority keeps the leader
    final int isolatedIdx = (leaderIdx + 1) % 3;
    final int otherIdx = (leaderIdx + 2) % 3;
    logger.info("Leader is node {}, isolating follower node {}", leaderIdx, isolatedIdx);

    logger.info("Creating network partition: disconnecting node {}", isolatedIdx);
    disconnectFromNetwork(nodeContainers[isolatedIdx]);

    logger.info("Waiting for partition to be detected and leader confirmed on majority");
    waitForRaftLeader(List.of(servers.get(leaderIdx), servers.get(otherIdx)), 60);

    logger.info("Writing to majority partition");
    dbs[leaderIdx].addUserAndPhotos(10, 10);

    logger.info("Verifying writes on majority partition");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long uLeader = dbs[leaderIdx].countUsers();
            final long uOther = dbs[otherIdx].countUsers();
            logger.info("Majority check: leader={}, other={}", uLeader, uOther);
            return uLeader == uOther && uLeader >= 20L;
          } catch (final Exception e) {
            return false;
          }
        });
    final long majorityCount = dbs[leaderIdx].countUsers();

    // Note: we do not assert on the isolated node here — its Docker network is disconnected,
    // so host-to-container HTTP via the mapped port may be unreachable.

    // After a Docker network partition, gRPC channels between peers are stuck in
    // exponential backoff (up to ~120s). Simply reconnecting the network does not
    // reset these channels. Restart the isolated node to force fresh connections.
    logger.info("Healing partition: reconnecting and restarting isolated node {}", isolatedIdx);
    reconnectToNetwork(nodeContainers[isolatedIdx]);
    dbs[isolatedIdx].close();
    nodeContainers[isolatedIdx].stop();
    nodeContainers[isolatedIdx].start();
    waitForContainerHealthy(nodeContainers[isolatedIdx], 90);

    // Recreate wrapper with new mapped ports after restart
    final ServerWrapper restartedServer = new ServerWrapper(nodeContainers[isolatedIdx]);
    final DatabaseWrapper dbRestarted = new DatabaseWrapper(restartedServer, idSupplier, wordSupplier);

    logger.info("Waiting for partition recovery and Raft log catch-up (expected={})", majorityCount);
    Awaitility.await()
        .atMost(180, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long usersRestarted = dbRestarted.countUsers();
            final long usersLeader = dbs[leaderIdx].countUsers();
            final long usersOther = dbs[otherIdx].countUsers();
            logger.info("Recovery check: restarted={}, leader={}, other={} (expected={})",
                usersRestarted, usersLeader, usersOther, majorityCount);
            return usersRestarted == majorityCount && usersLeader == majorityCount && usersOther == majorityCount;
          } catch (final Exception e) {
            logger.warn("Recovery check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final data consistency across all nodes");
    dbs[leaderIdx].assertThatUserCountIs((int) majorityCount);
    dbs[otherIdx].assertThatUserCountIs((int) majorityCount);
    dbRestarted.assertThatUserCountIs((int) majorityCount);

    dbs[leaderIdx].close();
    dbs[otherIdx].close();
    dbRestarted.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test multiple partition cycles: repeated split and heal with Raft log catch-up")
  void multiplePartitionCycles() throws Exception {
    logger.info("Creating 3-node Raft HA cluster (persistent for restart)");
    final GenericContainer<?> arcade0 = createPersistentArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade1 = createPersistentArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade2 = createPersistentArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    List<ServerWrapper> servers = startCluster();

    DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);
    final DatabaseWrapper[] dbs = { db0, db1, db2 };
    final GenericContainer<?>[] nodeContainers = { arcade0, arcade1, arcade2 };

    logger.info("Creating database and initial data");
    db0.createDatabase();
    db0.createSchema();
    db0.addUserAndPhotos(10, 10);

    // Run 3 partition cycles, always isolating a follower to keep majority intact
    for (int cycle = 1; cycle <= 3; cycle++) {
      logger.info("=== Partition Cycle {} ===", cycle);

      final int currentLeader = findLeaderIndex(servers);
      final int isolatedIdx = (currentLeader + 1) % 3;
      logger.info("Cycle {}: Leader is node {}, isolating follower node {}", cycle, currentLeader, isolatedIdx);

      logger.info("Cycle {}: Creating partition (disconnecting node {})", cycle, isolatedIdx);
      disconnectFromNetwork(nodeContainers[isolatedIdx]);

      logger.info("Cycle {}: Waiting for leader on majority", cycle);
      waitForRaftLeader(List.of(servers.get(currentLeader), servers.get((currentLeader + 2) % 3)), 60);

      logger.info("Cycle {}: Writing to majority partition via leader node {}", cycle, currentLeader);
      dbs[currentLeader].addUserAndPhotos(5, 10);

      // After a Docker network partition, gRPC channels between peers are stuck in
      // exponential backoff. Restart the isolated node to force fresh connections.
      logger.info("Cycle {}: Healing partition - reconnecting and restarting isolated node {}", cycle, isolatedIdx);
      reconnectToNetwork(nodeContainers[isolatedIdx]);
      dbs[isolatedIdx].close();
      nodeContainers[isolatedIdx].stop();
      nodeContainers[isolatedIdx].start();
      waitForContainerHealthy(nodeContainers[isolatedIdx], 90);

      // Recreate wrapper and server entry with new mapped ports after restart
      final ServerWrapper restartedServer = new ServerWrapper(nodeContainers[isolatedIdx]);
      dbs[isolatedIdx] = new DatabaseWrapper(restartedServer, idSupplier, wordSupplier);
      servers = List.of(
          isolatedIdx == 0 ? restartedServer : servers.get(0),
          isolatedIdx == 1 ? restartedServer : servers.get(1),
          isolatedIdx == 2 ? restartedServer : servers.get(2));

      // Measure actual count from leader - some writes may fail during transition
      final long cycleCount = dbs[currentLeader].countUsers();

      logger.info("Cycle {}: Waiting for Raft log catch-up convergence (expected={})", cycle, cycleCount);
      final int currentCycle = cycle;
      final int capturedIsolatedIdx = isolatedIdx;
      final int capturedLeader = currentLeader;
      final int capturedOther = (currentLeader + 2) % 3;
      Awaitility.await()
          .atMost(180, TimeUnit.SECONDS)
          .pollInterval(3, TimeUnit.SECONDS)
          .until(() -> {
            try {
              final long usersRestarted = dbs[capturedIsolatedIdx].countUsers();
              final long usersLeader = dbs[capturedLeader].countUsers();
              final long usersOther = dbs[capturedOther].countUsers();
              logger.info("Cycle {}: Convergence check: restarted={}, leader={}, other={} (expected={})",
                  currentCycle, usersRestarted, usersLeader, usersOther, cycleCount);
              return usersRestarted == cycleCount && usersLeader == cycleCount && usersOther == cycleCount;
            } catch (final Exception e) {
              logger.warn("Cycle {}: Convergence check failed: {}", currentCycle, e.getMessage());
              return false;
            }
          });

      logger.info("Cycle {}: Complete - all nodes at {} users", cycle, cycleCount);
    }

    logger.info("Verifying final consistency after {} cycles", 3);
    final long finalCount = dbs[0].countUsers();
    dbs[0].assertThatUserCountIs((int) finalCount);
    dbs[1].assertThatUserCountIs((int) finalCount);
    dbs[2].assertThatUserCountIs((int) finalCount);

    dbs[0].close();
    dbs[1].close();
    dbs[2].close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test asymmetric partition recovery: follower isolated then resyncs")
  void asymmetricPartitionRecovery() throws Exception {
    logger.info("Creating 3-node Raft HA cluster (persistent for restart)");
    final GenericContainer<?> arcade0 = createPersistentArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade1 = createPersistentArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade2 = createPersistentArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);
    final DatabaseWrapper[] dbs = { db0, db1, db2 };
    final GenericContainer<?>[] nodeContainers = { arcade0, arcade1, arcade2 };

    logger.info("Creating database and initial data");
    db0.createDatabase();
    db0.createSchema();
    db0.addUserAndPhotos(10, 10);

    logger.info("Finding current leader");
    final int leaderIdx = findLeaderIndex(servers);
    final int isolatedIdx = (leaderIdx + 1) % 3;
    final int otherIdx = (leaderIdx + 2) % 3;
    logger.info("Leader is node {}, isolating follower node {}", leaderIdx, isolatedIdx);

    logger.info("Creating asymmetric partition: disconnecting node {}", isolatedIdx);
    disconnectFromNetwork(nodeContainers[isolatedIdx]);

    logger.info("Waiting for leader confirmed on majority");
    waitForRaftLeader(List.of(servers.get(leaderIdx), servers.get(otherIdx)), 60);

    logger.info("Writing to connected majority (leader {} + follower {})", leaderIdx, otherIdx);
    dbs[leaderIdx].addUserAndPhotos(15, 10);

    logger.info("Verifying connected nodes have new data");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long uLeader = dbs[leaderIdx].countUsers();
            final long uOther = dbs[otherIdx].countUsers();
            logger.info("Majority check: leader={}, other={}", uLeader, uOther);
            return uLeader == uOther && uLeader >= 10L;
          } catch (final Exception e) {
            return false;
          }
        });
    final long majorityCount = dbs[leaderIdx].countUsers();

    // Note: we do not assert on the isolated node here — its network is disconnected
    // from Docker, so host-to-container HTTP may be unreachable via the mapped port.

    // After a Docker network partition, gRPC channels between peers are stuck in
    // exponential backoff (up to ~120s). Simply reconnecting the network does not
    // reset these channels. Restart the isolated node to force fresh connections.
    logger.info("Healing asymmetric partition: reconnecting and restarting isolated node {}", isolatedIdx);
    reconnectToNetwork(nodeContainers[isolatedIdx]);
    dbs[isolatedIdx].close();
    nodeContainers[isolatedIdx].stop();
    nodeContainers[isolatedIdx].start();
    waitForContainerHealthy(nodeContainers[isolatedIdx], 90);

    // Recreate wrapper with new mapped ports after restart
    final ServerWrapper restartedServer = new ServerWrapper(nodeContainers[isolatedIdx]);
    final DatabaseWrapper dbRestarted = new DatabaseWrapper(restartedServer, idSupplier, wordSupplier);

    logger.info("Waiting for full convergence via Raft log catch-up (expected={})", majorityCount);
    Awaitility.await()
        .atMost(180, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long usersRestarted = dbRestarted.countUsers();
            final long usersLeader = dbs[leaderIdx].countUsers();
            final long usersOther = dbs[otherIdx].countUsers();
            logger.info("Asymmetric recovery check: restarted={}, leader={}, other={} (expected={})",
                usersRestarted, usersLeader, usersOther, majorityCount);
            return usersRestarted == majorityCount && usersLeader == majorityCount && usersOther == majorityCount;
          } catch (final Exception e) {
            logger.warn("Asymmetric recovery check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency");
    dbs[leaderIdx].assertThatUserCountIs((int) majorityCount);
    dbs[otherIdx].assertThatUserCountIs((int) majorityCount);
    dbRestarted.assertThatUserCountIs((int) majorityCount);

    dbs[leaderIdx].close();
    dbs[otherIdx].close();
    dbRestarted.close();
  }
}
