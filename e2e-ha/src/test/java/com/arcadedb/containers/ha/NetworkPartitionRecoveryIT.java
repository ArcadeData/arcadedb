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
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Network partition recovery and data convergence tests for Raft HA cluster resilience.
 * Tests partition healing and Raft log catch-up after network failures.
 * <p>
 * In Raft, the isolated minority cannot accept writes (leader steps down without quorum).
 * After partition heals, the minority catches up via the Raft log from the majority leader.
 * There is no split-brain or conflict resolution needed -- Raft prevents divergent writes.
 */
@Testcontainers
class NetworkPartitionRecoveryIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "proxy:8666:9666,proxy:8667:9667,proxy:8668:9668";

  private Proxy raft0Proxy;
  private Proxy raft1Proxy;
  private Proxy raft2Proxy;
  private Proxy http0Proxy;
  private Proxy http1Proxy;
  private Proxy http2Proxy;

  private void createProxies() throws IOException {
    logger.info("Creating Raft and HTTP proxies for 3-node cluster");
    raft0Proxy = toxiproxyClient.createProxy("raft0Proxy", "0.0.0.0:8666", "ArcadeDB_0:2434");
    raft1Proxy = toxiproxyClient.createProxy("raft1Proxy", "0.0.0.0:8667", "ArcadeDB_1:2434");
    raft2Proxy = toxiproxyClient.createProxy("raft2Proxy", "0.0.0.0:8668", "ArcadeDB_2:2434");
    http0Proxy = toxiproxyClient.createProxy("http0Proxy", "0.0.0.0:9666", "ArcadeDB_0:2480");
    http1Proxy = toxiproxyClient.createProxy("http1Proxy", "0.0.0.0:9667", "ArcadeDB_1:2480");
    http2Proxy = toxiproxyClient.createProxy("http2Proxy", "0.0.0.0:9668", "ArcadeDB_2:2480");
  }

  private int findLeaderIndex(final List<ServerWrapper> servers) {
    for (int i = 0; i < servers.size(); i++) {
      try {
        final URL url = URI.create(
            "http://" + servers.get(i).host() + ":" + servers.get(i).httpPort() + "/api/v1/cluster").toURL();
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("Authorization",
            "Basic " + Base64.getEncoder().encodeToString("root:playwithdata".getBytes()));
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

  private void isolateNode(final Proxy raftProxy, final Proxy httpProxy, final String label) throws IOException {
    raftProxy.toxics().bandwidth(label + "_RAFT_DOWN", ToxicDirection.DOWNSTREAM, 0);
    raftProxy.toxics().bandwidth(label + "_RAFT_UP", ToxicDirection.UPSTREAM, 0);
    httpProxy.toxics().bandwidth(label + "_HTTP_DOWN", ToxicDirection.DOWNSTREAM, 0);
    httpProxy.toxics().bandwidth(label + "_HTTP_UP", ToxicDirection.UPSTREAM, 0);
  }

  private void reconnectNode(final Proxy raftProxy, final Proxy httpProxy, final String label) throws IOException {
    raftProxy.toxics().get(label + "_RAFT_DOWN").remove();
    raftProxy.toxics().get(label + "_RAFT_UP").remove();
    httpProxy.toxics().get(label + "_HTTP_DOWN").remove();
    httpProxy.toxics().get(label + "_HTTP_UP").remove();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test partition recovery: 2+1 split, heal partition, verify Raft log catch-up")
  void testPartitionRecovery() throws IOException, InterruptedException {
    createProxies();

    logger.info("Creating 3-node Raft HA cluster with majority quorum");
    final GenericContainer<?> arcade0 = createArcadeContainer("ArcadeDB_0", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade1 = createArcadeContainer("ArcadeDB_1", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade2 = createArcadeContainer("ArcadeDB_2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);
    final DatabaseWrapper[] dbs = { db0, db1, db2 };
    final Proxy[] raftProxies = { raft0Proxy, raft1Proxy, raft2Proxy };
    final Proxy[] httpProxies = { http0Proxy, http1Proxy, http2Proxy };

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

    logger.info("Creating network partition: isolating node {}", isolatedIdx);
    isolateNode(raftProxies[isolatedIdx], httpProxies[isolatedIdx], "ISOLATED");

    logger.info("Waiting for partition to be detected");
    TimeUnit.SECONDS.sleep(10);

    logger.info("Writing to majority partition");
    dbs[leaderIdx].addUserAndPhotos(10, 10);

    logger.info("Verifying writes on majority partition");
    dbs[leaderIdx].assertThatUserCountIs(30);
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            return dbs[otherIdx].countUsers() == 30L;
          } catch (final Exception e) {
            return false;
          }
        });

    logger.info("Verifying minority partition (node {}) still has old data", isolatedIdx);
    dbs[isolatedIdx].assertThatUserCountIs(20);

    logger.info("Healing partition - reconnecting node {}", isolatedIdx);
    reconnectNode(raftProxies[isolatedIdx], httpProxies[isolatedIdx], "ISOLATED");

    logger.info("Waiting for partition recovery and Raft log catch-up");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0.countUsers();
            final long users1 = db1.countUsers();
            final long users2 = db2.countUsers();
            logger.info("Recovery check: ArcadeDB_0={}, ArcadeDB_1={}, ArcadeDB_2={}", users0, users1, users2);
            return users0 == 30L && users1 == 30L && users2 == 30L;
          } catch (final Exception e) {
            logger.warn("Recovery check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final data consistency across all nodes");
    db0.assertThatUserCountIs(30);
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);

    db0.close();
    db1.close();
    db2.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test multiple partition cycles: repeated split and heal with Raft log catch-up")
  void testMultiplePartitionCycles() throws IOException, InterruptedException {
    createProxies();

    logger.info("Creating 3-node Raft HA cluster");
    final GenericContainer<?> arcade0 = createArcadeContainer("ArcadeDB_0", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade1 = createArcadeContainer("ArcadeDB_1", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade2 = createArcadeContainer("ArcadeDB_2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);
    final DatabaseWrapper[] dbs = { db0, db1, db2 };
    final Proxy[] raftProxies = { raft0Proxy, raft1Proxy, raft2Proxy };
    final Proxy[] httpProxies = { http0Proxy, http1Proxy, http2Proxy };

    logger.info("Creating database and initial data");
    db0.createDatabase();
    db0.createSchema();
    db0.addUserAndPhotos(10, 10);

    int expectedUsers = 10;

    // Run 3 partition cycles, always isolating a follower to keep majority intact
    for (int cycle = 1; cycle <= 3; cycle++) {
      logger.info("=== Partition Cycle {} ===", cycle);

      final int currentLeader = findLeaderIndex(servers);
      final int isolatedIdx = (currentLeader + 1) % 3;
      logger.info("Cycle {}: Leader is node {}, isolating follower node {}", cycle, currentLeader, isolatedIdx);

      logger.info("Cycle {}: Creating partition (isolating node {})", cycle, isolatedIdx);
      isolateNode(raftProxies[isolatedIdx], httpProxies[isolatedIdx], "CYCLE" + cycle);

      TimeUnit.SECONDS.sleep(5);

      logger.info("Cycle {}: Writing to majority partition via leader node {}", cycle, currentLeader);
      dbs[currentLeader].addUserAndPhotos(5, 10);
      expectedUsers += 5;

      logger.info("Cycle {}: Healing partition", cycle);
      reconnectNode(raftProxies[isolatedIdx], httpProxies[isolatedIdx], "CYCLE" + cycle);

      logger.info("Cycle {}: Waiting for Raft log catch-up convergence", cycle);
      final int currentCycle = cycle;
      final int expected = expectedUsers;
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(3, TimeUnit.SECONDS)
          .until(() -> {
            try {
              final long users0 = db0.countUsers();
              final long users1 = db1.countUsers();
              final long users2 = db2.countUsers();
              logger.info("Cycle {}: Convergence check: {} / {} / {} (expected={})",
                  currentCycle, users0, users1, users2, expected);
              return users0 == expected && users1 == expected && users2 == expected;
            } catch (final Exception e) {
              logger.warn("Cycle {}: Convergence check failed: {}", currentCycle, e.getMessage());
              return false;
            }
          });

      logger.info("Cycle {}: Complete - all nodes at {} users", cycle, expectedUsers);
    }

    logger.info("Verifying final consistency after {} cycles", 3);
    db0.assertThatUserCountIs(expectedUsers);
    db1.assertThatUserCountIs(expectedUsers);
    db2.assertThatUserCountIs(expectedUsers);

    db0.close();
    db1.close();
    db2.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test asymmetric partition recovery: follower isolated then resyncs")
  void testAsymmetricPartitionRecovery() throws IOException, InterruptedException {
    createProxies();

    logger.info("Creating 3-node Raft HA cluster");
    final GenericContainer<?> arcade0 = createArcadeContainer("ArcadeDB_0", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade1 = createArcadeContainer("ArcadeDB_1", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade2 = createArcadeContainer("ArcadeDB_2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);
    final DatabaseWrapper[] dbs = { db0, db1, db2 };
    final Proxy[] raftProxies = { raft0Proxy, raft1Proxy, raft2Proxy };
    final Proxy[] httpProxies = { http0Proxy, http1Proxy, http2Proxy };

    logger.info("Creating database and initial data");
    db0.createDatabase();
    db0.createSchema();
    db0.addUserAndPhotos(10, 10);

    logger.info("Finding current leader");
    final int leaderIdx = findLeaderIndex(servers);
    final int isolatedIdx = (leaderIdx + 1) % 3;
    final int otherIdx = (leaderIdx + 2) % 3;
    logger.info("Leader is node {}, isolating follower node {}", leaderIdx, isolatedIdx);

    logger.info("Creating asymmetric partition: isolating node {}", isolatedIdx);
    isolateNode(raftProxies[isolatedIdx], httpProxies[isolatedIdx], "ASYM");

    TimeUnit.SECONDS.sleep(10);

    logger.info("Writing to connected majority (leader {} + follower {})", leaderIdx, otherIdx);
    dbs[leaderIdx].addUserAndPhotos(15, 10);

    logger.info("Verifying connected nodes have new data");
    dbs[leaderIdx].assertThatUserCountIs(25);
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            return dbs[otherIdx].countUsers() == 25L;
          } catch (final Exception e) {
            return false;
          }
        });

    logger.info("Verifying isolated node has old data");
    dbs[isolatedIdx].assertThatUserCountIs(10);

    logger.info("Healing asymmetric partition");
    reconnectNode(raftProxies[isolatedIdx], httpProxies[isolatedIdx], "ASYM");

    logger.info("Waiting for full convergence via Raft log catch-up");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0.countUsers();
            final long users1 = db1.countUsers();
            final long users2 = db2.countUsers();
            logger.info("Asymmetric recovery check: ArcadeDB_0={}, ArcadeDB_1={}, ArcadeDB_2={}",
                users0, users1, users2);
            return users0 == 25L && users1 == 25L && users2 == 25L;
          } catch (final Exception e) {
            logger.warn("Asymmetric recovery check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency");
    db0.assertThatUserCountIs(25);
    db1.assertThatUserCountIs(25);
    db2.assertThatUserCountIs(25);

    db0.close();
    db1.close();
    db2.close();
  }
}
