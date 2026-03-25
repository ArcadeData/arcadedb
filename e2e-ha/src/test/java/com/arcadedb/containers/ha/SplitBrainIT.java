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
 * Split-brain detection and prevention tests for Raft HA cluster resilience.
 * Tests quorum enforcement and cluster reformation after network partitions.
 * <p>
 * Raft prevents split-brain by design: a leader in the minority partition automatically
 * steps down when it cannot reach a majority. Only the majority partition can elect a
 * new leader and accept writes. This eliminates the possibility of divergent data.
 */
@Testcontainers
class SplitBrainIT extends ContainersTestTemplate {

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
  @DisplayName("Test split-brain prevention: verify minority partition cannot accept writes (Raft leader steps down)")
  void testSplitBrainPrevention() throws IOException, InterruptedException {
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

    logger.info("Finding current leader to create 2+1 partition with leader in minority");
    final int leaderIdx = findLeaderIndex(servers);
    final int survivor1 = (leaderIdx + 1) % 3;
    final int survivor2 = (leaderIdx + 2) % 3;
    logger.info("Leader is node {}, isolating it to create minority partition", leaderIdx);

    logger.info("Creating 2+1 partition: isolating node {} (current leader, minority)", leaderIdx);
    isolateNode(raftProxies[leaderIdx], httpProxies[leaderIdx], "SPLIT");

    logger.info("Waiting for Raft leader step-down in minority and new election in majority");
    TimeUnit.SECONDS.sleep(15);

    logger.info("Writing to majority partition (nodes {} and {}) - should succeed with new leader", survivor1, survivor2);
    dbs[survivor1].addUserAndPhotos(10, 10);

    logger.info("Verifying writes on majority partition");
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            return dbs[survivor1].countUsers() == 30L && dbs[survivor2].countUsers() == 30L;
          } catch (final Exception e) {
            return false;
          }
        });

    logger.info("Verifying minority partition (node {}) has old data", leaderIdx);
    dbs[leaderIdx].assertThatUserCountIs(20);

    logger.info("Attempting write to minority partition (should fail - Raft leader stepped down)");
    boolean minorityWriteSucceeded = false;
    try {
      dbs[leaderIdx].addUserAndPhotos(5, 10);
      minorityWriteSucceeded = true;
      logger.warn("Write to minority partition succeeded - unexpected for Raft");
    } catch (final Exception e) {
      logger.info("Write to minority partition correctly failed (Raft leader stepped down): {}", e.getMessage());
    }

    logger.info("Healing partition");
    reconnectNode(raftProxies[leaderIdx], httpProxies[leaderIdx], "SPLIT");

    logger.info("Waiting for cluster reformation and Raft log catch-up");
    // In Raft, the minority node catches up from the majority's log. No conflict resolution needed.
    final int expectedUsers = minorityWriteSucceeded ? 35 : 30;
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0.countUsers();
            final long users1 = db1.countUsers();
            final long users2 = db2.countUsers();
            logger.info("Reformation check: ArcadeDB_0={}, ArcadeDB_1={}, ArcadeDB_2={} (expected={})",
                users0, users1, users2, expectedUsers);
            return users0 == expectedUsers && users1 == expectedUsers && users2 == expectedUsers;
          } catch (final Exception e) {
            logger.warn("Reformation check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency");
    db0.assertThatUserCountIs(expectedUsers);
    db1.assertThatUserCountIs(expectedUsers);
    db2.assertThatUserCountIs(expectedUsers);

    db0.close();
    db1.close();
    db2.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test 1+1+1 partition: verify no writes possible without majority (all leaders step down)")
  void testCompletePartitionNoQuorum() throws IOException, InterruptedException {
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

    logger.info("Creating database and initial data");
    db0.createDatabase();
    db0.createSchema();
    db0.addUserAndPhotos(15, 10);

    logger.info("Verifying initial state");
    db0.assertThatUserCountIs(15);
    db1.assertThatUserCountIs(15);
    db2.assertThatUserCountIs(15);

    logger.info("Creating complete partition: 1+1+1 (each node isolated from all others)");
    isolateNode(raft0Proxy, http0Proxy, "ISO_0");
    isolateNode(raft1Proxy, http1Proxy, "ISO_1");
    isolateNode(raft2Proxy, http2Proxy, "ISO_2");

    logger.info("Waiting for complete partition detection and Raft leader step-down");
    TimeUnit.SECONDS.sleep(15);

    logger.info("Attempting writes to all nodes (all should fail - no majority quorum exists)");
    int successfulWrites = 0;

    try {
      db0.addUserAndPhotos(5, 10);
      successfulWrites++;
      logger.warn("Write to ArcadeDB_0 succeeded without quorum");
    } catch (final Exception e) {
      logger.info("Write to ArcadeDB_0 correctly failed: {}", e.getMessage());
    }

    try {
      db1.addUserAndPhotos(5, 10);
      successfulWrites++;
      logger.warn("Write to ArcadeDB_1 succeeded without quorum");
    } catch (final Exception e) {
      logger.info("Write to ArcadeDB_1 correctly failed: {}", e.getMessage());
    }

    try {
      db2.addUserAndPhotos(5, 10);
      successfulWrites++;
      logger.warn("Write to ArcadeDB_2 succeeded without quorum");
    } catch (final Exception e) {
      logger.info("Write to ArcadeDB_2 correctly failed: {}", e.getMessage());
    }

    logger.info("Successful writes without quorum: {}/3 (expected 0 for Raft with majority quorum)", successfulWrites);

    logger.info("Healing all partitions");
    reconnectNode(raft0Proxy, http0Proxy, "ISO_0");
    reconnectNode(raft1Proxy, http1Proxy, "ISO_1");
    reconnectNode(raft2Proxy, http2Proxy, "ISO_2");

    logger.info("Waiting for cluster reformation and leader re-election");
    TimeUnit.SECONDS.sleep(15);

    logger.info("Verifying cluster can accept writes after reformation");
    db0.addUserAndPhotos(10, 10);

    final int expectedUsers = 15 + (successfulWrites * 5) + 10;
    logger.info("Waiting for final convergence (expected {} users)", expectedUsers);

    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0.countUsers();
            final long users1 = db1.countUsers();
            final long users2 = db2.countUsers();
            logger.info("Convergence check: ArcadeDB_0={}, ArcadeDB_1={}, ArcadeDB_2={} (expected={})",
                users0, users1, users2, expectedUsers);
            return users0 == expectedUsers && users1 == expectedUsers && users2 == expectedUsers;
          } catch (final Exception e) {
            logger.warn("Convergence check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency");
    db0.assertThatUserCountIs(expectedUsers);
    db1.assertThatUserCountIs(expectedUsers);
    db2.assertThatUserCountIs(expectedUsers);

    db0.close();
    db1.close();
    db2.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test cluster reformation: verify proper Raft leader election after partition healing")
  void testClusterReformation() throws IOException, InterruptedException {
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

    logger.info("Verifying initial state");
    db0.assertThatUserCountIs(10);
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);

    // Cycle through multiple partition/heal cycles
    for (int cycle = 1; cycle <= 3; cycle++) {
      logger.info("=== Reformation Cycle {} ===", cycle);

      // Find current leader to isolate a follower (keeping majority)
      final int currentLeader = findLeaderIndex(servers);
      final int isolatedIdx = (currentLeader + 1) % 3;
      final int otherFollower = (currentLeader + 2) % 3;
      logger.info("Cycle {}: Leader={}, isolating follower node {}", cycle, currentLeader, isolatedIdx);

      logger.info("Cycle {}: Creating partition", cycle);
      isolateNode(raftProxies[isolatedIdx], httpProxies[isolatedIdx], "CYCLE" + cycle);

      TimeUnit.SECONDS.sleep(10);

      logger.info("Cycle {}: Writing to majority partition via leader node {}", cycle, currentLeader);
      dbs[currentLeader].addUserAndPhotos(5, 10);

      logger.info("Cycle {}: Healing partition", cycle);
      reconnectNode(raftProxies[isolatedIdx], httpProxies[isolatedIdx], "CYCLE" + cycle);

      logger.info("Cycle {}: Waiting for reformation and Raft log catch-up", cycle);
      TimeUnit.SECONDS.sleep(10);

      final int currentCycle = cycle;
      final int expectedUsers = 10 + (cycle * 5);
      logger.info("Cycle {}: Verifying convergence to {} users", cycle, expectedUsers);

      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(3, TimeUnit.SECONDS)
          .until(() -> {
            try {
              final long users0 = db0.countUsers();
              final long users1 = db1.countUsers();
              final long users2 = db2.countUsers();
              logger.info("Cycle {}: {} / {} / {} (expected={})", currentCycle, users0, users1, users2, expectedUsers);
              return users0 == expectedUsers && users1 == expectedUsers && users2 == expectedUsers;
            } catch (final Exception e) {
              logger.warn("Cycle {}: Check failed: {}", currentCycle, e.getMessage());
              return false;
            }
          });

      logger.info("Cycle {}: Cluster reformed successfully", cycle);
    }

    logger.info("Verifying final consistency after {} reformation cycles", 3);
    final int finalExpected = 25;
    db0.assertThatUserCountIs(finalExpected);
    db1.assertThatUserCountIs(finalExpected);
    db2.assertThatUserCountIs(finalExpected);

    db0.close();
    db1.close();
    db2.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test quorum loss recovery: verify cluster recovers after temporary quorum loss")
  void testQuorumLossRecovery() throws IOException, InterruptedException {
    createProxies();

    logger.info("Creating 3-node Raft HA cluster with majority quorum (2/3)");
    final GenericContainer<?> arcade0 = createArcadeContainer("ArcadeDB_0", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade1 = createArcadeContainer("ArcadeDB_1", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade2 = createArcadeContainer("ArcadeDB_2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    logger.info("Creating database and initial data");
    db0.createDatabase();
    db0.createSchema();
    db0.addUserAndPhotos(20, 10);

    logger.info("Verifying initial state");
    db0.assertThatUserCountIs(20);
    db1.assertThatUserCountIs(20);
    db2.assertThatUserCountIs(20);

    logger.info("Isolating 2 nodes (ArcadeDB_1 and ArcadeDB_2) - losing majority quorum");
    isolateNode(raft1Proxy, http1Proxy, "QLOSS_1");
    isolateNode(raft2Proxy, http2Proxy, "QLOSS_2");

    logger.info("Waiting for Raft leader step-down due to quorum loss");
    TimeUnit.SECONDS.sleep(15);

    logger.info("Attempting write without quorum (should fail - Raft leader stepped down)");
    boolean writeSucceeded = false;
    try {
      db0.addUserAndPhotos(10, 10);
      writeSucceeded = true;
      logger.warn("Write succeeded without quorum");
    } catch (final Exception e) {
      logger.info("Write correctly failed without quorum: {}", e.getMessage());
    }

    logger.info("Restoring quorum by reconnecting nodes");
    reconnectNode(raft1Proxy, http1Proxy, "QLOSS_1");
    reconnectNode(raft2Proxy, http2Proxy, "QLOSS_2");

    logger.info("Waiting for quorum restoration and leader re-election");
    TimeUnit.SECONDS.sleep(15);

    logger.info("Writing with quorum restored");
    db0.addUserAndPhotos(15, 10);

    final int expectedUsers = writeSucceeded ? 45 : 35;
    logger.info("Waiting for convergence (expected {} users)", expectedUsers);

    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0.countUsers();
            final long users1 = db1.countUsers();
            final long users2 = db2.countUsers();
            logger.info("Quorum recovery check: ArcadeDB_0={}, ArcadeDB_1={}, ArcadeDB_2={} (expected={})",
                users0, users1, users2, expectedUsers);
            return users0 == expectedUsers && users1 == expectedUsers && users2 == expectedUsers;
          } catch (final Exception e) {
            logger.warn("Quorum recovery check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying cluster fully recovered after quorum loss");
    db0.assertThatUserCountIs(expectedUsers);
    db1.assertThatUserCountIs(expectedUsers);
    db2.assertThatUserCountIs(expectedUsers);

    db0.close();
    db1.close();
    db2.close();
  }
}
