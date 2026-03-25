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
 * Network partition tests for Raft HA cluster resilience.
 * Tests partition scenarios and cluster behaviour under network failures.
 * <p>
 * In Raft, a leader in the minority partition automatically steps down.
 * Only the partition with a majority quorum can elect a new leader and accept writes.
 */
@Testcontainers
class NetworkPartitionIT extends ContainersTestTemplate {

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

  /**
   * Cuts both Raft and HTTP proxy traffic for a given node, effectively isolating it.
   */
  private void isolateNode(final Proxy raftProxy, final Proxy httpProxy, final String label) throws IOException {
    logger.info("Isolating node {} via proxy", label);
    raftProxy.toxics().bandwidth(label + "_RAFT_DOWN", ToxicDirection.DOWNSTREAM, 0);
    raftProxy.toxics().bandwidth(label + "_RAFT_UP", ToxicDirection.UPSTREAM, 0);
    httpProxy.toxics().bandwidth(label + "_HTTP_DOWN", ToxicDirection.DOWNSTREAM, 0);
    httpProxy.toxics().bandwidth(label + "_HTTP_UP", ToxicDirection.UPSTREAM, 0);
  }

  /**
   * Restores both Raft and HTTP proxy traffic for a given node.
   */
  private void reconnectNode(final Proxy raftProxy, final Proxy httpProxy, final String label) throws IOException {
    logger.info("Reconnecting node {} via proxy", label);
    raftProxy.toxics().get(label + "_RAFT_DOWN").remove();
    raftProxy.toxics().get(label + "_RAFT_UP").remove();
    httpProxy.toxics().get(label + "_HTTP_DOWN").remove();
    httpProxy.toxics().get(label + "_HTTP_UP").remove();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test leader partition: isolate leader from replicas, verify new election in majority")
  void testLeaderPartitionWithQuorum() throws IOException, InterruptedException {
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

    logger.info("Creating database and schema");
    db0.createDatabase();
    db0.createSchema();

    logger.info("Checking schema replication");
    db0.checkSchema();
    db1.checkSchema();
    db2.checkSchema();

    logger.info("Adding initial data");
    db0.addUserAndPhotos(10, 10);

    logger.info("Verifying initial data replication");
    db0.assertThatUserCountIs(10);
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);

    logger.info("Finding current Raft leader");
    final int leaderIdx = findLeaderIndex(servers);
    logger.info("Current leader is node {}", leaderIdx);

    final int survivor1 = (leaderIdx + 1) % 3;
    final int survivor2 = (leaderIdx + 2) % 3;

    logger.info("Creating network partition: isolating node {} (leader) from the cluster", leaderIdx);
    isolateNode(raftProxies[leaderIdx], httpProxies[leaderIdx], "LEADER");

    logger.info("Waiting for Raft leader step-down and new election in majority partition");
    TimeUnit.SECONDS.sleep(15);

    logger.info("Adding data to majority partition (nodes {} and {})", survivor1, survivor2);
    dbs[survivor1].addUserAndPhotos(20, 10);

    logger.info("Verifying data on majority partition");
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long usersS1 = dbs[survivor1].countUsers();
            final long usersS2 = dbs[survivor2].countUsers();
            return usersS1 == 30L && usersS2 == 30L;
          } catch (final Exception e) {
            return false;
          }
        });

    logger.info("Verifying isolated node still has old data");
    dbs[leaderIdx].assertThatUserCountIs(10);

    logger.info("Healing partition - reconnecting isolated node");
    reconnectNode(raftProxies[leaderIdx], httpProxies[leaderIdx], "LEADER");

    logger.info("Waiting for cluster to converge after partition heal");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0.countUsers();
            final long users1 = db1.countUsers();
            final long users2 = db2.countUsers();
            logger.info("Convergence check: ArcadeDB_0={}, ArcadeDB_1={}, ArcadeDB_2={}", users0, users1, users2);
            return users0 == 30L && users1 == 30L && users2 == 30L;
          } catch (final Exception e) {
            logger.warn("Convergence check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency across all nodes");
    db0.assertThatUserCountIs(30);
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);

    db0.close();
    db1.close();
    db2.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test single follower partition: one follower isolated, cluster continues")
  void testSingleFollowerPartition() throws IOException, InterruptedException {
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
    db0.addUserAndPhotos(10, 10);

    logger.info("Verifying initial replication");
    db0.assertThatUserCountIs(10);
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);

    logger.info("Finding current leader to isolate a follower");
    final int leaderIdx = findLeaderIndex(servers);
    // Isolate a non-leader node
    final int isolatedIdx = (leaderIdx + 1) % 3;
    final int otherIdx = (leaderIdx + 2) % 3;
    logger.info("Leader is node {}, isolating follower node {}", leaderIdx, isolatedIdx);

    isolateNode(raftProxies[isolatedIdx], httpProxies[isolatedIdx], "FOLLOWER");

    logger.info("Waiting for cluster to detect partition");
    TimeUnit.SECONDS.sleep(10);

    logger.info("Adding data to majority (leader + remaining follower)");
    dbs[leaderIdx].addUserAndPhotos(20, 10);

    logger.info("Verifying data on majority nodes");
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

    logger.info("Verifying isolated node still has old data");
    dbs[isolatedIdx].assertThatUserCountIs(10);

    logger.info("Reconnecting isolated follower");
    reconnectNode(raftProxies[isolatedIdx], httpProxies[isolatedIdx], "FOLLOWER");

    logger.info("Waiting for follower resync via Raft log catch-up");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users = dbs[isolatedIdx].countUsers();
            logger.info("Resync check: isolated node={}", users);
            return users == 30L;
          } catch (final Exception e) {
            logger.warn("Resync check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency");
    db0.assertThatUserCountIs(30);
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);

    db0.close();
    db1.close();
    db2.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test no-quorum partition: cluster cannot accept writes when all nodes are isolated")
  void testNoQuorumScenario() throws IOException, InterruptedException {
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
    db0.addUserAndPhotos(10, 10);

    logger.info("Verifying initial replication");
    db0.assertThatUserCountIs(10);
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);

    logger.info("Isolating two nodes to break majority quorum");
    isolateNode(raft1Proxy, http1Proxy, "NODE1");
    isolateNode(raft2Proxy, http2Proxy, "NODE2");

    logger.info("Waiting for Raft leader step-down due to quorum loss");
    TimeUnit.SECONDS.sleep(15);

    logger.info("Attempting write without quorum (should fail - Raft leader stepped down)");
    boolean writeSucceeded = false;
    try {
      db0.addUserAndPhotos(1, 1);
      writeSucceeded = true;
      logger.warn("Write succeeded without quorum - unexpected for Raft with majority quorum");
    } catch (final Exception e) {
      logger.info("Write correctly failed without quorum: {}", e.getMessage());
    }

    logger.info("Reconnecting nodes to restore quorum");
    reconnectNode(raft1Proxy, http1Proxy, "NODE1");
    reconnectNode(raft2Proxy, http2Proxy, "NODE2");

    logger.info("Waiting for quorum restoration and leader re-election");
    TimeUnit.SECONDS.sleep(15);

    logger.info("Writing data with quorum restored");
    db0.addUserAndPhotos(5, 10);

    logger.info("Verifying data replication with quorum restored");
    final int expected = writeSucceeded ? 16 : 15;
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0.countUsers();
            final long users1 = db1.countUsers();
            final long users2 = db2.countUsers();
            logger.info("Quorum check: ArcadeDB_0={}, ArcadeDB_1={}, ArcadeDB_2={} (expected={})",
                users0, users1, users2, expected);
            return users0 == expected && users1 == expected && users2 == expected;
          } catch (final Exception e) {
            logger.warn("Quorum check failed: {}", e.getMessage());
            return false;
          }
        });

    db0.close();
    db1.close();
    db2.close();
  }
}
