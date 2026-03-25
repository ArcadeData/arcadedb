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

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Network latency and delay tests for Raft HA cluster resilience.
 * Tests behavior under high latency, jitter, and asymmetric delays.
 * Toxiproxy intercepts the Raft gRPC consensus port (2434) to inject faults.
 */
@Testcontainers
public class NetworkDelayIT extends ContainersTestTemplate {

  // Proxy ports for Raft (consensus) traffic per node
  private static final int RAFT_PROXY_PORT_0 = 8660;
  private static final int RAFT_PROXY_PORT_1 = 8661;
  private static final int RAFT_PROXY_PORT_2 = 8662;

  // Proxy ports for HTTP (command forwarding) traffic per node
  private static final int HTTP_PROXY_PORT_0 = 8670;
  private static final int HTTP_PROXY_PORT_1 = 8671;
  private static final int HTTP_PROXY_PORT_2 = 8672;

  private static final String SERVER_LIST_3 =
      "proxy:" + RAFT_PROXY_PORT_0 + ":" + HTTP_PROXY_PORT_0 + ","
          + "proxy:" + RAFT_PROXY_PORT_1 + ":" + HTTP_PROXY_PORT_1 + ","
          + "proxy:" + RAFT_PROXY_PORT_2 + ":" + HTTP_PROXY_PORT_2;

  private static final String SERVER_LIST_2 =
      "proxy:" + RAFT_PROXY_PORT_0 + ":" + HTTP_PROXY_PORT_0 + ","
          + "proxy:" + RAFT_PROXY_PORT_1 + ":" + HTTP_PROXY_PORT_1;

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test symmetric network delay: all nodes experience same latency")
  void testSymmetricDelay() throws IOException {
    logger.info("Creating Raft and HTTP proxies for 3-node cluster");
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "ArcadeDB_0:2434");
    final Proxy raftProxy1 = toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "ArcadeDB_1:2434");
    final Proxy raftProxy2 = toxiproxyClient.createProxy("raftProxy2", "0.0.0.0:" + RAFT_PROXY_PORT_2, "ArcadeDB_2:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "ArcadeDB_0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "ArcadeDB_1:2480");
    toxiproxyClient.createProxy("httpProxy2", "0.0.0.0:" + HTTP_PROXY_PORT_2, "ArcadeDB_2:2480");

    logger.info("Creating 3-node Raft HA cluster");
    createArcadeContainer("ArcadeDB_0", SERVER_LIST_3, "majority", network);
    createArcadeContainer("ArcadeDB_1", SERVER_LIST_3, "majority", network);
    createArcadeContainer("ArcadeDB_2", SERVER_LIST_3, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    logger.info("Creating database and schema");
    db1.createDatabase();
    db1.createSchema();

    logger.info("Adding initial data with no delay");
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);
    db3.assertThatUserCountIs(10);

    logger.info("Introducing 200ms symmetric latency on all Raft connections");
    raftProxy0.toxics().latency("latency_raft0", ToxicDirection.DOWNSTREAM, 200);
    raftProxy1.toxics().latency("latency_raft1", ToxicDirection.DOWNSTREAM, 200);
    raftProxy2.toxics().latency("latency_raft2", ToxicDirection.DOWNSTREAM, 200);

    logger.info("Adding data under latency conditions");
    final long startTime = System.currentTimeMillis();
    db1.addUserAndPhotos(20, 10);
    final long duration = System.currentTimeMillis() - startTime;
    logger.info("Write operation took {}ms under 200ms latency", duration);

    logger.info("Waiting for replication with latency");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final Long users1 = db1.countUsers();
            final Long users2 = db2.countUsers();
            final Long users3 = db3.countUsers();
            logger.info("Latency replication check: node0={}, node1={}, node2={}", users1, users2, users3);
            return users1.equals(30L) && users2.equals(30L) && users3.equals(30L);
          } catch (final Exception e) {
            logger.warn("Latency check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing latency");
    raftProxy0.toxics().get("latency_raft0").remove();
    raftProxy1.toxics().get("latency_raft1").remove();
    raftProxy2.toxics().get("latency_raft2").remove();

    logger.info("Verifying final consistency");
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);
    db3.assertThatUserCountIs(30);

    db1.close();
    db2.close();
    db3.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test asymmetric delay: leader has higher latency than followers")
  void testAsymmetricLeaderDelay() throws IOException, InterruptedException {
    logger.info("Creating Raft and HTTP proxies for 3-node cluster");
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "ArcadeDB_0:2434");
    toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "ArcadeDB_1:2434");
    toxiproxyClient.createProxy("raftProxy2", "0.0.0.0:" + RAFT_PROXY_PORT_2, "ArcadeDB_2:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "ArcadeDB_0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "ArcadeDB_1:2480");
    toxiproxyClient.createProxy("httpProxy2", "0.0.0.0:" + HTTP_PROXY_PORT_2, "ArcadeDB_2:2480");

    logger.info("Creating 3-node Raft HA cluster");
    createArcadeContainer("ArcadeDB_0", SERVER_LIST_3, "majority", network);
    createArcadeContainer("ArcadeDB_1", SERVER_LIST_3, "majority", network);
    createArcadeContainer("ArcadeDB_2", SERVER_LIST_3, "majority", network);

    logger.info("Starting cluster - ArcadeDB_0 is the preferred leader");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    logger.info("Creating database and initial data");
    db1.createDatabase();
    db1.createSchema();
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);
    db3.assertThatUserCountIs(10);

    logger.info("Introducing high latency (500ms) on ArcadeDB_0 Raft proxy (likely leader)");
    raftProxy0.toxics().latency("leader_latency", ToxicDirection.DOWNSTREAM, 500);
    raftProxy0.toxics().latency("leader_latency_up", ToxicDirection.UPSTREAM, 500);

    logger.info("Waiting for cluster to potentially adjust");
    TimeUnit.SECONDS.sleep(5);

    logger.info("Adding data from a follower under leader latency");
    db2.addUserAndPhotos(15, 10);

    logger.info("Waiting for replication despite leader latency");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final Long users1 = db1.countUsers();
            final Long users2 = db2.countUsers();
            final Long users3 = db3.countUsers();
            logger.info("Asymmetric latency check: node0={}, node1={}, node2={}", users1, users2, users3);
            return users1.equals(25L) && users2.equals(25L) && users3.equals(25L);
          } catch (final Exception e) {
            logger.warn("Asymmetric latency check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing leader latency");
    raftProxy0.toxics().get("leader_latency").remove();
    raftProxy0.toxics().get("leader_latency_up").remove();

    logger.info("Verifying final consistency");
    db1.assertThatUserCountIs(25);
    db2.assertThatUserCountIs(25);
    db3.assertThatUserCountIs(25);

    db1.close();
    db2.close();
    db3.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test high latency with jitter: variable delays simulate unstable network")
  void testHighLatencyWithJitter() throws IOException {
    logger.info("Creating Raft and HTTP proxies for 2-node cluster");
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "ArcadeDB_0:2434");
    final Proxy raftProxy1 = toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "ArcadeDB_1:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "ArcadeDB_0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "ArcadeDB_1:2480");

    logger.info("Creating 2-node Raft HA cluster");
    createArcadeContainer("ArcadeDB_0", SERVER_LIST_2, "none", network);
    createArcadeContainer("ArcadeDB_1", SERVER_LIST_2, "none", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);

    logger.info("Creating database and initial data");
    db1.createDatabase();
    db1.createSchema();
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);

    logger.info("Introducing 300ms latency with 150ms jitter on Raft connections");
    raftProxy0.toxics().latency("jitter_raft0", ToxicDirection.DOWNSTREAM, 300).setJitter(150);
    raftProxy1.toxics().latency("jitter_raft1", ToxicDirection.DOWNSTREAM, 300).setJitter(150);

    logger.info("Adding data under jittery network conditions");
    db1.addUserAndPhotos(20, 10);

    logger.info("Waiting for replication with jitter");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final Long users1 = db1.countUsers();
            final Long users2 = db2.countUsers();
            logger.info("Jitter check: node0={}, node1={}", users1, users2);
            return users1.equals(30L) && users2.equals(30L);
          } catch (final Exception e) {
            logger.warn("Jitter check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing jitter");
    raftProxy0.toxics().get("jitter_raft0").remove();
    raftProxy1.toxics().get("jitter_raft1").remove();

    logger.info("Verifying final consistency");
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);

    db1.close();
    db2.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test extreme latency: verify timeout handling")
  void testExtremeLatency() throws IOException {
    logger.info("Creating Raft and HTTP proxies for 2-node cluster");
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "ArcadeDB_0:2434");
    toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "ArcadeDB_1:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "ArcadeDB_0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "ArcadeDB_1:2480");

    logger.info("Creating 2-node Raft HA cluster with quorum=none for testing");
    createArcadeContainer("ArcadeDB_0", SERVER_LIST_2, "none", network);
    createArcadeContainer("ArcadeDB_1", SERVER_LIST_2, "none", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);

    logger.info("Creating database and initial data");
    db1.createDatabase();
    db1.createSchema();
    db1.addUserAndPhotos(5, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(5);
    db2.assertThatUserCountIs(5);

    logger.info("Introducing extreme latency (2000ms) on Raft connection");
    raftProxy0.toxics().latency("extreme_latency", ToxicDirection.DOWNSTREAM, 2000);

    logger.info("Adding data under extreme latency (should complete but be very slow)");
    final long startTime = System.currentTimeMillis();
    db1.addUserAndPhotos(3, 5);
    final long duration = System.currentTimeMillis() - startTime;
    logger.info("Write with extreme latency took {}ms", duration);

    logger.info("Waiting for eventual replication");
    Awaitility.await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final Long users2 = db2.countUsers();
            logger.info("Extreme latency replication check: node1={}", users2);
            return users2.equals(8L);
          } catch (final Exception e) {
            logger.warn("Extreme latency check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing extreme latency");
    raftProxy0.toxics().get("extreme_latency").remove();

    logger.info("Verifying final consistency");
    db1.assertThatUserCountIs(8);
    db2.assertThatUserCountIs(8);

    db1.close();
    db2.close();
  }
}
