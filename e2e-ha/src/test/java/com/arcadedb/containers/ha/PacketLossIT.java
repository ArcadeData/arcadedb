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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Packet loss tests for Raft HA cluster resilience.
 * Tests behavior under unreliable networks with dropped packets.
 * Toxiproxy intercepts the Raft gRPC consensus port (2434) to inject faults.
 */
@Testcontainers
public class PacketLossIT extends ContainersTestTemplate {

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
  @DisplayName("Test low packet loss (5%): cluster should remain stable")
  void testLowPacketLoss() throws IOException {
    logger.info("Creating Raft and HTTP proxies for 2-node cluster");
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "arcadedb-0:2434");
    final Proxy raftProxy1 = toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "arcadedb-1:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "arcadedb-0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "arcadedb-1:2480");

    logger.info("Creating 2-node Raft HA cluster");
    createArcadeContainer("arcadedb-0", SERVER_LIST_2, "none", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST_2, "none", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);

    logger.info("Creating database and schema");
    db1.createDatabase();
    db1.createSchema();

    logger.info("Adding initial data");
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);

    logger.info("Introducing 5% packet loss (simulating minor network issues)");
    raftProxy0.toxics().limitData("packet_loss_raft0", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.05f);
    raftProxy1.toxics().limitData("packet_loss_raft1", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.05f);

    logger.info("Adding data under 5% packet loss");
    db1.addUserAndPhotos(20, 10);

    logger.info("Waiting for replication with packet loss");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final Long users1 = db1.countUsers();
            final Long users2 = db2.countUsers();
            logger.info("Low packet loss check: node0={}, node1={}", users1, users2);
            return users1.equals(30L) && users2.equals(30L);
          } catch (final Exception e) {
            logger.warn("Low packet loss check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing packet loss");
    raftProxy0.toxics().get("packet_loss_raft0").remove();
    raftProxy1.toxics().get("packet_loss_raft1").remove();

    logger.info("Verifying final consistency");
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);

    db1.close();
    db2.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test moderate packet loss (20%): replication should succeed with retries")
  void testModeratePacketLoss() throws IOException {
    logger.info("Creating Raft and HTTP proxies for 2-node cluster");
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "arcadedb-0:2434");
    final Proxy raftProxy1 = toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "arcadedb-1:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "arcadedb-0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "arcadedb-1:2480");

    logger.info("Creating 2-node Raft HA cluster");
    createArcadeContainer("arcadedb-0", SERVER_LIST_2, "none", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST_2, "none", network);

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

    logger.info("Introducing 20% packet loss (simulating unreliable network)");
    raftProxy0.toxics().limitData("moderate_loss_raft0", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.20f);
    raftProxy1.toxics().limitData("moderate_loss_raft1", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.20f);

    logger.info("Adding data under 20% packet loss");
    db1.addUserAndPhotos(15, 10);

    logger.info("Waiting for replication with moderate packet loss (may take longer due to retries)");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final Long users1 = db1.countUsers();
            final Long users2 = db2.countUsers();
            logger.info("Moderate packet loss check: node0={}, node1={}", users1, users2);
            return users1.equals(25L) && users2.equals(25L);
          } catch (final Exception e) {
            logger.warn("Moderate packet loss check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing packet loss");
    raftProxy0.toxics().get("moderate_loss_raft0").remove();
    raftProxy1.toxics().get("moderate_loss_raft1").remove();

    logger.info("Verifying final consistency");
    db1.assertThatUserCountIs(25);
    db2.assertThatUserCountIs(25);

    db1.close();
    db2.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test high packet loss (50%): verify connection resilience")
  void testHighPacketLoss() throws IOException {
    logger.info("Creating Raft and HTTP proxies for 2-node cluster");
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "arcadedb-0:2434");
    final Proxy raftProxy1 = toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "arcadedb-1:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "arcadedb-0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "arcadedb-1:2480");

    logger.info("Creating 2-node Raft HA cluster");
    createArcadeContainer("arcadedb-0", SERVER_LIST_2, "none", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST_2, "none", network);

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

    logger.info("Introducing 50% packet loss (severe network degradation)");
    raftProxy0.toxics().limitData("high_loss_raft0", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.50f);
    raftProxy1.toxics().limitData("high_loss_raft1", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.50f);

    logger.info("Adding data under 50% packet loss (some writes may fail)");
    db1.addUserAndPhotos(10, 10);

    final long committedOnLeader = db1.countUsers();
    logger.info("Users committed on leader after packet loss writes: {}", committedOnLeader);

    logger.info("Waiting for node1 to replicate whatever the leader committed");
    Awaitility.await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users1 = db1.countUsers();
            final long users2 = db2.countUsers();
            logger.info("High packet loss check: node0={}, node1={}", users1, users2);
            return users1 >= committedOnLeader && users2 >= committedOnLeader;
          } catch (final Exception e) {
            logger.warn("High packet loss check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing packet loss");
    raftProxy0.toxics().get("high_loss_raft0").remove();
    raftProxy1.toxics().get("high_loss_raft1").remove();

    logger.info("Verifying final consistency");
    final long finalCount = db1.countUsers();
    assertThat(db2.countUsers()).isEqualTo(finalCount);

    db1.close();
    db2.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test directional packet loss: loss only in one direction")
  void testDirectionalPacketLoss() throws IOException {
    logger.info("Creating Raft and HTTP proxies for 3-node cluster");
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "arcadedb-0:2434");
    toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "arcadedb-1:2434");
    toxiproxyClient.createProxy("raftProxy2", "0.0.0.0:" + RAFT_PROXY_PORT_2, "arcadedb-2:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "arcadedb-0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "arcadedb-1:2480");
    toxiproxyClient.createProxy("httpProxy2", "0.0.0.0:" + HTTP_PROXY_PORT_2, "arcadedb-2:2480");

    logger.info("Creating 3-node Raft HA cluster");
    createArcadeContainer("arcadedb-0", SERVER_LIST_3, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST_3, "majority", network);
    createArcadeContainer("arcadedb-2", SERVER_LIST_3, "majority", network);

    logger.info("Starting cluster");
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

    logger.info("Introducing 30% packet loss DOWNSTREAM only on arcadedb-0 Raft proxy");
    raftProxy0.toxics().limitData("directional_loss", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.30f);

    logger.info("Adding data from arcadedb-1 (should replicate despite one-way loss)");
    db2.addUserAndPhotos(15, 10);

    logger.info("Waiting for replication with directional packet loss");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final Long users1 = db1.countUsers();
            final Long users2 = db2.countUsers();
            final Long users3 = db3.countUsers();
            logger.info("Directional loss check: node0={}, node1={}, node2={}", users1, users2, users3);
            return users1.equals(25L) && users2.equals(25L) && users3.equals(25L);
          } catch (final Exception e) {
            logger.warn("Directional loss check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing directional packet loss");
    raftProxy0.toxics().get("directional_loss").remove();

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
  @DisplayName("Test intermittent packet loss: verify recovery from transient issues")
  void testIntermittentPacketLoss() throws IOException, InterruptedException {
    logger.info("Creating Raft and HTTP proxies for 2-node cluster");
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "arcadedb-0:2434");
    toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "arcadedb-1:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "arcadedb-0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "arcadedb-1:2480");

    logger.info("Creating 2-node Raft HA cluster");
    createArcadeContainer("arcadedb-0", SERVER_LIST_2, "none", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST_2, "none", network);

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

    logger.info("Applying intermittent packet loss (3 cycles)");
    for (int cycle = 1; cycle <= 3; cycle++) {
      logger.info("Cycle {}: Introducing 25% packet loss on Raft connection", cycle);
      raftProxy0.toxics().limitData("intermittent_loss", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.25f);

      logger.info("Cycle {}: Adding data during packet loss", cycle);
      db1.addUserAndPhotos(5, 10);

      logger.info("Cycle {}: Removing packet loss", cycle);
      TimeUnit.SECONDS.sleep(2);
      raftProxy0.toxics().get("intermittent_loss").remove();

      logger.info("Cycle {}: Waiting for recovery", cycle);
      TimeUnit.SECONDS.sleep(3);
    }

    logger.info("Waiting for final convergence after intermittent issues");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final Long users1 = db1.countUsers();
            final Long users2 = db2.countUsers();
            logger.info("Intermittent loss recovery check: node0={}, node1={}", users1, users2);
            return users1.equals(25L) && users2.equals(25L);
          } catch (final Exception e) {
            logger.warn("Intermittent loss recovery check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency");
    db1.assertThatUserCountIs(25);
    db2.assertThatUserCountIs(25);

    db1.close();
    db2.close();
  }
}
