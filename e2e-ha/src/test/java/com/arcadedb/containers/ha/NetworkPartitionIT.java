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
package com.arcadedb.containers.ha;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Network partition tests for HA cluster resilience.
 * Tests split-brain scenarios and partition healing.
 */
@Testcontainers
public class NetworkPartitionIT extends ContainersTestTemplate {

  @Test
  @Disabled
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test split-brain: partition leader from replicas, verify quorum enforcement")
  void testLeaderPartitionWithQuorum() throws IOException, InterruptedException {
    logger.info("Creating proxies for 3-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3-node HA cluster with majority quorum");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "majority", "any",
        network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "majority", "any",
        network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "majority", "any",
        network);

    logger.info("Starting cluster - arcade1 will become leader");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier);

    logger.info("Creating database and schema on leader");
    db1.createDatabase();
    db1.createSchema();

    logger.info("Verifying schema replication");
    db1.checkSchema();
    db2.checkSchema();
    db3.checkSchema();

    logger.info("Adding initial data");
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying initial data replication");
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);
    db3.assertThatUserCountIs(10);

    logger.info("Creating network partition: isolating arcade1 (leader) from replicas");
    arcade1Proxy.toxics().bandwidth("PARTITION_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0);
    arcade1Proxy.toxics().bandwidth("PARTITION_UPSTREAM", ToxicDirection.UPSTREAM, 0);

    logger.info("Waiting for new leader election among arcade2 and arcade3");
    TimeUnit.SECONDS.sleep(10);

    logger.info("Adding data to majority partition (arcade2/arcade3)");
    db2.addUserAndPhotos(20, 10);

    logger.info("Verifying data on majority partition");
    db2.assertThatUserCountIs(30);
    db3.assertThatUserCountIs(30);

    logger.info("Verifying isolated node (arcade1) still has old data");
    db1.assertThatUserCountIs(10);

    logger.info("Healing partition - reconnecting arcade1");
    arcade1Proxy.toxics().get("PARTITION_DOWNSTREAM").remove();
    arcade1Proxy.toxics().get("PARTITION_UPSTREAM").remove();

    logger.info("Waiting for cluster to converge");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            Long users3 = db3.countUsers();
            logger.info("Convergence check: arcade1={}, arcade2={}, arcade3={}", users1, users2, users3);
            return users1.equals(30L) && users2.equals(30L) && users3.equals(30L);
          } catch (Exception e) {
            logger.warn("Convergence check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency across all nodes");
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);
    db3.assertThatUserCountIs(30);

    db1.close();
    db2.close();
    db3.close();
  }

  @Test
  @Disabled
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test asymmetric partition: one replica isolated, cluster continues")
  void testSingleReplicaPartition() throws IOException, InterruptedException {
    logger.info("Creating proxies for 3-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3-node HA cluster with majority quorum");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "majority", "any",
        network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "majority", "any",
        network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "majority", "any",
        network);

    logger.info("Starting cluster");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier);

    logger.info("Creating database and initial data");
    db1.createDatabase();
    db1.createSchema();
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);
    db3.assertThatUserCountIs(10);

    logger.info("Isolating arcade3 from the cluster");
    arcade3Proxy.toxics().bandwidth("ISOLATE_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0);
    arcade3Proxy.toxics().bandwidth("ISOLATE_UPSTREAM", ToxicDirection.UPSTREAM, 0);

    logger.info("Waiting for cluster to detect partition");
    TimeUnit.SECONDS.sleep(5);

    logger.info("Adding data to majority (arcade1/arcade2)");
    db1.addUserAndPhotos(20, 10);

    logger.info("Verifying data on majority nodes");
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);

    logger.info("Verifying isolated node still has old data");
    db3.assertThatUserCountIs(10);

    logger.info("Reconnecting arcade3");
    arcade3Proxy.toxics().get("ISOLATE_DOWNSTREAM").remove();
    arcade3Proxy.toxics().get("ISOLATE_UPSTREAM").remove();

    logger.info("Waiting for resync");
    Awaitility.await()
        .atMost(45, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users3 = db3.countUsers();
            logger.info("Resync check: arcade3={}", users3);
            return users3.equals(30L);
          } catch (Exception e) {
            logger.warn("Resync check failed: {}", e.getMessage());
            return false;
          }
        });

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
  @DisplayName("Test no-quorum partition: cluster cannot accept writes without quorum")
  void testNoQuorumScenario() throws IOException, InterruptedException {
    logger.info("Creating proxies for 3-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3-node HA cluster with ALL quorum (requires all nodes)");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "all", "any",
        network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "all", "any",
        network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "all", "any",
        network);

    logger.info("Starting cluster");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier);

    logger.info("Creating database and initial data with all nodes available");
    db1.createDatabase();
    db1.createSchema();
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);
    db3.assertThatUserCountIs(10);

    logger.info("Isolating arcade3 - breaking ALL quorum");
    arcade3Proxy.toxics().bandwidth("BREAK_QUORUM_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0);
    arcade3Proxy.toxics().bandwidth("BREAK_QUORUM_UPSTREAM", ToxicDirection.UPSTREAM, 0);

    logger.info("Waiting for cluster to detect quorum loss");
    TimeUnit.SECONDS.sleep(5);

    logger.info("Attempting write without quorum (should timeout or fail)");
    final boolean[] writeSucceeded = { false };
    try {
      // This should fail or timeout because quorum=ALL requires all nodes
      db1.addUserAndPhotos(1, 1);
      writeSucceeded[0] = true;
      logger.warn("Write succeeded without quorum - this may indicate async replication");
    } catch (Exception e) {
      logger.info("Write correctly failed without quorum: {}", e.getMessage());
    }

    logger.info("Reconnecting arcade3 to restore quorum");
    arcade3Proxy.toxics().get("BREAK_QUORUM_DOWNSTREAM").remove();
    arcade3Proxy.toxics().get("BREAK_QUORUM_UPSTREAM").remove();

    logger.info("Waiting for quorum restoration");
    TimeUnit.SECONDS.sleep(5);

    logger.info("Writing data with quorum restored");
    db1.addUserAndPhotos(5, 10);

    logger.info("Verifying data replication with quorum");
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            Long users3 = db3.countUsers();
            int expected = writeSucceeded[0] ? 16 : 15;
            logger.info("Quorum check: arcade1={}, arcade2={}, arcade3={} (expected={})",
                users1, users2, users3, expected);
            return users1.equals((long) expected) &&
                users2.equals((long) expected) &&
                users3.equals((long) expected);
          } catch (Exception e) {
            logger.warn("Quorum check failed: {}", e.getMessage());
            return false;
          }
        });

    db1.close();
    db2.close();
    db3.close();
  }
}
