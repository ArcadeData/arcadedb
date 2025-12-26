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
package com.arcadedb.containers.resilience;

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
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Network partition recovery and conflict resolution tests for HA cluster resilience.
 * Tests split-brain scenarios and data convergence after partition healing.
 */
@Testcontainers
public class NetworkPartitionRecoveryIT extends ContainersTestTemplate {

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test partition recovery: 2+1 split, heal partition, verify data convergence")
  void testPartitionRecovery() throws IOException, InterruptedException {
    logger.info("Creating proxies for 3-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3-node HA cluster with majority quorum");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "majority", "any", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "majority", "any", network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "majority", "any", network);

    logger.info("Starting cluster");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier);

    logger.info("Creating database and initial data");
    db1.createDatabase();
    db1.createSchema();
    db1.addUserAndPhotos(20, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(20);
    db2.assertThatUserCountIs(20);
    db3.assertThatUserCountIs(20);

    logger.info("Creating network partition: isolating arcade3 from arcade1 and arcade2");
    arcade3Proxy.toxics().bandwidth("PARTITION_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0);
    arcade3Proxy.toxics().bandwidth("PARTITION_UPSTREAM", ToxicDirection.UPSTREAM, 0);

    logger.info("Waiting for partition to be detected");
    TimeUnit.SECONDS.sleep(5);

    logger.info("Writing to majority partition (arcade1/arcade2)");
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying writes on majority partition");
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);

    logger.info("Verifying minority partition (arcade3) still has old data");
    db3.assertThatUserCountIs(20);

    logger.info("Healing partition - reconnecting arcade3");
    arcade3Proxy.toxics().get("PARTITION_DOWNSTREAM").remove();
    arcade3Proxy.toxics().get("PARTITION_UPSTREAM").remove();

    logger.info("Waiting for partition recovery and data convergence");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            Long users3 = db3.countUsers();
            logger.info("Recovery check: arcade1={}, arcade2={}, arcade3={}", users1, users2, users3);
            return users1.equals(30L) && users2.equals(30L) && users3.equals(30L);
          } catch (Exception e) {
            logger.warn("Recovery check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final data consistency across all nodes");
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);
    db3.assertThatUserCountIs(30);

    db1.close();
    db2.close();
    db3.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test conflict resolution: write to both sides of partition, verify convergence")
  void testConflictResolution() throws IOException, InterruptedException {
    logger.info("Creating proxies for 3-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3-node HA cluster with NONE quorum (allows concurrent writes)");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "none", "any", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "none", "any", network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "none", "any", network);

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

    logger.info("Creating full partition: arcade1 | arcade2 | arcade3");
    // Isolate each node from the others
    arcade1Proxy.toxics().bandwidth("ISOLATE_1_DOWN", ToxicDirection.DOWNSTREAM, 0);
    arcade1Proxy.toxics().bandwidth("ISOLATE_1_UP", ToxicDirection.UPSTREAM, 0);
    arcade2Proxy.toxics().bandwidth("ISOLATE_2_DOWN", ToxicDirection.DOWNSTREAM, 0);
    arcade2Proxy.toxics().bandwidth("ISOLATE_2_UP", ToxicDirection.UPSTREAM, 0);
    arcade3Proxy.toxics().bandwidth("ISOLATE_3_DOWN", ToxicDirection.DOWNSTREAM, 0);
    arcade3Proxy.toxics().bandwidth("ISOLATE_3_UP", ToxicDirection.UPSTREAM, 0);

    logger.info("Waiting for partitions to be detected");
    TimeUnit.SECONDS.sleep(5);

    logger.info("Writing different data to each partition");
    db1.addUserAndPhotos(5, 10);  // arcade1: 10 + 5 = 15 users
    db2.addUserAndPhotos(3, 10);  // arcade2: 10 + 3 = 13 users
    db3.addUserAndPhotos(7, 10);  // arcade3: 10 + 7 = 17 users

    logger.info("Verifying each partition has its own data");
    db1.assertThatUserCountIs(15);
    db2.assertThatUserCountIs(13);
    db3.assertThatUserCountIs(17);

    logger.info("Healing all partitions");
    arcade1Proxy.toxics().get("ISOLATE_1_DOWN").remove();
    arcade1Proxy.toxics().get("ISOLATE_1_UP").remove();
    arcade2Proxy.toxics().get("ISOLATE_2_DOWN").remove();
    arcade2Proxy.toxics().get("ISOLATE_2_UP").remove();
    arcade3Proxy.toxics().get("ISOLATE_3_DOWN").remove();
    arcade3Proxy.toxics().get("ISOLATE_3_UP").remove();

    logger.info("Waiting for conflict resolution and convergence");
    // Total unique users should be 10 (initial) + 5 + 3 + 7 = 25
    Awaitility.await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            Long users3 = db3.countUsers();
            logger.info("Convergence check: arcade1={}, arcade2={}, arcade3={}", users1, users2, users3);
            // All nodes should converge to the same count
            return users1.equals(users2) && users2.equals(users3) && users1 >= 10L;
          } catch (Exception e) {
            logger.warn("Convergence check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency (all nodes have same data after conflict resolution)");
    Long finalCount = db1.countUsers();
    db2.assertThatUserCountIs(finalCount.intValue());
    db3.assertThatUserCountIs(finalCount.intValue());

    logger.info("Final converged user count: {}", finalCount);

    db1.close();
    db2.close();
    db3.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test multiple partition cycles: repeated split and heal")
  void testMultiplePartitionCycles() throws IOException, InterruptedException {
    logger.info("Creating proxies for 3-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3-node HA cluster");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "majority", "any", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "majority", "any", network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "majority", "any", network);

    logger.info("Starting cluster");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier);

    logger.info("Creating database and initial data");
    db1.createDatabase();
    db1.createSchema();
    db1.addUserAndPhotos(10, 10);

    int expectedUsers = 10;

    // Run 3 partition cycles
    for (int cycle = 1; cycle <= 3; cycle++) {
      logger.info("=== Partition Cycle {} ===", cycle);

      logger.info("Cycle {}: Creating partition (isolating arcade3)", cycle);
      arcade3Proxy.toxics().bandwidth("CYCLE_PARTITION_DOWN", ToxicDirection.DOWNSTREAM, 0);
      arcade3Proxy.toxics().bandwidth("CYCLE_PARTITION_UP", ToxicDirection.UPSTREAM, 0);

      TimeUnit.SECONDS.sleep(3);

      logger.info("Cycle {}: Writing to majority partition", cycle);
      db1.addUserAndPhotos(5, 10);
      expectedUsers += 5;

      logger.info("Cycle {}: Healing partition", cycle);
      arcade3Proxy.toxics().get("CYCLE_PARTITION_DOWN").remove();
      arcade3Proxy.toxics().get("CYCLE_PARTITION_UP").remove();

      logger.info("Cycle {}: Waiting for convergence", cycle);
      final int currentCycle = cycle;
      final int expected = expectedUsers;
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(3, TimeUnit.SECONDS)
          .until(() -> {
            try {
              Long users3 = db3.countUsers();
              logger.info("Cycle {}: Convergence check: arcade3={} (expected={})", currentCycle, users3, expected);
              return users3.equals((long) expected);
            } catch (Exception e) {
              logger.warn("Cycle {}: Convergence check failed: {}", currentCycle, e.getMessage());
              return false;
            }
          });

      logger.info("Cycle {}: Complete - all nodes at {} users", cycle, expectedUsers);
    }

    logger.info("Verifying final consistency after {} cycles", 3);
    db1.assertThatUserCountIs(expectedUsers);
    db2.assertThatUserCountIs(expectedUsers);
    db3.assertThatUserCountIs(expectedUsers);

    db1.close();
    db2.close();
    db3.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test asymmetric partition recovery: different partition patterns")
  void testAsymmetricPartitionRecovery() throws IOException, InterruptedException {
    logger.info("Creating proxies for 3-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3-node HA cluster");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "majority", "any", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "majority", "any", network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "majority", "any", network);

    logger.info("Starting cluster");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier);

    logger.info("Creating database and initial data");
    db1.createDatabase();
    db1.createSchema();
    db1.addUserAndPhotos(10, 10);

    logger.info("Creating asymmetric partition: arcade1 can talk to arcade2, but arcade3 is isolated from both");
    arcade3Proxy.toxics().bandwidth("ASYM_PARTITION_DOWN", ToxicDirection.DOWNSTREAM, 0);
    arcade3Proxy.toxics().bandwidth("ASYM_PARTITION_UP", ToxicDirection.UPSTREAM, 0);

    TimeUnit.SECONDS.sleep(5);

    logger.info("Writing to connected pair (arcade1/arcade2)");
    db1.addUserAndPhotos(15, 10);

    logger.info("Verifying connected nodes have new data");
    db1.assertThatUserCountIs(25);
    db2.assertThatUserCountIs(25);

    logger.info("Verifying isolated node has old data");
    db3.assertThatUserCountIs(10);

    logger.info("Healing asymmetric partition");
    arcade3Proxy.toxics().get("ASYM_PARTITION_DOWN").remove();
    arcade3Proxy.toxics().get("ASYM_PARTITION_UP").remove();

    logger.info("Waiting for full convergence");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            Long users3 = db3.countUsers();
            logger.info("Asymmetric recovery check: arcade1={}, arcade2={}, arcade3={}", users1, users2, users3);
            return users1.equals(25L) && users2.equals(25L) && users3.equals(25L);
          } catch (Exception e) {
            logger.warn("Asymmetric recovery check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency");
    db1.assertThatUserCountIs(25);
    db2.assertThatUserCountIs(25);
    db3.assertThatUserCountIs(25);

    db1.close();
    db2.close();
    db3.close();
  }
}
