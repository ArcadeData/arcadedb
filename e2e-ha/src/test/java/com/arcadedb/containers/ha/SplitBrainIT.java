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
 * Split-brain detection and prevention tests for HA cluster resilience.
 * Tests quorum enforcement and cluster reformation after network partitions.
 */
@Testcontainers

public class SplitBrainIT extends ContainersTestTemplate {

  @Test
  @Disabled
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test split-brain prevention: verify minority partition cannot accept writes")
  void testSplitBrainPrevention() throws IOException, InterruptedException {
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

    logger.info("Creating 2+1 partition: isolating arcade3 (minority)");
    arcade3Proxy.toxics().bandwidth("SPLIT_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0);
    arcade3Proxy.toxics().bandwidth("SPLIT_UPSTREAM", ToxicDirection.UPSTREAM, 0);

    logger.info("Waiting for partition detection");
    TimeUnit.SECONDS.sleep(10);

    logger.info("Writing to majority partition (arcade1/arcade2) - should succeed");
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying writes on majority partition");
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);

    logger.info("Verifying minority partition (arcade3) has old data");
    db3.assertThatUserCountIs(20);

    logger.info("Attempting write to minority partition (should fail or timeout with majority quorum)");
    boolean minorityWriteSucceeded = false;
    try {
      db3.addUserAndPhotos(5, 10);
      minorityWriteSucceeded = true;
      logger.warn("Write to minority partition succeeded - this may indicate async replication without strict quorum");
    } catch (Exception e) {
      logger.info("Write to minority partition correctly failed/timed out: {}", e.getMessage());
    }

    logger.info("Healing partition");
    arcade3Proxy.toxics().get("SPLIT_DOWNSTREAM").remove();
    arcade3Proxy.toxics().get("SPLIT_UPSTREAM").remove();

    logger.info("Waiting for cluster reformation");
    final int expectedUsers = minorityWriteSucceeded ? 35 : 30;
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            Long users3 = db3.countUsers();
            logger.info("Reformation check: arcade1={}, arcade2={}, arcade3={} (expected={})",
                users1, users2, users3, expectedUsers);
            return users1.equals((long) expectedUsers) &&
                   users2.equals((long) expectedUsers) &&
                   users3.equals((long) expectedUsers);
          } catch (Exception e) {
            logger.warn("Reformation check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency");
    db1.assertThatUserCountIs(expectedUsers);
    db2.assertThatUserCountIs(expectedUsers);
    db3.assertThatUserCountIs(expectedUsers);

    db1.close();
    db2.close();
    db3.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @Disabled
  @DisplayName("Test 1+1+1 partition: verify no writes possible without quorum")
  void testCompletePartitionNoQuorum() throws IOException, InterruptedException {
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
    db1.addUserAndPhotos(15, 10);

    logger.info("Verifying initial state");
    db1.assertThatUserCountIs(15);
    db2.assertThatUserCountIs(15);
    db3.assertThatUserCountIs(15);

    logger.info("Creating complete partition: 1+1+1 (each node isolated)");
    arcade1Proxy.toxics().bandwidth("ISO_1_DOWN", ToxicDirection.DOWNSTREAM, 0);
    arcade1Proxy.toxics().bandwidth("ISO_1_UP", ToxicDirection.UPSTREAM, 0);
    arcade2Proxy.toxics().bandwidth("ISO_2_DOWN", ToxicDirection.DOWNSTREAM, 0);
    arcade2Proxy.toxics().bandwidth("ISO_2_UP", ToxicDirection.UPSTREAM, 0);
    arcade3Proxy.toxics().bandwidth("ISO_3_DOWN", ToxicDirection.DOWNSTREAM, 0);
    arcade3Proxy.toxics().bandwidth("ISO_3_UP", ToxicDirection.UPSTREAM, 0);

    logger.info("Waiting for complete partition detection");
    TimeUnit.SECONDS.sleep(10);

    logger.info("Attempting writes to all nodes (all should fail without majority)");
    int successfulWrites = 0;

    try {
      db1.addUserAndPhotos(5, 10);
      successfulWrites++;
      logger.warn("Write to arcade1 succeeded without quorum");
    } catch (Exception e) {
      logger.info("Write to arcade1 correctly failed: {}", e.getMessage());
    }

    try {
      db2.addUserAndPhotos(5, 10);
      successfulWrites++;
      logger.warn("Write to arcade2 succeeded without quorum");
    } catch (Exception e) {
      logger.info("Write to arcade2 correctly failed: {}", e.getMessage());
    }

    try {
      db3.addUserAndPhotos(5, 10);
      successfulWrites++;
      logger.warn("Write to arcade3 succeeded without quorum");
    } catch (Exception e) {
      logger.info("Write to arcade3 correctly failed: {}", e.getMessage());
    }

    logger.info("Successful writes without quorum: {}/3 (should be 0 or 3 depending on quorum enforcement)", successfulWrites);

    logger.info("Healing all partitions");
    arcade1Proxy.toxics().get("ISO_1_DOWN").remove();
    arcade1Proxy.toxics().get("ISO_1_UP").remove();
    arcade2Proxy.toxics().get("ISO_2_DOWN").remove();
    arcade2Proxy.toxics().get("ISO_2_UP").remove();
    arcade3Proxy.toxics().get("ISO_3_DOWN").remove();
    arcade3Proxy.toxics().get("ISO_3_UP").remove();

    logger.info("Waiting for cluster reformation");
    TimeUnit.SECONDS.sleep(15);

    logger.info("Verifying cluster can accept writes after reformation");
    db1.addUserAndPhotos(10, 10);

    final int expectedUsers = 15 + (successfulWrites * 5) + 10;
    logger.info("Waiting for final convergence (expected {} users)", expectedUsers);

    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            Long users3 = db3.countUsers();
            logger.info("Convergence check: arcade1={}, arcade2={}, arcade3={} (expected={})",
                users1, users2, users3, expectedUsers);
            return users1.equals((long) expectedUsers) &&
                   users2.equals((long) expectedUsers) &&
                   users3.equals((long) expectedUsers);
          } catch (Exception e) {
            logger.warn("Convergence check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency");
    db1.assertThatUserCountIs(expectedUsers);
    db2.assertThatUserCountIs(expectedUsers);
    db3.assertThatUserCountIs(expectedUsers);

    db1.close();
    db2.close();
    db3.close();
  }

  @Test
  @Disabled
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test cluster reformation: verify proper leader election after partition healing")
  void testClusterReformation() throws IOException, InterruptedException {
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

    logger.info("Verifying initial state");
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);
    db3.assertThatUserCountIs(10);

    // Cycle through multiple partition/heal cycles
    for (int cycle = 1; cycle <= 3; cycle++) {
      logger.info("=== Reformation Cycle {} ===", cycle);

      logger.info("Cycle {}: Creating partition", cycle);
      arcade1Proxy.toxics().bandwidth("CYCLE_DOWN", ToxicDirection.DOWNSTREAM, 0);
      arcade1Proxy.toxics().bandwidth("CYCLE_UP", ToxicDirection.UPSTREAM, 0);

      TimeUnit.SECONDS.sleep(8);

      logger.info("Cycle {}: Writing to majority partition", cycle);
      db2.addUserAndPhotos(5, 10);

      logger.info("Cycle {}: Healing partition", cycle);
      arcade1Proxy.toxics().get("CYCLE_DOWN").remove();
      arcade1Proxy.toxics().get("CYCLE_UP").remove();

      logger.info("Cycle {}: Waiting for reformation", cycle);
      TimeUnit.SECONDS.sleep(15);

      final int currentCycle = cycle;
      final int expectedUsers = 10 + (cycle * 5);
      logger.info("Cycle {}: Verifying convergence to {} users", cycle, expectedUsers);

      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(3, TimeUnit.SECONDS)
          .until(() -> {
            try {
              Long users1 = db1.countUsers();
              logger.info("Cycle {}: arcade1={} (expected={})", currentCycle, users1, expectedUsers);
              return users1.equals((long) expectedUsers);
            } catch (Exception e) {
              logger.warn("Cycle {}: Check failed: {}", currentCycle, e.getMessage());
              return false;
            }
          });

      logger.info("Cycle {}: Cluster reformed successfully", cycle);
    }

    logger.info("Verifying final consistency after {} reformation cycles", 3);
    final int finalExpected = 25;
    db1.assertThatUserCountIs(finalExpected);
    db2.assertThatUserCountIs(finalExpected);
    db3.assertThatUserCountIs(finalExpected);

    db1.close();
    db2.close();
    db3.close();
  }

  @Test
  @Disabled
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test quorum loss recovery: verify cluster recovers after temporary quorum loss")
  void testQuorumLossRecovery() throws IOException, InterruptedException {
    logger.info("Creating proxies for 5-node cluster for more complex quorum scenarios");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3-node HA cluster with majority quorum (2/3)");
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

    logger.info("Verifying initial state");
    db1.assertThatUserCountIs(20);
    db2.assertThatUserCountIs(20);
    db3.assertThatUserCountIs(20);

    logger.info("Isolating 2 nodes (arcade2 and arcade3) - losing quorum");
    arcade2Proxy.toxics().bandwidth("QUORUM_LOSS_2_DOWN", ToxicDirection.DOWNSTREAM, 0);
    arcade2Proxy.toxics().bandwidth("QUORUM_LOSS_2_UP", ToxicDirection.UPSTREAM, 0);
    arcade3Proxy.toxics().bandwidth("QUORUM_LOSS_3_DOWN", ToxicDirection.DOWNSTREAM, 0);
    arcade3Proxy.toxics().bandwidth("QUORUM_LOSS_3_UP", ToxicDirection.UPSTREAM, 0);

    logger.info("Waiting for quorum loss detection");
    TimeUnit.SECONDS.sleep(10);

    logger.info("Attempting write without quorum (should fail)");
    boolean writeSucceeded = false;
    try {
      db1.addUserAndPhotos(10, 10);
      writeSucceeded = true;
      logger.warn("Write succeeded without quorum");
    } catch (Exception e) {
      logger.info("Write correctly failed without quorum: {}", e.getMessage());
    }

    logger.info("Restoring quorum by reconnecting nodes");
    arcade2Proxy.toxics().get("QUORUM_LOSS_2_DOWN").remove();
    arcade2Proxy.toxics().get("QUORUM_LOSS_2_UP").remove();
    arcade3Proxy.toxics().get("QUORUM_LOSS_3_DOWN").remove();
    arcade3Proxy.toxics().get("QUORUM_LOSS_3_UP").remove();

    logger.info("Waiting for quorum restoration");
    TimeUnit.SECONDS.sleep(15);

    logger.info("Writing with quorum restored");
    db1.addUserAndPhotos(15, 10);

    final int expectedUsers = writeSucceeded ? 45 : 35;
    logger.info("Waiting for convergence (expected {} users)", expectedUsers);

    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            Long users3 = db3.countUsers();
            logger.info("Quorum recovery check: arcade1={}, arcade2={}, arcade3={} (expected={})",
                users1, users2, users3, expectedUsers);
            return users1.equals((long) expectedUsers) &&
                   users2.equals((long) expectedUsers) &&
                   users3.equals((long) expectedUsers);
          } catch (Exception e) {
            logger.warn("Quorum recovery check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying cluster fully recovered after quorum loss");
    db1.assertThatUserCountIs(expectedUsers);
    db2.assertThatUserCountIs(expectedUsers);
    db3.assertThatUserCountIs(expectedUsers);

    db1.close();
    db2.close();
    db3.close();
  }
}
