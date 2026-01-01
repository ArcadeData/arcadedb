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
 * Packet loss tests for HA cluster resilience.
 * Tests behavior under unreliable networks with dropped packets.
 */
@Testcontainers
public class PacketLossIT extends ContainersTestTemplate {

  @Test
  @Disabled
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test low packet loss (5%): cluster should remain stable")
  void testLowPacketLoss() throws IOException {
    logger.info("Creating proxies for 2-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");

    logger.info("Creating 2-node HA cluster");
    createArcadeContainer("arcade1", "{arcade2}proxy:8667", "none", "any", network);
    createArcadeContainer("arcade2", "{arcade1}proxy:8666", "none", "any", network);

    logger.info("Starting cluster");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);

    logger.info("Creating database and schema");
    db1.createDatabase();
    db1.createSchema();

    logger.info("Adding initial data");
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);

    logger.info("Introducing 5% packet loss (simulating minor network issues)");
    arcade1Proxy.toxics().limitData("packet_loss_arcade1", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.05f);
    arcade2Proxy.toxics().limitData("packet_loss_arcade2", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.05f);

    logger.info("Adding data under 5% packet loss");
    db1.addUserAndPhotos(20, 10);

    logger.info("Waiting for replication with packet loss");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            logger.info("Low packet loss check: arcade1={}, arcade2={}", users1, users2);
            return users1.equals(30L) && users2.equals(30L);
          } catch (Exception e) {
            logger.warn("Low packet loss check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing packet loss");
    arcade1Proxy.toxics().get("packet_loss_arcade1").remove();
    arcade2Proxy.toxics().get("packet_loss_arcade2").remove();

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
    logger.info("Creating proxies for 2-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");

    logger.info("Creating 2-node HA cluster");
    createArcadeContainer("arcade1", "{arcade2}proxy:8667", "none", "any", network);
    createArcadeContainer("arcade2", "{arcade1}proxy:8666", "none", "any", network);

    logger.info("Starting cluster");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);

    logger.info("Creating database and initial data");
    db1.createDatabase();
    db1.createSchema();
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);

    logger.info("Introducing 20% packet loss (simulating unreliable network)");
    arcade1Proxy.toxics().limitData("moderate_loss_arcade1", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.20f);
    arcade2Proxy.toxics().limitData("moderate_loss_arcade2", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.20f);

    logger.info("Adding data under 20% packet loss");
    db1.addUserAndPhotos(15, 10);

    logger.info("Waiting for replication with moderate packet loss (may take longer due to retries)");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            logger.info("Moderate packet loss check: arcade1={}, arcade2={}", users1, users2);
            return users1.equals(25L) && users2.equals(25L);
          } catch (Exception e) {
            logger.warn("Moderate packet loss check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing packet loss");
    arcade1Proxy.toxics().get("moderate_loss_arcade1").remove();
    arcade2Proxy.toxics().get("moderate_loss_arcade2").remove();

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
    logger.info("Creating proxies for 2-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");

    logger.info("Creating 2-node HA cluster");
    createArcadeContainer("arcade1", "{arcade2}proxy:8667", "none", "any", network);
    createArcadeContainer("arcade2", "{arcade1}proxy:8666", "none", "any", network);

    logger.info("Starting cluster");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);

    logger.info("Creating database and initial data");
    db1.createDatabase();
    db1.createSchema();
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);

    logger.info("Introducing 50% packet loss (severe network degradation)");
    arcade1Proxy.toxics().limitData("high_loss_arcade1", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.50f);
    arcade2Proxy.toxics().limitData("high_loss_arcade2", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.50f);

    logger.info("Adding data under 50% packet loss");
    db1.addUserAndPhotos(10, 10);

    logger.info("Waiting for eventual replication (will be very slow with retries)");
    Awaitility.await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            logger.info("High packet loss check: arcade1={}, arcade2={}", users1, users2);
            return users1.equals(20L) && users2.equals(20L);
          } catch (Exception e) {
            logger.warn("High packet loss check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing packet loss");
    arcade1Proxy.toxics().get("high_loss_arcade1").remove();
    arcade2Proxy.toxics().get("high_loss_arcade2").remove();

    logger.info("Verifying final consistency");
    db1.assertThatUserCountIs(20);
    db2.assertThatUserCountIs(20);

    db1.close();
    db2.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test directional packet loss: loss only in one direction")
  void testDirectionalPacketLoss() throws IOException {
    logger.info("Creating proxies for 3-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3-node HA cluster");
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

    logger.info("Introducing 30% packet loss DOWNSTREAM only on arcade1");
    arcade1Proxy.toxics().limitData("directional_loss", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.30f);

    logger.info("Adding data from arcade2 (should replicate to arcade1 despite one-way loss)");
    db2.addUserAndPhotos(15, 10);

    logger.info("Waiting for replication with directional packet loss");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            Long users3 = db3.countUsers();
            logger.info("Directional loss check: arcade1={}, arcade2={}, arcade3={}", users1, users2, users3);
            return users1.equals(25L) && users2.equals(25L) && users3.equals(25L);
          } catch (Exception e) {
            logger.warn("Directional loss check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing directional packet loss");
    arcade1Proxy.toxics().get("directional_loss").remove();

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
    logger.info("Creating proxies for 2-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");

    logger.info("Creating 2-node HA cluster");
    createArcadeContainer("arcade1", "{arcade2}proxy:8667", "none", "any", network);
    createArcadeContainer("arcade2", "{arcade1}proxy:8666", "none", "any", network);

    logger.info("Starting cluster");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);

    logger.info("Creating database and initial data");
    db1.createDatabase();
    db1.createSchema();
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);

    logger.info("Applying intermittent packet loss (3 cycles)");
    for (int cycle = 1; cycle <= 3; cycle++) {
      logger.info("Cycle {}: Introducing 25% packet loss", cycle);
      arcade1Proxy.toxics().limitData("intermittent_loss", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.25f);

      logger.info("Cycle {}: Adding data during packet loss", cycle);
      db1.addUserAndPhotos(5, 10);

      logger.info("Cycle {}: Removing packet loss", cycle);
      TimeUnit.SECONDS.sleep(2);
      arcade1Proxy.toxics().get("intermittent_loss").remove();

      logger.info("Cycle {}: Waiting for recovery", cycle);
      TimeUnit.SECONDS.sleep(3);
    }

    logger.info("Waiting for final convergence after intermittent issues");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            logger.info("Intermittent loss recovery check: arcade1={}, arcade2={}", users1, users2);
            return users1.equals(25L) && users2.equals(25L);
          } catch (Exception e) {
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
