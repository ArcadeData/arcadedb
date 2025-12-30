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
 * Rolling restart tests for HA cluster zero-downtime maintenance.
 * Tests sequential node restarts while maintaining cluster availability.
 */
@Testcontainers
public class RollingRestartIT extends ContainersTestTemplate {

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test rolling restart: restart each node sequentially, verify zero downtime")
  void testRollingRestart() throws IOException, InterruptedException {
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
    db1.addUserAndPhotos(30, 10);

    logger.info("Verifying initial state");
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);
    db3.assertThatUserCountIs(30);

    // Restart arcade1
    logger.info("=== Restarting arcade1 ===");
    db1.close();
    arcade1.stop();
    logger.info("arcade1 stopped");

    TimeUnit.SECONDS.sleep(5);

    logger.info("Writing during arcade1 restart (cluster should remain available)");
    db2.addUserAndPhotos(10, 10);

    logger.info("Verifying writes succeeded on remaining nodes");
    db2.assertThatUserCountIs(40);
    db3.assertThatUserCountIs(40);

    logger.info("Restarting arcade1");
    arcade1.start();
    TimeUnit.SECONDS.sleep(10);

    ServerWrapper server1 = new ServerWrapper(arcade1);
    DatabaseWrapper db1Restart = new DatabaseWrapper(server1, idSupplier);

    logger.info("Waiting for arcade1 to resync");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1Restart.countUsers();
            logger.info("arcade1 resync check: {}", users1);
            return users1.equals(40L);
          } catch (Exception e) {
            logger.warn("arcade1 resync failed: {}", e.getMessage());
            return false;
          }
        });

    // Restart arcade2
    logger.info("=== Restarting arcade2 ===");
    db2.close();
    arcade2.stop();
    logger.info("arcade2 stopped");

    TimeUnit.SECONDS.sleep(5);

    logger.info("Writing during arcade2 restart");
    db1Restart.addUserAndPhotos(10, 10);

    logger.info("Verifying writes on remaining nodes");
    db1Restart.assertThatUserCountIs(50);
    db3.assertThatUserCountIs(50);

    logger.info("Restarting arcade2");
    arcade2.start();
    TimeUnit.SECONDS.sleep(10);

    ServerWrapper server2 = new ServerWrapper(arcade2);
    DatabaseWrapper db2Restart = new DatabaseWrapper(server2, idSupplier);

    logger.info("Waiting for arcade2 to resync");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users2 = db2Restart.countUsers();
            logger.info("arcade2 resync check: {}", users2);
            return users2.equals(50L);
          } catch (Exception e) {
            logger.warn("arcade2 resync failed: {}", e.getMessage());
            return false;
          }
        });

    // Restart arcade3
    logger.info("=== Restarting arcade3 ===");
    db3.close();
    arcade3.stop();
    logger.info("arcade3 stopped");

    TimeUnit.SECONDS.sleep(5);

    logger.info("Writing during arcade3 restart");
    db1Restart.addUserAndPhotos(10, 10);

    logger.info("Verifying writes on remaining nodes");
    db1Restart.assertThatUserCountIs(60);
    db2Restart.assertThatUserCountIs(60);

    logger.info("Restarting arcade3");
    arcade3.start();
    TimeUnit.SECONDS.sleep(10);

    ServerWrapper server3 = new ServerWrapper(arcade3);
    DatabaseWrapper db3Restart = new DatabaseWrapper(server3, idSupplier);

    logger.info("Waiting for arcade3 to resync");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users3 = db3Restart.countUsers();
            logger.info("arcade3 resync check: {}", users3);
            return users3.equals(60L);
          } catch (Exception e) {
            logger.warn("arcade3 resync failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency after rolling restart");
    db1Restart.assertThatUserCountIs(60);
    db2Restart.assertThatUserCountIs(60);
    db3Restart.assertThatUserCountIs(60);

    db1Restart.close();
    db2Restart.close();
    db3Restart.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test rapid rolling restart: minimal wait between restarts")
  void testRapidRollingRestart() throws IOException, InterruptedException {
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
    db1.addUserAndPhotos(20, 10);

    logger.info("Verifying initial state");
    db1.assertThatUserCountIs(20);
    db2.assertThatUserCountIs(20);
    db3.assertThatUserCountIs(20);

    logger.info("Performing rapid sequential restarts with minimal wait time");

    // Restart arcade1
    logger.info("Rapidly restarting arcade1");
    db1.close();
    arcade1.stop();
    arcade1.start();
    TimeUnit.SECONDS.sleep(8);

    // Immediately restart arcade2
    logger.info("Rapidly restarting arcade2");
    db2.close();
    arcade2.stop();
    arcade2.start();
    TimeUnit.SECONDS.sleep(8);

    // Immediately restart arcade3
    logger.info("Rapidly restarting arcade3");
    db3.close();
    arcade3.stop();
    arcade3.start();
    TimeUnit.SECONDS.sleep(8);

    logger.info("Waiting for cluster stabilization");
    TimeUnit.SECONDS.sleep(10);

    // Reconnect to all nodes
    ServerWrapper server1 = new ServerWrapper(arcade1);
    ServerWrapper server2 = new ServerWrapper(arcade2);
    ServerWrapper server3 = new ServerWrapper(arcade3);
    DatabaseWrapper db1Restart = new DatabaseWrapper(server1, idSupplier);
    DatabaseWrapper db2Restart = new DatabaseWrapper(server2, idSupplier);
    DatabaseWrapper db3Restart = new DatabaseWrapper(server3, idSupplier);

    logger.info("Verifying cluster recovered and data is consistent");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1Restart.countUsers();
            Long users2 = db2Restart.countUsers();
            Long users3 = db3Restart.countUsers();
            logger.info("Recovery check: arcade1={}, arcade2={}, arcade3={}", users1, users2, users3);
            return users1.equals(20L) && users2.equals(20L) && users3.equals(20L);
          } catch (Exception e) {
            logger.warn("Recovery check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency");
    db1Restart.assertThatUserCountIs(20);
    db2Restart.assertThatUserCountIs(20);
    db3Restart.assertThatUserCountIs(20);

    db1Restart.close();
    db2Restart.close();
    db3Restart.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test rolling restart with continuous writes: verify no data loss")
  void testRollingRestartWithContinuousWrites() throws IOException, InterruptedException {
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

    logger.info("Creating database and schema");
    db1.createDatabase();
    db1.createSchema();

    int expectedUsers = 0;

    logger.info("Writing initial data");
    db1.addUserAndPhotos(10, 10);
    expectedUsers += 10;

    // Restart arcade1 while writing
    logger.info("Restarting arcade1");
    db2.addUserAndPhotos(5, 10);
    expectedUsers += 5;
    db1.close();
    arcade1.stop();

    TimeUnit.SECONDS.sleep(3);
    db3.addUserAndPhotos(5, 10);
    expectedUsers += 5;

    arcade1.start();
    TimeUnit.SECONDS.sleep(10);

    // Restart arcade2 while writing
    logger.info("Restarting arcade2");
    db3.addUserAndPhotos(5, 10);
    expectedUsers += 5;
    db2.close();
    arcade2.stop();

    TimeUnit.SECONDS.sleep(3);
    db3.addUserAndPhotos(5, 10);
    expectedUsers += 5;

    arcade2.start();
    TimeUnit.SECONDS.sleep(10);

    // Restart arcade3 while writing
    logger.info("Restarting arcade3");
    ServerWrapper server1 = new ServerWrapper(arcade1);
    ServerWrapper server2 = new ServerWrapper(arcade2);
    DatabaseWrapper db1Restart = new DatabaseWrapper(server1, idSupplier);
    DatabaseWrapper db2Restart = new DatabaseWrapper(server2, idSupplier);

    db1Restart.addUserAndPhotos(5, 10);
    expectedUsers += 5;
    db3.close();
    arcade3.stop();

    TimeUnit.SECONDS.sleep(3);
    db2Restart.addUserAndPhotos(5, 10);
    expectedUsers += 5;

    arcade3.start();
    TimeUnit.SECONDS.sleep(10);

    ServerWrapper server3 = new ServerWrapper(arcade3);
    DatabaseWrapper db3Restart = new DatabaseWrapper(server3, idSupplier);

    logger.info("Waiting for final convergence (expected {} users)", expectedUsers);
    final int finalExpected = expectedUsers;
    Awaitility.await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1Restart.countUsers();
            Long users2 = db2Restart.countUsers();
            Long users3 = db3Restart.countUsers();
            logger.info("Final convergence: arcade1={}, arcade2={}, arcade3={} (expected={})",
                users1, users2, users3, finalExpected);
            return users1.equals((long) finalExpected) &&
                   users2.equals((long) finalExpected) &&
                   users3.equals((long) finalExpected);
          } catch (Exception e) {
            logger.warn("Convergence check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying no data loss after rolling restart with continuous writes");
    db1Restart.assertThatUserCountIs(expectedUsers);
    db2Restart.assertThatUserCountIs(expectedUsers);
    db3Restart.assertThatUserCountIs(expectedUsers);

    db1Restart.close();
    db2Restart.close();
    db3Restart.close();
  }
}
