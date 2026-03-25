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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Rolling restart tests for Raft HA cluster zero-downtime maintenance.
 * Tests sequential node restarts while maintaining cluster availability.
 * Raft peer list is static, so restarted nodes rejoin automatically via the fixed peer list.
 */
class RollingRestartIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "ArcadeDB_0:2434:2480,ArcadeDB_1:2434:2480,ArcadeDB_2:2434:2480";

  @AfterEach
  @Override
  public void tearDown() {
    stopContainers();
    logger.info("Comparing databases for consistency verification");
    compareAllDatabases();
    super.tearDown();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Rolling restart: restart each node sequentially, verify zero downtime")
  void testRollingRestart() throws InterruptedException {
    logger.info("Creating 3-node Raft HA cluster with majority quorum");
    final GenericContainer<?> arcade0 = createArcadeContainer("ArcadeDB_0", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade1 = createArcadeContainer("ArcadeDB_1", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade2 = createArcadeContainer("ArcadeDB_2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    logger.info("Creating database and initial data");
    db0.createDatabase();
    db0.createSchema();
    db0.addUserAndPhotos(30, 10);

    logger.info("Verifying initial state");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0.countUsers();
            final long users1 = db1.countUsers();
            final long users2 = db2.countUsers();
            logger.info("Initial replication check: {} / {} / {}", users0, users1, users2);
            return users0 == 30 && users1 == 30 && users2 == 30;
          } catch (final Exception e) {
            logger.warn("Initial replication check failed: {}", e.getMessage());
            return false;
          }
        });

    // --- Restart ArcadeDB_0 ---
    logger.info("=== Restarting ArcadeDB_0 ===");
    db0.close();
    arcade0.stop();
    logger.info("ArcadeDB_0 stopped");

    TimeUnit.SECONDS.sleep(5);

    logger.info("Writing during ArcadeDB_0 restart (cluster should remain available)");
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying writes succeeded on remaining nodes");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users1 = db1.countUsers();
            final long users2 = db2.countUsers();
            logger.info("During ArcadeDB_0 restart: db1={}, db2={}", users1, users2);
            return users1 == 40 && users2 == 40;
          } catch (final Exception e) {
            return false;
          }
        });

    logger.info("Restarting ArcadeDB_0");
    arcade0.start();
    TimeUnit.SECONDS.sleep(15);

    final ServerWrapper server0Restart = new ServerWrapper(arcade0);
    final DatabaseWrapper db0Restart = new DatabaseWrapper(server0Restart, idSupplier, wordSupplier);

    logger.info("Waiting for ArcadeDB_0 to resync via Raft log catch-up");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0Restart.countUsers();
            logger.info("ArcadeDB_0 resync check: {}", users0);
            return users0 == 40;
          } catch (final Exception e) {
            logger.warn("ArcadeDB_0 resync failed: {}", e.getMessage());
            return false;
          }
        });

    // --- Restart ArcadeDB_1 ---
    logger.info("=== Restarting ArcadeDB_1 ===");
    db1.close();
    arcade1.stop();
    logger.info("ArcadeDB_1 stopped");

    TimeUnit.SECONDS.sleep(5);

    logger.info("Writing during ArcadeDB_1 restart");
    db0Restart.addUserAndPhotos(10, 10);

    logger.info("Verifying writes on remaining nodes");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0Restart.countUsers();
            final long users2 = db2.countUsers();
            logger.info("During ArcadeDB_1 restart: db0={}, db2={}", users0, users2);
            return users0 == 50 && users2 == 50;
          } catch (final Exception e) {
            return false;
          }
        });

    logger.info("Restarting ArcadeDB_1");
    arcade1.start();
    TimeUnit.SECONDS.sleep(15);

    final ServerWrapper server1Restart = new ServerWrapper(arcade1);
    final DatabaseWrapper db1Restart = new DatabaseWrapper(server1Restart, idSupplier, wordSupplier);

    logger.info("Waiting for ArcadeDB_1 to resync via Raft log catch-up");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users1 = db1Restart.countUsers();
            logger.info("ArcadeDB_1 resync check: {}", users1);
            return users1 == 50;
          } catch (final Exception e) {
            logger.warn("ArcadeDB_1 resync failed: {}", e.getMessage());
            return false;
          }
        });

    // --- Restart ArcadeDB_2 ---
    logger.info("=== Restarting ArcadeDB_2 ===");
    db2.close();
    arcade2.stop();
    logger.info("ArcadeDB_2 stopped");

    TimeUnit.SECONDS.sleep(5);

    logger.info("Writing during ArcadeDB_2 restart");
    db0Restart.addUserAndPhotos(10, 10);

    logger.info("Verifying writes on remaining nodes");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0Restart.countUsers();
            final long users1 = db1Restart.countUsers();
            logger.info("During ArcadeDB_2 restart: db0={}, db1={}", users0, users1);
            return users0 == 60 && users1 == 60;
          } catch (final Exception e) {
            return false;
          }
        });

    logger.info("Restarting ArcadeDB_2");
    arcade2.start();
    TimeUnit.SECONDS.sleep(15);

    final ServerWrapper server2Restart = new ServerWrapper(arcade2);
    final DatabaseWrapper db2Restart = new DatabaseWrapper(server2Restart, idSupplier, wordSupplier);

    logger.info("Waiting for ArcadeDB_2 to resync via Raft log catch-up");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users2 = db2Restart.countUsers();
            logger.info("ArcadeDB_2 resync check: {}", users2);
            return users2 == 60;
          } catch (final Exception e) {
            logger.warn("ArcadeDB_2 resync failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency after rolling restart");
    db0Restart.assertThatUserCountIs(60);
    db1Restart.assertThatUserCountIs(60);
    db2Restart.assertThatUserCountIs(60);

    db0Restart.close();
    db1Restart.close();
    db2Restart.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Rapid rolling restart: minimal wait between restarts")
  void testRapidRollingRestart() throws InterruptedException {
    logger.info("Creating 3-node Raft HA cluster");
    final GenericContainer<?> arcade0 = createArcadeContainer("ArcadeDB_0", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade1 = createArcadeContainer("ArcadeDB_1", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade2 = createArcadeContainer("ArcadeDB_2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    logger.info("Creating database and initial data");
    db0.createDatabase();
    db0.createSchema();
    db0.addUserAndPhotos(20, 10);

    logger.info("Verifying initial state");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0.countUsers();
            final long users1 = db1.countUsers();
            final long users2 = db2.countUsers();
            logger.info("Initial: {} / {} / {}", users0, users1, users2);
            return users0 == 20 && users1 == 20 && users2 == 20;
          } catch (final Exception e) {
            return false;
          }
        });

    logger.info("Performing rapid sequential restarts with minimal wait time");

    // Restart ArcadeDB_0
    logger.info("Rapidly restarting ArcadeDB_0");
    db0.close();
    arcade0.stop();
    arcade0.start();
    TimeUnit.SECONDS.sleep(10);

    // Restart ArcadeDB_1
    logger.info("Rapidly restarting ArcadeDB_1");
    db1.close();
    arcade1.stop();
    arcade1.start();
    TimeUnit.SECONDS.sleep(10);

    // Restart ArcadeDB_2
    logger.info("Rapidly restarting ArcadeDB_2");
    db2.close();
    arcade2.stop();
    arcade2.start();
    TimeUnit.SECONDS.sleep(10);

    logger.info("Waiting for cluster stabilization after rapid restarts");
    TimeUnit.SECONDS.sleep(15);

    // Reconnect to all nodes
    final ServerWrapper server0 = new ServerWrapper(arcade0);
    final ServerWrapper server1 = new ServerWrapper(arcade1);
    final ServerWrapper server2 = new ServerWrapper(arcade2);
    final DatabaseWrapper db0Restart = new DatabaseWrapper(server0, idSupplier, wordSupplier);
    final DatabaseWrapper db1Restart = new DatabaseWrapper(server1, idSupplier, wordSupplier);
    final DatabaseWrapper db2Restart = new DatabaseWrapper(server2, idSupplier, wordSupplier);

    logger.info("Verifying cluster recovered and data is consistent");
    Awaitility.await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0Restart.countUsers();
            final long users1 = db1Restart.countUsers();
            final long users2 = db2Restart.countUsers();
            logger.info("Recovery check: db0={}, db1={}, db2={}", users0, users1, users2);
            return users0 == 20 && users1 == 20 && users2 == 20;
          } catch (final Exception e) {
            logger.warn("Recovery check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency");
    db0Restart.assertThatUserCountIs(20);
    db1Restart.assertThatUserCountIs(20);
    db2Restart.assertThatUserCountIs(20);

    db0Restart.close();
    db1Restart.close();
    db2Restart.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Rolling restart with continuous writes: verify no data loss")
  void testRollingRestartWithContinuousWrites() throws InterruptedException {
    logger.info("Creating 3-node Raft HA cluster");
    final GenericContainer<?> arcade0 = createArcadeContainer("ArcadeDB_0", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade1 = createArcadeContainer("ArcadeDB_1", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade2 = createArcadeContainer("ArcadeDB_2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    logger.info("Creating database and schema");
    db0.createDatabase();
    db0.createSchema();

    int expectedUsers = 0;

    logger.info("Writing initial data");
    db0.addUserAndPhotos(10, 10);
    expectedUsers += 10;

    // Restart ArcadeDB_0 while writing
    logger.info("Restarting ArcadeDB_0 while writing");
    db1.addUserAndPhotos(5, 10);
    expectedUsers += 5;
    db0.close();
    arcade0.stop();

    TimeUnit.SECONDS.sleep(5);
    db2.addUserAndPhotos(5, 10);
    expectedUsers += 5;

    arcade0.start();
    TimeUnit.SECONDS.sleep(15);

    // Restart ArcadeDB_1 while writing
    logger.info("Restarting ArcadeDB_1 while writing");
    db2.addUserAndPhotos(5, 10);
    expectedUsers += 5;
    db1.close();
    arcade1.stop();

    TimeUnit.SECONDS.sleep(5);
    db2.addUserAndPhotos(5, 10);
    expectedUsers += 5;

    arcade1.start();
    TimeUnit.SECONDS.sleep(15);

    // Restart ArcadeDB_2 while writing
    logger.info("Restarting ArcadeDB_2 while writing");
    final ServerWrapper server0 = new ServerWrapper(arcade0);
    final ServerWrapper server1 = new ServerWrapper(arcade1);
    final DatabaseWrapper db0Restart = new DatabaseWrapper(server0, idSupplier, wordSupplier);
    final DatabaseWrapper db1Restart = new DatabaseWrapper(server1, idSupplier, wordSupplier);

    db0Restart.addUserAndPhotos(5, 10);
    expectedUsers += 5;
    db2.close();
    arcade2.stop();

    TimeUnit.SECONDS.sleep(5);
    db1Restart.addUserAndPhotos(5, 10);
    expectedUsers += 5;

    arcade2.start();
    TimeUnit.SECONDS.sleep(15);

    final ServerWrapper server2 = new ServerWrapper(arcade2);
    final DatabaseWrapper db2Restart = new DatabaseWrapper(server2, idSupplier, wordSupplier);

    logger.info("Waiting for final convergence (expected {} users)", expectedUsers);
    final int finalExpected = expectedUsers;
    Awaitility.await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0Restart.countUsers();
            final long users1 = db1Restart.countUsers();
            final long users2 = db2Restart.countUsers();
            logger.info("Final convergence: db0={}, db1={}, db2={} (expected={})",
                users0, users1, users2, finalExpected);
            return users0 == finalExpected && users1 == finalExpected && users2 == finalExpected;
          } catch (final Exception e) {
            logger.warn("Convergence check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying no data loss after rolling restart with continuous writes");
    db0Restart.assertThatUserCountIs(expectedUsers);
    db1Restart.assertThatUserCountIs(expectedUsers);
    db2Restart.assertThatUserCountIs(expectedUsers);

    db0Restart.close();
    db1Restart.close();
    db2Restart.close();
  }
}
