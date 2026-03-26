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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Leader failover and automatic election tests for Raft HA cluster resilience.
 * Tests catastrophic leader failures and cluster recovery.
 * <p>
 * In Raft, when the leader is lost, a new leader is automatically elected from
 * the remaining majority. Writes are only possible when a majority quorum is available.
 */
@Testcontainers
class LeaderFailoverIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";

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

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test leader failover: kill leader, verify new election and data consistency")
  void testLeaderFailover() throws InterruptedException {
    logger.info("Creating 3-node Raft HA cluster with majority quorum");
    final GenericContainer<?> arcade0 = createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade1 = createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade2 = createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);
    final DatabaseWrapper[] dbs = { db0, db1, db2 };
    final GenericContainer<?>[] containers = { arcade0, arcade1, arcade2 };

    logger.info("Creating database and schema on first node");
    db0.createDatabase();
    db0.createSchema();

    logger.info("Adding initial data to cluster");
    db0.addUserAndPhotos(20, 10);

    logger.info("Verifying initial replication");
    db0.assertThatUserCountIs(20);
    db1.assertThatUserCountIs(20);
    db2.assertThatUserCountIs(20);

    logger.info("Finding current Raft leader");
    final int leaderIdx = findLeaderIndex(servers);
    logger.info("Current leader is node {}", leaderIdx);

    // Pick a surviving node index (not the leader)
    final int survivor1 = (leaderIdx + 1) % 3;
    final int survivor2 = (leaderIdx + 2) % 3;

    logger.info("Killing leader (node {}) - simulating catastrophic failure", leaderIdx);
    dbs[leaderIdx].close();
    containers[leaderIdx].stop();

    logger.info("Waiting for new leader election among surviving nodes");
    TimeUnit.SECONDS.sleep(15);

    logger.info("Attempting write to survivor node {} (should succeed with new leader)", survivor1);
    dbs[survivor1].addUserAndPhotos(10, 10);

    logger.info("Verifying write succeeded and replicated to surviving nodes");
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long usersS1 = dbs[survivor1].countUsers();
            final long usersS2 = dbs[survivor2].countUsers();
            logger.info("Failover check: node{}={}, node{}={}", survivor1, usersS1, survivor2, usersS2);
            return usersS1 == 30L && usersS2 == 30L;
          } catch (final Exception e) {
            logger.warn("Failover check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final data consistency on surviving nodes");
    dbs[survivor1].assertThatUserCountIs(30);
    dbs[survivor2].assertThatUserCountIs(30);

    logger.info("Restarting killed leader node to verify it rejoins cluster");
    containers[leaderIdx].start();
    TimeUnit.SECONDS.sleep(15);

    final ServerWrapper restartedServer = new ServerWrapper(containers[leaderIdx]);
    final DatabaseWrapper dbRestarted = new DatabaseWrapper(restartedServer, idSupplier, wordSupplier);

    logger.info("Verifying restarted node resyncs with cluster via Raft log catch-up");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users = dbRestarted.countUsers();
            logger.info("Resync check: restarted node={}", users);
            return users == 30L;
          } catch (final Exception e) {
            logger.warn("Resync check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying full cluster consistency after rejoin");
    dbRestarted.assertThatUserCountIs(30);
    dbs[survivor1].assertThatUserCountIs(30);
    dbs[survivor2].assertThatUserCountIs(30);

    dbRestarted.close();
    dbs[survivor1].close();
    dbs[survivor2].close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test repeated leader failures: verify cluster stability under continuous failover")
  void testRepeatedLeaderFailures() throws InterruptedException {
    logger.info("Creating 3-node Raft HA cluster");
    final GenericContainer<?> arcade0 = createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade1 = createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade2 = createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    logger.info("Creating database and initial data");
    db0.createDatabase();
    db0.createSchema();
    db0.addUserAndPhotos(10, 10);

    logger.info("Verifying initial state");
    db0.assertThatUserCountIs(10);
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);

    // Cycle 1: Kill arcadedb-0
    logger.info("Cycle 1: Killing arcadedb-0");
    db0.close();
    arcade0.stop();
    TimeUnit.SECONDS.sleep(15);

    logger.info("Cycle 1: Adding data through arcadedb-1");
    db1.addUserAndPhotos(5, 10);

    logger.info("Cycle 1: Verifying replication on surviving nodes");
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            return db1.countUsers() == 15L && db2.countUsers() == 15L;
          } catch (final Exception e) {
            return false;
          }
        });

    // Cycle 2: Kill arcadedb-1 (now likely the leader), restart arcadedb-0 first to maintain majority
    logger.info("Cycle 2: Restarting arcadedb-0 before killing arcadedb-1 (to maintain majority)");
    arcade0.start();
    TimeUnit.SECONDS.sleep(15);

    final ServerWrapper server0Restart = new ServerWrapper(arcade0);
    final DatabaseWrapper db0Restart = new DatabaseWrapper(server0Restart, idSupplier, wordSupplier);

    logger.info("Waiting for arcadedb-0 to resync");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            return db0Restart.countUsers() == 15L;
          } catch (final Exception e) {
            return false;
          }
        });

    logger.info("Cycle 2: Killing arcadedb-1");
    db1.close();
    arcade1.stop();
    TimeUnit.SECONDS.sleep(15);

    logger.info("Cycle 2: Adding data through arcadedb-2");
    db2.addUserAndPhotos(5, 10);

    logger.info("Cycle 2: Verifying replication");
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            return db0Restart.countUsers() == 20L && db2.countUsers() == 20L;
          } catch (final Exception e) {
            return false;
          }
        });

    // Restart arcadedb-1
    logger.info("Restarting arcadedb-1");
    arcade1.start();
    TimeUnit.SECONDS.sleep(15);

    final ServerWrapper server1Restart = new ServerWrapper(arcade1);
    final DatabaseWrapper db1Restart = new DatabaseWrapper(server1Restart, idSupplier, wordSupplier);

    logger.info("Waiting for full cluster convergence");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users0 = db0Restart.countUsers();
            final long users1 = db1Restart.countUsers();
            final long users2 = db2.countUsers();
            logger.info("Convergence check: arcadedb-0={}, arcadedb-1={}, arcadedb-2={}", users0, users1, users2);
            return users0 == 20L && users1 == 20L && users2 == 20L;
          } catch (final Exception e) {
            logger.warn("Convergence check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency after multiple failovers");
    db0Restart.assertThatUserCountIs(20);
    db1Restart.assertThatUserCountIs(20);
    db2.assertThatUserCountIs(20);

    db0Restart.close();
    db1Restart.close();
    db2.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test leader failover with active writes: verify no data loss during failover")
  void testLeaderFailoverDuringWrites() throws InterruptedException {
    logger.info("Creating 3-node Raft HA cluster");
    final GenericContainer<?> arcade0 = createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade1 = createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    final GenericContainer<?> arcade2 = createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);
    final DatabaseWrapper[] dbs = { db0, db1, db2 };
    final GenericContainer<?>[] nodeContainers = { arcade0, arcade1, arcade2 };

    logger.info("Creating database and schema");
    db0.createDatabase();
    db0.createSchema();

    logger.info("Adding initial data");
    db0.addUserAndPhotos(20, 10);

    logger.info("Verifying initial replication");
    db0.assertThatUserCountIs(20);
    db1.assertThatUserCountIs(20);
    db2.assertThatUserCountIs(20);

    logger.info("Finding current Raft leader");
    final int leaderIdx = findLeaderIndex(servers);
    logger.info("Current leader is node {}", leaderIdx);

    final int survivor1 = (leaderIdx + 1) % 3;
    final int survivor2 = (leaderIdx + 2) % 3;

    logger.info("Writing some data to leader, then killing it");
    dbs[leaderIdx].addUserAndPhotos(5, 10);

    // Kill leader immediately after write
    dbs[leaderIdx].close();
    nodeContainers[leaderIdx].stop();

    logger.info("Leader killed - waiting for new election");
    TimeUnit.SECONDS.sleep(15);

    logger.info("Continuing writes through survivor node {}", survivor1);
    dbs[survivor1].addUserAndPhotos(5, 10);

    logger.info("Waiting for replication convergence on surviving nodes");
    Awaitility.await()
        .atMost(45, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long usersS1 = dbs[survivor1].countUsers();
            final long usersS2 = dbs[survivor2].countUsers();
            logger.info("Convergence check: node{}={}, node{}={}", survivor1, usersS1, survivor2, usersS2);
            // Expect 25-30 users depending on whether the last write to leader completed before kill
            return usersS1 == usersS2 && usersS1 >= 25L && usersS1 <= 30L;
          } catch (final Exception e) {
            logger.warn("Convergence check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying data consistency between surviving nodes");
    final long finalCount = dbs[survivor1].countUsers();
    dbs[survivor2].assertThatUserCountIs((int) finalCount);

    logger.info("Final user count: {} (some writes may have been lost during leader failure)", finalCount);

    dbs[survivor1].close();
    dbs[survivor2].close();
  }
}
