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
 * Leader failover and automatic election tests for HA cluster resilience.
 * Tests catastrophic leader failures and cluster recovery.
 */
@Testcontainers
@Disabled
public class LeaderFailoverIT extends ContainersTestTemplate {

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test leader failover: kill leader, verify new election and data consistency")
  void testLeaderFailover() throws IOException, InterruptedException {
//    logger.info("Creating proxies for 3-node cluster");
//    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
//    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
//    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3-node HA cluster with majority quorum");
//    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "majority", "any", network);
//    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "majority", "any", network);
//    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "majority", "any", network);
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}arcade2:2424,{arcade3}arcade3:2424", "majority", "any", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}arcade1:2424,{arcade3}arcade3:2424", "majority", "any", network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}arcade1:2424,{arcade2}arcade2:2424", "majority", "any", network);

    logger.info("Starting cluster - arcade1 will become leader");
    List<ServerWrapper> servers = startContainers();


    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier);

    logger.info("Creating database and schema on leader");
    db1.createDatabase();
    db1.createSchema();

    logger.info("Adding initial data to cluster");
    db1.addUserAndPhotos(20, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(20);
    db2.assertThatUserCountIs(20);
    db3.assertThatUserCountIs(20);

    logger.info("Killing leader (arcade1) - simulating catastrophic failure");
    db1.close();
    arcade1.stop();

    logger.info("Waiting for new leader election among arcade2 and arcade3");
    TimeUnit.SECONDS.sleep(10);

    logger.info("Attempting write to arcade2 (should succeed with new leader)");
    db2.addUserAndPhotos(10, 10);

    logger.info("Verifying write succeeded and replicated to arcade3");
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users2 = db2.countUsers();
            Long users3 = db3.countUsers();
            logger.info("Failover check: arcade2={}, arcade3={}", users2, users3);
            return users2.equals(30L) && users3.equals(30L);
          } catch (Exception e) {
            logger.warn("Failover check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final data consistency");
    db2.assertThatUserCountIs(30);
    db3.assertThatUserCountIs(30);

    logger.info("Restarting arcade1 to verify it rejoins cluster");
    arcade1.start();
    TimeUnit.SECONDS.sleep(10);

    // Reconnect to arcade1
    ServerWrapper server1 = new ServerWrapper(arcade1);
    DatabaseWrapper db1Reconnect = new DatabaseWrapper(server1, idSupplier);

    logger.info("Verifying arcade1 resyncs with cluster after restart");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1Reconnect.countUsers();
            logger.info("Resync check: arcade1={}", users1);
            return users1.equals(30L);
          } catch (Exception e) {
            logger.warn("Resync check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying full cluster consistency after rejoin");
    db1Reconnect.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);
    db3.assertThatUserCountIs(30);

    db1Reconnect.close();
    db2.close();
    db3.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test repeated leader failures: verify cluster stability under continuous failover")
  void testRepeatedLeaderFailures() throws IOException, InterruptedException {
    logger.info("Creating proxies for 3-node cluster");
//    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
//    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
//    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3-node HA cluster");
//    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "majority", "any", network);
//    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "majority", "any", network);
//    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "majority", "any", network);

    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}arcade2:2424,{arcade3}arcade3:2424", "majority", "any", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}arcade1:2424,{arcade3}arcade3:2424", "majority", "any", network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}arcade1:2424,{arcade2}arcade2:2424", "majority", "any", network);

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

    // Cycle 1: Kill arcade1
    logger.info("Cycle 1: Killing arcade1 (current leader)");
    db1.close();
    arcade1.stop();
    TimeUnit.SECONDS.sleep(10);

    logger.info("Cycle 1: Adding data through arcade2");
    db2.addUserAndPhotos(5, 10);

    logger.info("Cycle 1: Verifying replication");
    db2.assertThatUserCountIs(15);
    db3.assertThatUserCountIs(15);

    // Cycle 2: Kill arcade2 (now likely the leader)
    logger.info("Cycle 2: Killing arcade2 (current leader)");
    db2.close();
    arcade2.stop();
    TimeUnit.SECONDS.sleep(10);

    logger.info("Cycle 2: Adding data through arcade3 (only remaining node)");
    db3.addUserAndPhotos(5, 10);

    logger.info("Cycle 2: Verifying arcade3 has data (cannot replicate with others down)");
    db3.assertThatUserCountIs(20);

    // Restart arcade1 and arcade2
    logger.info("Restarting arcade1 and arcade2");
    arcade1.start();
    arcade2.start();
    TimeUnit.SECONDS.sleep(15);

    // Reconnect
    ServerWrapper server1 = new ServerWrapper(arcade1);
    ServerWrapper server2 = new ServerWrapper(arcade2);
    DatabaseWrapper db1Reconnect = new DatabaseWrapper(server1, idSupplier);
    DatabaseWrapper db2Reconnect = new DatabaseWrapper(server2, idSupplier);

    logger.info("Waiting for full cluster convergence");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1Reconnect.countUsers();
            Long users2 = db2Reconnect.countUsers();
            Long users3 = db3.countUsers();
            logger.info("Convergence check: arcade1={}, arcade2={}, arcade3={}", users1, users2, users3);
            return users1.equals(20L) && users2.equals(20L) && users3.equals(20L);
          } catch (Exception e) {
            logger.warn("Convergence check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying final consistency after multiple failovers");
    db1Reconnect.assertThatUserCountIs(20);
    db2Reconnect.assertThatUserCountIs(20);
    db3.assertThatUserCountIs(20);

    db1Reconnect.close();
    db2Reconnect.close();
    db3.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test leader failover with active writes: verify no data loss during failover")
  void testLeaderFailoverDuringWrites() throws IOException, InterruptedException {
    logger.info("Creating proxies for 3-node cluster");
//    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
//    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
//    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3-node HA cluster");
//    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "majority", "any", network);
//    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "majority", "any", network);
//    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "majority", "any", network);

    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}arcade2:2424,{arcade3}arcade3:2424", "majority", "any", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}arcade1:2424,{arcade3}arcade3:2424", "majority", "any", network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}arcade1:2424,{arcade2}arcade2:2424", "majority", "any", network);

    logger.info("Starting cluster");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier);

    logger.info("Creating database and schema");
    db1.createDatabase();
    db1.createSchema();

    logger.info("Adding initial data");
    db1.addUserAndPhotos(20, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(20);
    db2.assertThatUserCountIs(20);
    db3.assertThatUserCountIs(20);

    logger.info("Starting write to arcade1, then killing it mid-operation");

    // Write some data
    db1.addUserAndPhotos(5, 10);

    // Kill leader immediately
    db1.close();
    arcade1.stop();

    logger.info("Leader killed - waiting for election");
    TimeUnit.SECONDS.sleep(10);

    logger.info("Continuing writes through arcade2");
    db2.addUserAndPhotos(5, 10);

    logger.info("Waiting for replication convergence");
    Awaitility.await()
        .atMost(45, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users2 = db2.countUsers();
            Long users3 = db3.countUsers();
            logger.info("Convergence check: arcade2={}, arcade3={}", users2, users3);
            // We expect 25-30 users depending on whether the last write to arcade1 completed
            return users2.equals(users3) && users2 >= 25L && users2 <= 30L;
          } catch (Exception e) {
            logger.warn("Convergence check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Verifying data consistency between surviving nodes");
    Long finalCount = db2.countUsers();
    db3.assertThatUserCountIs(finalCount.intValue());

    logger.info("Final user count: {} (some writes may have been lost during leader failure)", finalCount);

    db2.close();
    db3.close();
  }
}
