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
 * Network latency and delay tests for HA cluster resilience.
 * Tests behavior under high latency, jitter, and asymmetric delays.
 */
@Testcontainers
public class NetworkDelayIT extends ContainersTestTemplate {

  @Test
  @Disabled
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test symmetric network delay: all nodes experience same latency")
  void testSymmetricDelay() throws IOException {
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

    logger.info("Adding initial data with no delay");
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);
    db3.assertThatUserCountIs(10);

    logger.info("Introducing 200ms symmetric latency on all connections");
    arcade1Proxy.toxics().latency("latency_arcade1", ToxicDirection.DOWNSTREAM, 200);
    arcade2Proxy.toxics().latency("latency_arcade2", ToxicDirection.DOWNSTREAM, 200);
    arcade3Proxy.toxics().latency("latency_arcade3", ToxicDirection.DOWNSTREAM, 200);

    logger.info("Adding data under latency conditions");
    long startTime = System.currentTimeMillis();
    db1.addUserAndPhotos(20, 10);
    long duration = System.currentTimeMillis() - startTime;
    logger.info("Write operation took {}ms under 200ms latency", duration);

    logger.info("Waiting for replication with latency");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            Long users3 = db3.countUsers();
            logger.info("Latency replication check: arcade1={}, arcade2={}, arcade3={}", users1, users2, users3);
            return users1.equals(30L) && users2.equals(30L) && users3.equals(30L);
          } catch (Exception e) {
            logger.warn("Latency check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing latency");
    arcade1Proxy.toxics().get("latency_arcade1").remove();
    arcade2Proxy.toxics().get("latency_arcade2").remove();
    arcade3Proxy.toxics().get("latency_arcade3").remove();

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
  @DisplayName("Test asymmetric delay: leader has higher latency than replicas")
  void testAsymmetricLeaderDelay() throws IOException, InterruptedException {
    logger.info("Creating proxies for 3-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3-node HA cluster");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "majority", "any", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "majority", "any", network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "majority", "any", network);

    logger.info("Starting cluster - arcade1 will be leader");
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

    logger.info("Introducing high latency (500ms) on leader (arcade1)");
    arcade1Proxy.toxics().latency("leader_latency", ToxicDirection.DOWNSTREAM, 500);
    arcade1Proxy.toxics().latency("leader_latency_up", ToxicDirection.UPSTREAM, 500);

    logger.info("Waiting for cluster to potentially adjust");
    TimeUnit.SECONDS.sleep(5);

    logger.info("Adding data from a replica under leader latency");
    db2.addUserAndPhotos(15, 10);

    logger.info("Waiting for replication despite leader latency");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            Long users3 = db3.countUsers();
            logger.info("Asymmetric latency check: arcade1={}, arcade2={}, arcade3={}", users1, users2, users3);
            return users1.equals(25L) && users2.equals(25L) && users3.equals(25L);
          } catch (Exception e) {
            logger.warn("Asymmetric latency check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing leader latency");
    arcade1Proxy.toxics().get("leader_latency").remove();
    arcade1Proxy.toxics().get("leader_latency_up").remove();

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

    logger.info("Introducing 300ms latency with 150ms jitter");
    arcade1Proxy.toxics().latency("jitter_arcade1", ToxicDirection.DOWNSTREAM, 300).setJitter(150);
    arcade2Proxy.toxics().latency("jitter_arcade2", ToxicDirection.DOWNSTREAM, 300).setJitter(150);

    logger.info("Adding data under jittery network conditions");
    db1.addUserAndPhotos(20, 10);

    logger.info("Waiting for replication with jitter");
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long users2 = db2.countUsers();
            logger.info("Jitter check: arcade1={}, arcade2={}", users1, users2);
            return users1.equals(30L) && users2.equals(30L);
          } catch (Exception e) {
            logger.warn("Jitter check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing jitter");
    arcade1Proxy.toxics().get("jitter_arcade1").remove();
    arcade2Proxy.toxics().get("jitter_arcade2").remove();

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
    logger.info("Creating proxies for 2-node cluster");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");

    logger.info("Creating 2-node HA cluster with quorum=none for testing");
    createArcadeContainer("arcade1", "{arcade2}proxy:8667", "none", "any", network);
    createArcadeContainer("arcade2", "{arcade1}proxy:8666", "none", "any", network);

    logger.info("Starting cluster");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);

    logger.info("Creating database and initial data");
    db1.createDatabase();
    db1.createSchema();
    db1.addUserAndPhotos(5, 10);

    logger.info("Verifying initial replication");
    db1.assertThatUserCountIs(5);
    db2.assertThatUserCountIs(5);

    logger.info("Introducing extreme latency (2000ms)");
    arcade1Proxy.toxics().latency("extreme_latency", ToxicDirection.DOWNSTREAM, 2000);

    logger.info("Adding data under extreme latency (should complete but be very slow)");
    long startTime = System.currentTimeMillis();
    db1.addUserAndPhotos(3, 5);
    long duration = System.currentTimeMillis() - startTime;
    logger.info("Write with extreme latency took {}ms", duration);

    logger.info("Waiting for eventual replication");
    Awaitility.await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users2 = db2.countUsers();
            logger.info("Extreme latency replication check: arcade2={}", users2);
            return users2.equals(8L);
          } catch (Exception e) {
            logger.warn("Extreme latency check failed: {}", e.getMessage());
            return false;
          }
        });

    logger.info("Removing extreme latency");
    arcade1Proxy.toxics().get("extreme_latency").remove();

    logger.info("Verifying final consistency");
    db1.assertThatUserCountIs(8);
    db2.assertThatUserCountIs(8);

    db1.close();
    db2.close();
  }
}
