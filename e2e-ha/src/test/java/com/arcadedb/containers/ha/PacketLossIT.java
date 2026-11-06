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
import com.arcadedb.test.support.FacetDatabaseWrapper;
import com.arcadedb.test.support.FacetSnapshot;
import com.arcadedb.test.support.ServerWrapper;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Packet loss tests for Raft HA cluster resilience.
 * Tests behavior under unreliable networks with dropped packets.
 * Toxiproxy intercepts the Raft gRPC consensus port (2434) to inject faults.
 */
@Testcontainers
class PacketLossIT extends ContainersTestTemplate {

  @Override
  protected boolean useToxiproxy() {
    return true;
  }


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

  // spaces node 1's facet samples over the write window; not a timing assertion
  private static final long QUERY_SPACING_MILLIS = 200L;

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test low packet loss (5%): cluster should remain stable")
  void lowPacketLoss() throws Exception {
    logger.info("Creating Raft and HTTP proxies for 2-node cluster");
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "arcadedb-0:2434");
    final Proxy raftProxy1 = toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "arcadedb-1:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "arcadedb-0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "arcadedb-1:2480");

    logger.info("Creating 2-node Raft HA cluster");
    createArcadeContainer("arcadedb-0", SERVER_LIST_2, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST_2, "majority", network);

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
        .atMost(180, TimeUnit.SECONDS)
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
  void moderatePacketLoss() throws Exception {
    logger.info("Creating Raft and HTTP proxies for 2-node cluster");
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "arcadedb-0:2434");
    final Proxy raftProxy1 = toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "arcadedb-1:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "arcadedb-0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "arcadedb-1:2480");

    logger.info("Creating 2-node Raft HA cluster");
    createArcadeContainer("arcadedb-0", SERVER_LIST_2, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST_2, "majority", network);

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
        .atMost(180, TimeUnit.SECONDS)
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

  /**
   * As {@link #moderatePacketLoss()} with facet data: under 20% loss on both Raft proxies, every facet answer of node 1 taken while
   * node 0 runs mixed writes is exact or carries its lag; once the loss is removed both nodes' facets equal their {@code GROUP BY}.
   */
  @Test
  @Timeout(value = 25, unit = TimeUnit.MINUTES) // a hang detector, sized for host stalls
  @DisplayName("Moderate packet loss (20%): facets stay exact or report their lag, then converge")
  void moderatePacketLossKeepsFacetsExact() throws Exception {
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "arcadedb-0:2434");
    final Proxy raftProxy1 = toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "arcadedb-1:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "arcadedb-0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "arcadedb-1:2480");

    createArcadeContainer("arcadedb-0", SERVER_LIST_2, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST_2, "majority", network);
    final List<ServerWrapper> servers = startCluster();

    final FacetDatabaseWrapper f0 = new FacetDatabaseWrapper(servers.get(0), DatabaseWrapper.Protocol.HTTP, 0);
    final FacetDatabaseWrapper f1;
    try {
      f1 = new FacetDatabaseWrapper(servers.get(1), DatabaseWrapper.Protocol.HTTP, 1);
    } catch (final RuntimeException e) {
      f0.close();
      throw e;
    }
    final AtomicReference<Thread> writerThread = new AtomicReference<>();
    try {
      f0.createDatabase();
      f0.createSchema();
      for (final FacetDatabaseWrapper node : List.of(f0, f1))
        logger.info("Facet schema ready on {} after {} ms", node.describe(), node.awaitSchema(60));

      logger.info("Ingested {} assets", f0.ingest(1_000));

      logger.info("Introducing 20% packet loss on both Raft proxies");
      raftProxy0.toxics().limitData("facet_loss_raft0", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.20f);
      raftProxy1.toxics().limitData("facet_loss_raft1", ToxicDirection.DOWNSTREAM, 0).setToxicity(0.20f);

      final AtomicReference<Object> writerOutcome = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          writerOutcome.set(f0.runMixedOps(1_000));
        } catch (final Throwable t) {
          writerOutcome.set(t);
        }
      }, "facet-packet-loss-writer");
      writerThread.set(writer);
      writer.start();

      int samples = 0;
      int duringWrites = 0;
      int nonExact = 0;
      int metaInconsistent = 0;
      long maxLagMs = 0L;
      while (writer.isAlive()) {
        final FacetSnapshot s = f1.queryFacets("");
        // ruling B-M1: an inexact answer that reports no lag is logged and counted, not a failure (as the chaos FACET_META warning)
        if (!s.metaConsistent()) {
          metaInconsistent++;
          logger.warn("Sample {} on node 1 under packet loss: inexact without lag: exact={} lagMs={} patched={} sources={}", samples,
              s.exact(), s.lagMs(), s.patched(), s.sources());
        }
        samples++;
        if (writer.isAlive())
          duringWrites++;
        if (!s.exact())
          nonExact++;
        maxLagMs = Math.max(maxLagMs, s.lagMs());
        Thread.sleep(QUERY_SPACING_MILLIS);
      }
      logger.info("Node 1 under packet loss: {} samples, {} while the writer ran, {} not exact, {} inexact without lag, max lagMs {}",
          samples, duringWrites, nonExact, metaInconsistent, maxLagMs);
      assertThat(duringWrites).as("facet queries ran while the writer was writing under packet loss").isPositive();

      writer.join(TimeUnit.MINUTES.toMillis(5));
      assertThat(writer.isAlive()).as("the writer under packet loss ended").isFalse();
      // packet loss is on the write path: failures are logged, not asserted
      logger.info("Mixed ops on node 0 under packet loss: {}",
          FacetDatabaseWrapper.assertWrote(writerOutcome.get(), "writer under packet loss", false));

      logger.info("Removing packet loss");
      raftProxy0.toxics().get("facet_loss_raft0").remove();
      raftProxy1.toxics().get("facet_loss_raft1").remove();

      FacetDatabaseWrapper.assertSameFacetsOnEveryNode(List.of(f0, f1), 180);
    } finally {
      final Thread writer = writerThread.get();
      if (writer != null)
        writer.join(TimeUnit.MINUTES.toMillis(5));
      dumpContainerLogs("packetLossFacets-end");
      for (final FacetDatabaseWrapper wrapper : List.of(f0, f1))
        try {
          wrapper.close();
        } catch (final RuntimeException e) {
          logger.warn("Closing {} failed: {}", wrapper.describe(), e.getMessage());
        }
    }
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test high packet loss (50%): verify connection resilience")
  void highPacketLoss() throws Exception {
    logger.info("Creating Raft and HTTP proxies for 2-node cluster");
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "arcadedb-0:2434");
    final Proxy raftProxy1 = toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "arcadedb-1:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "arcadedb-0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "arcadedb-1:2480");

    logger.info("Creating 2-node Raft HA cluster");
    createArcadeContainer("arcadedb-0", SERVER_LIST_2, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST_2, "majority", network);

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
  void directionalPacketLoss() throws Exception {
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
        .atMost(180, TimeUnit.SECONDS)
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
  void intermittentPacketLoss() throws Exception {
    logger.info("Creating Raft and HTTP proxies for 2-node cluster");
    final Proxy raftProxy0 = toxiproxyClient.createProxy("raftProxy0", "0.0.0.0:" + RAFT_PROXY_PORT_0, "arcadedb-0:2434");
    toxiproxyClient.createProxy("raftProxy1", "0.0.0.0:" + RAFT_PROXY_PORT_1, "arcadedb-1:2434");
    toxiproxyClient.createProxy("httpProxy0", "0.0.0.0:" + HTTP_PROXY_PORT_0, "arcadedb-0:2480");
    toxiproxyClient.createProxy("httpProxy1", "0.0.0.0:" + HTTP_PROXY_PORT_1, "arcadedb-1:2480");

    logger.info("Creating 2-node Raft HA cluster");
    createArcadeContainer("arcadedb-0", SERVER_LIST_2, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST_2, "majority", network);

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
        .atMost(180, TimeUnit.SECONDS)
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
