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
package com.arcadedb.server.ha.ratis;

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.ha.ratis.RaftHAServer;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for the Ratis-based HA system. Starts 3 ArcadeDB servers with HA_ENABLED=true,
 * verifies leader election, creates a database, writes data on the leader, and verifies replication
 * to all nodes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("IntegrationTest")
class RaftReplicationIT {

  private static final int    SERVER_COUNT  = 3;
  private static final String DATABASE_NAME = "raft-test-db";
  private static final int    BASE_HA_PORT  = 22424;
  private static final int    BASE_HTTP_PORT = 22480;

  private ArcadeDBServer[] servers;

  @BeforeEach
  void setUp() throws Exception {
    GlobalConfiguration.TEST.setValue(true);
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue("test1234");

    // Clean up any leftover databases and Ratis storage from previous runs
    for (int i = 0; i < SERVER_COUNT; i++)
      FileUtils.deleteRecursively(new File("./target/raft-databases" + i));
    FileUtils.deleteRecursively(new File("./target/ratis-storage"));

    // Create and pre-populate the database for each server
    for (int i = 0; i < SERVER_COUNT; i++) {
      final String dbPath = "./target/raft-databases" + i + "/" + DATABASE_NAME;
      if (i == 0) {
        // Create the database on the first server
        final DatabaseFactory factory = new DatabaseFactory(dbPath);
        try (final Database db = factory.create()) {
          db.transaction(() -> {
            final var type = db.getSchema().buildVertexType().withName("TestVertex").withTotalBuckets(3).create();
            type.createProperty("id", Long.class);
            type.createProperty("name", String.class);
            db.getSchema().createTypeIndex(com.arcadedb.schema.Schema.INDEX_TYPE.LSM_TREE, true, "TestVertex", "id");
          });
        }
      } else {
        // Copy the database to other server directories
        try {
          FileUtils.copyDirectory(new File("./target/raft-databases0/" + DATABASE_NAME),
              new File(dbPath));
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    // Build the server address list
    final StringBuilder serverList = new StringBuilder();
    for (int i = 0; i < SERVER_COUNT; i++) {
      if (i > 0)
        serverList.append(",");
      serverList.append("localhost:").append(BASE_HA_PORT + i);
    }

    // Start all servers
    servers = new ArcadeDBServer[SERVER_COUNT];
    for (int i = 0; i < SERVER_COUNT; i++) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, Constants.PRODUCT + "_raft_" + i);
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/raft-databases" + i);
      config.setValue(GlobalConfiguration.HA_ENABLED, true);
      // Ratis is the only HA engine - no flag needed
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList.toString());
      config.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS, String.valueOf(BASE_HA_PORT + i));
      config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "raft-test-cluster");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(BASE_HTTP_PORT + i));
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");

      servers[i] = new ArcadeDBServer(config);
      servers[i].start();
    }

    // Wait for a Ratis leader to be elected
    waitForRatisLeader();
  }

  @AfterEach
  void tearDown() {
    if (servers != null)
      for (int i = servers.length - 1; i >= 0; i--)
        if (servers[i] != null)
          try {
            servers[i].stop();
          } catch (final Exception e) {
            // ignore
          }

    // Allow ports to be released
    try { Thread.sleep(2000); } catch (final InterruptedException e) { Thread.currentThread().interrupt(); }

    for (int i = 0; i < SERVER_COUNT; i++)
      FileUtils.deleteRecursively(new File("./target/raft-databases" + i));

    // Clean up Ratis storage (all possible paths)
    FileUtils.deleteRecursively(new File("./target/ratis-storage"));
    // Also clean from the server root path perspective
    for (int i = 0; i < SERVER_COUNT; i++)
      FileUtils.deleteRecursively(new File("./target/ratis-storage/localhost:" + (BASE_HA_PORT + i)));

    GlobalConfiguration.resetAll();

    TestServerHelper.checkActiveDatabases(true);
  }

  @Test
  void testLeaderElection() {
    // Verify exactly one Ratis leader exists
    int leaderCount = 0;
    for (final ArcadeDBServer server : servers)
      if (server.getRaftHA() != null && server.getRaftHA().isLeader())
        leaderCount++;

    assertThat(leaderCount).isEqualTo(1);
  }

  @Test
  void testWriteOnLeader() {
    // Find the leader server
    ArcadeDBServer leader = null;
    for (final ArcadeDBServer server : servers)
      if (server.getRaftHA() != null && server.getRaftHA().isLeader()) {
        leader = server;
        break;
      }

    assertThat(leader).isNotNull();

    // Write 10 vertices on the leader, one per transaction
    final Database leaderDb = leader.getDatabase(DATABASE_NAME);
    for (int i = 0; i < 10; i++) {
      final int idx = i;
      leaderDb.transaction(() -> {
        final MutableVertex v = leaderDb.newVertex("TestVertex");
        v.set("id", (long) idx);
        v.set("name", "vertex-" + idx);
        v.save();
      });
    }

    // Verify the leader has 10 vertices using a scan (not cached count)
    final long leaderCount = leaderDb.query("sql", "SELECT count(*) as cnt FROM TestVertex")
        .nextIfAvailable().getProperty("cnt", 0L);
    assertThat(leaderCount).as("Leader should have 10 vertices").isEqualTo(10);

    // Wait for replication to followers and verify
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          for (final ArcadeDBServer server : servers) {
            final Database db = server.getDatabase(DATABASE_NAME);
            final long count = db.query("sql", "SELECT count(*) as cnt FROM TestVertex")
                .nextIfAvailable().getProperty("cnt", 0L);
            assertThat(count)
                .as("Server %s should have 10 vertices", server.getServerName())
                .isEqualTo(10);
          }
        });
  }

  @Test
  void testWriteOnFollowerRedirects() {
    // Find a follower server (not the leader)
    ArcadeDBServer follower = null;
    for (final ArcadeDBServer server : servers)
      if (server.getRaftHA() != null && !server.getRaftHA().isLeader()) {
        follower = server;
        break;
      }

    assertThat(follower).isNotNull();

    // Writing on a follower should throw an exception indicating the leader address
    final Database followerDb = follower.getDatabase(DATABASE_NAME);
    // ServerIsNotTheLeaderException extends NeedRetryException, so it's rethrown directly (not wrapped)
    assertThatThrownBy(() -> followerDb.transaction(() -> {
      final MutableVertex v = followerDb.newVertex("TestVertex");
      v.set("id", 100L);
      v.set("name", "follower-vertex");
      v.save();
    })).isInstanceOf(com.arcadedb.network.binary.ServerIsNotTheLeaderException.class)
        .satisfies(e -> {
          final var notLeader = (com.arcadedb.network.binary.ServerIsNotTheLeaderException) e;
          assertThat(notLeader.getLeaderAddress()).isNotNull();
          assertThat(notLeader.getLeaderAddress()).contains("localhost:");
        });
  }

  @Test
  void testClusterStatus() {
    // Verify cluster status API
    for (final ArcadeDBServer server : servers) {
      final RaftHAServer raftHA = server.getRaftHA();
      assertThat(raftHA).isNotNull();
      assertThat(raftHA.getClusterName()).isEqualTo("raft-test-cluster");
      assertThat(raftHA.getConfiguredServers()).isEqualTo(3);
      assertThat(raftHA.getLeaderName()).isNotNull();
      assertThat(raftHA.getElectionStatus()).isIn("LEADER", "FOLLOWER");
    }

    // Verify exactly one leader
    long leaderCount = 0;
    for (final ArcadeDBServer server : servers)
      if (server.getRaftHA().isLeader())
        leaderCount++;
    assertThat(leaderCount).isEqualTo(1);
  }

  @Test
  void testPeerHTTPAddresses() {
    // Verify each peer has a resolvable HTTP address
    for (final ArcadeDBServer server : servers) {
      final RaftHAServer raftHA = server.getRaftHA();
      final String leaderAddr = raftHA.getLeaderHTTPAddress();
      assertThat(leaderAddr).isNotNull();
      assertThat(leaderAddr).contains("localhost:");
    }

    // Verify replica addresses are populated
    for (final ArcadeDBServer server : servers) {
      final String replicas = server.getRaftHA().getReplicaAddresses();
      assertThat(replicas).isNotEmpty();
    }
  }

  // testSnapshotEndpoint: The SnapshotHttpHandler is functional but requires auth integration
  // for HTTP testing. The snapshot installation flow is tested indirectly when Ratis triggers
  // notifyInstallSnapshotFromLeader() on a lagging follower.

  private void waitForRatisLeader() {
    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (final ArcadeDBServer server : servers)
            if (server.getRaftHA() != null && server.getRaftHA().isLeader())
              return true;
          return false;
        });

    LogManager.instance().log(this, Level.INFO, "Ratis leader elected");
  }
}
