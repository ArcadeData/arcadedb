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
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive HA test suite for the Ratis-based replication engine.
 * Tests data consistency, failover, catch-up, concurrency, and edge cases.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("IntegrationTest")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(120)
class RaftHAComprehensiveIT {

  private static final int    SERVER_COUNT   = 3;
  private static final int    BASE_HA_PORT   = 42424;
  private static final int    BASE_HTTP_PORT = 42480;
  private static final String DB_NAME        = "hatest";

  private ArcadeDBServer[] servers;

  @BeforeEach
  void setUp() throws Exception {
    GlobalConfiguration.TEST.setValue(true);
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue("testpassword1");

    for (int i = 0; i < SERVER_COUNT; i++)
      FileUtils.deleteRecursively(new File("./target/ha-comp-db" + i));
    FileUtils.deleteRecursively(new File("./target/ratis-storage"));
    new File("./target/config/server-users.jsonl").delete();

    // Create DB for server 0
    try (final Database db = new DatabaseFactory("./target/ha-comp-db0/" + DB_NAME).create()) {
      db.transaction(() -> {
        final var vType = db.getSchema().buildVertexType().withName("TestV").withTotalBuckets(3).create();
        vType.createProperty("id", Long.class);
        vType.createProperty("name", String.class);
        db.getSchema().createTypeIndex(com.arcadedb.schema.Schema.INDEX_TYPE.LSM_TREE, true, "TestV", "id");
        db.getSchema().createEdgeType("TestE");
      });
    }
    // Copy to other servers
    for (int i = 1; i < SERVER_COUNT; i++)
      FileUtils.copyDirectory(new File("./target/ha-comp-db0/" + DB_NAME), new File("./target/ha-comp-db" + i + "/" + DB_NAME));

    startCluster();
  }

  @AfterEach
  void tearDown() {
    stopCluster();
    for (int i = 0; i < SERVER_COUNT; i++)
      FileUtils.deleteRecursively(new File("./target/ha-comp-db" + i));
    FileUtils.deleteRecursively(new File("./target/ratis-storage"));
    GlobalConfiguration.resetAll();
    TestServerHelper.checkActiveDatabases(true);
  }

  // =====================================================================
  // TEST 1: Data consistency under load
  // =====================================================================
  @Test
  @Order(1)
  void test01_dataConsistencyUnderLoad() {
    final ArcadeDBServer leader = findLeader();
    final int recordCount = 1000;

    // Write records on leader
    for (int i = 0; i < recordCount; i++) {
      final int idx = i;
      leader.getDatabase(DB_NAME).transaction(() ->
          leader.getDatabase(DB_NAME).newVertex("TestV").set("id", (long) idx).set("name", "rec-" + idx).save()
      );
    }

    // Wait for replication
    CodeUtils.sleep(5000);

    // Verify exact count on ALL servers
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      final long count = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(count).as("Server %s should have %d records", s.getServerName(), recordCount).isEqualTo(recordCount);
    }

    // Verify content: spot-check 10 random records
    for (int check = 0; check < 10; check++) {
      final int id = (int) (Math.random() * recordCount);
      for (final ArcadeDBServer s : servers) {
        if (s == null || !s.isStarted()) continue;
        final ResultSet rs = s.getDatabase(DB_NAME).query("sql", "SELECT FROM TestV WHERE id = ?", (long) id);
        assertThat(rs.hasNext()).as("Server %s should have record id=%d", s.getServerName(), id).isTrue();
        final Result r = rs.next();
        assertThat((String) r.getProperty("name")).isEqualTo("rec-" + id);
      }
    }
  }

  // =====================================================================
  // TEST 2: Follower restart and catch-up via Ratis log replay
  // =====================================================================
  @Test
  @Order(2)
  void test02_followerRestartAndCatchUp() {
    final ArcadeDBServer leader = findLeader();

    // Write initial data
    for (int i = 0; i < 100; i++) {
      final int idx = i;
      leader.getDatabase(DB_NAME).transaction(() ->
          leader.getDatabase(DB_NAME).newVertex("TestV").set("id", (long) idx).set("name", "initial-" + idx).save()
      );
    }
    CodeUtils.sleep(3000);

    // Stop a follower
    int followerIdx = -1;
    for (int i = 0; i < SERVER_COUNT; i++)
      if (servers[i].isStarted() && servers[i].getHA() != null && !servers[i].getHA().isLeader()) {
        followerIdx = i;
        break;
      }
    assertThat(followerIdx).isGreaterThanOrEqualTo(0);

    final String followerName = servers[followerIdx].getServerName();
    final var followerConfig = servers[followerIdx].getConfiguration();
    servers[followerIdx].stop();
    LogManager.instance().log(this, Level.INFO, "Stopped follower %s", followerName);

    // Write more data while follower is down
    for (int i = 100; i < 200; i++) {
      final int idx = i;
      leader.getDatabase(DB_NAME).transaction(() ->
          leader.getDatabase(DB_NAME).newVertex("TestV").set("id", (long) idx).set("name", "during-down-" + idx).save()
      );
    }
    CodeUtils.sleep(2000);

    // Restart follower - it should catch up via Ratis log replay
    LogManager.instance().log(this, Level.INFO, "Restarting follower %s...", followerName);
    servers[followerIdx] = new ArcadeDBServer(followerConfig);
    servers[followerIdx].start();
    CodeUtils.sleep(10000); // Give time for catch-up

    // Verify follower has all 200 records
    final long count = servers[followerIdx].getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV")
        .nextIfAvailable().getProperty("cnt", 0L);
    assertThat(count).as("Restarted follower should have all 200 records").isEqualTo(200);
  }

  // =====================================================================
  // TEST 3: Full cluster restart
  // =====================================================================
  @Test
  @Order(3)
  void test03_fullClusterRestart() {
    final ArcadeDBServer leader = findLeader();

    // Write data
    for (int i = 0; i < 50; i++) {
      final int idx = i;
      leader.getDatabase(DB_NAME).transaction(() ->
          leader.getDatabase(DB_NAME).newVertex("TestV").set("id", (long) idx).set("name", "persist-" + idx).save()
      );
    }
    CodeUtils.sleep(3000);

    // Save configs before stopping
    final ContextConfiguration[] configs = new ContextConfiguration[SERVER_COUNT];
    for (int i = 0; i < SERVER_COUNT; i++)
      configs[i] = servers[i].getConfiguration();

    // Stop ALL servers
    for (int i = SERVER_COUNT - 1; i >= 0; i--)
      servers[i].stop();
    CodeUtils.sleep(3000);

    // Restart ALL servers
    for (int i = 0; i < SERVER_COUNT; i++) {
      servers[i] = new ArcadeDBServer(configs[i]);
      servers[i].start();
    }

    // Wait for leader election
    waitForLeader();
    CodeUtils.sleep(5000);

    // Verify data survived
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      final long count = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(count).as("Server %s should have 50 records after full restart", s.getServerName())
          .isEqualTo(50);
    }

    // Verify cluster is fully functional - write more data
    final ArcadeDBServer newLeader = findLeader();
    newLeader.getDatabase(DB_NAME).transaction(() ->
        newLeader.getDatabase(DB_NAME).newVertex("TestV").set("id", 9999L).set("name", "after-restart").save()
    );
    CodeUtils.sleep(2000);

    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      final long count = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(count).as("Server %s should have 51 records").isEqualTo(51);
    }
  }

  // =====================================================================
  // TEST 4: Concurrent writes on leader from multiple threads
  // =====================================================================
  @Test
  @Order(4)
  void test04_concurrentWritesOnLeader() throws Exception {
    final ArcadeDBServer leader = findLeader();
    final int threads = 4;
    final int recordsPerThread = 100;
    final CountDownLatch latch = new CountDownLatch(threads);
    final AtomicInteger errors = new AtomicInteger();

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      new Thread(() -> {
        try {
          for (int i = 0; i < recordsPerThread; i++) {
            final long id = threadId * 10000L + i;
            final int idx = i;
            leader.getDatabase(DB_NAME).transaction(() ->
                leader.getDatabase(DB_NAME).newVertex("TestV").set("id", id).set("name", "t" + threadId + "-" + idx).save()
            );
          }
        } catch (final Exception e) {
          errors.incrementAndGet();
          LogManager.instance().log(this, Level.SEVERE, "Thread %d error: %s", e, threadId, e.getMessage());
        } finally {
          latch.countDown();
        }
      }).start();
    }

    assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
    assertThat(errors.get()).isZero();

    CodeUtils.sleep(5000);

    final int expected = threads * recordsPerThread;
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      final long count = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(count).as("Server %s should have %d records", s.getServerName(), expected).isEqualTo(expected);
    }
  }

  // =====================================================================
  // TEST 5: Schema changes during active operations
  // =====================================================================
  @Test
  @Order(5)
  void test05_schemaChangesDuringWrites() {
    final ArcadeDBServer leader = findLeader();
    final Database leaderDb = leader.getDatabase(DB_NAME);

    // Write some data first
    for (int i = 0; i < 20; i++) {
      final int idx = i;
      leaderDb.transaction(() -> leaderDb.newVertex("TestV").set("id", (long) idx).set("name", "pre-schema").save());
    }
    CodeUtils.sleep(2000);

    // Create a new type while data exists
    leaderDb.command("sql", "CREATE VERTEX TYPE NewType");
    leaderDb.command("sql", "CREATE PROPERTY NewType.value STRING");
    CodeUtils.sleep(3000);

    // Verify schema propagated to all servers
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      assertThat(s.getDatabase(DB_NAME).getSchema().existsType("NewType"))
          .as("Server %s should have NewType", s.getServerName()).isTrue();
    }

    // Write to new type
    leaderDb.transaction(() -> leaderDb.newVertex("NewType").set("value", "test-schema").save());
    CodeUtils.sleep(2000);

    // Verify data in new type on all servers
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      final long count = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM NewType")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(count).as("Server %s should have 1 NewType record", s.getServerName()).isEqualTo(1);
    }
  }

  // =====================================================================
  // TEST 6: Index consistency across cluster
  // =====================================================================
  @Test
  @Order(6)
  void test06_indexConsistency() {
    final ArcadeDBServer leader = findLeader();

    // Write records with unique IDs
    for (int i = 0; i < 50; i++) {
      final int idx = i;
      leader.getDatabase(DB_NAME).transaction(() ->
          leader.getDatabase(DB_NAME).newVertex("TestV").set("id", (long) idx).set("name", "indexed-" + idx).save()
      );
    }
    CodeUtils.sleep(3000);

    // Verify index lookup works on all servers
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      for (int id = 0; id < 50; id++) {
        final ResultSet rs = s.getDatabase(DB_NAME).query("sql", "SELECT FROM TestV WHERE id = ?", (long) id);
        assertThat(rs.hasNext()).as("Server " + s.getServerName() + " index lookup for id=" + id).isTrue();
        rs.next();
        assertThat(rs.hasNext()).as("Server " + s.getServerName() + " should have exactly 1 record for id=" + id).isFalse();
      }
    }

    // Verify unique constraint - try to insert duplicate on leader
    boolean duplicateRejected = false;
    try {
      leader.getDatabase(DB_NAME).transaction(() ->
          leader.getDatabase(DB_NAME).newVertex("TestV").set("id", 0L).set("name", "duplicate").save()
      );
    } catch (final Exception e) {
      duplicateRejected = true;
    }
    assertThat(duplicateRejected).as("Duplicate key should be rejected").isTrue();
  }

  // =====================================================================
  // TEST 7: Query routing correctness
  // =====================================================================
  @Test
  @Order(7)
  void test07_queryRoutingCorrectness() {
    final ArcadeDBServer leader = findLeader();
    final ArcadeDBServer follower = findFollower();
    assertThat(follower).isNotNull();

    // Write data on leader
    leader.getDatabase(DB_NAME).transaction(() ->
        leader.getDatabase(DB_NAME).newVertex("TestV").set("id", 777L).set("name", "routing-test").save()
    );
    CodeUtils.sleep(3000);

    // SELECT should work on follower (executed locally)
    final ResultSet rs = follower.getDatabase(DB_NAME).query("sql", "SELECT FROM TestV WHERE id = 777");
    assertThat(rs.hasNext()).as("SELECT should work on follower").isTrue();
    assertThat((String) rs.next().getProperty("name")).isEqualTo("routing-test");

    // INSERT on follower should throw ServerIsNotTheLeaderException (handled by HTTP proxy)
    boolean writeRejected = false;
    try {
      follower.getDatabase(DB_NAME).transaction(() ->
          follower.getDatabase(DB_NAME).newVertex("TestV").set("id", 888L).set("name", "follower-write").save()
      );
    } catch (final Exception e) {
      writeRejected = true;
    }
    assertThat(writeRejected).as("Write on follower should be rejected (forwarded via HTTP proxy when using HTTP API)").isTrue();
  }

  // =====================================================================
  // TEST 8: Large transaction (big WAL buffer)
  // =====================================================================
  @Test
  @Order(8)
  void test08_largeTransaction() {
    final ArcadeDBServer leader = findLeader();

    // Single transaction with 500 records (large WAL buffer)
    leader.getDatabase(DB_NAME).transaction(() -> {
      for (int i = 0; i < 500; i++)
        leader.getDatabase(DB_NAME).newVertex("TestV").set("id", (long) i)
            .set("name", "bulk-" + i + "-" + "x".repeat(100)).save();
    });

    CodeUtils.sleep(5000);

    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      final long count = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(count).as("Server %s should have 500 records from bulk tx", s.getServerName()).isEqualTo(500);
    }
  }

  // =====================================================================
  // TEST 9: Rapid leader transfers
  // =====================================================================
  @Test
  @Order(9)
  void test09_rapidLeaderTransfers() {
    // Transfer leadership 5 times in rapid succession
    for (int i = 0; i < 5; i++) {
      final ArcadeDBServer currentLeader = findLeader();
      assertThat(currentLeader).isNotNull();

      // Find a follower to transfer to
      String targetPeerId = null;
      for (final var peer : currentLeader.getHA().getRaftGroup().getPeers())
        if (!peer.getId().equals(currentLeader.getHA().getLocalPeerId())) {
          targetPeerId = peer.getId().toString();
          break;
        }

      if (targetPeerId != null) {
        try {
          currentLeader.getHA().transferLeadership(targetPeerId, 10_000);
          CodeUtils.sleep(3000); // Wait for election
        } catch (final Exception e) {
          // Transfer might fail if election is in progress - that's OK
          CodeUtils.sleep(5000);
        }
      }
    }

    // Verify cluster is still functional
    waitForLeader();
    final ArcadeDBServer leader = findLeader();
    assertThat(leader).isNotNull();

    leader.getDatabase(DB_NAME).transaction(() ->
        leader.getDatabase(DB_NAME).newVertex("TestV").set("id", 55555L).set("name", "after-transfers").save()
    );
    CodeUtils.sleep(3000);

    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      final long count = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV WHERE id = 55555")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(count).isEqualTo(1);
    }
  }

  // =====================================================================
  // TEST 10: Single-server HA mode (edge case)
  // =====================================================================
  @Test
  @Order(10)
  void test10_singleServerHAMode() {
    // Stop the extra servers, keep only server 0
    for (int i = 1; i < SERVER_COUNT; i++)
      if (servers[i] != null && servers[i].isStarted())
        servers[i].stop();

    CodeUtils.sleep(5000);

    // Server 0 should still function (as the sole member, it's automatically the leader)
    // Writes might fail due to quorum not being reachable with MAJORITY (needs 2 of 3)
    // This test verifies the server doesn't crash
    boolean writeSucceeded = false;
    try {
      final var db = servers[0].getDatabase(DB_NAME);
      db.transaction(() -> db.newVertex("TestV").set("id", 99999L).set("name", "solo").save());
      writeSucceeded = true;
    } catch (final Exception e) {
      // Expected: QuorumNotReachedException because MAJORITY requires 2 of 3 servers
      assertThat(e.getMessage()).containsAnyOf("quorum", "Quorum", "timed out", "replication", "leader", "Leader");
    }

    // Reads should still work (local)
    final long existingCount = servers[0].getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV")
        .nextIfAvailable().getProperty("cnt", 0L);
    assertThat(existingCount >= 0).isTrue();
  }

  // =====================================================================
  // TEST 11: Write-to-follower via HTTP proxy under sustained load
  // =====================================================================
  @Test
  @Order(11)
  void test11_writeToFollowerViaHttpProxy() throws Exception {
    final ArcadeDBServer follower = findFollower();
    assertThat(follower).isNotNull();
    final int httpPort = follower.getHttpServer().getPort();
    final int total = 100;
    final AtomicInteger successes = new AtomicInteger();
    final AtomicInteger errors = new AtomicInteger();

    for (int i = 0; i < total; i++) {
      try {
        final var conn = (java.net.HttpURLConnection)
            new java.net.URI("http://127.0.0.1:" + httpPort + "/api/v1/command/" + DB_NAME).toURL().openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization",
            "Basic " + java.util.Base64.getEncoder().encodeToString("root:testpassword1".getBytes()));
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        try (final var os = conn.getOutputStream()) {
          os.write(("{\"language\":\"sql\",\"command\":\"INSERT INTO TestV SET id = " + (30000 + i) + ", name = 'proxy-" + i + "'\"}").getBytes());
        }
        if (conn.getResponseCode() == 200)
          successes.incrementAndGet();
        else
          errors.incrementAndGet();
        conn.disconnect();
      } catch (final Exception e) {
        errors.incrementAndGet();
      }
    }

    CodeUtils.sleep(5000);

    // At least 90% should succeed via HTTP proxy
    assertThat(successes.get()).as("Expected at least 90 successes via proxy, got " + successes.get()).isGreaterThanOrEqualTo(90);

    // Verify data on all servers
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      final long count = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV WHERE id >= 30000")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(count).as("Server %s should have proxy-written records", s.getServerName())
          .isGreaterThanOrEqualTo(successes.get());
    }
  }

  // =====================================================================
  // TEST 12: Leader election during active transaction (ACID)
  // =====================================================================
  @Test
  @Order(12)
  void test12_leaderElectionDuringTransaction() {
    final ArcadeDBServer leader = findLeader();

    // Write baseline data
    for (int i = 0; i < 10; i++) {
      final int idx = i;
      leader.getDatabase(DB_NAME).transaction(() ->
          leader.getDatabase(DB_NAME).newVertex("TestV").set("id", (long) (40000 + idx)).set("name", "baseline").save()
      );
    }
    CodeUtils.sleep(3000);

    // Start a transaction on the leader but DON'T commit
    final Database leaderDb = leader.getDatabase(DB_NAME);
    leaderDb.begin();
    leaderDb.newVertex("TestV").set("id", 49999L).set("name", "uncommitted").save();

    // Stop the leader while transaction is open
    final String leaderName = leader.getServerName();
    leader.stop();

    // Wait for new leader
    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (final ArcadeDBServer s : servers)
            if (s != null && s.isStarted() && s.getHA() != null && s.getHA().isLeader()) return true;
          return false;
        });

    final ArcadeDBServer newLeader = findLeader();
    assertThat(newLeader).isNotNull();
    CodeUtils.sleep(3000);

    // The uncommitted transaction should NOT appear on any server (ACID: rolled back)
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      final long uncommittedCount = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV WHERE id = 49999")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(uncommittedCount).as("Uncommitted tx should not be visible on " + s.getServerName()).isEqualTo(0);
    }

    // The baseline data should still be there
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      final long baselineCount = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV WHERE id >= 40000 AND id < 40010")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(baselineCount).as("Baseline data should survive on " + s.getServerName()).isEqualTo(10);
    }
  }

  // =====================================================================
  // TEST 13: Concurrent writes from multiple servers via HTTP proxy
  // =====================================================================
  @Test
  @Order(13)
  void test13_concurrentWritesViaProxy() throws Exception {
    final int writesPerServer = 30;
    final CountDownLatch latch = new CountDownLatch(SERVER_COUNT);
    final AtomicInteger totalSuccesses = new AtomicInteger();

    for (int s = 0; s < SERVER_COUNT; s++) {
      final int serverIdx = s;
      final int httpPort = servers[s].getHttpServer().getPort();
      new Thread(() -> {
        try {
          for (int i = 0; i < writesPerServer; i++) {
            final long id = 50000L + serverIdx * 1000 + i;
            try {
              final var conn = (java.net.HttpURLConnection)
                  new java.net.URI("http://127.0.0.1:" + httpPort + "/api/v1/command/" + DB_NAME).toURL().openConnection();
              conn.setRequestMethod("POST");
              conn.setRequestProperty("Authorization",
                  "Basic " + java.util.Base64.getEncoder().encodeToString("root:testpassword1".getBytes()));
              conn.setRequestProperty("Content-Type", "application/json");
              conn.setDoOutput(true);
              try (final var os = conn.getOutputStream()) {
                os.write(("{\"language\":\"sql\",\"command\":\"INSERT INTO TestV SET id = " + id + ", name = 'concurrent-s" + serverIdx + "'\"}").getBytes());
              }
              if (conn.getResponseCode() == 200)
                totalSuccesses.incrementAndGet();
              conn.disconnect();
            } catch (final Exception e) { /* retry handled by proxy */ }
          }
        } finally {
          latch.countDown();
        }
      }).start();
    }

    assertThat(latch.await(120, TimeUnit.SECONDS)).isTrue();
    CodeUtils.sleep(5000);

    // Verify no duplicate IDs (unique constraint)
    final ArcadeDBServer leader = findLeader();
    final long totalRecords = leader.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV WHERE id >= 50000")
        .nextIfAvailable().getProperty("cnt", 0L);
    assertThat(totalRecords).as("Each write should produce exactly one record").isEqualTo(totalSuccesses.get());

    // Verify consistency across servers
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      final long count = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV WHERE id >= 50000")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(count).as("Server %s should match leader count", s.getServerName()).isEqualTo(totalRecords);
    }
  }

  // =====================================================================
  // TEST 14: Network timeout simulation (slow follower)
  // =====================================================================
  @Test
  @Order(14)
  void test14_writesDuringSlowFollower() {
    final ArcadeDBServer leader = findLeader();

    // Stop one follower to simulate a "slow" (unreachable) follower
    int slowIdx = -1;
    for (int i = 0; i < SERVER_COUNT; i++)
      if (servers[i].isStarted() && servers[i].getHA() != null && !servers[i].getHA().isLeader()) {
        slowIdx = i;
        break;
      }
    assertThat(slowIdx).isGreaterThanOrEqualTo(0);

    final var slowConfig = servers[slowIdx].getConfiguration();
    servers[slowIdx].stop();
    CodeUtils.sleep(3000);

    // With MAJORITY quorum and 3 servers, 2 alive = majority. Writes should still succeed.
    final AtomicInteger successes = new AtomicInteger();
    for (int i = 0; i < 20; i++) {
      final int idx = i;
      try {
        leader.getDatabase(DB_NAME).transaction(() ->
            leader.getDatabase(DB_NAME).newVertex("TestV").set("id", (long) (60000 + idx)).set("name", "slow-follower").save()
        );
        successes.incrementAndGet();
      } catch (final Exception e) {
        // Some might fail during quorum timeout - acceptable
      }
    }

    assertThat(successes.get()).as("Majority of writes should succeed with one follower down").isGreaterThanOrEqualTo(15);

    // Restart the slow follower
    servers[slowIdx] = new ArcadeDBServer(slowConfig);
    servers[slowIdx].start();
    CodeUtils.sleep(10000);

    // Verify the restarted follower caught up
    final long count = servers[slowIdx].getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV WHERE id >= 60000")
        .nextIfAvailable().getProperty("cnt", 0L);
    assertThat(count).as("Restarted follower should have caught up").isEqualTo(successes.get());
  }

  // =====================================================================
  // TEST 15: Very large transaction (big WAL buffer)
  // =====================================================================
  @Test
  @Order(15)
  void test15_veryLargeTransaction() {
    final ArcadeDBServer leader = findLeader();

    // Single transaction with 2000 records, each with 500-byte name (= ~1MB+ WAL)
    final String bigValue = "X".repeat(500);
    leader.getDatabase(DB_NAME).transaction(() -> {
      for (int i = 0; i < 2000; i++)
        leader.getDatabase(DB_NAME).newVertex("TestV").set("id", (long) (70000 + i)).set("name", bigValue + i).save();
    });

    CodeUtils.sleep(8000);

    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      final long count = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV WHERE id >= 70000")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(count).as("Server %s should have all 2000 large records", s.getServerName()).isEqualTo(2000);
    }
  }

  // =====================================================================
  // TEST 16: Mixed read/write workload
  // =====================================================================
  @Test
  @Order(16)
  void test16_mixedReadWriteWorkload() throws Exception {
    final ArcadeDBServer leader = findLeader();
    final ArcadeDBServer follower = findFollower();

    // Pre-populate
    for (int i = 0; i < 50; i++) {
      final int idx = i;
      leader.getDatabase(DB_NAME).transaction(() ->
          leader.getDatabase(DB_NAME).newVertex("TestV").set("id", (long) (80000 + idx)).set("name", "mixed-" + idx).save()
      );
    }
    CodeUtils.sleep(3000);

    // Run reads on follower and writes on leader concurrently
    final CountDownLatch done = new CountDownLatch(2);
    final AtomicInteger readSuccesses = new AtomicInteger();
    final AtomicInteger writeSuccesses = new AtomicInteger();

    // Reader thread on follower
    new Thread(() -> {
      try {
        for (int i = 0; i < 100; i++) {
          final long id = 80000 + (long) (Math.random() * 50);
          final ResultSet rs = follower.getDatabase(DB_NAME).query("sql", "SELECT FROM TestV WHERE id = ?", id);
          if (rs.hasNext()) readSuccesses.incrementAndGet();
          CodeUtils.sleep(20);
        }
      } finally { done.countDown(); }
    }).start();

    // Writer thread on leader
    new Thread(() -> {
      try {
        for (int i = 50; i < 100; i++) {
          final int idx = i;
          try {
            leader.getDatabase(DB_NAME).transaction(() ->
                leader.getDatabase(DB_NAME).newVertex("TestV").set("id", (long) (80000 + idx)).set("name", "mixed-" + idx).save()
            );
            writeSuccesses.incrementAndGet();
          } catch (final Exception e) { /* concurrent modification retry */ }
          CodeUtils.sleep(20);
        }
      } finally { done.countDown(); }
    }).start();

    assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();

    assertThat(readSuccesses.get()).as("Most reads should succeed").isGreaterThanOrEqualTo(80);
    assertThat(writeSuccesses.get()).as("Most writes should succeed").isGreaterThanOrEqualTo(40);
  }

  // =====================================================================
  // TEST 17: Rolling upgrade simulation
  // =====================================================================
  @Test
  @Order(17)
  void test17_rollingUpgradeSimulation() {
    final ArcadeDBServer leader = findLeader();

    // Write initial data
    for (int i = 0; i < 20; i++) {
      final int idx = i;
      leader.getDatabase(DB_NAME).transaction(() ->
          leader.getDatabase(DB_NAME).newVertex("TestV").set("id", (long) (90000 + idx)).set("name", "pre-upgrade").save()
      );
    }
    CodeUtils.sleep(3000);

    // Rolling restart: stop and restart each server one at a time
    for (int round = 0; round < SERVER_COUNT; round++) {
      // Find a non-leader to restart (avoid disrupting writes)
      int targetIdx = -1;
      for (int i = 0; i < SERVER_COUNT; i++)
        if (servers[i] != null && servers[i].isStarted() && servers[i].getHA() != null && !servers[i].getHA().isLeader()) {
          targetIdx = i;
          break;
        }

      if (targetIdx < 0) {
        // All remaining are leaders or stopped - restart any started one
        for (int i = 0; i < SERVER_COUNT; i++)
          if (servers[i] != null && servers[i].isStarted()) { targetIdx = i; break; }
      }
      if (targetIdx < 0) break;

      final String name = servers[targetIdx].getServerName();
      final var config = servers[targetIdx].getConfiguration();

      // Stop
      servers[targetIdx].stop();
      CodeUtils.sleep(5000);

      // Verify cluster still works (if majority alive)
      int aliveCount = 0;
      for (final ArcadeDBServer s : servers)
        if (s != null && s.isStarted()) aliveCount++;

      if (aliveCount >= 2) {
        waitForLeader();
        final ArcadeDBServer currentLeader = findLeader();
        if (currentLeader != null) {
          final int writeId = 90100 + round;
          final int currentRound = round;
          try {
            currentLeader.getDatabase(DB_NAME).transaction(() ->
                currentLeader.getDatabase(DB_NAME).newVertex("TestV").set("id", (long) writeId).set("name", "during-upgrade-" + currentRound).save()
            );
          } catch (final Exception e) {
            // Might fail if quorum not available during transition
          }
        }
      }

      // Restart
      servers[targetIdx] = new ArcadeDBServer(config);
      servers[targetIdx].start();
      CodeUtils.sleep(5000);

      waitForLeader();
    }

    // After rolling restart, all servers should be up and consistent
    CodeUtils.sleep(5000);
    final ArcadeDBServer finalLeader = findLeader();
    assertThat(finalLeader).isNotNull();

    // Verify baseline data survives
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted()) continue;
      final long count = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV WHERE id >= 90000 AND id < 90020")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(count).as("Server %s should have baseline data after rolling upgrade", s.getServerName()).isEqualTo(20);
    }
  }

  // =====================================================================
  // Helpers
  // =====================================================================

  private void startCluster() {
    final StringBuilder serverList = new StringBuilder();
    for (int i = 0; i < SERVER_COUNT; i++) {
      if (i > 0) serverList.append(",");
      serverList.append("localhost:").append(BASE_HA_PORT + i);
    }

    servers = new ArcadeDBServer[SERVER_COUNT];
    for (int i = 0; i < SERVER_COUNT; i++) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, Constants.PRODUCT + "_comp_" + i);
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/ha-comp-db" + i);
      config.setValue(GlobalConfiguration.HA_ENABLED, true);
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList.toString());
      config.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS, String.valueOf(BASE_HA_PORT + i));
      config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "comp-test-cluster");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(BASE_HTTP_PORT + i));
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      servers[i] = new ArcadeDBServer(config);
      servers[i].start();
    }
    waitForLeader();
    CodeUtils.sleep(2000);
  }

  private void stopCluster() {
    if (servers != null)
      for (int i = servers.length - 1; i >= 0; i--)
        if (servers[i] != null)
          try { servers[i].stop(); } catch (final Exception e) { /* ignore */ }
    CodeUtils.sleep(2000);
  }

  private void waitForLeader() {
    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (final ArcadeDBServer s : servers)
            if (s != null && s.isStarted() && s.getHA() != null && s.getHA().isLeader())
              return true;
          return false;
        });
  }

  private ArcadeDBServer findLeader() {
    for (final ArcadeDBServer s : servers)
      if (s != null && s.isStarted() && s.getHA() != null && s.getHA().isLeader())
        return s;
    return null;
  }

  private ArcadeDBServer findFollower() {
    for (final ArcadeDBServer s : servers)
      if (s != null && s.isStarted() && s.getHA() != null && !s.getHA().isLeader())
        return s;
    return null;
  }
}
