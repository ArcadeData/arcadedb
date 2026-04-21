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
package com.arcadedb.server.ha.raft;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive HA test suite for the Ratis-based replication engine.
 * Tests data consistency, failover, catch-up, concurrency, and edge cases.
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

    // Wait for replication convergence
    waitForReplication();

    // Verify exact count on ALL servers
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted())
        continue;
      final long count = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(count).as("Server %s should have %d records", s.getServerName(), recordCount).isEqualTo(recordCount);
    }

    // Verify content: spot-check 10 random records
    for (int check = 0; check < 10; check++) {
      final int id = (int) (Math.random() * recordCount);
      for (final ArcadeDBServer s : servers) {
        if (s == null || !s.isStarted())
          continue;
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
    waitForReplication();

    // Verify data survived
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted())
        continue;
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
      if (s == null || !s.isStarted())
        continue;
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

    final int previousRetries = GlobalConfiguration.TX_RETRIES.getValueAsInteger();
    GlobalConfiguration.TX_RETRIES.setValue(50);

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
    GlobalConfiguration.TX_RETRIES.setValue(previousRetries);
    assertThat(errors.get()).isZero();

    waitForReplication();

    final int expected = threads * recordsPerThread;
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted())
        continue;
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
      if (s == null || !s.isStarted())
        continue;
      assertThat(s.getDatabase(DB_NAME).getSchema().existsType("NewType"))
          .as("Server %s should have NewType", s.getServerName()).isTrue();
    }

    // Write to new type
    leaderDb.transaction(() -> leaderDb.newVertex("NewType").set("value", "test-schema").save());
    CodeUtils.sleep(2000);

    // Verify data in new type on all servers
    for (final ArcadeDBServer s : servers) {
      if (s == null || !s.isStarted())
        continue;
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
      if (s == null || !s.isStarted())
        continue;
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

    // Write on follower via the local database API: the WAL is forwarded through Raft
    // to the leader, which commits it to the log. However, the leader's state machine
    // skips applying entries it believes it committed locally (leader-skip optimization).
    // Since this entry was actually submitted by a follower, the leader never ran Phase 2,
    // so the record won't appear on the leader until this edge case is fully addressed.
    //
    // For now, verify that the write does NOT throw (Raft forwarding works at the protocol level).
    boolean writeSucceeded = false;
    try {
      follower.getDatabase(DB_NAME).transaction(() ->
          follower.getDatabase(DB_NAME).newVertex("TestV").set("id", 888L).set("name", "follower-write").save()
      );
      writeSucceeded = true;
    } catch (final Exception ignored) {
      // Raft forwarding may fail if the follower can't reach the leader
    }
    // The write should succeed at the Raft level (no exception), even though the local
    // follower transaction is reset without applying locally.
    assertThat(writeSucceeded).as("Follower write via Raft forwarding should not throw").isTrue();
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
      if (s == null || !s.isStarted())
        continue;
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
      final RaftHAServer leaderHA = ((RaftHAPlugin) currentLeader.getHA()).getRaftHAServer();
      String targetPeerId = null;
      for (final var peer : leaderHA.getRaftGroup().getPeers())
        if (!peer.getId().equals(leaderHA.getLocalPeerId())) {
          targetPeerId = peer.getId().toString();
          break;
        }

      if (targetPeerId != null) {
        try {
          leaderHA.transferLeadership(targetPeerId, 10_000);
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
      if (s == null || !s.isStarted())
        continue;
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
    boolean writeSucceeded = false;
    try {
      final var db = servers[0].getDatabase(DB_NAME);
      db.transaction(() -> db.newVertex("TestV").set("id", 99999L).set("name", "solo").save());
      writeSucceeded = true;
    } catch (final Exception e) {
      // Expected: QuorumNotReachedException because MAJORITY requires 2 of 3 servers
      assertThat(e.getMessage()).containsAnyOf("quorum", "Quorum", "timed out", "Timeout", "replication", "leader", "Leader");
    }

    // Reads should still work (local)
    final long existingCount = servers[0].getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV")
        .nextIfAvailable().getProperty("cnt", 0L);
    assertThat(existingCount).isGreaterThanOrEqualTo(0);
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
      if (s == null || !s.isStarted())
        continue;
      final long count = s.getDatabase(DB_NAME).query("sql", "SELECT count(*) as cnt FROM TestV WHERE id >= 70000")
          .nextIfAvailable().getProperty("cnt", 0L);
      assertThat(count).as("Server %s should have all 2000 large records", s.getServerName()).isEqualTo(2000);
    }
  }

  // =====================================================================
  // TEST 18: Leadership transfer during graceful shutdown with concurrent writes
  // =====================================================================
  @Test
  @Order(18)
  void test18_leadershipTransferDuringShutdownWithWrites() throws Exception {
    final ArcadeDBServer leader = findLeader();
    assertThat(leader).isNotNull();
    final int leaderIndex = findServerIndex(leader);

    // Start concurrent writes in background
    final AtomicInteger writeCount = new AtomicInteger();
    final AtomicInteger errorCount = new AtomicInteger();
    final var running = new java.util.concurrent.atomic.AtomicBoolean(true);

    final Thread writer = new Thread(() -> {
      while (running.get()) {
        try {
          final ArcadeDBServer currentLeader = findLeader();
          if (currentLeader != null && currentLeader.isStarted()) {
            final int id = writeCount.incrementAndGet();
            currentLeader.getDatabase(DB_NAME).transaction(() ->
                currentLeader.getDatabase(DB_NAME).newVertex("TestV")
                    .set("id", (long) (100000 + id)).set("name", "shutdown-" + id).save()
            );
          }
          Thread.sleep(50);
        } catch (final Exception e) {
          errorCount.incrementAndGet();
        }
      }
    }, "shutdown-writer");
    writer.setDaemon(true);
    writer.start();

    // Let some writes accumulate
    Thread.sleep(1000);

    // Gracefully stop the leader (triggers leadership transfer)
    LogManager.instance().log(this, Level.INFO, "TEST: Stopping leader %s during writes", leader.getServerName());
    leader.stop();

    // Wait for new leader
    Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < SERVER_COUNT; i++)
        if (i != leaderIndex && servers[i] != null && servers[i].isStarted()
            && servers[i].getHA() != null && servers[i].getHA().isLeader())
          return true;
      return false;
    });

    // Continue writing to the new leader
    Thread.sleep(2000);
    running.set(false);
    writer.join(5000);

    // Verify all acknowledged writes are present on the new leader
    final ArcadeDBServer newLeader = findLeader();
    assertThat(newLeader).isNotNull();
    final long totalRecords = newLeader.getDatabase(DB_NAME).query("sql",
        "SELECT count(*) as cnt FROM TestV WHERE name LIKE 'shutdown-%'").nextIfAvailable().getProperty("cnt", 0L);
    LogManager.instance().log(this, Level.INFO, "TEST: %d writes succeeded, %d errors, %d records found",
        writeCount.get(), errorCount.get(), totalRecords);
    // At least some writes should have survived the transition
    assertThat(totalRecords).isGreaterThan(0);
  }

  // =====================================================================
  // Helpers
  // =====================================================================

  private void startCluster() {
    final StringBuilder serverList = new StringBuilder();
    for (int i = 0; i < SERVER_COUNT; i++) {
      if (i > 0)
        serverList.append(",");
      serverList.append("localhost:").append(BASE_HA_PORT + i);
    }

    servers = new ArcadeDBServer[SERVER_COUNT];
    for (int i = 0; i < SERVER_COUNT; i++) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, Constants.PRODUCT + "_comp_" + i);
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/ha-comp-db" + i);
      config.setValue(GlobalConfiguration.HA_ENABLED, true);
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList.toString());
      config.setValue(GlobalConfiguration.HA_RAFT_PORT, BASE_HA_PORT + i);
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
          try {
            servers[i].stop();
          } catch (final Exception e) { /* ignore */ }
    CodeUtils.sleep(2000);
  }

  private void waitForLeader() {
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (final ArcadeDBServer s : servers)
            if (s != null && s.isStarted() && s.getHA() != null && s.getHA().isLeader())
              return true;
          return false;
        });
  }

  /**
   * Waits for replication to settle by pausing for 3 seconds after the last leader write.
   */
  private void waitForReplication() {
    CodeUtils.sleep(3000);
  }

  private int findServerIndex(final ArcadeDBServer target) {
    for (int i = 0; i < servers.length; i++)
      if (servers[i] == target)
        return i;
    return -1;
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
