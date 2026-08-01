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
package com.arcadedb.test.load;

import com.arcadedb.remote.RemoteException;
import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import io.micrometer.core.instrument.Metrics;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class ThreeNodesLoadTestIT extends ContainersTestTemplate {

  private static final String SERVER_LIST            = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";
  private static final int    SCHEMA_TIMEOUT_SECONDS = 60;
  /** Logged exactly once by every server that starts, so it proves the log scan itself is working. */
  private static final String CANARY_MARKER          = "ArcadeDB Server started";

  @AfterEach
  @Override
  public void tearDown() {
    // Skip compareAllDatabases(): with non-persistent containers, database files are not
    // on the host after stop. The test body already verifies convergence via Awaitility.
    super.tearDown();
  }

  @ParameterizedTest(name = "Three-node Raft HA Load test with {0} protocol")
  @EnumSource(DatabaseWrapper.Protocol.class)
  @DisplayName("Three-node Raft HA: replication across all nodes with consistency check")
  void threeNodeReplication(DatabaseWrapper.Protocol protocol) throws InterruptedException {
    runThreeNodeLoad(protocol, false);
  }

  /**
   * Materialized-view arm of the #5492 A/B. Identical to {@link #threeNodeReplication} in every respect except that
   * the schema carries a {@code REFRESH INCREMENTAL} view over {@code User}; the issue reports that this alone turns a
   * clean 3000/3000 run into 2041/3000 failed writes, a non-terminating snapshot-resync loop and one node holding a
   * strict subset of the committed users.
   * <p>
   * Runs over HTTP only, matching the client the original report used, so the arms differ by the view and nothing else.
   */
  @Test
  @Tag("slow")
  @DisplayName("Three-node Raft HA: a REFRESH INCREMENTAL materialized view must not cost committed writes (#5492)")
  void threeNodeReplicationWithMaterializedView() throws InterruptedException {
    runThreeNodeLoad(DatabaseWrapper.Protocol.HTTP, true);
  }

  private void runThreeNodeLoad(final DatabaseWrapper.Protocol protocol, final boolean withMaterializedView)
      throws InterruptedException {
    createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting all containers");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    waitForRaftLeader(servers, 10);
    logger.info("Creating database and schema on leader");
    ServerWrapper leaderServer = servers.get(findLeaderIndex(servers));
    final DatabaseWrapper leaderDb = new DatabaseWrapper(leaderServer, idSupplier, wordSupplier);
    leaderDb.createDatabase();
    leaderDb.createSchema(withMaterializedView);
    leaderDb.close();

    logger.info("Waiting for the schema to replicate to all nodes");
    final long schemaStart = System.currentTimeMillis();
    db1.awaitSchema(SCHEMA_TIMEOUT_SECONDS);
    db2.awaitSchema(SCHEMA_TIMEOUT_SECONDS);
    db3.awaitSchema(SCHEMA_TIMEOUT_SECONDS);
    logger.info("Schema readable on all three nodes after {} ms", System.currentTimeMillis() - schemaStart);
    if (withMaterializedView) {
      final long viewStart = System.currentTimeMillis();
      db1.awaitMaterializedView(SCHEMA_TIMEOUT_SECONDS);
      db2.awaitMaterializedView(SCHEMA_TIMEOUT_SECONDS);
      db3.awaitMaterializedView(SCHEMA_TIMEOUT_SECONDS);
      logger.info("Materialized view readable on all three nodes after {} ms", System.currentTimeMillis() - viewStart);
    }

    final int numOfThreads = 3; //number of threads to use to insert users and photos
    final int numOfUsers = 1000; // Each thread will create 200000 users
    final int numOfPhotos = 10; // Each user will have 5 photos
    final int numOfFriendship = 0; // Each thread will create 100000 friendships
    final int numOfLike = 0; // Each thread will create 100000 likes

    int expectedUsersCount = numOfUsers * numOfThreads;
    int expectedPhotoCount = expectedUsersCount * numOfPhotos;
    int expectedFriendshipCount = numOfFriendship;
    int expectedLikeCount = numOfLike;
    LocalDateTime startedAt = LocalDateTime.now();
    logger.info("Starting load test on protocol {}", protocol.name());
    logger.info("Creating {} users using {} threads", expectedUsersCount, numOfThreads);
    logger.info("Expected users: {} - photos: {} - friendships: {} - likes: {}", expectedUsersCount, expectedPhotoCount,
        expectedFriendshipCount, expectedLikeCount);
    logger.info("Starting at {}", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(startedAt));

    ExecutorService executor = Executors.newFixedThreadPool(10);
    for (int i = 0; i < numOfThreads; i++) {
      // Each thread will create users and photos
      executor.submit(() -> {
        DatabaseWrapper db = new DatabaseWrapper(leaderServer, idSupplier, wordSupplier, protocol);
        db.addUserAndPhotos(numOfUsers, numOfPhotos);
        db.close();
      });
    }
    // Each thread will create friendships
    executor.submit(() -> {
      DatabaseWrapper db = new DatabaseWrapper(leaderServer, idSupplier, wordSupplier, protocol);
      db.createFriendships(numOfFriendship);
      db.close();
    });
    // Each thread will create friendships
    executor.submit(() -> {
      DatabaseWrapper db = new DatabaseWrapper(leaderServer, idSupplier, wordSupplier, protocol);
      db.createLike(numOfLike);
      db.close();
    });

    executor.shutdown();

    while (!executor.isTerminated()) {
      try {
        final long users1 = db1.countUsers();
        final long photos1 = db1.countPhotos();
        final long users2 = db2.countUsers();
        final long photos2 = db2.countPhotos();
        final long users3 = db3.countUsers();
        final long photos3 = db3.countPhotos();
        logger.info("Users: {} / {} / {} | Photos: {} / {} / {}", users1, users2, users3, photos1, photos2, photos3);

      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
      try {
        // Wait for 2 seconds before checking again
        TimeUnit.SECONDS.sleep(5);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    LocalDateTime finishedAt = LocalDateTime.now();
    logger.info("Finishing at {}", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(finishedAt));
    logger.info("Total time: {} minutes", Duration.between(startedAt, finishedAt).toMinutes());

    try {
      awaitConvergence(db1, db2, db3);
    } finally {
      // Always report the replication signatures, on the happy path too: a run can converge on record counts and
      // still have logged version gaps and resyncs, and that difference is what decides #5492.
      logReplicationSignatures(withMaterializedView, db1, db2, db3);
    }

    Metrics.globalRegistry.getMeters().forEach(meter ->
        logger.info("Meter: {} - {}", meter.getId().getName(), meter.measure()));

    db1.assertThatUserCountIs(expectedUsersCount);
    db1.assertThatPhotoCountIs(expectedPhotoCount);
    db1.assertThatFriendshipCountIs(expectedFriendshipCount);
    db1.assertThatLikesCountIs(expectedLikeCount);

    db2.assertThatUserCountIs(expectedUsersCount);
    db2.assertThatPhotoCountIs(expectedPhotoCount);
    db2.assertThatFriendshipCountIs(expectedFriendshipCount);
    db2.assertThatLikesCountIs(expectedLikeCount);

    db3.assertThatUserCountIs(expectedUsersCount);
    db3.assertThatPhotoCountIs(expectedPhotoCount);
    db3.assertThatFriendshipCountIs(expectedFriendshipCount);
    db3.assertThatLikesCountIs(expectedLikeCount);

    // The assertions above read countType()'s cached counter, which drifts independently of replication
    // (#5152/#5154). A run that lost committed writes passes them whenever the drift cancels the loss out - so the
    // guard that actually decides this test is a full scan on every node. This is the shape #5492 produced: one
    // follower held 29991 of 30000 photos by scan while the counters agreed.
    for (final DatabaseWrapper db : List.of(db1, db2, db3)) {
      db.assertThatScannedCountIs("User", expectedUsersCount);
      db.assertThatScannedCountIs("Photo", expectedPhotoCount);
    }

    if (withMaterializedView)
      assertViewConvergedAcrossNodes(db1, db2, db3);

    db1.close();
    db2.close();
    db3.close();
  }

  /**
   * Requires the view to hold the same non-empty row count on every node.
   * <p>
   * Deliberately not an equality check against the user count: the refresh is scheduled from a post-commit callback,
   * so the exact number of rows at the end depends on refresh timing and asserting it would trade a real guard for a
   * flaky one. Equal-and-non-empty is the replication invariant, and it is exactly what #5492 broke - the view stood
   * at 3000 / 3000 / 0, and earlier at 999 / 3000 / 3000.
   */
  private void assertViewConvergedAcrossNodes(final DatabaseWrapper db1, final DatabaseWrapper db2,
      final DatabaseWrapper db3) {
    Awaitility.await("materialized view converges on every node")
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long v1 = db1.countUserStats();
            final long v2 = db2.countUserStats();
            final long v3 = db3.countUserStats();
            logger.info("UserStats convergence check: {} / {} / {}", v1, v2, v3);
            return v1 > 0 && v1 == v2 && v1 == v3;
          } catch (final RuntimeException e) {
            logger.debug("View convergence check transient failure: {}", e.getMessage());
            return false;
          }
        });
  }

  private void awaitConvergence(final DatabaseWrapper db1, final DatabaseWrapper db2, final DatabaseWrapper db3) {
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(10, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users1 = db1.countUsers();
            final long photos1 = db1.countPhotos();
            final long users2 = db2.countUsers();
            final long photos2 = db2.countPhotos();
            final long users3 = db3.countUsers();
            final long photos3 = db3.countPhotos();
            logger.info("Final check - Users: {} / {} / {} | Photos: {} / {} / {}", users1, users2, users3, photos1, photos2,
                photos3);
            return users1 == users2 &&
                users1 == users3 &&
                photos1 == photos2 &&
                photos1 == photos3;
          } catch (final RemoteException e) {
            // Transient: a node has not caught up or a connection blip - keep polling.
            logger.debug("Quorum recovery check transient failure: {}", e.getMessage());
            return false;
          } catch (final RuntimeException e) {
            // Unexpected: programming error or infra failure. Log loudly so the cause is visible
            // before the 1-minute timeout instead of after.
            logger.warn("Quorum recovery check threw unexpected exception, will keep polling", e);
            return false;
          }
        });
  }

  /**
   * Logs the numbers that distinguish the two arms of the #5492 A/B. Counting the log markers matters because record
   * counts alone cannot tell a cluster that never diverged from one that diverged and resynced back into agreement.
   */
  private void logReplicationSignatures(final boolean withMaterializedView, final DatabaseWrapper db1,
      final DatabaseWrapper db2, final DatabaseWrapper db3) {
    logger.info("=== #5492 signatures (materialized view: {}) ===", withMaterializedView ? "PRESENT" : "absent");
    dumpContainerLogs(withMaterializedView ? "5492-with-view" : "5492-no-view");
    // Match the text the servers actually log. The exception's class name never reaches the log, and the phrase
    // "snapshot resync required" belongs to the exception message rather than to any logged line - grepping for
    // either reports a clean run against a log holding tens of thousands of gaps.
    //
    // All four in one call: each one re-pulls every container's full stdout, which on a diverging run is megabytes.
    // "Serving database snapshot for" is counted on the leader, which serves one snapshot per resync cycle; the 503 a
    // resyncing follower returns is not logged server-side, so the client-side counters below stand in for the
    // writes it deflected.
    final Map<String, Integer> markers = countInContainerLogs(
        "does not match with existent version",
        "triggering snapshot resync",
        "Snapshot resync completed",
        "Serving database snapshot for",
        CANARY_MARKER);
    logger.info("Total page version gaps: {}", markers.get("does not match with existent version"));
    logger.info("Total gap-triggered resyncs: {}", markers.get("triggering snapshot resync"));
    logger.info("Total resyncs completed: {}", markers.get("Snapshot resync completed"));
    logger.info("Total snapshots served: {}", markers.get("Serving database snapshot for"));

    // The four counters above read zero both when the cluster is healthy and when the scan is broken - a marker that
    // no longer matches what the servers log is indistinguishable from a clean run, and that already happened once on
    // this harness. The canary appears exactly once per container in every run, so a zero here means the numbers above
    // carry no information rather than good news.
    final int canary = markers.get(CANARY_MARKER);
    if (canary < containers.size())
      logger.error("Log scan is unreliable: canary '{}' found {} times across {} containers, expected one each. "
          + "Treat the counters above as unmeasured, not as zero.", CANARY_MARKER, canary, containers.size());
    else
      logger.info("Log scan self-check: canary '{}' found {} times across {} containers", CANARY_MARKER, canary,
          containers.size());
    logger.info("Failed user writes: {}", sumCounter("arcadedb.test.inserted.users.error"));
    logger.info("Failed photo writes: {}", sumCounter("arcadedb.test.inserted.photos.error"));
    // Authoritative per-node record counts. countType() reads a cached counter that drifts independently of
    // replication (#5152/#5154), so a disagreement there has to be confirmed by a scan before it means data loss.
    try {
      logger.info("Scanned Users per node: {} / {} / {}", db1.scanCount("User"), db2.scanCount("User"),
          db3.scanCount("User"));
      logger.info("Scanned Photos per node: {} / {} / {}", db1.scanCount("Photo"), db2.scanCount("Photo"),
          db3.scanCount("Photo"));
    } catch (final RuntimeException e) {
      logger.warn("Could not scan per-node record counts: {}", e.getMessage());
    }
    if (withMaterializedView) {
      try {
        logger.info("UserStats rows per node: {} / {} / {}", db1.countUserStats(), db2.countUserStats(),
            db3.countUserStats());
      } catch (final RuntimeException e) {
        logger.warn("Could not read UserStats row counts: {}", e.getMessage());
      }
    }
  }
}
