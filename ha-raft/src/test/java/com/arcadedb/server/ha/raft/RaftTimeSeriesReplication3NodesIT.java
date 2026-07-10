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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the Discord-reported issue: time-series data written to the
 * Raft leader is not visible on followers, while vertex/edge/document data replicates
 * normally.
 *
 * <p>Root cause: the {@code TimeSeriesShard} append path opens and commits its own nested
 * transaction directly on the inner {@code LocalDatabase} (obtained via
 * {@code schema.getDatabase()}), bypassing the {@code RaftReplicatedDatabase} wrapper whose
 * {@code commit()} ships the WAL to the Raft quorum. As a consequence the mutable bucket
 * pages are never replicated and followers serve queries from an empty bucket.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftTimeSeriesReplication3NodesIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    // Required by the lagging-follower test, which stops a node, compacts on the leader, then restarts
    // the node and expects it to catch up (Raft storage must survive the in-test restart).
    return true;
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Time-series sealed-store files are compacted independently per node and use direct file I/O
    // (not page-level replication), so byte-level page comparison is not meaningful here. The test
    // asserts logical equality of the replicated mutable samples explicitly below.
  }

  @Test
  void timeSeriesDataReplicatesToFollowers() throws Exception {
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> findLeaderIndex() >= 0);
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a leader must be elected").isGreaterThanOrEqualTo(0);

    // Create the TIMESERIES type on the leader; the schema change replicates to all peers.
    executeCommand(leaderIndex,
        "sql", "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");
    waitForReplicationIsCompleted(leaderIndex);

    // Insert a handful of samples on the leader. These stay in the mutable bucket (no compaction).
    final int samples = 5;
    for (int i = 0; i < samples; i++)
      executeCommand(leaderIndex, "sql",
          "INSERT INTO weather SET ts = " + (1000 + i) + ", location = 'us-east', temperature = " + (20.0 + i));

    // Make sure every node applied the Raft log up to the leader's last index.
    waitForAllServers();

    // Every node - leader and followers - must return the same number of samples.
    for (int i = 0; i < getServerCount(); i++) {
      final long count = countSamples(i);
      assertThat(count)
          .withFailMessage("Server %d (leader=%d) has %d time-series samples, expected %d",
              i, leaderIndex, count, samples)
          .isEqualTo(samples);
    }
  }

  /**
   * After a leader-side compaction, the rewritten sealed-store blocks and the mutable-bucket clear
   * must reach every follower atomically: all nodes return the same samples, their {@code .ts.sealed}
   * files are byte-identical, and the (replicated) mutable bucket is empty on every node. This is the
   * Option-2 invariant: compaction is leader-only and its full effect is shipped, so follower page
   * versions never diverge (no WALVersionGapException) and no data is lost.
   */
  @Test
  @Tag("slow")
  void compactionReplicatesSealedBlocksAndClearsMutable() throws Exception {
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> findLeaderIndex() >= 0);
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a leader must be elected").isGreaterThanOrEqualTo(0);

    // A single shard keeps the test deterministic and easy to reason about (the data either ends in
    // the one sealed store or the one mutable bucket; no cross-shard round-robin timing).
    executeCommand(leaderIndex,
        "sql", "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE) SHARDS 1");
    waitForReplicationIsCompleted(leaderIndex);

    final int samples = 50;
    for (int i = 0; i < samples; i++)
      executeCommand(leaderIndex, "sql",
          "INSERT INTO weather SET ts = " + (1000 + i) + ", location = 'us-east', temperature = " + (20.0 + i));

    // Mutable-bucket replication: every node converges to all samples (eventual consistency, so wait).
    awaitAllServersReportSamples(samples);

    // Force a leader-side compaction: seals the mutable data and clears the bucket, shipping both to
    // followers in a single SCHEMA_ENTRY. Re-resolve the leader in case an election moved it.
    timeSeriesEngine(findLeaderIndex()).compactAll();

    // Counts stay identical on every node (sealed + mutable merge) once the compaction entry applies.
    awaitAllServersReportSamples(samples);

    // The (replicated) mutable bucket is empty on every node after the clear; the data lives in sealed.
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++)
        assertThat(mutableSampleCount(i)).as("mutable bucket samples on server %d after compaction", i).isZero();
    });

    // Sealed-store files are byte-identical across all nodes (the leader's bytes were shipped verbatim).
    final int finalLeader = findLeaderIndex();
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      final byte[] leaderBytes = readSealedFile(finalLeader, 0);
      assertThat(leaderBytes.length).as("leader sealed store must be non-empty after compaction").isGreaterThan(0);
      for (int i = 0; i < getServerCount(); i++)
        if (i != finalLeader)
          assertThat(readSealedFile(i, 0))
              .as("sealed store on server %d must match leader %d", i, finalLeader)
              .isEqualTo(leaderBytes);
    });
  }

  /**
   * Compaction must keep working - and stay consistent - across a leadership change (failover). After
   * the leader steps down, the new leader continues to seal + clear + replicate, and every node ends
   * with identical data, an empty mutable bucket, and byte-identical sealed stores.
   */
  @Test
  @Tag("slow")
  void compactionSurvivesLeadershipChange() throws Exception {
    awaitLeaderElected();
    final int firstLeader = findLeaderIndex();

    executeCommand(firstLeader,
        "sql", "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE) SHARDS 1");
    waitForReplicationIsCompleted(firstLeader);

    insertSamples(0, 30);
    awaitAllServersReportSamples(30);
    timeSeriesEngine(findLeaderIndex()).compactAll();
    awaitAllServersReportSamples(30);

    // Step the current leader down and wait for a leader to be (re)elected.
    getRaftPlugin(firstLeader).getRaftHAServer().transferLeadership(10_000);
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> findLeaderIndex() >= 0);

    // Write + compact under the new leadership.
    insertSamples(30, 50);
    awaitAllServersReportSamples(50);
    timeSeriesEngine(findLeaderIndex()).compactAll();
    awaitAllServersReportSamples(50);

    awaitMutableEmptyAndSealedConsistent();
  }

  /**
   * A follower that is offline while the leader compacts must, on restart, catch up to the full state:
   * it receives the missed compaction (sealed blocks + mutable clear) via Raft and converges to the
   * leader - same sample count, empty mutable bucket, byte-identical sealed store.
   */
  @Test
  @Tag("slow")
  void laggingFollowerCatchesUpWithSealedDataAfterRestart() throws Exception {
    awaitLeaderElected();
    final int leader = findLeaderIndex();

    executeCommand(leader,
        "sql", "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE) SHARDS 1");
    waitForReplicationIsCompleted(leader);

    insertSamples(0, 30);
    awaitAllServersReportSamples(30);

    // Take a follower offline, then compact + write more on the leader so the follower misses it all.
    final int follower = (leader + 1) % getServerCount();
    getServer(follower).stop();

    timeSeriesEngine(findLeaderIndex()).compactAll();
    insertSamples(30, 50);

    // The still-online nodes converge to 50.
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++)
        if (i != follower)
          assertThat(countSamples(i)).as("online server %d before follower restart", i).isEqualTo(50L);
    });

    // Restart the follower; it must catch up (Raft log replay or snapshot) including the sealed data.
    restartServer(follower);

    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      assertThat(countSamples(follower)).as("restarted follower sample count").isEqualTo(50L);
      assertThat(mutableSampleCount(follower)).as("restarted follower mutable bucket").isZero();
    });
  }

  private void awaitLeaderElected() {
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> findLeaderIndex() >= 0);
    assertThat(findLeaderIndex()).as("a leader must be elected").isGreaterThanOrEqualTo(0);
  }

  private void insertSamples(final int fromInclusive, final int toExclusive) throws Exception {
    for (int i = fromInclusive; i < toExclusive; i++)
      executeCommand(findLeaderIndex(), "sql",
          "INSERT INTO weather SET ts = " + (1000 + i) + ", location = 'us-east', temperature = " + (20.0 + i));
  }

  private void awaitMutableEmptyAndSealedConsistent() {
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++)
        assertThat(mutableSampleCount(i)).as("mutable bucket samples on server %d", i).isZero();
      final int leader = findLeaderIndex();
      final byte[] leaderBytes = readSealedFile(leader, 0);
      for (int i = 0; i < getServerCount(); i++)
        if (i != leader)
          assertThat(readSealedFile(i, 0)).as("sealed store on server %d must match leader %d", i, leader)
              .isEqualTo(leaderBytes);
    });
  }

  private void awaitAllServersReportSamples(final long expected) {
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++)
        assertThat(countSamples(i)).as("sample count on server %d", i).isEqualTo(expected);
    });
  }

  private long countSamples(final int serverIndex) {
    final Database db = getServer(serverIndex).getDatabase(getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS cnt FROM weather")) {
      return rs.hasNext() ? rs.next().<Number>getProperty("cnt").longValue() : 0L;
    }
  }

  private TimeSeriesEngine timeSeriesEngine(final int serverIndex) {
    final DatabaseInternal db = (DatabaseInternal) getServer(serverIndex).getDatabase(getDatabaseName());
    return ((LocalTimeSeriesType) db.getSchema().getType("weather")).getEngine();
  }

  private long mutableSampleCount(final int serverIndex) throws IOException {
    final TimeSeriesEngine engine = timeSeriesEngine(serverIndex);
    long total = 0;
    for (int s = 0; s < engine.getShardCount(); s++)
      total += engine.getShard(s).getMutableBucket().getSampleCount();
    return total;
  }

  private byte[] readSealedFile(final int serverIndex, final int shardIndex) throws IOException {
    final File sealed = new File(getDatabasePath(serverIndex), "weather_shard_" + shardIndex + ".ts.sealed");
    return sealed.exists() ? Files.readAllBytes(sealed.toPath()) : new byte[0];
  }
}
