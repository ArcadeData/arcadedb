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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.index.Index;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6136 (1): an index rebuild on the leader used to buffer its WHOLE WAL in leader heap.
 * <p>
 * {@code BucketIndexBuilder.create()} wraps the build in {@code recordFileChanges}, which marks the thread so that
 * {@code RaftReplicatedDatabase.commit()} buffers instead of replicating; {@code LSMTreeIndex.build()} then commits
 * once per {@code IndexBuilder.BUILD_BATCH_SIZE} records and every one of those WAL images stayed resident until the
 * callback returned. Peak leader heap was roughly the whole rebuilt index, plus a repeat of every page that more
 * than one batch touched - on a node that is, by construction, the cluster's leader. A {@code CHECK DATABASE FIX}
 * over a large damaged type is exactly that shape, which is where the report came from.
 * <p>
 * The buffer is now shipped in ORDERED INSTALMENTS as it fills, so heap is bounded by the threshold rather than by
 * the size of the index. That is a change to the schema-replication protocol, the one area of this module whose
 * history - #4083, #4743, #5443, #5492 - is silent follower divergence rather than exceptions, so what this test
 * exists to prove is not that the flushing happens but that the cluster is still correct when it does.
 * <p>
 * The append buffer is pinned two orders of magnitude below its default, which drags the instalment threshold
 * (half the maximum entry size) down with it, so an ordinary rebuild crosses it many times. Both halves are
 * asserted: that instalments actually went out - otherwise the test would pass on the old code and prove nothing -
 * and that all three nodes end up with a complete, queryable index.
 * <p>
 * Sibling of {@code Issue6136SchemaWalInstalmentTest}, which pins the entry shape without a cluster.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftSchemaWalInstalment3NodesIT extends BaseRaftHATest {
  private static final String TYPE_NAME        = "InstalmentDoc";
  private static final String INDEX_NAME       = "instalmentIdxName";
  private static final String ABANDONED_BUCKET = "instalment_abandoned";
  /**
   * Comfortably more than one {@code IndexBuilder.BUILD_BATCH_SIZE} (5000), so the rebuild produces several
   * buffered WAL entries rather than one - which is what there is to ship in instalments in the first place.
   */
  private static final int    RECORDS    = 12_000;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // Two orders of magnitude below the 32MB default, with the write buffer kept above it (the server refuses to
    // start otherwise - it must stay >= appendBufferSize + 8 bytes). The instalment threshold is derived from this,
    // so lowering it is what makes an ordinary-sized rebuild exercise the path at all.
    config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "128KB");
    config.setValue(GlobalConfiguration.HA_WRITE_BUFFER_SIZE, "256KB");
  }

  @Test
  void anIndexRebuildShipsItsWalInInstalmentsAndStillConvergesEveryNode() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);

    final DatabaseInternal leaderDb = wrapped(leaderIndex);

    ddlOnLeader("CREATE DOCUMENT TYPE " + TYPE_NAME);
    ddlOnLeader("CREATE PROPERTY " + TYPE_NAME + ".name STRING");
    ddlOnLeader("CREATE INDEX `" + INDEX_NAME + "` ON " + TYPE_NAME + "(name) NOTUNIQUE");

    // A payload per record so the index build has real page volume to buffer.
    final String payload = "x".repeat(400);
    for (int batch = 0; batch < RECORDS / 1_000; batch++) {
      final int base = batch * 1_000;
      leaderDb.transaction(() -> {
        for (int i = 0; i < 1_000; i++)
          leaderDb.newDocument(TYPE_NAME).set("name", "n" + (base + i)).set("payload", payload).save();
      });
    }

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(awaitIndexEntriesOn(i, RECORDS))
          .as("server %d must hold the whole index before the rebuild", i).isEqualTo(RECORDS);

    final long instalmentsBefore = instalmentsShippedBy(leaderDb);

    try (final ResultSet rs = leaderDb.command("sql", "REBUILD INDEX `" + INDEX_NAME + "`")) {
      assertThat(rs.hasNext()).as("REBUILD INDEX must return a result").isTrue();
      rs.next();
    }

    assertThat(instalmentsShippedBy(leaderDb) - instalmentsBefore)
        .as("the rebuild must have shipped its buffered WAL incrementally rather than holding all of it: without "
            + "instalments this assertion is the only thing that fails, and every other one below passes on the "
            + "old code")
        .isPositive();

    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++)
      assertThat(awaitIndexEntriesOn(i, RECORDS))
          .as("server %d must hold a COMPLETE rebuilt index: an instalment sequence whose prefix a follower "
              + "misread would leave it serving only part of it, silently", i)
          .isEqualTo(RECORDS);

    // And the index is actually usable, not merely the right size: a follower that detached the component on a
    // mis-read instalment answers a lookup from its mutable pages alone.
    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      assertThat(countByNameOn(server, "n7777")).as("server %d must resolve an indexed lookup", server).isEqualTo(1);
    }

    assertClusterConsistency();
  }

  /**
   * The risk instalments introduce, and the compensation that pays for it.
   * <p>
   * Before them, the whole buffered WAL sat in leader heap and NOTHING reached a follower until the
   * {@code recordFileChanges} callback returned, so a build that threw part-way left the followers untouched. Now
   * the files an instalment announced are already there, and the entry that would have published or retired them
   * is never sent, because it lives after the line that threw - and every instalment chunk is marked
   * {@code moreChunksFollow}, so there is no signal a follower could act on by itself.
   * <p>
   * This reproduces exactly the shape the principal caller produces: {@code BucketIndexBuilder.create()} drops the
   * half-built index from its own error handler, so the leader lets the file go and only the followers are left
   * holding it. The assertion is the invariant that matters - EVERY node agrees on whether the file exists - not
   * merely "the followers dropped it", because a compensation that retired a file the leader had KEPT would
   * satisfy the narrow assertion while creating the divergence the retirement exists to prevent.
   */
  @Test
  void aSessionThatFailsAfterAnInstalmentRetiresWhatItAnnounced() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);

    ddlOnLeader("CREATE DOCUMENT TYPE " + TYPE_NAME);

    final DatabaseInternal leaderDb = wrapped(leaderIndex);
    final long instalmentsBefore = instalmentsShippedBy(leaderDb);
    final int[] abandonedFileId = { -1 };

    // A DDL session that creates a file, ships more than one instalment's worth of WAL, then fails the way a
    // half-way index build does: it lets its own file go before the exception leaves the callback.
    assertThatThrownBy(() -> leaderDb.recordFileChanges(() -> {
      leaderDb.getSchema().createBucket(ABANDONED_BUCKET);
      abandonedFileId[0] = leaderDb.getSchema().getBucketByName(ABANDONED_BUCKET).getFileId();

      final String payload = "y".repeat(400);
      for (int batch = 0; batch < 8; batch++) {
        leaderDb.begin();
        for (int i = 0; i < 500; i++)
          leaderDb.newDocument(TYPE_NAME).set("name", "abandoned" + batch + "_" + i).set("payload", payload).save();
        leaderDb.commit();
      }

      leaderDb.getSchema().dropBucket(ABANDONED_BUCKET);
      throw new IllegalStateException("simulated mid-build failure");
    })).as("the failure must reach the caller unchanged, not be swallowed or replaced by the compensation")
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("simulated mid-build failure");

    assertThat(instalmentsShippedBy(leaderDb) - instalmentsBefore)
        .as("the session must actually have shipped an instalment, or this test proves nothing").isPositive();
    assertThat(((RaftReplicatedDatabase) leaderDb).getSchemaWalInstalmentTotalTimeMs())
        .as("the instalments must be TIMED as well as counted (issue #6144): the elapsed time is what a stalled "
            + "writer on this database is waiting on, and it is the number the metric exports")
        .isNotNegative();
    assertThat(abandonedFileId[0]).as("the session must have created a file to abandon").isNotNegative();

    waitForAllServers();

    final boolean onLeader = fileExistsOn(leaderIndex, abandonedFileId[0]);
    assertThat(onLeader).as("the callback dropped the bucket, so this node must not have the file").isFalse();

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      assertThat(awaitFileExistsOn(server, abandonedFileId[0], onLeader))
          .as("server %d must agree with the node that ran the DDL about file %d: an instalment announced it and "
              + "nothing ever published it, so a follower left holding it leaks a file no schema references",
              server, abandonedFileId[0])
          .isEqualTo(onLeader);
    }
  }

  // ---------------------------------------------------------------------------------------------------------------

  /**
   * Runs a DDL statement on whichever node is leader RIGHT NOW, re-resolving on the way.
   * <p>
   * {@code findLeaderIndex()} answers for the moment it is called, and an election can land between that answer and
   * the next statement - schema changes must run on the leader, so the statement then fails with
   * {@code ServerIsNotTheLeaderException} during setup and the test reports a flake that says nothing about what it
   * is testing. It is a {@code NeedRetryException} precisely because retrying is the designed response.
   */
  private void ddlOnLeader(final String sql) throws InterruptedException {
    ServerIsNotTheLeaderException lastError = null;
    for (int attempt = 0; attempt < 20; attempt++) {
      final int leaderIndex = findLeaderIndex();
      if (leaderIndex >= 0)
        try {
          wrapped(leaderIndex).command("sql", sql);
          return;
        } catch (final ServerIsNotTheLeaderException e) {
          lastError = e;
        }
      Thread.sleep(500);
    }
    throw lastError != null ? lastError : new IllegalStateException("no leader to run '" + sql + "' on");
  }

  /**
   * The instalment counter is PER DATABASE (issue #6144), so it is read from the database that ships them rather
   * than from a JVM-wide static - which, on a multi-database server, would answer for every database at once.
   */
  private long instalmentsShippedBy(final DatabaseInternal db) {
    return ((RaftReplicatedDatabase) db).getSchemaWalInstalmentsShipped();
  }

  private DatabaseInternal wrapped(final int serverIndex) {
    return ((DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName())).getWrappedDatabaseInstance();
  }

  /** Live entries of the index on one server, or -1 when it cannot be read at all. */
  private long indexEntriesOn(final int serverIndex) {
    return withResyncRetry(serverIndex, db -> {
      try {
        final Index index = db.getSchema().getIndexByName(INDEX_NAME);
        return index != null ? index.countEntries() : -1L;
      } catch (final DatabaseIsClosedException e) {
        throw e;
      } catch (final Exception e) {
        return -1L;
      }
    });
  }

  private long awaitIndexEntriesOn(final int serverIndex, final long expected) throws InterruptedException {
    return awaitValue(expected, () -> indexEntriesOn(serverIndex));
  }

  /** Whether the paginated file is present in one server's FileManager, which is what an instalment created. */
  private boolean fileExistsOn(final int serverIndex, final int fileId) {
    return withResyncRetry(serverIndex,
        db -> ((DatabaseInternal) db).getFileManager().existsFile(fileId));
  }

  private boolean awaitFileExistsOn(final int serverIndex, final int fileId, final boolean expected)
      throws InterruptedException {
    return awaitValue(expected ? 1 : 0, () -> fileExistsOn(serverIndex, fileId) ? 1 : 0) == 1;
  }

  private long countByNameOn(final int serverIndex, final String name) {
    return withResyncRetry(serverIndex, db -> {
      try (final ResultSet rs = db.query("sql", "SELECT count(*) AS total FROM " + TYPE_NAME + " WHERE name = ?",
          name)) {
        return rs.hasNext() ? rs.next().<Long>getProperty("total") : -1L;
      }
    });
  }
}
