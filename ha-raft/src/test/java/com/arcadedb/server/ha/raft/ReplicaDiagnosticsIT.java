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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompactor;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import java.util.Collection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Replica-side diagnostics: read-only checks, errors on the committing thread and failed compactions.
 */
@Tag("slow")
class ReplicaDiagnosticsIT extends BaseRaftHATest {
  // ---- #12347 ----
  // Issues #12347 and #12348, both reported from one out-of-memory incident on a 3-node cluster:
  // <ul>
  // <li>{@code CHECK DATABASE} sent to a replica was forwarded to the leader like any non-idempotent statement, so it reported the
  // LEADER's files: a replica whose record counter drifted, or whose index lost its compacted sub-index, was reported clean;</li>
  // <li>an {@link Error} on the committing thread after the apply thread claimed the commit left the transaction in its commit
  // phase, holding its file locks, for the caller's cleanup to roll back while the apply thread published it;</li>
  // <li>a compaction failing after its swap was not quarantined on the leader, which then served a compacted file the log never
  // carried: the next round published a mutable file naming it, and every replica loaded the index without its sub-index.</li>
  // </ul>

  private static final String TYPE_NAME = "Event";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void clearHooks() {
    RaftReplicatedDatabase.TEST_POST_REPLICATION_HOOK = null;
  }

  // Regression for #12347: a read only check on A replica reports that replicas counter
  @Test
  void aReadOnlyCheckOnAReplicaReportsThatReplicasCounter() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    final int replica = firstFollowerOf(leader);

    createType(wrapped(leader), false);
    insert(wrapped(leader), 0, 100);
    waitForAllServers();
    assertThat(awaitCountOn(replica, TYPE_NAME, 100)).isEqualTo(100);

    final LocalBucket bucket = (LocalBucket) getServerDatabase(replica, getDatabaseName()).getSchema().getType(TYPE_NAME)
        .getBuckets(false).getFirst();
    assertThat(bucket.count()).isEqualTo(100);
    bucket.setCachedRecordCount(100 + 56);
    try {
      final Result onReplica = runCheck(replica, "CHECK DATABASE TYPE " + TYPE_NAME);
      assertThat(onReplica.<Number>getProperty("staleRecordCounters").longValue())
          .as("the check sent to the replica must inspect the replica, not the leader").isEqualTo(1L);
      assertThat(String.valueOf(onReplica.<Collection<String>>getProperty("warnings"))).contains("over-reports by 56");

      // and it is the replica's only repair: a FIX goes to the leader, a restart reloads the same counter. The check dropped it,
      // so the next count(*) recounts what the replica holds
      assertThat(onReplica.<Number>getProperty("staleRecordCountersFixed").longValue()).isEqualTo(1L);
      assertThat(cachedCountOn(replica)).isEqualTo(100);

      final Result onLeader = runCheck(leader, "CHECK DATABASE TYPE " + TYPE_NAME);
      assertThat(onLeader.<Number>getProperty("staleRecordCounters").longValue()).isZero();
    } finally {
      bucket.setCachedRecordCount(-1);
    }
    assertClusterConsistency();
  }

  // Regression for #12347: an error on the committing thread after the claim still completes the commit
  @Test
  void anErrorOnTheCommittingThreadAfterTheClaimStillCompletesTheCommit() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    final DatabaseInternal db = wrapped(leader);

    createType(db, false);
    insert(db, 0, 10);
    waitForAllServers();

    // on the leader the entry is acknowledged after its own apply thread published it, so the claim is taken by then
    RaftReplicatedDatabase.TEST_POST_REPLICATION_HOOK = name -> {
      throw new OutOfMemoryError("injected after the claim");
    };
    db.begin();
    db.newDocument(TYPE_NAME).set("eid", 10).save();
    assertThatThrownBy(db::commit).isInstanceOf(OutOfMemoryError.class);
    RaftReplicatedDatabase.TEST_POST_REPLICATION_HOOK = null;

    assertThat(db.isTransactionActive()).as("the commit the cluster holds must be completed, not left to a rollback").isFalse();

    // the locks are released: the next commit on the same bucket goes through
    insert(db, 11, 1);

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++) {
      assertThat(awaitCountOn(i, TYPE_NAME, 12)).isEqualTo(12);
      assertThat(cachedCountOn(i)).as("count(*) on server %d must match its records", i).isEqualTo(12);
    }
    assertClusterConsistency();
  }

  // Regression for #12347: a compaction failing past its swap leaves no replica without its sub index
  @Test
  void aCompactionFailingPastItsSwapLeavesNoReplicaWithoutItsSubIndex() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    final DatabaseInternal db = wrapped(leader);

    createType(db, true);
    insert(db, 0, 3_000);
    waitForAllServers();

    final LSMTreeIndex leaderIndex = bucketIndex(db);
    assertThatThrownBy(() -> ((RaftReplicatedDatabase) db).runWithCompactionReplication(() -> {
      new LSMTreeIndexCompactor().compact(leaderIndex);
      throw new OutOfMemoryError("injected past the swap");
    })).isInstanceOf(OutOfMemoryError.class);

    // the leader holds a layout no other node has: quarantined, it hands the leadership off and resyncs. Waited out, so the round
    // below runs on a settled cluster
    assertThat(awaitValue(0, this::quarantinedNodes)).as("every quarantine must be cleared by its resync").isZero();

    // more writes and a compaction by whoever leads now: before the fix this round appended to the unpublished compacted file and
    // published a mutable file naming it
    insertOnLeader(3_000, 3_000);
    compactOnLeader();
    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      assertThat(awaitValue(6_000, () -> withResyncRetry(server, d -> keysFoundByIndex(d, 6_000))))
          .as("server %d must find every key through the index", server).isEqualTo(6_000);
      final String detached = withResyncRetry(server, d -> bucketIndex((DatabaseInternal) d).getMutableIndex().getDetachedSubIndex());
      assertThat(detached)
          .as("server %d must hold the sub-index its mutable file names", server).isNull();
    }
  }

  // ---------------------------------------------------------------------------------------------------------------

  private long quarantinedNodes() {
    long quarantined = 0;
    for (int i = 0; i < getServerCount(); i++)
      if (isDatabaseQuarantined(i, getDatabaseName()))
        ++quarantined;
    return quarantined;
  }

  /** Inserts through whoever leads, retrying a write a leadership change refused or left undecided. */
  private void insertOnLeader(final int from, final int count) throws InterruptedException {
    for (int attempt = 0; ; attempt++) {
      try {
        insertMissing(wrapped(findLeaderIndex()), from, count);
        return;
      } catch (final RuntimeException e) {
        if (attempt >= 10)
          throw e;
        Thread.sleep(1_000);
      }
    }
  }

  /** Inserts the keys of [from, from + count) the index does not hold yet, so a retry does not collide with a write that landed. */
  private void insertMissing(final DatabaseInternal db, final int from, final int count) {
    final TypeIndex typeIndex = db.getSchema().getType(TYPE_NAME).getIndexesByProperties("eid").getFirst();
    db.transaction(() -> {
      for (int i = from; i < from + count; i++)
        if (!typeIndex.get(new Object[] { i }).hasNext())
          db.newDocument(TYPE_NAME).set("eid", i).save();
    });
  }

  private void compactOnLeader() throws Exception {
    final LSMTreeIndex index = bucketIndex(wrapped(findLeaderIndex()));
    if (index.scheduleCompaction())
      index.compact();
  }

  private DatabaseInternal wrapped(final int serverIndex) {
    return ((DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName())).getWrappedDatabaseInstance();
  }

  private void createType(final DatabaseInternal db, final boolean index) {
    final DocumentType type = db.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("eid", Type.INTEGER);
    if (index)
      db.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "eid" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .withPageSize(4_096).create();
  }

  private void insert(final DatabaseInternal db, final int from, final int count) {
    final int batch = 1_000;
    for (int start = from; start < from + count; start += batch) {
      final int s = start;
      final int end = Math.min(start + batch, from + count);
      db.transaction(() -> {
        for (int i = s; i < end; i++)
          db.newDocument(TYPE_NAME).set("eid", i).save();
      });
    }
  }

  private Result runCheck(final int serverIndex, final String statement) {
    try (final ResultSet rs = wrapped(serverIndex).command("sql", statement)) {
      return rs.next();
    }
  }

  private long cachedCountOn(final int serverIndex) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    return db.countType(TYPE_NAME, false);
  }

  private static LSMTreeIndex bucketIndex(final DatabaseInternal db) {
    final TypeIndex typeIndex = db.getSchema().getType(TYPE_NAME).getIndexesByProperties("eid").getFirst();
    return (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private static long keysFoundByIndex(final Database db, final int keys) {
    final TypeIndex typeIndex = db.getSchema().getType(TYPE_NAME).getIndexesByProperties("eid").getFirst();
    long found = 0;
    for (int i = 0; i < keys; i++)
      if (typeIndex.get(new Object[] { i }).hasNext())
        ++found;
    return found;
  }
}
