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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end regression test for issue #8717, asked for by issue #8728. A follower applying a replicated schema entry
 * re-reads its schema through {@code LocalSchema.loadIncremental()} or the {@code load()} fallback
 * ({@code ArcadeStateMachine.applySchemaEntry}), and that used to re-apply the per-bucket record counts of
 * {@code statistics.json}, written by the follower's last graceful close, over the live counters. Every insert
 * replicated since the restart then vanished from {@code count(*)}, although the records were intact.
 * <p>
 * Each test restarts a follower gracefully (which writes its {@code statistics.json} with {@link #INITIAL}), inserts
 * {@link #INSERTED} more records through the leader, then ships one schema entry and checks the cached
 * {@code count(*)} on every node. The two tests pin which reload the entry took on the follower by the identity of
 * its bucket component, the same evidence {@code Issue6988FullRebuildFallbackIT} uses: the incremental refresh keeps
 * an untouched bucket instance, the full {@code load()} replaces it.
 */
class Issue8728FollowerSchemaEntryKeepsCountIT extends BaseRaftHATest {

  private static final String TYPE     = "Issue8728Counted";
  private static final String DROPPED  = "Issue8728Dropped";
  private static final int    INITIAL  = 200;
  private static final int    INSERTED = 150;
  private static final long   TOTAL    = INITIAL + INSERTED;

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  /**
   * A DDL that creates a type reaches the follower as a schema entry the incremental refresh can express.
   */
  @Test
  void anIncrementalSchemaApplyKeepsTheFollowerCountOfInsertsSinceItsRestart() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> createCountedType(leaderDb));

    final int followerIndex = restartFollowerAfterInsertsAndInsertMore(leaderIndex);
    final LocalBucket bucketBefore = followerBucket(followerIndex);

    // Idempotent DDL: a leadership transfer mid-commit makes LocalDatabase.transaction() re-run the lambda.
    leaderDb.transaction(() -> leaderDb.getSchema().getOrCreateDocumentType("Issue8728Trigger"));
    waitForReplicationOnAllServers();

    assertThat(getServerDatabase(followerIndex, getDatabaseName()).getSchema().existsType("Issue8728Trigger"))
        .as("the schema entry must have been applied on follower %d", followerIndex).isTrue();
    assertThat(followerBucket(followerIndex))
        .as("the create-type entry must take the incremental refresh, which keeps the untouched bucket instance")
        .isSameAs(bucketBefore);

    assertCachedCountOnEveryServer();
    assertClusterConsistency();
  }

  /**
   * Dropping a type retires its files, and {@code loadIncremental()} refuses any entry with a retired file, so the
   * follower takes the full {@code load()}. An index compaction would reach the same fallback, but is not used here
   * because of the unrelated post-compaction index-name split filed as #9213, which fails the cluster comparison.
   */
  @Test
  void aFullSchemaReloadKeepsTheFollowerCountOfInsertsSinceItsRestart() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      createCountedType(leaderDb);
      leaderDb.getSchema().getOrCreateDocumentType(DROPPED);
    });

    final int followerIndex = restartFollowerAfterInsertsAndInsertMore(leaderIndex);
    final LocalBucket bucketBefore = followerBucket(followerIndex);

    leaderDb.getSchema().dropType(DROPPED);
    waitForReplicationOnAllServers();

    assertThat(getServerDatabase(followerIndex, getDatabaseName()).getSchema().existsType(DROPPED))
        .as("the drop-type entry must have been applied on follower %d", followerIndex).isFalse();
    assertThat(followerBucket(followerIndex))
        .as("the drop-type entry must take the full load(), which rebuilds every component (if loadIncremental() ever "
            + "learns retired files, pick another fallback trigger, see Issue6988FullRebuildFallbackIT)")
        .isNotSameAs(bucketBefore);

    assertCachedCountOnEveryServer();
    assertClusterConsistency();
  }

  /**
   * Inserts {@link #INITIAL} records, restarts a follower gracefully so its {@code statistics.json} holds
   * {@link #INITIAL}, then inserts {@link #INSERTED} more through the leader and waits for them to replicate.
   */
  private int restartFollowerAfterInsertsAndInsertMore(final int leaderIndex) {
    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    insert(leaderDb, INITIAL);
    waitForReplicationOnAllServers();
    assertThat(getServerDatabase(followerIndex, getDatabaseName()).countType(TYPE, false)).isEqualTo(INITIAL);

    LogManager.instance().log(this, Level.INFO, "TEST: gracefully restarting follower %d", followerIndex);
    restartServer(followerIndex);
    assertLeaderUnchanged(leaderIndex);

    // The precondition the bug needs: the restarted follower took its counter from statistics.json. An unknown
    // counter (-1) would be recomputed from the pages and the test could not tell the fix from its absence.
    assertThat(followerBucket(followerIndex).getCachedRecordCount())
        .as("follower %d must start with the counter its graceful close stored", followerIndex).isEqualTo(INITIAL);

    insert(leaderDb, INSERTED);
    waitForReplicationOnAllServers();
    assertLeaderUnchanged(leaderIndex);
    assertThat(getServerDatabase(followerIndex, getDatabaseName()).countType(TYPE, false))
        .as("follower %d must count the replicated inserts before any schema entry", followerIndex).isEqualTo(TOTAL);
    return followerIndex;
  }

  private static DocumentType createCountedType(final Database db) {
    return db.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(1).withIgnoreIfExists(true).create();
  }

  private void insert(final Database db, final int count) {
    db.transaction(() -> {
      for (int i = 0; i < count; i++)
        db.newDocument(TYPE).set("uuid", UUID.randomUUID().toString()).save();
    });
  }

  private void waitForReplicationOnAllServers() {
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);
  }

  /**
   * {@code count(*)} and {@code countType()} both answer from the cached per-bucket counter, the value #8717 reset;
   * {@code count(uuid)} scans the records and shows they were never lost.
   */
  private void assertCachedCountOnEveryServer() throws Exception {
    testEachServer(serverIndex -> {
      final Database db = getServerDatabase(serverIndex, getDatabaseName());
      assertThat(db.countType(TYPE, false)).as("countType on server %d", serverIndex).isEqualTo(TOTAL);
      assertThat(((Number) db.query("sql", "SELECT count(*) AS cnt FROM " + TYPE).next().getProperty("cnt")).longValue())
          .as("count(*) on server %d", serverIndex).isEqualTo(TOTAL);
      assertThat(((Number) db.query("sql", "SELECT count(uuid) AS cnt FROM " + TYPE).next().getProperty("cnt")).longValue())
          .as("count(uuid) on server %d", serverIndex).isEqualTo(TOTAL);
    });
  }

  // getFirst() is the whole type only because createCountedType() builds it with withTotalBuckets(1).
  private LocalBucket followerBucket(final int followerIndex) {
    final LocalSchema schema = getServerDatabase(followerIndex, getDatabaseName()).getSchema().getEmbedded();
    return (LocalBucket) schema.getFileByIdIfExists(schema.getType(TYPE).getBuckets(false).getFirst().getFileId());
  }

  /**
   * The test writes through one leader handle and computes {@link #TOTAL} up front: a leadership move would re-run an
   * insert lambda or leave the handle on a follower, so it is reported as such rather than as a wrong count.
   */
  private void assertLeaderUnchanged(final int leaderIndex) {
    assertThat(findLeaderIndex()).as("leadership must stay on server %d for the expected counts to hold", leaderIndex)
        .isEqualTo(leaderIndex);
  }
}
