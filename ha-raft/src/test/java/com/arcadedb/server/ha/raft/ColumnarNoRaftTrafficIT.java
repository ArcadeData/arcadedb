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
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarSchemaEvent;
import com.arcadedb.index.columnar.ColumnarTestAccess;
import com.arcadedb.index.columnar.ColumnarTestData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The projection is node-local derived state (plan 3, Task 23): its flushes, merges and rebuilds write no Raft log entry on any node,
 * and the database comparison of the cluster ignores it, so nodes whose {@code columnar/} directories differ are still identical.
 */
class ColumnarNoRaftTrafficIT extends ColumnarHATestSupport {
  /** No time-driven flush or merge: the maintenance the test counts is the one it asks for. */
  private static final long NO_FLUSH_MS = 3_600_000L;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL.setValue(NO_FLUSH_MS);
    GlobalConfiguration.COLUMNAR_MERGE_MAX_SEGMENTS.setValue(1);
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, NO_FLUSH_MS);
    config.setValue(GlobalConfiguration.COLUMNAR_MERGE_MAX_SEGMENTS, 1);
  }

  @Test
  @Timeout(180)
  void projectionIsNeverReplicated() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    loadAndFlushOnce(leader);

    final long[] before = new long[getServerCount()];
    for (int i = 0; i < getServerCount(); i++)
      before[i] = lastRaftLogIndex(getRaftPlugin(i).getRaftHAServer());

    // flush (a second segment), then a merge (mergeMaxSegments 1), on every node
    for (int i = 0; i < getServerCount(); i++) {
      final ColumnarIndexManager manager = manager(i);
      final int bucketId = assetBucketId(i);
      final long merges = manager.getStats().mergeCount(ColumnarTestData.TYPE);
      manager.flushNow();
      assertThat(ColumnarTestAccess.publishedSegments(manager, bucketId)).as("the fixture: server %d has segments to merge", i)
          .isGreaterThan(1);
      ColumnarTestAccess.maintainNow(manager, bucketId);
      assertThat(manager.getStats().mergeCount(ColumnarTestData.TYPE)).as("server %d merged its segments", i).isGreaterThan(merges);
    }
    // a rebuild of the follower's projection, node-local
    forceRebuild(follower);

    for (int i = 0; i < getServerCount(); i++)
      assertThat(lastRaftLogIndex(getRaftPlugin(i).getRaftHAServer()))
          .as("server %d: flush, merge and rebuild append no Raft log entry", i).isEqualTo(before[i]);
    assertEquivalentEverywhere();
  }

  @Test
  @Timeout(180)
  void checksumsIgnoreTheProjection() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    loadAndFlushOnce(leader);
    // two node-local rebuilds, each proven to publish a newer generation: the follower rewrote its columnar/, the leader did not
    forceRebuild(follower);
    forceRebuild(follower);

    checkDatabasesAreIdentical();
    assertEquivalentEverywhere();
  }

  /** Schema and data through the leader, then one flush on every node: each projection holds one segment. */
  private void loadAndFlushOnce(final int leader) throws Exception {
    final ColumnarTestData data = new ColumnarTestData(2308L);
    data.deleteRatio = 0.1;
    data.createSchema(database(leader));
    data.insertAssets(database(leader), 2_000);
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      awaitCaughtUpOn(i);
      manager(i).flushNow();
    }
    data.mutate(database(leader), 500);
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++)
      awaitCaughtUpOn(i);
  }

  /**
   * Rebuilds the node's projection through its own manager (the {@code REBUILD} event a {@code check(fix=true)} repair sends), and
   * waits for the rebuild to publish: the counter proves it ran, the published generation that it replaced the state.
   */
  private void forceRebuild(final int serverIndex) throws Exception {
    final ColumnarIndexManager manager = manager(serverIndex);
    final int bucketId = assetBucketId(serverIndex);
    final long rebuilds = manager.getStats().rebuilds(ColumnarTestData.TYPE, ColumnarSchemaEvent.CHECK_FIX);
    final long generation = ColumnarTestAccess.publishedGeneration(manager, bucketId);
    manager.onSchemaEvent(ColumnarSchemaEvent.rebuild(bucketId, ColumnarSchemaEvent.CHECK_FIX));
    assertThat(awaitValue(rebuilds + 1, () -> manager.getStats().rebuilds(ColumnarTestData.TYPE, ColumnarSchemaEvent.CHECK_FIX)))
        .as("the rebuild of server %d ran", serverIndex).isEqualTo(rebuilds + 1);
    assertThat(awaitValue(0, () -> ColumnarTestAccess.isRebuildQueuedOrRunning(manager, bucketId) ? 1 : 0))
        .as("the rebuild of server %d ended", serverIndex).isZero();
    assertThat(ColumnarTestAccess.publishedGeneration(manager, bucketId)).as("the rebuild of server %d published", serverIndex)
        .isGreaterThan(generation);
    assertThat(manager.awaitCaughtUp(CAUGHT_UP_MS)).isTrue();
  }
}
