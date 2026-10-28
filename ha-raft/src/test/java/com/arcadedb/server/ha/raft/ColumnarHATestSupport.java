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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared fixture of the columnar HA integration tests (plan 3, Task 23): a 3-node cluster with a majority quorum and persistent Raft
 * storage (so a node can be restarted), the {@link ColumnarTestData} data set written through the leader, and the oracle checks run
 * against one node's own database.
 */
abstract class ColumnarHATestSupport extends BaseRaftHATest {
  /** Facets every test compares: an ordinal column, a BY ITEM ordinal column and a LONG column. */
  static final String[] PROPERTIES       = { "brand", "tags", "fileSize" };
  static final long     CAUGHT_UP_MS     = 30_000L;
  static final String[] REBUILD_REASONS  = { "create", "snapshot_install", "missing", "corrupt", "unclean_no_wal", "schema", "check_fix" };

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  Database database(final int serverIndex) {
    return getServerDatabase(serverIndex, getDatabaseName());
  }

  ColumnarIndexManager manager(final int serverIndex) {
    return ((LocalDatabase) ((DatabaseInternal) database(serverIndex)).getEmbedded()).getColumnarIndexManager();
  }

  /** The file id of the single bucket of {@link ColumnarTestData#TYPE} on the node. */
  int assetBucketId(final int serverIndex) {
    return database(serverIndex).getSchema().getType(ColumnarTestData.TYPE).getBuckets(false).getFirst().getFileId();
  }

  /** The node's rebuild counts of {@link ColumnarTestData#TYPE} by reason, for assertion messages. */
  String rebuildsByReason(final int serverIndex) {
    final StringBuilder counts = new StringBuilder();
    for (final String reason : REBUILD_REASONS)
      counts.append(counts.isEmpty() ? "" : ", ").append(reason).append('=')
          .append(manager(serverIndex).getStats().rebuilds(ColumnarTestData.TYPE, reason));
    return counts.toString();
  }

  /** Waits for the node to apply the leader's entries and for its projection to apply every mark they made. */
  void awaitCaughtUpOn(final int serverIndex) throws InterruptedException {
    assertThat(awaitReplicationIsCompleted(serverIndex)).as("server %d applies the leader's entries", serverIndex).isTrue();
    assertThat(manager(serverIndex).awaitCaughtUp(CAUGHT_UP_MS)).as("the projection of server %d catches up", serverIndex).isTrue();
  }

  /** {@link #awaitCaughtUpOn} on every running node. */
  void awaitCaughtUpEverywhere() throws InterruptedException {
    for (final int i : startedServers())
      awaitCaughtUpOn(i);
  }

  /** {@link #awaitCaughtUpOn} then the oracle over {@link #PROPERTIES} against the node's own database. */
  void assertEquivalentOn(final int serverIndex) throws InterruptedException {
    awaitCaughtUpOn(serverIndex);
    final Database db = database(serverIndex);
    for (final String property : PROPERTIES)
      FacetOracle.assertEquivalent(db, ColumnarTestData.TYPE, property, null);
  }

  /** {@link #assertEquivalentOn} on every running node. */
  void assertEquivalentEverywhere() throws InterruptedException {
    for (final int i : startedServers())
      assertEquivalentOn(i);
  }
}
