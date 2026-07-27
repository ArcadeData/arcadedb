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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5443, SPLIT variant: same guarantee as {@link Issue5443FollowerIndexGapIT}
 * but with the compaction's schema change split across many Raft entries.
 * <p>
 * After an LSM index compaction, a FOLLOWER's index can hold fewer
 * entries than the leader's even though every record replicated. Keys that live only in the missing
 * range become unfindable on that node, so a query answered by the follower silently returns fewer rows
 * than the same query on the leader.
 * <p>
 * This is a correctness bug, not a performance one: the records are all there, only the index is short.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5443SplitCompactionEntryIT extends BaseRaftHATest {

  private static final int TOTAL_RECORDS = 60_000;
  private static final int TX_CHUNK      = 250;
  private static final String INDEX_NAME = "Address[uid]";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 30_000L);
    // Only the explicit compact() below may run, so the assertion cannot race a background compaction.
    config.setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 1);
    // Force the compaction's schema change to be SPLIT across many Raft entries (#4743), which is what a
    // large enough index does at the stock 32MB ceiling. This is the path where a follower used to detach
    // its compacted sub-index altogether and serve only its mutable pages.
    config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "512KB");
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Compaction replaces the mutable index file and its name embeds a per-node nanoTime, so
    // DatabaseComparator's type-level check (which pairs bucket indexes BY NAME) cannot pass after a
    // compaction. Equality that matters is asserted in the test body instead.
  }

  @Test
  @Tag("slow")
  void everyNodeHoldsTheWholeIndexAfterASplitCompaction() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database database = getServerDatabase(leaderIndex, getDatabaseName());
    final VertexType v = database.getSchema().buildVertexType().withName("Address").withTotalBuckets(1).create();
    v.createProperty("uid", String.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Address", "uid");

    for (int from = 0; from < TOTAL_RECORDS; from += TX_CHUNK) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < Math.min(start + TX_CHUNK, TOTAL_RECORDS); i++)
          database.newVertex("Address").set("uid", uid(i)).save();
      });
    }

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX_NAME);
    index.scheduleCompaction();
    index.compact();

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer(serverIndex -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      assertThat(serverDb.countType("Address", false))
          .as("every record must replicate to server %d", serverIndex).isEqualTo(TOTAL_RECORDS);

      final Index serverIdx = serverDb.getSchema().getIndexByName(INDEX_NAME);
      assertThat(serverIdx.countEntries())
          .as("server %d must hold the WHOLE index after the compaction, not a truncated one", serverIndex)
          .isEqualTo(TOTAL_RECORDS);
    });

    // Spot-check lookups across the whole key range on every node: a short index shows up as specific
    // keys that cannot be found, which is what makes the bug silent for a client.
    testEachServer(serverIndex -> {
      final Index serverIdx = getServerDatabase(serverIndex, getDatabaseName()).getSchema().getIndexByName(INDEX_NAME);
      for (int i = 0; i < TOTAL_RECORDS; i += TOTAL_RECORDS / 40)
        assertThat(serverIdx.get(new Object[] { uid(i) }).hasNext())
            .as("key %d must be findable on server %d", i, serverIndex).isTrue();
    });
  }

  /** High-entropy key so the compacted index does not compress away to a trivial size. */
  private static String uid(final int i) {
    final long mixed = i * 0x9E3779B97F4A7C15L;
    return Long.toHexString(mixed) + "-" + Long.toHexString(Long.reverse(mixed) ^ i) + "-" + i;
  }
}
