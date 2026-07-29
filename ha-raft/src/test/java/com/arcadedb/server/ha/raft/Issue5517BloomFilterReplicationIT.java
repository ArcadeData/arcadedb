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
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexBloomFilter;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The per-series bloom filters of #5517 live in a {@link LSMTreeIndexBloomFilter} component precisely so that they
 * travel the way the compacted pages they describe travel. This test is what makes that claim more than an argument.
 * <p>
 * A follower never runs a compaction of its own - {@code runWithCompactionReplication} refuses on a non-leader - so it
 * receives the filter file the same way it receives the compacted index: as a new file shipped whole in a SCHEMA_ENTRY,
 * and thereafter as the page range that file GREW by. The second half is why the component is append-only, and it is
 * the half a single-compaction test would never reach: the first round CREATES the file (every page ships), only a
 * later incremental round APPENDS to it.
 * <p>
 * What has to hold on every node, leader or follower:
 * <ul>
 *   <li>the filter file is there and describes every published series;</li>
 *   <li>every key is still findable THROUGH those filters - a filter that arrived truncated or stale would hide rows
 *   on that node alone, silently, while the records themselves replicated perfectly;</li>
 *   <li>and the filters are actually consulted, or the first two prove nothing.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5517BloomFilterReplicationIT extends BaseRaftHATest {
  private static final int    FIRST_BATCH  = 40_000;
  private static final int    SECOND_BATCH = 40_000;
  private static final int    TX_CHUNK     = 250;
  private static final String TYPE_NAME    = "Address";
  private static final String INDEX_NAME   = "Address[uid]";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 30_000L);
    // Only the explicit compactions below may run, or an assertion could race a background round.
    config.setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    // Force a RAM-bounded merge so one round emits SEVERAL series, each with its own filter.
    config.setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1L);
    // Never turn a compaction into a full one: this test is about the INCREMENTAL round, which appends to a file the
    // followers already have.
    config.setValue(GlobalConfiguration.INDEX_COMPACTION_FULL_SERIES, 0);
    config.setValue(GlobalConfiguration.INDEX_BLOOM_FILTER_RATE, 0.01f);
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // A compaction replaces the mutable index file and its name embeds a per-node nanoTime, so the comparator's
    // pair-bucket-indexes-by-name check cannot pass. What must be equal is asserted below instead.
  }

  @Test
  public void theFiltersReachEveryNodeAndHideNothing() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final VertexType type = leader.getSchema().buildVertexType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("uid", String.class);
    leader.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, "uid");

    // ROUND 1 - creates the compacted file and, with it, the filter file. Followers receive both whole.
    insert(leader, 0, FIRST_BATCH);
    compact(leader);
    awaitReplication();
    assertEveryNodeHoldsCompleteFilters("after the compaction that CREATED the filter file", FIRST_BATCH);

    // ROUND 2 - an incremental round APPENDS a series to files the followers already hold, so only the grown page
    // range ships. A directory rewritten in place would stop here: the followers would keep the round-1 directory and
    // never see the new series' filter.
    insert(leader, FIRST_BATCH, FIRST_BATCH + SECOND_BATCH);
    compact(leader);
    awaitReplication();
    assertEveryNodeHoldsCompleteFilters("after the incremental round that APPENDED to it", FIRST_BATCH + SECOND_BATCH);
  }

  /**
   * Every node must hold the whole index, a filter for every series, and answer every key through them.
   */
  private void assertEveryNodeHoldsCompleteFilters(final String stage, final int totalRecords) throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServerDatabase(serverIndex, getDatabaseName());

      assertThat(database.countType(TYPE_NAME, false))
          .as("every record must replicate to server %d %s", serverIndex, stage).isEqualTo(totalRecords);

      final Index index = database.getSchema().getIndexByName(INDEX_NAME);
      assertThat(index.countEntries())
          .as("server %d must hold the WHOLE index %s", serverIndex, stage).isEqualTo(totalRecords);

      final LSMTreeIndexCompacted compacted = compactedOf((TypeIndex) index);
      assertThat(compacted).as("server %d must have a compacted sub-index %s", serverIndex, stage).isNotNull();

      final LSMTreeIndexBloomFilter filter = compacted.getBloomFilter();
      assertThat(filter).as("server %d must have received the bloom filter file %s", serverIndex, stage).isNotNull();
      assertThat(filter.getPublishedFilters())
          .as("server %d must have a filter for EVERY series %s (a directory that did not travel would be short here)",
              serverIndex, stage)
          .isEqualTo(compacted.getSeriesCount());

      // The gate. A filter that arrived truncated, stale, or built for a different series would hide rows on this node
      // alone, while the records themselves replicated perfectly - so this has to be checked THROUGH the filters, on
      // the node that received them, over the whole key range.
      for (int i = 0; i < totalRecords; i++)
        assertThat(index.get(new Object[] { uid(i) }).hasNext())
            .as("key %d must be findable on server %d %s", i, serverIndex, stage).isTrue();

      // ... and the filters must be doing something, or the assertion above would pass just as well without them.
      final long skippedBefore = compacted.getBloomSkippedSeries();
      for (int i = 0; i < 2_000; i++)
        assertThat(index.get(new Object[] { absentUid(i) }).hasNext())
            .as("absent key %d must not be found on server %d %s", i, serverIndex, stage).isFalse();

      assertThat(compacted.getBloomSkippedSeries() - skippedBefore)
          .as("server %d must actually be USING the replicated filters %s", serverIndex, stage).isGreaterThan(0);
    });
  }

  private void insert(final Database database, final int from, final int to) {
    for (int start = from; start < to; start += TX_CHUNK) {
      final int batchStart = start;
      database.transaction(() -> {
        for (int i = batchStart; i < Math.min(batchStart + TX_CHUNK, to); i++)
          database.newVertex(TYPE_NAME).set("uid", uid(i)).save();
      });
    }
  }

  private static void compact(final Database database) throws Exception {
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX_NAME);
    assertThat(index.scheduleCompaction()).as("the index must be schedulable for compaction").isTrue();
    assertThat(index.compact()).as("the compaction must run on the leader").isTrue();
  }

  private void awaitReplication() throws Exception {
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);
  }

  private static LSMTreeIndexCompacted compactedOf(final TypeIndex index) {
    return ((LSMTreeIndex) index.getIndexesOnBuckets()[0]).getMutableIndex().getSubIndex();
  }

  /** High-entropy key so the compacted index does not compress away to a trivial size. */
  private static String uid(final int i) {
    final long mixed = i * 0x9E3779B97F4A7C15L;
    return Long.toHexString(mixed) + "-" + Long.toHexString(Long.reverse(mixed) ^ i) + "-" + i;
  }

  /**
   * A key that was never inserted but sorts AMONG the stored ones, so the series' root page cannot rule it out and the
   * filter is the only thing that can. Drawing absent keys from their own prefix would leave every one of them outside
   * every series' range, where no filter is ever consulted.
   */
  private static String absentUid(final int i) {
    final long mixed = i * 0x9E3779B97F4A7C15L;
    return Long.toHexString(mixed) + "-" + Long.toHexString(Long.reverse(mixed) ^ i) + "-absent-" + i;
  }
}
