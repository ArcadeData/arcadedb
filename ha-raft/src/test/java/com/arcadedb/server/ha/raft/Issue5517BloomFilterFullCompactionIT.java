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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A FULL compaction is the other half of the replication story, and the destructive half.
 * <p>
 * An incremental round appends to files every node already holds. A full round instead merges every series into a
 * BRAND NEW compacted file, retires the old one, and drops it - so the leader must ship two new files (the compacted
 * index and its filters), the followers must drop two old ones, and every node has to end up with a filter directory
 * describing the single surviving series. Three ways that could go wrong and stay silent:
 * <ul>
 *   <li>the new {@code .bfidx} never ships, and followers lose their filters until the next round;</li>
 *   <li>the old {@code .bfidx} is never dropped on the followers, leaking a file per full compaction that the next
 *   open has to sweep;</li>
 *   <li>worst, the old directory somehow survives against the new compacted file, whose series sits at root page 0
 *   just like the old one's first series did - the reused-position case the entry's page count and last-page
 *   fingerprint exist to catch.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5517BloomFilterFullCompactionIT extends BaseRaftHATest {
  private static final int    FIRST_BATCH  = 20_000;
  private static final int    SECOND_BATCH = 20_000;
  private static final int    THIRD_BATCH  = 20_000;
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
    config.setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    // Each incremental round appends one series; at 2 the NEXT round is a full one. Series are accumulated by
    // compacting repeatedly rather than by starving the compaction of RAM, because a full merge that does not fit its
    // RAM budget silently falls back to another incremental round - and this test would then prove nothing.
    config.setValue(GlobalConfiguration.INDEX_COMPACTION_FULL_SERIES, 2);
    config.setValue(GlobalConfiguration.INDEX_BLOOM_FILTER_RATE, 0.01f);
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Compacted file names embed a per-node nanoTime, so the comparator cannot pair bucket indexes by name after a
    // compaction. What must be equal is asserted in the test body.
  }

  @Test
  public void aFullCompactionShipsTheNewFiltersAndDropsTheOld() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final VertexType type = leader.getSchema().buildVertexType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("uid", String.class);
    leader.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, "uid");

    // ROUNDS 1 and 2 - incremental, one series each.
    insert(leader, 0, FIRST_BATCH);
    compact(leader);
    insert(leader, FIRST_BATCH, FIRST_BATCH + SECOND_BATCH);
    compact(leader);
    awaitReplication();

    final Map<Integer, File> filtersBefore = new HashMap<>();
    final Map<Integer, Integer> compactedFileIdsBefore = new HashMap<>();
    testEachServer(serverIndex -> {
      final LSMTreeIndexCompacted compacted = compactedOf(indexOn(serverIndex));
      assertThat(compacted.getSeriesCount())
          .as("server %d needs several series for the next round to merge", serverIndex).isGreaterThan(1);
      assertThat(compacted.getBloomFilter()).as("server %d must have filters before the full round", serverIndex)
          .isNotNull();
      filtersBefore.put(serverIndex, compacted.getBloomFilter().getOSFile());
      compactedFileIdsBefore.put(serverIndex, compacted.getFileId());
    });

    // Set the threshold on the leader's own database: the server-level configuration does not reach the value
    // isFullCompactionDue() reads, so relying on onServerConfiguration alone leaves it at the default of 10 and the
    // round below stays incremental - which is exactly how the first version of this test passed nothing.
    leader.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_FULL_SERIES, 2);

    // ROUND 3 - full: every series merges into a NEW file holding ONE, and the old file is retired.
    insert(leader, FIRST_BATCH + SECOND_BATCH, FIRST_BATCH + SECOND_BATCH + THIRD_BATCH);
    compact(leader);
    awaitReplication();

    final int totalRecords = FIRST_BATCH + SECOND_BATCH + THIRD_BATCH;
    testEachServer(serverIndex -> {
      final Database database = getServerDatabase(serverIndex, getDatabaseName());
      final Index index = indexOn(serverIndex);
      final LSMTreeIndexCompacted compacted = compactedOf((TypeIndex) index);

      assertThat(compacted.getFileId())
          .as("server %d must be on the NEW compacted file, i.e. the round really was full", serverIndex)
          .isNotEqualTo(compactedFileIdsBefore.get(serverIndex));
      assertThat(compacted.getSeriesCount())
          .as("a full compaction merges every series into one, on server %d", serverIndex).isEqualTo(1);

      assertThat(database.countType(TYPE_NAME, false))
          .as("every record must be on server %d", serverIndex).isEqualTo(totalRecords);
      assertThat(index.countEntries())
          .as("server %d must hold the WHOLE index after the full compaction", serverIndex).isEqualTo(totalRecords);

      final LSMTreeIndexBloomFilter filter = compacted.getBloomFilter();
      assertThat(filter).as("server %d must have received the NEW filter file", serverIndex).isNotNull();
      assertThat(filter.getOSFile())
          .as("server %d must be reading a different filter file than before the full round", serverIndex)
          .isNotEqualTo(filtersBefore.get(serverIndex));
      assertThat(filter.getPublishedFilters())
          .as("server %d must have a filter for the one surviving series", serverIndex).isEqualTo(1);

      // The gate, on the node that received the file rather than the one that wrote it.
      for (int i = 0; i < totalRecords; i++)
        assertThat(index.get(new Object[] { uid(i) }).hasNext())
            .as("key %d must be findable on server %d after the full compaction", i, serverIndex).isTrue();

      final long skippedBefore = compacted.getBloomSkippedSeries();
      for (int i = 0; i < 2_000; i++)
        assertThat(index.get(new Object[] { absentUid(i) }).hasNext())
            .as("absent key %d must not be found on server %d", i, serverIndex).isFalse();
      assertThat(compacted.getBloomSkippedSeries() - skippedBefore)
          .as("server %d must be USING the replicated filters", serverIndex).isGreaterThan(0);

      // The old filter file must go with the compacted file it described - on the FOLLOWERS too, which never ran the
      // compaction and only saw it as a file to remove. One leaked file per full compaction would accumulate.
      assertThat(filtersBefore.get(serverIndex))
          .as("the retired filter file must be dropped on server %d", serverIndex).doesNotExist();

      assertThat(bloomFilterFilesOnDisk(serverIndex))
          .as("server %d must keep exactly one filter file, for its one bucket index", serverIndex).isEqualTo(1);
    });
  }

  private int bloomFilterFilesOnDisk(final int serverIndex) {
    final String[] files = new File(getDatabasePath(serverIndex)).list((dir, name) -> name.endsWith(".bfidx"));
    return files == null ? 0 : files.length;
  }

  private TypeIndex indexOn(final int serverIndex) {
    return (TypeIndex) getServerDatabase(serverIndex, getDatabaseName()).getSchema().getIndexByName(INDEX_NAME);
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

  /** Never inserted, but sorts AMONG the stored keys, so only a filter can rule it out. */
  private static String absentUid(final int i) {
    final long mixed = i * 0x9E3779B97F4A7C15L;
    return Long.toHexString(mixed) + "-" + Long.toHexString(Long.reverse(mixed) ^ i) + "-absent-" + i;
  }
}
