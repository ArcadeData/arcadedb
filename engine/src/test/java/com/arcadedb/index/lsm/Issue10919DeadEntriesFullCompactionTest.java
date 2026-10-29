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
package com.arcadedb.index.lsm;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10919: under a sliding window over monotonic keys (insert the newest ids, {@code DELETE ... WHERE id < ?} the
 * oldest) the incremental compactions keep every tombstone and the entry it kills, and the only thing that dropped them
 * was the full compaction started by the series count ({@code arcadedb.indexCompactionFullSeriesThreshold}, 10). Until
 * then every scan from the low end walked every key deleted since the last full compaction.
 * <p>
 * A full compaction now also starts once the dead keys the scans skipped since the last one reach the entries of the
 * compacted sub-index ({@code arcadedb.indexCompactionFullDeadEntriesRatio}): the scans have then paid what the
 * compaction costs. A deleted key no scan crosses costs nothing and starts nothing, and a few dead keys never rewrite a
 * large index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10919DeadEntriesFullCompactionTest extends TestHelper {
  private static final String TYPE_NAME     = "Doc";
  private static final int    PER_ROUND     = 100;
  private static final int    WINDOW        = 10;
  private static final int    ROUNDS        = 260;
  // the issue compacted about every 8 windows (a series per ~470 rounds over a 60-round window)
  private static final int    COMPACT_EVERY = 20;

  @Test
  void scansOfASlidingWindowStartAFullCompactionLongBeforeTheSeriesThreshold() throws Exception {
    final WindowRun withRatio = runSlidingWindow(1f, true);
    final WindowRun seriesOnly = runSlidingWindow(0f, true);

    // what the issue measured: with the series threshold alone the series pile up to 10 before anything drops them
    assertThat(seriesOnly.maxSeries).as("series threshold alone: %s vs %s", seriesOnly, withRatio).isGreaterThanOrEqualTo(9);

    // the scans pay for one compaction cycle at most, so every compaction after the first finds a full one due
    assertThat(withRatio.maxSeries).as("series with the dead-entries trigger").isLessThanOrEqualTo(2);
    assertThat(withRatio.earlyFullCompactions).as("full compactions started by the scans")
        .isGreaterThanOrEqualTo(ROUNDS / COMPACT_EVERY - 2);
    assertThat(withRatio.maxCompactedEntries).as("compacted entries with the dead-entries trigger")
        .isLessThan(seriesOnly.maxCompactedEntries / 3);
    assertThat(withRatio.deadEntriesSkipped).as("dead keys walked by the scans")
        .isLessThan(seriesOnly.deadEntriesSkipped / 3);
  }

  @Test
  void deletedKeysNoScanCrossesDoNotStartAFullCompaction() throws Exception {
    final WindowRun run = runSlidingWindow(1f, false);

    assertThat(run.deadEntriesSkipped).as("no scan crossed a deleted key").isZero();
    assertThat(run.earlyFullCompactions).as("full compactions before the series threshold").isZero();
    assertThat(run.maxSeries).as("left to the series threshold").isGreaterThanOrEqualTo(9);
  }

  @Test
  void aFewDeadKeysDoNotRewriteALargeIndex() throws Exception {
    final LSMTreeIndex index = createTypeAndIndex(1f);

    insert(0, 20_000);
    compact(index);
    final LSMTreeIndexCompacted compacted = index.getMutableIndex().getSubIndex();

    // 10 dead keys at the low end, crossed by 20 scans: 200 skipped keys against 20,000 compacted entries
    database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE_NAME + " WHERE id < 10"));
    for (int i = 0; i < 20; i++)
      assertThat(countBelow(100)).isEqualTo(90);
    assertThat(index.getDeadEntriesSkippedSinceFullCompaction()).isEqualTo(200);

    insert(20_000, 22_000);
    compact(index);

    assertThat(index.getMutableIndex().getSubIndex()).as("an incremental round appends to the same file").isSameAs(compacted);
    assertThat(compacted.getSeriesCount()).isEqualTo(2);
  }

  @Test
  void theEntryCountAfterAReopenMatchesTheOneKeptByTheCompactions() throws Exception {
    LSMTreeIndex index = createTypeAndIndex(0f);

    insert(0, 3_000);
    compact(index);
    deleteBelow(1_000);
    insert(3_000, 5_000);
    compact(index);
    insert(5_000, 5_500);
    compact(index);

    // the newest mutable page stays in the mutable file, so not every key is compacted yet
    final long kept = index.getMutableIndex().getSubIndex().getEntryCount();
    assertThat(kept).as("tombstones and the entries they kill are both still there").isBetween(4_500L, 6_500L);

    reopenDatabase();
    index = index();
    assertThat(index.getMutableIndex().getSubIndex().getEntryCount()).as("counted from the page headers").isEqualTo(kept);

    // a full compaction keeps the count of the file it writes: only the live entries
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_FULL_SERIES, 1);
    insert(5_500, 6_000);
    compact(index);
    final LSMTreeIndexCompacted afterFull = index.getMutableIndex().getSubIndex();
    assertThat(afterFull.getSeriesCount()).isEqualTo(1);
    final long live = afterFull.getEntryCount();
    assertThat(live).as("only live keys are left").isBetween(4_000L, 5_000L);

    reopenDatabase();
    assertThat(index().getMutableIndex().getSubIndex().getEntryCount()).isEqualTo(live);
  }

  // ---------------------------------------------------------------------------------------------

  private record WindowRun(int maxSeries, long maxCompactedEntries, long deadEntriesSkipped, int earlyFullCompactions) {
  }

  /**
   * The workload of the issue, scaled down: each round inserts the next {@link #PER_ROUND} ids and, once the window is
   * full, deletes the oldest round, either with the range scan of the issue or record by record (no scan crosses a dead
   * key). Compacts every {@link #COMPACT_EVERY} rounds, with automatic compaction off so the run is deterministic.
   */
  private WindowRun runSlidingWindow(final float ratio, final boolean deleteWithARangeScan) throws Exception {
    final LSMTreeIndex index = createTypeAndIndex(ratio);
    final Deque<List<RID>> rounds = new ArrayDeque<>();

    int maxSeries = 0;
    long maxCompactedEntries = 0;
    int earlyFullCompactions = 0;
    LSMTreeIndexCompacted previous = null;

    for (int round = 1; round <= ROUNDS; round++) {
      rounds.addLast(insert((round - 1) * PER_ROUND, round * PER_ROUND));

      if (round > WINDOW) {
        final List<RID> oldest = rounds.removeFirst();
        if (deleteWithARangeScan)
          deleteBelow((round - WINDOW) * PER_ROUND);
        else
          database.transaction(() -> {
            for (final RID rid : oldest)
              database.deleteRecord(rid.asDocument());
          });
      }

      // a compaction needs a full page to move: the last one stays mutable
      if (round % COMPACT_EVERY == 0 && index.getMutableIndex().getTotalPages() > 1 && index.scheduleCompaction()) {
        final int seriesBefore = previous != null ? previous.getSeriesCount() : 0;
        assertThat(index.compact()).isTrue();

        final LSMTreeIndexCompacted compacted = index.getMutableIndex().getSubIndex();
        if (previous != null && compacted != previous) {
          // a full compaction writes a new file
          if (seriesBefore < 10)
            ++earlyFullCompactions;
          assertThat(index.getDeadEntriesSkippedSinceFullCompaction()).as("reset by the full compaction").isZero();
        }
        previous = compacted;

        maxSeries = Math.max(maxSeries, compacted.getSeriesCount());
        maxCompactedEntries = Math.max(maxCompactedEntries, compacted.getEntryCount());
      }

      // counted on the bucket, so the only scans crossing the dead keys are the deletes of the window
      assertThat(database.countType(TYPE_NAME, false)).as("live records at round %d", round)
          .isEqualTo(Math.min(round, WINDOW) * PER_ROUND);
    }

    final long deadEntriesSkipped = index.getStats().get("deadEntriesSkipped");
    // the index still answers exactly the live window
    assertThat(countBelow(Integer.MAX_VALUE)).isEqualTo(WINDOW * PER_ROUND);
    database.getSchema().dropType(TYPE_NAME);
    return new WindowRun(maxSeries, maxCompactedEntries, deadEntriesSkipped, earlyFullCompactions);
  }

  private LSMTreeIndex createTypeAndIndex(final float ratio) {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_FULL_SERIES, 10);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_FULL_DEAD_ENTRIES_RATIO, ratio);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("id", Integer.class);
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(4_096).create();

    return index();
  }

  private List<RID> insert(final int fromInclusive, final int toExclusive) {
    final List<RID> rids = new ArrayList<>(toExclusive - fromInclusive);
    database.transaction(() -> {
      for (int i = fromInclusive; i < toExclusive; ++i)
        rids.add(database.newDocument(TYPE_NAME).set("id", i).save().getIdentity());
    });
    return rids;
  }

  private void deleteBelow(final int cutoff) {
    database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE_NAME + " WHERE id < ?", cutoff));
  }

  private long countBelow(final int cutoff) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE id < ?", cutoff)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private void compact(final LSMTreeIndex index) throws Exception {
    assertThat(index.scheduleCompaction()).isTrue();
    assertThat(index.compact()).isTrue();
  }

  private LSMTreeIndex index() {
    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next();
    return (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
  }
}
