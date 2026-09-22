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
package com.arcadedb.engine.timeseries;

import com.arcadedb.engine.timeseries.TimeSeriesSealedStore.BlockDirectorySnapshot;
import com.arcadedb.engine.timeseries.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.timeseries.codec.DictionaryCodec;
import com.arcadedb.engine.timeseries.codec.GorillaXORCodec;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8166, the sibling issue #8043 named in its own text and PR #8095 did not close: a walk that crosses a
 * DOWNSAMPLE returned neither the fine rows nor the coarse ones.
 * <p>
 * #8043's half is fixed and is not retested here: a block's identity is now written into its record, so a block
 * a compaction, a truncate rewrite or an HA install copied verbatim resolves again and the walk reads it.
 * {@code downsampleBlocks} is the one rewrite that does neither - it neither keeps a block nor drops it, it
 * REPLACES it with coarser rows computed from it and re-chunks whatever comes out - so every remaining snapshot
 * entry resolved to {@code null} and {@code walkBlocks} counted it as gone. The rows were not gone; a fresh walk
 * over the same range returned them, coarsened.
 * <p>
 * The fix does not hand those coarse rows to the in-flight walk, because that answer is not consistent either:
 * the rows it has already emitted are FINE and these are their COARSE replacements, so the bucket it is standing
 * in would be counted at two resolutions. It raises instead, and tells that case apart from the one where a short
 * answer IS the right answer - a retention {@code truncateBefore}, whose rows really are gone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8166">issue #8166</a>
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8043">issue #8043</a>
 */
class Issue8166WalkAcrossDownsampleTest {

  private static final String BASE_DIR   = "target/databases/Issue8166WalkAcrossDownsampleTest";
  private static final String STORE_PATH = BASE_DIR + "/store";

  /** Coarser than the 1000 ms span of the blocks below, so they qualify; the cutoff leaves none behind. */
  private static final long GRANULARITY_MS = 10_000L;
  private static final long CUTOFF_TS      = 9_000L;

  private static final List<Integer> TAG_COLUMNS     = List.of(1);
  private static final List<Integer> NUMERIC_COLUMNS = List.of(2);

  private List<ColumnDefinition> columns;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(BASE_DIR));
    new File(BASE_DIR).mkdirs();

    columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor_id", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(BASE_DIR));
  }

  /**
   * The issue as reported: three sealed blocks, six rows, a walk in flight, and a downsample landing between two
   * blocks. It used to return zero rows and report the walk as COMPLETED.
   */
  @Test
  void aWalkCrossingADownsampleRaisesInsteadOfAnsweringSilentlyShort() throws Exception {
    try (final TimeSeriesSealedStore store = threeBlockStore()) {
      final BlockDirectorySnapshot snapshot = store.snapshotBlockDirectory(0L, Long.MAX_VALUE);
      assertThat(snapshot.blocks()).hasSize(3);
      assertThat(readAll(store, snapshot)).as("the baseline the walk would have produced").hasSize(6);

      store.downsampleBlocks(CUTOFF_TS, GRANULARITY_MS, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      final AggregationMetrics metrics = new AggregationMetrics();
      final List<Object[]> rows = new ArrayList<>();
      assertThatThrownBy(() -> store.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, metrics, rows::add))
          .isInstanceOf(TimeSeriesWalkCoarsenedException.class)
          .hasMessageContaining("downsample");

      // And the reason it must not answer short: the rows ARE still in the store, coarsened into one row per
      // (bucket, tag). A caller that runs the read again gets a whole answer at one resolution.
      assertThat(readAll(store, store.snapshotBlockDirectory(0L, Long.MAX_VALUE)))
          .as("one row per sensor, all six samples folded into the single 10s bucket").hasSize(3);
    }
  }

  /**
   * The two arms of the same walk must answer the same question - the contract PR #8095 established for the
   * install case and #8043's own tests pin for the truncate one.
   */
  @Test
  void theTagCombinationArmRaisesForTheSameCrossing() throws Exception {
    try (final TimeSeriesSealedStore store = threeBlockStore()) {
      final BlockDirectorySnapshot snapshot = store.snapshotBlockDirectory(0L, Long.MAX_VALUE);
      store.downsampleBlocks(CUTOFF_TS, GRANULARITY_MS, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      final List<Object[]> combinations = new ArrayList<>();
      assertThatThrownBy(() -> store.forEachTagCombination(snapshot, 0L, Long.MAX_VALUE, new int[] { 0 }, null,
          combinations::add))
          .isInstanceOf(TimeSeriesWalkCoarsenedException.class);
    }
  }

  /**
   * The discriminator, and the reason the fix is not simply "raise whenever a block fails to resolve". A
   * retention truncate removes rows for good, so a short answer is the CORRECT answer and raising would turn
   * every retention pass into a failed read. Counted, as #8043 made it, and not raised.
   */
  @Test
  void aRetentionTruncateIsStillCountedAndNeverRaises() throws Exception {
    try (final TimeSeriesSealedStore store = threeBlockStore()) {
      final BlockDirectorySnapshot snapshot = store.snapshotBlockDirectory(0L, Long.MAX_VALUE);

      store.truncateBefore(5000L);

      final AggregationMetrics metrics = new AggregationMetrics();
      final List<Object[]> rows = new ArrayList<>();
      assertThat(store.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, metrics, rows::add)).isTrue();

      assertThat(rows).hasSize(2);
      assertThat(metrics.getVanishedBlocks()).isEqualTo(2);
    }
  }

  /**
   * A downsample that finished BEFORE the snapshot was taken is not a crossing: the snapshot already names the
   * coarse blocks, they all resolve, and nothing is raised. This is what keeps the epoch comparison from firing
   * on the ordinary steady state of a store that downsamples on a schedule.
   */
  @Test
  void aDownsampleThatPrecedesTheSnapshotIsNotACrossing() throws Exception {
    try (final TimeSeriesSealedStore store = threeBlockStore()) {
      store.downsampleBlocks(CUTOFF_TS, GRANULARITY_MS, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      final BlockDirectorySnapshot snapshot = store.snapshotBlockDirectory(0L, Long.MAX_VALUE);
      final AggregationMetrics metrics = new AggregationMetrics();

      assertThatCode(() -> assertThat(readAll(store, snapshot)).hasSize(3)).doesNotThrowAnyException();
      assertThat(metrics.getVanishedBlocks()).isZero();
    }
  }

  /**
   * A maintenance cycle that selects nothing does not rewrite, so it does not move the epoch either and a walk
   * spanning it is untouched. Without this the guard would fire on every idle downsampling cycle.
   */
  @Test
  void anIdleDownsampleCycleDoesNotDisturbAWalk() throws Exception {
    try (final TimeSeriesSealedStore store = threeBlockStore()) {
      final BlockDirectorySnapshot snapshot = store.snapshotBlockDirectory(0L, Long.MAX_VALUE);

      // Nothing is older than the cutoff, so downsampleBlocks selects no block and returns without rewriting.
      store.downsampleBlocks(0L, GRANULARITY_MS, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      assertThat(readAll(store, snapshot)).hasSize(6);
    }
  }

  /**
   * The review point on PR #8197, and the reason the epoch is not the whole discriminator: a long walk can meet
   * BOTH maintenance passes. A retention truncate legitimately drops one of its blocks while an unrelated
   * downsample runs elsewhere in the same store, and a store-wide "did any downsample happen" test would refuse
   * an answer that is perfectly honest. Retention is asked first and answers definitively, so each vanished
   * block is attributed to the pass that actually removed it.
   */
  @Test
  void aTruncatedBlockIsStillCountedEvenWhenADownsampleAlsoRan() throws Exception {
    try (final TimeSeriesSealedStore store = threeBlockStore()) {
      // A fourth block, NEWER than the downsample cutoff, so the downsample cannot touch it and the truncate can.
      appendBlock(store, 20_000L, "D");
      store.flushHeader();

      final BlockDirectorySnapshot snapshot = store.snapshotBlockDirectory(0L, Long.MAX_VALUE);
      assertThat(snapshot.blocks()).hasSize(4);

      // Retention drops the three old blocks; the downsample then finds nothing left to coarsen, so to make both
      // passes real the order is reversed: coarsen first, then truncate the coarse result away entirely.
      store.downsampleBlocks(CUTOFF_TS, GRANULARITY_MS, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      store.truncateBefore(20_000L);

      final AggregationMetrics metrics = new AggregationMetrics();
      final List<Object[]> rows = new ArrayList<>();
      assertThat(store.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, metrics, rows::add))
          .as("every block retention removed is accounted for, so nothing is refused").isTrue();

      assertThat(rows).as("the one block retention kept is still read").hasSize(2);
      assertThat(metrics.getVanishedBlocks())
          .as("and the three blocks retention provably removed are counted, not blamed on the downsample")
          .isEqualTo(3);
    }
  }

  /**
   * The TAIL truncate gets NO boundary of its own, and the review of PR #8197 is why: an upper boundary cannot be
   * monotonic in a store that keeps compacting forward, and {@code truncateToBlockCount} is not a retention
   * decision in the first place - both its callers are rollback paths in {@code TimeSeriesShard}, undoing an
   * interrupted or failed compaction rather than retiring a time range. Feeding it a watermark pinned the store
   * near "now" for the rest of the process, after which every later block looked retired and a real downsample
   * over it was answered silently short: the defect this class exists to close, reintroduced through the back
   * door.
   * <p>
   * So a tail truncate takes the ordinary route. On its own it leaves the walk correctly short and counted; with
   * a downsample beside it the walk is refused, which is the conservative half of the trade and the one that
   * cannot return a wrong answer.
   */
  @Test
  void aTailTruncateAloneIsCountedAndWithADownsampleIsRefused() throws Exception {
    try (final TimeSeriesSealedStore store = threeBlockStore()) {
      final BlockDirectorySnapshot snapshot = store.snapshotBlockDirectory(0L, Long.MAX_VALUE);
      // Keeps the first block, drops the two above it - a rollback of blocks this store had speculatively added.
      store.truncateToBlockCount(1);

      final AggregationMetrics metrics = new AggregationMetrics();
      final List<Object[]> rows = new ArrayList<>();
      assertThat(store.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, metrics, rows::add))
          .as("no downsample ran, so nothing is refused").isTrue();
      assertThat(rows).as("the block it kept still reads").hasSize(2);
      assertThat(metrics.getVanishedBlocks()).isEqualTo(2);
    }

    FileUtils.deleteRecursively(new File(BASE_DIR));
    new File(BASE_DIR).mkdirs();

    try (final TimeSeriesSealedStore store = threeBlockStore()) {
      final BlockDirectorySnapshot snapshot = store.snapshotBlockDirectory(0L, Long.MAX_VALUE);
      store.downsampleBlocks(CUTOFF_TS, GRANULARITY_MS, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      store.truncateToBlockCount(0);

      final List<Object[]> rows = new ArrayList<>();
      assertThatThrownBy(() -> store.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, null, rows::add))
          .as("a downsample ran and no boundary excuses these blocks, so the walk is refused")
          .isInstanceOf(TimeSeriesWalkCoarsenedException.class);
    }
  }

  /**
   * The reproducer the review of PR #8197 described, and the reason the tail boundary is gone rather than merely
   * moved: a shard whose compaction once rolled back must not spend the rest of the process excusing every block
   * it compacts afterwards as retired.
   * <p>
   * Rolls back a tail truncate, seals NEW blocks past the line it drew, downsamples THOSE, and walks across the
   * downsample with a snapshot taken before it. With a tail watermark this answered silently short; the refusal
   * is the whole point of the class.
   */
  @Test
  void aRolledBackTailTruncateDoesNotExcuseTheBlocksCompactedAfterIt() throws Exception {
    try (final TimeSeriesSealedStore store = threeBlockStore()) {
      // The rollback: every speculative block goes, as a failed compact() would undo them. The line it draws is
      // at timestamp 1000, the first block dropped - and everything the shard seals from here on is above it,
      // which is exactly what a watermark fed from this call would then excuse for the rest of the process.
      store.truncateToBlockCount(0);
      assertThat(store.getBlockCount()).isZero();

      // The shard carries on, sealing new blocks ABOVE the line that rollback drew.
      appendBlock(store, 3000L, "B");
      appendBlock(store, 5000L, "C");
      store.flushHeader();

      final BlockDirectorySnapshot snapshot = store.snapshotBlockDirectory(0L, Long.MAX_VALUE);
      assertThat(snapshot.blocks()).hasSize(2);

      // ... and those new blocks are coarsened, which is a REPLACEMENT and not a retirement.
      store.downsampleBlocks(CUTOFF_TS, GRANULARITY_MS, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      final List<Object[]> rows = new ArrayList<>();
      assertThatThrownBy(() -> store.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, null, rows::add))
          .as("a rollback is not retention, so it may not excuse a later downsample as one")
          .isInstanceOf(TimeSeriesWalkCoarsenedException.class);
    }
  }

  /**
   * The path the epoch alone cannot see, raised in the review of PR #8197: the rewrite happened on ANOTHER node.
   * <p>
   * An HA follower installs the leader's sealed file wholesale. If the leader downsampled between this walk's
   * snapshot and that install, the image carries coarse blocks with new ids, the follower's snapshot entries
   * resolve to nothing, and its own downsample epoch never moved - so the walk would have counted them as
   * retention loss and answered short. That is issue #8166 exactly, reached through the one path where this
   * process did no downsampling at all.
   */
  @Test
  void aWalkCrossingAnInstallOfALeaderDownsampledImageIsRefusedToo() throws Exception {
    final byte[] leaderV1;
    final byte[] leaderCoarsened;
    try (final TimeSeriesSealedStore leader = new TimeSeriesSealedStore(BASE_DIR + "/leader", columns)) {
      appendBlock(leader, 1000L, "A");
      appendBlock(leader, 3000L, "B");
      appendBlock(leader, 5000L, "C");
      leader.flushHeader();
      leaderV1 = leader.readWholeSealedFile();
      leader.downsampleBlocks(CUTOFF_TS, GRANULARITY_MS, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      leaderCoarsened = leader.readWholeSealedFile();
    }

    try (final TimeSeriesSealedStore follower = new TimeSeriesSealedStore(STORE_PATH, columns)) {
      // A follower's sealed file IS the leader's, byte for byte - that is what makes the block ids the same on
      // both nodes (issue #8043), and asserting the refusal against independently written blocks would only be
      // asserting that two random ids differ.
      follower.installSealedFileBytes(leaderV1);
      final BlockDirectorySnapshot snapshot = follower.snapshotBlockDirectory(0L, Long.MAX_VALUE);
      assertThat(snapshot.blocks()).hasSize(3);

      follower.installSealedFileBytes(leaderCoarsened);

      final List<Object[]> rows = new ArrayList<>();
      assertThatThrownBy(() -> follower.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, null, rows::add))
          .as("this node ran no downsample, so only the image itself can say the rows were rewritten")
          .isInstanceOf(TimeSeriesWalkCoarsenedException.class);
    }
  }

  /**
   * The counter-case that keeps the containment test from becoming "raise on every install": a leader that
   * RETIRED old blocks rather than coarsening them leaves the follower's walk correctly short, and counted.
   */
  @Test
  void aWalkCrossingAnInstallOfALeaderRetiredImageIsStillCounted() throws Exception {
    final byte[] leaderV1;
    final byte[] leaderRetired;
    try (final TimeSeriesSealedStore leader = new TimeSeriesSealedStore(BASE_DIR + "/leader2", columns)) {
      appendBlock(leader, 1000L, "A");
      appendBlock(leader, 3000L, "B");
      appendBlock(leader, 5000L, "C");
      leader.flushHeader();
      leaderV1 = leader.readWholeSealedFile();
      leader.truncateBefore(5000L);
      leaderRetired = leader.readWholeSealedFile();
    }

    try (final TimeSeriesSealedStore follower = new TimeSeriesSealedStore(STORE_PATH, columns)) {
      follower.installSealedFileBytes(leaderV1);
      final BlockDirectorySnapshot snapshot = follower.snapshotBlockDirectory(0L, Long.MAX_VALUE);

      follower.installSealedFileBytes(leaderRetired);

      final AggregationMetrics metrics = new AggregationMetrics();
      final List<Object[]> rows = new ArrayList<>();
      assertThat(follower.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, metrics, rows::add))
          .as("the leader deleted those rows; a short answer is the right one").isTrue();

      assertThat(rows).as("the block the leader kept still reads").hasSize(2);
      assertThat(metrics.getVanishedBlocks()).isEqualTo(2);
    }
  }

  // ---- Helpers ----

  private TimeSeriesSealedStore threeBlockStore() throws Exception {
    final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, columns);
    appendBlock(store, 1000L, "A");
    appendBlock(store, 3000L, "B");
    appendBlock(store, 5000L, "C");
    store.flushHeader();
    return store;
  }

  private static List<Object[]> readAll(final TimeSeriesSealedStore store, final BlockDirectorySnapshot snapshot)
      throws Exception {
    final List<Object[]> rows = new ArrayList<>();
    assertThat(store.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, null, rows::add)).isTrue();
    return rows;
  }

  private void appendBlock(final TimeSeriesSealedStore store, final long baseTs, final String tag) throws Exception {
    store.appendBlock(2, baseTs, baseTs + 1000L, new byte[][] {
        DeltaOfDeltaCodec.encode(new long[] { baseTs, baseTs + 1000L }),
        DictionaryCodec.encode(new String[] { tag, tag }),
        GorillaXORCodec.encode(new double[] { 1.0, 2.0 })
    }, new double[] { Double.NaN, Double.NaN, 1.0 }, new double[] { Double.NaN, Double.NaN, 2.0 },
        new double[] { Double.NaN, Double.NaN, 3.0 }, new long[] { 0, 0, 2 },
        new String[][] { null, { tag }, null });
  }
}
