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

import com.arcadedb.engine.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.codec.DictionaryCodec;
import com.arcadedb.engine.codec.GorillaXORCodec;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Issue #11984, on the sealed store itself: a block is downsampled when that reduces it, that is when two of its samples share a
 * (tag values, bucket), and for no other reason. What the marker of a downsampled block cannot say - it lives in memory, so a restart
 * resets it - the samples do, and that is what these tests pin: a store that was closed and opened again does not rewrite a block that
 * was downsampled before (and does not move its timestamps onto a bucket of the next finer tier), a block that has one sample per
 * group is left as it is whatever its average spacing, a block that is not sorted is judged by its groups like any other, and every
 * rewrite leaves fewer samples than it found, which is what the replication reads to know the file changed.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue11984DownsampleReducibilityTest {

  private static final String BASE_DIR   = "target/databases/Issue11984DownsampleReducibilityTest";
  private static final String STORE_PATH = BASE_DIR + "/store";

  private static final long MINUTE = 60_000L;
  private static final long H      = 60 * MINUTE;
  private static final long DAY    = 24 * H;

  private static final List<Integer> TAG_COLUMNS     = List.of(1, 2);
  private static final List<Integer> NUMERIC_COLUMNS = List.of(3);

  private static final List<ColumnDefinition> COLUMNS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
      new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
      new ColumnDefinition("dc", Type.STRING, ColumnDefinition.ColumnRole.TAG),
      new ColumnDefinition("v", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

  private record Sample(long ts, String host, String dc, double v) {
  }

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(BASE_DIR));
    new File(BASE_DIR).mkdirs();
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(BASE_DIR));
  }

  /**
   * A block downsampled to a day on a grid that starts at 18:30 holds one sample per day, at 18:30. After a restart its marker is
   * gone, and a pass of the hourly tier (offset 0, which the daily grid is not nested in) used to select it, floor every timestamp to
   * 18:00 and leave the daily tier to put each of them into the PREVIOUS day: one bucket back per restart.
   */
  @Test
  void aReopenedStoreDoesNotMoveABlockDownsampledOnANonNestedGrid() throws Exception {
    final long dailyOffset = 18 * H + 30 * MINUTE;
    final List<Sample> raw = new ArrayList<>();
    for (int day = 0; day < 4; day++)
      for (int i = 0; i < 3; i++)
        raw.add(new Sample(dailyOffset + day * DAY + i * 10 * MINUTE, "a", "x", day * 10 + i));

    final List<Sample> daily = new ArrayList<>();
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, raw);
      store.downsampleBlocks(Long.MAX_VALUE, DAY, dailyOffset, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      assertThat(store.getDownsampleRewriteCount()).isEqualTo(1L);
      assertThat(store.getTotalSampleCount()).as("12 samples folded into one per day").isEqualTo(4L);
      daily.addAll(readAll(store));
    }
    assertThat(daily).extracting(Sample::ts).containsExactly(dailyOffset, dailyOffset + DAY, dailyOffset + 2 * DAY, dailyOffset + 3 * DAY);

    // a restart, as many as it takes: the marker is lost every time and the rows must not move
    for (int restart = 0; restart < 3; restart++)
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
        store.downsampleBlocks(Long.MAX_VALUE, H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
        store.downsampleBlocks(Long.MAX_VALUE, DAY, dailyOffset, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

        assertThat(store.getDownsampleRewriteCount()).as("rewrites after restart %d", restart + 1).isZero();
        assertThat(readAll(store)).containsExactlyElementsOf(daily);
      }
  }

  /**
   * Three series with one sample each per hour, interleaved every 20 minutes: an average spacing of 20 minutes, which the density test
   * read as "downsampling would fold these", and not one hour holds two samples of a series. Nothing to fold, so nothing is rewritten:
   * before, the pass moved every timestamp to its bucket start and left as many samples as it found.
   */
  @Test
  void aBlockWithOneSamplePerGroupIsNotRewrittenWhateverItsAverageSpacing() throws Exception {
    final List<Sample> raw = new ArrayList<>();
    for (int i = 0; i < 18; i++)
      raw.add(new Sample(i * 20 * MINUTE, String.valueOf((char) ('a' + i % 3)), "x", i));

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, raw);

      store.downsampleBlocks(Long.MAX_VALUE, H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      assertThat(store.getDownsampleRewriteCount()).isZero();
      assertThat(readAll(store)).containsExactlyElementsOf(sorted(raw));
      assertThat(store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks()).as("the verdict of the pass")
          .extracting(block -> block.isIrreducibleOn(new TimeSeriesSealedStore.DownsampleGrid(H, 0L))).containsExactly(true);

      // the verdict is the one of THAT grid: two-hour buckets hold two samples of every series, and the block is folded
      store.downsampleBlocks(Long.MAX_VALUE, 2 * H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      assertThat(store.getDownsampleRewriteCount()).isEqualTo(1L);
      assertThat(store.getTotalSampleCount()).as("18 samples, three series, three buckets").isEqualTo(9L);
    }
  }

  /** The same after a restart: the first pass of the new process proves the block irreducible again, by reading it, and writes nothing. */
  @Test
  void aReopenedStoreProvesAnIrreducibleBlockAgainWithoutRewritingIt() throws Exception {
    final List<Sample> raw = new ArrayList<>();
    for (int i = 0; i < 12; i++)
      raw.add(new Sample(i * 90 * MINUTE, "a", "x", i));

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, raw);
    }
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      store.downsampleBlocks(Long.MAX_VALUE, H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      store.downsampleBlocks(Long.MAX_VALUE, H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      assertThat(store.getDownsampleRewriteCount()).isZero();
      assertThat(readAll(store)).containsExactlyElementsOf(raw);
    }
  }

  /** Samples that agree on the bucket and on some tag only are not in one group: the series are told apart by ALL their tags. */
  @Test
  void samplesAreOneGroupOnlyWhenEveryTagAgrees() throws Exception {
    final List<Sample> distinct = List.of(
        new Sample(5 * MINUTE, "a", "x", 1), new Sample(10 * MINUTE, "a", "y", 2), new Sample(15 * MINUTE, "b", "x", 3),
        new Sample(5 * H + MINUTE, "a", "x", 4));
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, distinct);
      store.downsampleBlocks(Long.MAX_VALUE, H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      assertThat(store.getDownsampleRewriteCount()).as("four groups of one sample").isZero();
      assertThat(store.getTotalSampleCount()).isEqualTo(4L);
    }

    final List<Sample> shared = List.of(
        new Sample(5 * MINUTE, "a", "x", 1), new Sample(10 * MINUTE, "a", "y", 2), new Sample(15 * MINUTE, "b", "x", 3),
        new Sample(20 * MINUTE, "a", "x", 5), new Sample(5 * H + MINUTE, "a", "x", 4));
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_DIR + "/shared", COLUMNS)) {
      appendBlock(store, shared);
      store.downsampleBlocks(Long.MAX_VALUE, H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      assertThat(store.getDownsampleRewriteCount()).isEqualTo(1L);
      assertThat(readAll(store)).containsExactly(new Sample(0L, "a", "x", 3.0), new Sample(0L, "a", "y", 2.0),
          new Sample(0L, "b", "x", 3.0), new Sample(5 * H, "a", "x", 4.0));
    }
  }

  /** The verdict is about the samples, which a retention rewrite of the file copies verbatim: it must come across, or every pass would read the history again. */
  @Test
  void theVerdictSurvivesARewriteOfTheFile() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, List.of(new Sample(0L, "a", "x", 1), new Sample(H, "a", "x", 2), new Sample(2 * H, "a", "x", 3)));
      appendBlock(store, List.of(new Sample(10 * H, "a", "x", 4), new Sample(11 * H, "a", "x", 5), new Sample(12 * H, "a", "x", 6)));
      store.downsampleBlocks(Long.MAX_VALUE, H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      assertThat(store.getDownsampleRewriteCount()).isZero();

      store.truncateBefore(5 * H);

      assertThat(store.getBlockCount()).isEqualTo(1);
      assertThat(store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks())
          .extracting(block -> block.isIrreducibleOn(new TimeSeriesSealedStore.DownsampleGrid(H, 0L))).containsExactly(true);
    }
  }

  /**
   * A type has a tier per grid and a pass runs all of them in turn: a block that is irreducible on every one of them has to remember
   * every one, or the tiers would overwrite each other's verdict and read the block again on each pass.
   */
  @Test
  void everyTierKeepsItsOwnVerdict() throws Exception {
    final List<Sample> raw = new ArrayList<>();
    for (int day = 0; day < 5; day++)
      raw.add(new Sample(day * DAY + 7 * MINUTE, "a", "x", day));

    final TimeSeriesSealedStore.DownsampleGrid hourly = new TimeSeriesSealedStore.DownsampleGrid(H, 0L);
    final TimeSeriesSealedStore.DownsampleGrid sixHourly = new TimeSeriesSealedStore.DownsampleGrid(6 * H, 30 * MINUTE);
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, raw);
      for (int pass = 0; pass < 3; pass++) {
        store.downsampleBlocks(Long.MAX_VALUE, hourly.granularityMs(), hourly.offsetMs(), 0, TAG_COLUMNS, NUMERIC_COLUMNS);
        store.downsampleBlocks(Long.MAX_VALUE, sixHourly.granularityMs(), sixHourly.offsetMs(), 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      }

      assertThat(store.getDownsampleRewriteCount()).isZero();
      assertThat(store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks())
          .extracting(block -> block.isIrreducibleOn(hourly), block -> block.isIrreducibleOn(sixHourly))
          .containsExactly(tuple(true, true));
    }
  }

  /** What a block remembers is bounded: the grids of tiers altered over and over push the oldest verdicts out instead of piling up. */
  @Test
  void aBlockRemembersAtMostAFewGrids() throws Exception {
    // samples far enough apart to share no bucket of any grid below
    final List<Sample> raw = new ArrayList<>();
    for (int i = 0; i < 3; i++)
      raw.add(new Sample(i * 1_000 * DAY, "a", "x", i));

    final int grids = TimeSeriesSealedStore.MAX_VERIFIED_GRIDS + 12;
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, raw);
      for (int i = 1; i <= grids; i++)
        store.downsampleBlocks(Long.MAX_VALUE, i * H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      assertThat(store.getDownsampleRewriteCount()).isZero();
      assertThat(store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks()).singleElement()
          .extracting(block -> block.irreducibleOn.length).isEqualTo(TimeSeriesSealedStore.MAX_VERIFIED_GRIDS);
    }
  }

  /**
   * Samples of one bucket are adjacent in a block that is sorted by time, and a block that is not has them anywhere: it is judged by
   * its groups all the same, because the sweep folds it as any other and a rewrite that left as many samples as it found would not
   * be handed to the followers.
   */
  @Test
  void aBlockOutOfOrderIsJudgedByItsGroups() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, List.of(new Sample(2 * H + 5 * MINUTE, "a", "x", 1), new Sample(5 * MINUTE, "a", "x", 2),
          new Sample(3 * H + 5 * MINUTE, "a", "x", 3), new Sample(H + 5 * MINUTE, "a", "x", 4)));
      store.downsampleBlocks(Long.MAX_VALUE, H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      assertThat(store.getDownsampleRewriteCount()).as("four buckets of one sample, in no order").isZero();
      assertThat(store.getTotalSampleCount()).isEqualTo(4L);
    }

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_DIR + "/shared", COLUMNS)) {
      appendBlock(store, List.of(new Sample(2 * H + 5 * MINUTE, "a", "x", 1), new Sample(5 * MINUTE, "a", "x", 2),
          new Sample(2 * H + 10 * MINUTE, "a", "x", 5), new Sample(6 * MINUTE, "a", "x", 4)));
      store.downsampleBlocks(Long.MAX_VALUE, H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      assertThat(store.getDownsampleRewriteCount()).isEqualTo(1L);
      assertThat(readAll(store)).containsExactly(new Sample(0L, "a", "x", 3.0), new Sample(2 * H, "a", "x", 3.0));
    }
  }

  /**
   * Two small downsampled blocks of buckets that are apart are merged into one, as the merge of small blocks does (the consolidation
   * a feed slower than a block per pass relies on), and the merged block, which a restart strips of its marker, still has one sample
   * per group: an hourly and a daily pass rewrite nothing and move nothing.
   */
  @Test
  void aMergeOfDownsampledBlocksOfDisjointBucketsIsNotRewrittenAfterARestart() throws Exception {
    final long off = 18 * H + 30 * MINUTE;
    final List<Sample> rows;
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, List.of(new Sample(off, "a", "x", 1), new Sample(off + 10 * MINUTE, "a", "x", 3),
          new Sample(off + DAY, "a", "x", 5), new Sample(off + DAY + 10 * MINUTE, "a", "x", 7)));
      appendBlock(store, List.of(new Sample(off + 2 * DAY, "a", "x", 9), new Sample(off + 2 * DAY + 10 * MINUTE, "a", "x", 11),
          new Sample(off + 3 * DAY, "a", "x", 13), new Sample(off + 3 * DAY + 10 * MINUTE, "a", "x", 15)));
      store.downsampleBlocks(off + 2 * DAY, DAY, off, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      store.downsampleBlocks(Long.MAX_VALUE, DAY, off, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      rows = readAll(store);
      assertThat(rows).extracting(Sample::ts).containsExactly(off, off + DAY, off + 2 * DAY, off + 3 * DAY);
      assertThat(store.getBlockCount()).isEqualTo(2);

      final TimeSeriesSealedStore.MergePlan plan = store.prepareMerge(DeltaOfDeltaCodec.MAX_BLOCK_SIZE, 0L, 1);
      assertThat(plan).isNotNull();
      assertThat(store.commitMerge(plan)).isTrue();
      assertThat(store.getBlockCount()).isEqualTo(1);
      assertThat(readAll(store)).containsExactlyElementsOf(rows);
    }

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      store.downsampleBlocks(Long.MAX_VALUE, H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      store.downsampleBlocks(Long.MAX_VALUE, DAY, off, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      assertThat(store.getDownsampleRewriteCount()).isZero();
      assertThat(readAll(store)).containsExactlyElementsOf(rows);
    }
  }

  /**
   * A block that cannot be read (its CRC does not match) fails the pass, before anything is written. A rewrite copies the blocks it
   * keeps and stamps a CRC computed over the bytes it copied, so a pass that went on around the damaged block to fold the one next to
   * it would have made the damage look like data: after the reopen the block must still fail its CRC, as it did before.
   */
  @Test
  void aBlockThatCannotBeReadFailsThePassBeforeAnythingIsRewritten() throws Exception {
    writeStoreWithAFirstBlockThatCannotBeRead(
        List.of(new Sample(5 * MINUTE, "a", "x", 1), new Sample(10 * MINUTE, "a", "x", 3), new Sample(15 * MINUTE, "a", "x", 5)),
        List.of(new Sample(5 * H + 5 * MINUTE, "a", "x", 2), new Sample(5 * H + 10 * MINUTE, "a", "x", 4)));

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      assertThatThrownBy(() -> store.downsampleBlocks(Long.MAX_VALUE, H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS))
          .isInstanceOf(IOException.class).hasMessageContaining("CRC mismatch");

      assertThat(store.getDownsampleRewriteCount()).as("the block that can be read was not folded either").isZero();
      assertThat(store.getTotalSampleCount()).isEqualTo(5L);
    }

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      assertThat(store.getTotalSampleCount()).isEqualTo(5L);
      assertThatThrownBy(() -> readAll(store)).as("the damaged block, read after the failed pass and a reopen")
          .hasStackTraceContaining("CRC mismatch");
    }
  }

  /**
   * The proof that precedes a pass reads every old block, and one it cannot read must not stop it from proving the others: it
   * leaves that block without a verdict (it is not taken for an irreducible one), and the pass that follows fails on it.
   */
  @Test
  void theProofSkipsABlockThatCannotBeReadAndProvesTheOthers() throws Exception {
    writeStoreWithAFirstBlockThatCannotBeRead(
        List.of(new Sample(5 * MINUTE, "a", "x", 1), new Sample(10 * MINUTE, "a", "x", 3), new Sample(15 * MINUTE, "a", "x", 5)),
        List.of(new Sample(5 * H + 5 * MINUTE, "a", "x", 2), new Sample(6 * H + 10 * MINUTE, "a", "x", 4)));

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      store.proveBlocksIrreducible(Long.MAX_VALUE, H, 0L, 0, TAG_COLUMNS);

      assertThat(store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks())
          .extracting(block -> block.isIrreducibleOn(new TimeSeriesSealedStore.DownsampleGrid(H, 0L)))
          .as("the block nobody could read, then the one with a sample per bucket").containsExactly(false, true);

      assertThatThrownBy(() -> store.downsampleBlocks(Long.MAX_VALUE, H, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS))
          .isInstanceOf(IOException.class).hasMessageContaining("CRC mismatch");
      assertThat(store.getDownsampleRewriteCount()).isZero();
    }
  }

  /** Writes two blocks and flips a byte in the data of the first, so its CRC no longer matches once the store is opened again. */
  private static void writeStoreWithAFirstBlockThatCannotBeRead(final List<Sample> first, final List<Sample> second) throws IOException {
    final long corruptAt;
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, first);
      appendBlock(store, second);
      final TimeSeriesSealedStore.BlockEntry entry = store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks().get(0);
      corruptAt = entry.columnOffsets[entry.columnOffsets.length - 1];
    }
    try (final RandomAccessFile file = new RandomAccessFile(STORE_PATH + TimeSeriesSealedStore.FILE_EXTENSION, "rw")) {
      file.seek(corruptAt);
      final int original = file.read();
      file.seek(corruptAt);
      file.write(original ^ 0xFF);
    }
  }

  // ---- Helpers ----

  private static List<Sample> sorted(final List<Sample> samples) {
    final List<Sample> copy = new ArrayList<>(samples);
    copy.sort(Comparator.comparingLong(Sample::ts).thenComparing(Sample::host).thenComparing(Sample::dc).thenComparingDouble(Sample::v));
    return copy;
  }

  private static List<Sample> readAll(final TimeSeriesSealedStore store) throws IOException {
    final List<Sample> rows = new ArrayList<>();
    for (final Object[] row : store.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null))
      rows.add(new Sample((long) row[0], (String) row[1], (String) row[2], (double) row[3]));
    // rows of one bucket have no defined order between tag sets
    return sorted(rows);
  }

  /** Writes {@code block} as one sealed block, in the order given, with the statistics a compaction would declare. */
  private static void appendBlock(final TimeSeriesSealedStore store, final List<Sample> block) throws IOException {
    final int n = block.size();
    final long[] timestamps = new long[n];
    final String[] hosts = new String[n];
    final String[] dcs = new String[n];
    final double[] values = new double[n];
    final Set<String> distinctHosts = new LinkedHashSet<>();
    final Set<String> distinctDcs = new LinkedHashSet<>();
    long minTs = Long.MAX_VALUE;
    long maxTs = Long.MIN_VALUE;
    double min = Double.NaN;
    double max = Double.NaN;
    double sum = 0;
    for (int i = 0; i < n; i++) {
      final Sample sample = block.get(i);
      timestamps[i] = sample.ts();
      hosts[i] = sample.host();
      dcs[i] = sample.dc();
      values[i] = sample.v();
      distinctHosts.add(sample.host());
      distinctDcs.add(sample.dc());
      minTs = Math.min(minTs, sample.ts());
      maxTs = Math.max(maxTs, sample.ts());
      min = Double.isNaN(min) ? sample.v() : Math.min(min, sample.v());
      max = Double.isNaN(max) ? sample.v() : Math.max(max, sample.v());
      sum += sample.v();
    }
    store.appendBlock(n, minTs, maxTs, new byte[][] {
            DeltaOfDeltaCodec.encode(timestamps),
            DictionaryCodec.encode(hosts),
            DictionaryCodec.encode(dcs),
            GorillaXORCodec.encode(values) },
        new double[] { Double.NaN, Double.NaN, Double.NaN, min }, new double[] { Double.NaN, Double.NaN, Double.NaN, max },
        new double[] { Double.NaN, Double.NaN, Double.NaN, sum }, new long[] { 0, 0, 0, n },
        new String[][] { null, distinctHosts.toArray(new String[0]), distinctDcs.toArray(new String[0]), null });
  }
}
