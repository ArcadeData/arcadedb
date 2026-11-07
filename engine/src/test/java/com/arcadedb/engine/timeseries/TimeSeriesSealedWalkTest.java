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

import com.arcadedb.TestHelper;
import com.arcadedb.engine.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.codec.DictionaryCodec;
import com.arcadedb.engine.codec.GorillaXORCodec;
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore.BlockDirectorySnapshot;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Sealed block walks that cross a rewrite of the directory (install, downsample, merge) and descending reads over
 * overlapping blocks. Each nested group keeps its own fixture and carries the issue number it regression-tests.
 */
class TimeSeriesSealedWalkTest {

  /**
   * Issue #8043: a sealed-store walk that crosses a replicated install used to return the blocks it got through
   * first and silently drop every remaining one.
   * <p>
   * Since issue #7897 the walk holds no compaction lock across the sealed blocks, so an HA follower adopting the
   * leader's sealed file ({@code installSealedFile}) can land between two of them. The install clears the block
   * directory and reloads it, and the block identity {@code resolveLiveBlock} turns on was an in-memory counter
   * that {@code loadDirectory} minted afresh - so after an install NO snapshot entry matched ANY live entry, every
   * remaining block resolved to {@code null}, and {@code walkBlocks} skipped it as "the block is gone". The rows
   * were still in the file the leader shipped; an {@code EXPORT DATABASE} or a PromQL range simply stopped
   * returning them, with no exception, no metric and no log line.
   * <p>
   * The identity is now a 64-bit block id written INTO the block record and carried verbatim by every path that
   * retains a block, so it survives the reload and - because a follower's sealed file is byte-for-byte the leader's
   * (the sealed store is replicated out of band) - survives the install as well.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WalkAcrossInstall {

    private static final String BASE_DIR     = "target/databases/Issue8043SealedWalkAcrossInstallTest";
    private static final String LEADER_PATH  = BASE_DIR + "/leader";
    private static final String FOLLOWER_PATH = BASE_DIR + "/follower";

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
     * The bug as reported: the walk snapshots the directory, the leader's sealed file is installed underneath it,
     * and every block the walk had not reached yet must still be readable - the install brought the SAME blocks
     * (plus a new one), not different ones.
     */
    // #8043
    @Test
    void aWalkCrossingAReplicatedInstallStillReadsTheBlocksItSnapshotted() throws Exception {
      final byte[] sealedV1;
      final byte[] sealedV2;
      try (final TimeSeriesSealedStore leader = new TimeSeriesSealedStore(LEADER_PATH, columns)) {
        appendBlock(leader, 1000L, "A");
        appendBlock(leader, 3000L, "B");
        appendBlock(leader, 5000L, "C");
        leader.flushHeader();
        sealedV1 = leader.readWholeSealedFile();

        // What the leader ships after its next compaction: the three blocks copied verbatim plus a new one.
        leader.appendBlock(2, 7000L, 8000L, new byte[][] {
            DeltaOfDeltaCodec.encode(new long[] { 7000L, 8000L }),
            DictionaryCodec.encode(new String[] { "D", "D" }),
            GorillaXORCodec.encode(new double[] { 7.0, 8.0 })
        }, new double[] { Double.NaN, Double.NaN, 7.0 }, new double[] { Double.NaN, Double.NaN, 8.0 },
            new double[] { Double.NaN, Double.NaN, 15.0 }, new long[] { 0, 0, 2 },
            new String[][] { null, { "D" }, null });
        leader.flushHeader();
        sealedV2 = leader.readWholeSealedFile();
      }

      try (final TimeSeriesSealedStore follower = new TimeSeriesSealedStore(FOLLOWER_PATH, columns)) {
        follower.installSealedFileBytes(sealedV1);
        assertThat(follower.getBlockCount()).isEqualTo(3);

        // A reader (EXPORT DATABASE, a PromQL range) takes its directory snapshot here.
        final BlockDirectorySnapshot snapshot = follower.snapshotBlockDirectory(0L, Long.MAX_VALUE);
        assertThat(snapshot.blocks()).hasSize(3);

        // ... and the leader's newly compacted sealed file lands mid-walk.
        follower.installSealedFileBytes(sealedV2);

        final AggregationMetrics metrics = new AggregationMetrics();
        final List<Object[]> rows = new ArrayList<>();
        assertThat(follower.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, metrics, rows::add)).isTrue();

        // Six samples: the three blocks the snapshot named. The fourth block is NOT in the snapshot, so it is
        // correctly absent - what must not be absent is anything the snapshot did name.
        assertThat(rows).hasSize(6);
        assertThat(metrics.getVanishedBlocks()).isZero();
        assertThat(rows.stream().map(r -> (String) r[1]).distinct().sorted().toList()).containsExactly("A", "B", "C");
      }
    }

    /**
     * The identity has to survive a plain reopen too - that is what makes it an identity rather than a counter -
     * so a walk whose blocks were snapshotted before a restart resolves them afterwards.
     */
    // #8043
    @Test
    void theBlockIdentitySurvivesAReopen() throws Exception {
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(FOLLOWER_PATH, columns)) {
        appendBlock(store, 1000L, "A");
        appendBlock(store, 3000L, "B");
        store.flushHeader();
      }

      try (final TimeSeriesSealedStore reopened = new TimeSeriesSealedStore(FOLLOWER_PATH, columns)) {
        final BlockDirectorySnapshot snapshot = reopened.snapshotBlockDirectory(0L, Long.MAX_VALUE);
        // Re-installing the very same bytes is the cheapest way to force the directory to be rebuilt wholesale,
        // which is what an install does and what used to renumber every entry.
        reopened.installSealedFileBytes(reopened.readWholeSealedFile());

        final List<Object[]> rows = new ArrayList<>();
        assertThat(reopened.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, null, rows::add)).isTrue();
        assertThat(rows).hasSize(4);
      }
    }

    /**
     * A block retention really did delete is still skipped - the rows are gone - but the walk now COUNTS it, so a
     * short answer is distinguishable from "no row matched" by the caller's own metrics.
     */
    // #8043
    @Test
    void aBlockRetentionRemovedIsSkippedAndCounted() throws Exception {
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(FOLLOWER_PATH, columns)) {
        appendBlock(store, 1000L, "A");
        appendBlock(store, 3000L, "B");
        appendBlock(store, 5000L, "C");
        store.flushHeader();

        final BlockDirectorySnapshot snapshot = store.snapshotBlockDirectory(0L, Long.MAX_VALUE);
        assertThat(snapshot.blocks()).hasSize(3);

        // Drops the first two blocks entirely; the third is copied verbatim and keeps its id.
        store.truncateBefore(5000L);
        assertThat(store.getBlockCount()).isEqualTo(1);

        final AggregationMetrics metrics = new AggregationMetrics();
        final List<Object[]> rows = new ArrayList<>();
        assertThat(store.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, metrics, rows::add)).isTrue();

        assertThat(rows).hasSize(2);
        assertThat(metrics.getVanishedBlocks()).isEqualTo(2);
      }
    }

    /**
     * The two arms of the same walk have to answer the same question. The tag-combination fast path used to emit a
     * combination straight off the SNAPSHOT entry without re-resolving it, so one walk reported a series for a
     * block the other arm would have refused to read a row from.
     */
    // #8043
    @Test
    void theTagCombinationArmRefusesTheSameBlocksTheRowArmRefuses() throws Exception {
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(FOLLOWER_PATH, columns)) {
        appendBlock(store, 1000L, "A");
        appendBlock(store, 3000L, "B");
        appendBlock(store, 5000L, "C");
        store.flushHeader();

        final BlockDirectorySnapshot snapshot = store.snapshotBlockDirectory(0L, Long.MAX_VALUE);
        store.truncateBefore(5000L);

        final AggregationMetrics metrics = new AggregationMetrics();
        final List<Object[]> combinations = new ArrayList<>();
        assertThat(store.forEachTagCombination(snapshot, 0L, Long.MAX_VALUE, new int[] { 0 }, metrics,
            combinations::add)).isTrue();

        // Only the surviving block's combination, not the two whose rows were deleted.
        assertThat(combinations.stream().map(r -> (String) r[1]).toList()).containsExactly("C");
        assertThat(metrics.getVanishedBlocks()).isEqualTo(2);
      }
    }

    /**
     * The window a rolling upgrade opens: a sealed file that still holds blocks written before this change records
     * no id for them, and the walk has to resolve them across an install anyway.
     * <p>
     * Review of PR #8095 found that the first cut minted a fresh RANDOM id for such a block on every load, which is
     * the #8043 defect again, narrowed to exactly the blocks that predate the fix and lasting until the leader's
     * maintenance rewrites them. The id is derived from the block's own record instead, so it is the same on every
     * load of the same file and therefore the same on a follower as on the leader whose bytes it is holding.
     */
    // #8043
    @Test
    void aWalkCrossingAnInstallOfPreIssue8043BlocksKeepsThemToo() throws Exception {
      // A "TSB2" file: the layout every block had between issues #7089 and #8043, which records no block id.
      writeLegacyTsb2File(FOLLOWER_PATH, new long[][] { { 1000L, 2000L }, { 3000L, 4000L }, { 5000L, 6000L } });

      try (final TimeSeriesSealedStore follower = new TimeSeriesSealedStore(FOLLOWER_PATH, columns)) {
        assertThat(follower.getBlockCount()).isEqualTo(3);

        final BlockDirectorySnapshot snapshot = follower.snapshotBlockDirectory(0L, Long.MAX_VALUE);
        assertThat(snapshot.blocks()).hasSize(3);
        assertThat(snapshot.blocks().stream().map(b -> b.blockId).distinct().count())
            .as("a derived id still has to be unique within the file").isEqualTo(3);

        // The leader ships the same file back - byte for byte, as a follower's sealed store always is - and it
        // lands mid-walk.
        follower.installSealedFileBytes(follower.readWholeSealedFile());

        final AggregationMetrics metrics = new AggregationMetrics();
        final List<Object[]> rows = new ArrayList<>();
        assertThat(follower.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, metrics, rows::add)).isTrue();

        assertThat(rows).hasSize(6);
        assertThat(metrics.getVanishedBlocks()).isZero();
      }
    }

    /**
     * ... and the derived id is not thrown away by the first rewrite: a retained block carries it into the "TSB3"
     * record, so it is PERSISTED from then on rather than derived again from an offset the rewrite has changed.
     */
    // #8043
    @Test
    void aRewriteOfALegacyBlockPersistsTheIdItWasDerivedWith() throws Exception {
      writeLegacyTsb2File(FOLLOWER_PATH, new long[][] { { 1000L, 2000L }, { 5000L, 6000L } });

      final long retainedId;
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(FOLLOWER_PATH, columns)) {
        retainedId = store.snapshotBlockDirectory(5000L, 6000L).blocks().getFirst().blockId;
        // Drops the first block and copies the second verbatim - into the current layout, which records the id.
        store.truncateBefore(5000L);
        assertThat(store.getBlockCount()).isEqualTo(1);
        assertThat(store.snapshotBlockDirectory(0L, Long.MAX_VALUE).blocks().getFirst().blockId)
            .as("a verbatim copy keeps the id, derived or not").isEqualTo(retainedId);
      }

      try (final TimeSeriesSealedStore reopened = new TimeSeriesSealedStore(FOLLOWER_PATH, columns)) {
        assertThat(reopened.snapshotBlockDirectory(0L, Long.MAX_VALUE).blocks().getFirst().blockId)
            .as("and the rewrite wrote it down, so the reopen reads it back rather than deriving it at a new offset")
            .isEqualTo(retainedId);
      }
    }

    /**
     * A "TSB2" sealed file, written by hand because no path in the tree writes one any more: magic, min/max
     * timestamp, sample count, column sizes, the [min, max, sum, count] statistics quadruple of issue #7089, the
     * tag section, the data and the CRC - and no block id, which is the whole point.
     */
    private void writeLegacyTsb2File(final String path, final long[][] blocks) throws Exception {
      try (final RandomAccessFile raf = new RandomAccessFile(path + ".ts.sealed", "rw")) {
        raf.setLength(0);
        final ByteBuffer header = ByteBuffer.allocate(27);
        header.putInt(0x54534958);                                   // "TSIX"
        header.put((byte) 1);                                        // the version TSB2 blocks were written under
        header.putShort((short) columns.size());
        header.putInt(blocks.length);
        header.putLong(blocks[0][0]);
        header.putLong(blocks[blocks.length - 1][blocks[blocks.length - 1].length - 1]);
        raf.write(header.array());

        for (final long[] timestamps : blocks) {
          final String[] tags = new String[timestamps.length];
          final double[] values = new double[timestamps.length];
          for (int i = 0; i < timestamps.length; i++) {
            tags[i] = "A";
            values[i] = i + 1.0;
          }
          final byte[] tsBytes = DeltaOfDeltaCodec.encode(timestamps);
          final byte[] tagBytes = DictionaryCodec.encode(tags);
          final byte[] valBytes = GorillaXORCodec.encode(values);

          double sum = 0;
          for (final double v : values)
            sum += v;

          // magic + minTs + maxTs + sampleCount + 3 column sizes + numericColCount + one quadruple + tagColCount
          final ByteBuffer meta = ByteBuffer.allocate(4 + 8 + 8 + 4 + 4 * 3 + 4 + (8 + 8 + 8 + 8) + 2);
          meta.putInt(0x54534232);                                   // "TSB2" - no block id follows
          meta.putLong(timestamps[0]);
          meta.putLong(timestamps[timestamps.length - 1]);
          meta.putInt(timestamps.length);
          meta.putInt(tsBytes.length);
          meta.putInt(tagBytes.length);
          meta.putInt(valBytes.length);
          meta.putInt(1);                                            // one column carries statistics
          meta.putDouble(values[0]);
          meta.putDouble(values[values.length - 1]);
          meta.putDouble(sum);
          meta.putLong(values.length);
          meta.putShort((short) 0);                                  // no TAG declaration

          final CRC32 crc = new CRC32();
          crc.update(meta.array());
          crc.update(tsBytes);
          crc.update(tagBytes);
          crc.update(valBytes);
          raf.write(meta.array());
          raf.write(tsBytes);
          raf.write(tagBytes);
          raf.write(valBytes);
          raf.writeInt((int) crc.getValue());
        }
      }
    }

    /**
     * One block of two samples, all carrying a single tag value so the block DECLARES exactly one combination -
     * which is what lets {@link TimeSeriesSealedStore#forEachTagCombination} answer it without a file read.
     */
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
  @Nested
  class WalkAcrossDownsample {

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
    // #8166
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
    // #8166
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
    // #8166
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
    // #8166
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
    // #8166
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
    // #8166
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
    // #8166
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
    // #8166
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
    // #8166
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
    // #8166
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

  /**
   * #9488: a merge of small sealed blocks keeps every row, yet a walk that had read some of the old blocks and met one
   * the merge replaced raised {@link TimeSeriesWalkCoarsenedException} - an error the caller of a plain aggregate read
   * cannot do anything about, raised by the engine's own maintenance. The walk now follows the merge to the block that
   * holds the rows and emits exactly the rows it had not handed over yet: each row once, none lost.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WalkAcrossMerge extends TestHelper {
    private static final long T0 = 1_700_000_000_000L;

    private TimeSeriesEngine engine(final String type) {
      return ((LocalTimeSeriesType) database.getSchema().getType(type)).getEngine();
    }

    /** {@code passes} small blocks, each sealed by itself. Odd passes are written BEFORE the even ones in time, so the blocks overlap. */
    private long feedAndCompact(final TimeSeriesEngine engine, final int passes, final int samplesPerPass, final long firstValue,
        final boolean interleave) throws Exception {
      long value = firstValue;
      for (int p = 0; p < passes; p++) {
        final long[] timestamps = new long[samplesPerPass];
        final Object[] tags = new Object[samplesPerPass];
        final Object[] values = new Object[samplesPerPass];
        for (int i = 0; i < samplesPerPass; i++) {
          // interleaved: every pass spans the whole window, so the merged block is NOT in source order
          timestamps[i] = interleave ? T0 + (long) i * 1_000L * passes + p * 1_000L : T0 + (value - firstValue) * 1_000L;
          tags[i] = "series-" + (i % 3);
          values[i] = (double) value++;
        }
        engine.appendSamples(timestamps, tags, values);
        engine.compactAll();
      }
      return value;
    }

    private static List<Double> values(final List<Object[]> rows) {
      final List<Double> out = new ArrayList<>(rows.size());
      for (final Object[] r : rows)
        out.add((Double) r[2]);
      out.sort(null);
      return out;
    }

    private List<Double> expected(final int count) {
      final List<Double> out = new ArrayList<>(count);
      for (int i = 0; i < count; i++)
        out.add((double) i);
      return out;
    }

    // #9488
    @Test
    void aWalkOverAMergedRunAnswersTheWholeRows() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Slow");
      feedAndCompact(engine, 5, 20, 0, false);

      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      final BlockDirectorySnapshot snapshot = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);
      assertThat(snapshot.blocks()).hasSize(5);

      engine.mergeSmallBlocks();
      assertThat(sealed.getBlockCount()).isEqualTo(1);

      final List<Object[]> rows = new ArrayList<>();
      sealed.forEachRow(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, null, null, new AggregationMetrics(), rows::add);
      assertThat(values(rows)).isEqualTo(expected(100));
    }

    // #9488
    @Test
    void aWalkThatIsHalfWayWhenTheMergeLandsEmitsEachRowOnce() throws Exception {
      for (final boolean interleave : new boolean[] { false, true }) {
        final String name = interleave ? "Interleaved" : "Sequential";
        database.command("sql", "CREATE TIMESERIES TYPE " + name + " TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
        final TimeSeriesEngine engine = engine(name);
        feedAndCompact(engine, 6, 20, 0, interleave);

        final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
        final BlockDirectorySnapshot snapshot = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);
        assertThat(snapshot.blocks()).hasSize(6);

        final List<Object[]> rows = new ArrayList<>();
        final boolean[] merged = { false };
        sealed.forEachRow(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, null, null, new AggregationMetrics(), row -> {
          rows.add(row);
          // the merge lands in the middle of the third block (sequential) or of the walk's first block (interleaved)
          if (!merged[0] && rows.size() == 45) {
            merged[0] = true;
            try {
              engine.mergeSmallBlocks();
            } catch (final Exception e) {
              throw new IllegalStateException(e);
            }
          }
          return true;
        });

        assertThat(merged[0]).isTrue();
        assertThat(sealed.getBlockCount()).as(name).isEqualTo(1);
        assertThat(values(rows)).as(name).isEqualTo(expected(120));
      }
    }

    // #9488
    @Test
    void aLazyIteratorThatIsHalfWayWhenTheMergeLandsEmitsEachRowOnce() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Slow");
      feedAndCompact(engine, 6, 20, 0, false);

      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      final Iterator<Object[]> it = sealed.iterateRange(sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE), Long.MIN_VALUE,
          Long.MAX_VALUE, null, null, null);

      final List<Object[]> rows = new ArrayList<>();
      for (int i = 0; i < 45; i++)
        rows.add(it.next());
      engine.mergeSmallBlocks();
      while (it.hasNext())
        rows.add(it.next());

      assertThat(values(rows)).isEqualTo(expected(120));
    }

    // #9488
    @Test
    void aWalkSurvivesTwoMergesAndIgnoresTheBlocksAppendedAfterItsSnapshot() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Slow");
      final long next = feedAndCompact(engine, 6, 20, 0, false);

      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      final BlockDirectorySnapshot snapshot = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);

      final List<Object[]> rows = new ArrayList<>();
      final int[] stage = { 0 };
      sealed.forEachRow(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, null, null, new AggregationMetrics(), row -> {
        rows.add(row);
        try {
          if (stage[0] == 0 && rows.size() == 30) {
            stage[0] = 1;
            engine.mergeSmallBlocks();
          } else if (stage[0] == 1 && rows.size() == 50) {
            stage[0] = 2;
            // new small blocks, then a second merge that folds the first merge's block together with them
            feedAndCompact(engine, 3, 20, next, false);
            engine.mergeSmallBlocks();
          }
        } catch (final Exception e) {
          throw new IllegalStateException(e);
        }
        return true;
      });

      assertThat(stage[0]).isEqualTo(2);
      assertThat(sealed.getBlockCount()).isEqualTo(1);
      assertThat(values(rows)).as("only the rows of the snapshot, each once").isEqualTo(expected(120));
    }

    // #9488
    @Test
    void aSqlResultSetPulledHalfWayWhenTheMergeLandsIsWhole() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Slow");
      feedAndCompact(engine, 6, 20, 0, false);

      // Deterministic: the merge lands between two pulls of the same statement
      final List<Double> seen = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM Slow")) {
        for (int i = 0; i < 45; i++)
          seen.add((Double) rs.next().getProperty("v"));
        engine.mergeSmallBlocks();
        assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);
        while (rs.hasNext())
          seen.add((Double) rs.next().getProperty("v"));
      }
      seen.sort(null);
      assertThat(seen).isEqualTo(expected(120));
    }

    // #9488
    @Test
    void sqlReadsRacingWithTheMaintenanceNeverFail() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Slow");
      final long next = feedAndCompact(engine, 10, 20, 0, false);
      final long lastInRange = T0 + (next - 1) * 1_000L;

      // The reader asks for the rows of the first 200 samples while the writer keeps adding small blocks AFTER them and merging
      // them into the one the reader is walking: its answer cannot change, and it must never be refused
      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicInteger reads = new AtomicInteger();
      final Thread reader = new Thread(() -> {
        try {
          while (!stop.get()) {
            long rows = 0;
            try (final ResultSet rs = database.query("sql", "SELECT FROM Slow WHERE ts BETWEEN " + T0 + " AND " + lastInRange)) {
              while (rs.hasNext()) {
                rs.next();
                rows++;
              }
            }
            if (rows != 200)
              throw new AssertionError("the read returned " + rows + " rows instead of 200");
            reads.incrementAndGet();
          }
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      }, "reader");
      reader.start();
      try {
        long value = next;
        long ts = lastInRange + 1_000L;
        for (int round = 0; round < 40 && failure.get() == null; round++) {
          final long[] timestamps = new long[20];
          final Object[] tags = new Object[20];
          final Object[] values = new Object[20];
          for (int i = 0; i < 20; i++) {
            timestamps[i] = ts;
            tags[i] = "series-" + (i % 3);
            values[i] = (double) value++;
            ts += 1_000L;
          }
          engine.appendSamples(timestamps, tags, values);
          engine.compactAll();
          engine.mergeSmallBlocks();
        }
      } finally {
        stop.set(true);
        reader.join();
      }

      assertThat(failure.get()).isNull();
      assertThat(reads.get()).isPositive();
    }

    // #9488
    @Test
    void theTagCombinationWalkAcrossAMergeAnswersTheSameCombinations() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Slow");
      feedAndCompact(engine, 6, 20, 0, false);

      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      final BlockDirectorySnapshot snapshot = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);
      final Set<Object> before = new TreeSet<>();
      sealed.forEachTagCombination(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, new int[] { 0 }, null, row -> {
        before.add(row[1]);
        return true; // keep walking: add() answers false for a tag already seen
      });

      engine.mergeSmallBlocks();

      final Set<Object> after = new TreeSet<>();
      sealed.forEachTagCombination(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, new int[] { 0 }, null, row -> {
        after.add(row[1]);
        return true; // keep walking: add() answers false for a tag already seen
      });
      assertThat(after).isEqualTo(before).containsExactly("series-0", "series-1", "series-2");
    }

    // #9488
    @Test
    void aWalkThatAlsoCrossesADownsampleStillRaises() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Slow");
      feedAndCompact(engine, 6, 20, 0, false);

      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      final BlockDirectorySnapshot snapshot = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);

      engine.mergeSmallBlocks();
      // the merged block is then replaced by coarser rows: no hand-over can join the two resolutions
      sealed.downsampleBlocks(T0 + 1_000_000L, 10 * 60_000L, 0, List.of(1), List.of(2));

      assertThatThrownBy(() -> sealed.forEachRow(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, null, null, new AggregationMetrics(), row -> true))
          .isInstanceOf(TimeSeriesWalkCoarsenedException.class);
    }

    // #9488
    @Test
    void aTagFilteredWalkAcrossAMergeKeepsOnlyTheMatchingRows() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Slow");
      feedAndCompact(engine, 6, 21, 0, false);

      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      final BlockDirectorySnapshot snapshot = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);
      final TagFilter filter = TagFilter.eq(0, "series-1");

      final List<Object[]> rows = new ArrayList<>();
      final boolean[] merged = { false };
      sealed.forEachRow(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, null, filter, new AggregationMetrics(), row -> {
        rows.add(row);
        if (!merged[0] && rows.size() == 10) {
          merged[0] = true;
          try {
            engine.mergeSmallBlocks();
          } catch (final Exception e) {
            throw new IllegalStateException(e);
          }
        }
        return true;
      });

      final List<Double> expected = new ArrayList<>();
      for (int i = 0; i < 126; i++)
        if (i % 21 % 3 == 1)
          expected.add((double) i);
      assertThat(merged[0]).isTrue();
      assertThat(values(rows)).isEqualTo(expected);
    }
  }

  /**
   * #9982: {@link TimeSeriesSealedStore#scanRangeDescending} walked the block directory as if {@code maxTimestamp} were
   * monotonic along it. The directory is ordered by {@code minTimestamp} and its blocks overlap (#8813), so one block
   * ending before the lower bound - or before the oldest row retained - stopped the walk while an earlier block still
   * held newer rows. The guarantee is the differential one: for any range and limit, the descending scan answers the
   * newest rows of what {@link TimeSeriesSealedStore#scanRange} returns, over directories with overlapping blocks.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DescendingOverlappingBlocks {
    private static final String ROOT      = "target/databases/Issue9982DescendingOverlappingBlocksTest";
    private static final String BASE_PATH = ROOT + "/sealed";
    private static final long   T0        = 1_700_000_000_000L;

    private List<ColumnDefinition> columns;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(ROOT));
      new File(ROOT).mkdirs();
      columns = List.of(
          new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
          new ColumnDefinition("usage", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(ROOT));
    }

    private static void appendBlock(final TimeSeriesSealedStore store, final long fromTs, final long toTs) throws IOException {
      final int samples = (int) (toTs - fromTs + 1);
      final long[] timestamps = new long[samples];
      final double[] usage = new double[samples];
      double sum = 0;
      for (int i = 0; i < samples; i++) {
        timestamps[i] = fromTs + i;
        usage[i] = i;
        sum += i;
      }
      store.appendBlock(samples, fromTs, toTs, new byte[][] { DeltaOfDeltaCodec.encode(timestamps), GorillaXORCodec.encode(usage) },
          new double[] { Double.NaN, 0 }, new double[] { Double.NaN, samples - 1 }, new double[] { Double.NaN, sum },
          new long[] { 0, samples }, new String[][] { null, null });
    }

    /** The directory shape of {@code TimeSeriesSealedStoreTest.SealedStoreDirectoryOrder}: a wide block that starts early and ends last. */
    private static void loadReportedShape(final TimeSeriesSealedStore store) throws IOException {
      appendBlock(store, T0 + 1_000, T0 + 1_099);
      appendBlock(store, T0 + 2_000, T0 + 2_099);
      appendBlock(store, T0 + 3_000, T0 + 3_099);
      appendBlock(store, T0, T0 + 9);
      appendBlock(store, T0 + 500, T0 + 3_500);
    }

    private static long[] timestamps(final List<Object[]> rows) {
      final long[] ts = new long[rows.size()];
      for (int i = 0; i < ts.length; i++)
        ts[i] = (long) rows.get(i)[0];
      return ts;
    }

    /** The newest {@code limit} timestamps of {@code scanRange} over the same range, newest first. */
    private static long[] expectedDescending(final TimeSeriesSealedStore store, final long fromTs, final long toTs, final int limit)
        throws IOException {
      final long[] all = timestamps(store.scanRange(fromTs, toTs, null, null));
      Arrays.sort(all);
      final int n = limit > 0 ? Math.min(limit, all.length) : all.length;
      final long[] expected = new long[n];
      for (int i = 0; i < n; i++)
        expected[i] = all[all.length - 1 - i];
      return expected;
    }

    private static void assertDescendingMirrorsScanRange(final TimeSeriesSealedStore store, final long fromTs, final long toTs,
        final int limit) throws IOException {
      final long[] actual = timestamps(store.scanRangeDescending(fromTs, toTs, null, null, limit, null));
      assertThat(actual).as("descending [%d..%d] limit %d", fromTs - T0, toTs - T0, limit)
          .containsExactly(expectedDescending(store, fromTs, toTs, limit));
    }

    // #9982
    @Test
    void aRangeInsideTheWideBlockIsNotCutShortByANarrowOneEndingBeforeIt() throws IOException {
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_PATH, columns)) {
        loadReportedShape(store);

        assertThat(store.scanRange(T0 + 3_200, T0 + 3_300, null, null)).hasSize(101);
        assertThat(store.scanRangeDescending(T0 + 3_200, T0 + 3_300, null, null, 0, null)).hasSize(101);
        assertDescendingMirrorsScanRange(store, T0 + 3_200, T0 + 3_300, 0);
        assertDescendingMirrorsScanRange(store, T0 + 3_200, T0 + 3_300, 10);
      }
    }

    // #9982
    @Test
    void theLatestSampleIsTheNewestOneInTheStore() throws IOException {
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_PATH, columns)) {
        loadReportedShape(store);

        final List<Object[]> latest = store.scanRangeDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, null, 1, null);
        assertThat(latest).hasSize(1);
        assertThat((long) latest.getFirst()[0]).isEqualTo(T0 + 3_500);
        assertThat((long) latest.getFirst()[0]).isEqualTo(store.getGlobalMaxTimestamp());

        for (final int limit : new int[] { 1, 2, 50, 401, 1_000, 0 })
          assertDescendingMirrorsScanRange(store, Long.MIN_VALUE, Long.MAX_VALUE, limit);
      }
    }

    // #9982
    @Test
    void descendingMirrorsScanRangeOverRandomOverlappingDirectories() throws IOException {
      final Random rnd = new Random(9982);
      for (int round = 0; round < 20; round++) {
        FileUtils.deleteRecursively(new File(ROOT));
        new File(ROOT).mkdirs();
        try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_PATH, columns)) {
          final int blocks = 2 + rnd.nextInt(12);
          for (int b = 0; b < blocks; b++) {
            final long from = T0 + rnd.nextInt(5_000);
            // mostly narrow blocks, a few wide ones that overlap many others
            final long length = rnd.nextInt(10) == 0 ? 500 + rnd.nextInt(4_000) : 1 + rnd.nextInt(200);
            appendBlock(store, from, from + length - 1);
          }

          for (int q = 0; q < 30; q++) {
            final long a = T0 - 100 + rnd.nextInt(9_500);
            final long b = T0 - 100 + rnd.nextInt(9_500);
            final int limit = switch (rnd.nextInt(4)) {
              case 0 -> 0;
              case 1 -> 1;
              case 2 -> 1 + rnd.nextInt(20);
              default -> 1 + rnd.nextInt(2_000);
            };
            assertDescendingMirrorsScanRange(store, Math.min(a, b), Math.max(a, b), limit);
          }
          assertDescendingMirrorsScanRange(store, Long.MIN_VALUE, Long.MAX_VALUE, 1);
        }
      }
    }
  }
}
