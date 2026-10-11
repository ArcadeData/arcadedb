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

import com.arcadedb.engine.timeseries.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.timeseries.codec.GorillaXORCodec;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #9770: a rewrite of the sealed file that reuses a fixed temporary path owns that file, so it must truncate it before it
 * writes ({@code RandomAccessFile} in {@code "rw"} mode neither truncates on open nor shrinks on write). Five of the six
 * writers did; {@code truncateBefore}, the retention pass, did not. Worse than a surviving tail, the block writer positions
 * every block at {@code tempFile.length()}, so a leftover of ANY size past the header was entirely preserved and the retained
 * blocks were appended after it: the file swapped over the live store held every block of the abandoned attempt, the blocks
 * retention was told to drop came back on the next open, and the whole bloated file is what HA ships to the followers.
 * <p>
 * The test asserts the RULE and not the cell: for every rewrite entry point, whatever an abandoned rewrite left at the path,
 * the store that is reopened afterwards is the one the same rewrite produces without a leftover.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9770SealedRewriteLeftoverTempTest {
  private static final String ROOT      = "target/databases/Issue9770SealedRewriteLeftoverTempTest";
  private static final long   T0        = 1_700_000_000_000L;
  private static final int    BLOCKS    = 6;
  private static final int    PER_BLOCK = 100;

  private List<ColumnDefinition> columns;

  /** The rewrite entry points that write a temp file and swap it over the live sealed store. */
  enum Rewrite {
    TRUNCATE_BEFORE {
      @Override
      void apply(final TimeSeriesSealedStore store) throws IOException {
        store.truncateBefore(T0 + 4_000);
      }
    },
    TRUNCATE_TO_BLOCK_COUNT {
      @Override
      void apply(final TimeSeriesSealedStore store) throws IOException {
        store.truncateToBlockCount(3);
      }
    },
    DOWNSAMPLE {
      @Override
      void apply(final TimeSeriesSealedStore store) throws IOException {
        store.downsampleBlocks(T0 + 4_000, 10L, 0, List.of(), List.of(1));
      }
    },
    COMPACTION_SWAP {
      @Override
      void apply(final TimeSeriesSealedStore store) throws IOException {
        final long fromTs = T0 + 7_000;
        final List<byte[][]> compressed = new ArrayList<>();
        final List<long[]> meta = new ArrayList<>();
        final List<TimeSeriesSealedStore.BlockStats> stats = new ArrayList<>();
        final long[] timestamps = new long[PER_BLOCK];
        final double[] usage = new double[PER_BLOCK];
        for (int i = 0; i < PER_BLOCK; i++) {
          timestamps[i] = fromTs + i;
          usage[i] = i;
        }
        compressed.add(new byte[][] { DeltaOfDeltaCodec.encode(timestamps), GorillaXORCodec.encode(usage) });
        meta.add(new long[] { fromTs, fromTs + PER_BLOCK - 1, PER_BLOCK });
        stats.add(new TimeSeriesSealedStore.BlockStats(new double[] { Double.NaN, 0 }, new double[] { Double.NaN, PER_BLOCK - 1 },
            new double[] { Double.NaN, 4_950 }, new long[] { 0, PER_BLOCK }));
        final List<TimeSeriesSealedStore.BlockEntry> directory = store.writeTempCompactionFile(compressed, meta, stats, null);
        store.commitTempCompactionFile(directory);
      }
    },
    MERGE {
      @Override
      void apply(final TimeSeriesSealedStore store) throws IOException {
        final TimeSeriesSealedStore.MergePlan plan = store.prepareMerge(1_000, 0L, 1);
        assertThat(plan).as("the six small blocks must be mergeable").isNotNull();
        assertThat(store.commitMerge(plan)).isTrue();
      }
    };

    abstract void apply(TimeSeriesSealedStore store) throws IOException;
  }

  /** What an abandoned rewrite can leave behind at the temp path. */
  enum Leftover {
    NONE,
    /** What the store's own Phase 3 writer leaves when the compaction that started it never committed. */
    ABANDONED_PHASE_3,
    /** Garbage longer than anything the rewrite writes. */
    LONG_GARBAGE,
    /** A stub shorter than a header. */
    SHORT_STUB
  }

  private record Outcome(int blocks, long samples, long fileSize, List<Long> timestamps, List<String> problems) {
  }

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

  private static void appendBlock(final TimeSeriesSealedStore store, final long fromTs) throws IOException {
    final long[] timestamps = new long[PER_BLOCK];
    final double[] usage = new double[PER_BLOCK];
    double sum = 0;
    for (int i = 0; i < PER_BLOCK; i++) {
      timestamps[i] = fromTs + i;
      usage[i] = i;
      sum += i;
    }
    store.appendBlock(PER_BLOCK, fromTs, fromTs + PER_BLOCK - 1,
        new byte[][] { DeltaOfDeltaCodec.encode(timestamps), GorillaXORCodec.encode(usage) }, new double[] { Double.NaN, 0 },
        new double[] { Double.NaN, PER_BLOCK - 1 }, new double[] { Double.NaN, sum }, new long[] { 0, PER_BLOCK },
        new String[][] { null, null });
  }

  private void plant(final TimeSeriesSealedStore store, final String basePath, final Leftover leftover) throws IOException {
    final File temp = new File(basePath + ".ts.sealed.tmp");
    switch (leftover) {
    case NONE -> {
    }
    case ABANDONED_PHASE_3 ->
        // the real writer, left uncommitted: exactly the file a compaction that died after Phase 3 leaves behind
        store.writeTempCompactionFile(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), null);
    case LONG_GARBAGE -> {
      final byte[] garbage = new byte[8_192];
      Arrays.fill(garbage, (byte) 0x5A);
      Files.write(temp.toPath(), garbage);
    }
    case SHORT_STUB -> Files.write(temp.toPath(), new byte[] { 1, 2, 3 });
    }
    if (leftover != Leftover.NONE) {
      assertThat(temp).as("the leftover must be in place").exists();
      // the merge of small blocks writes to a path of its own: give it the same leftover
      Files.copy(temp.toPath(), new File(basePath + TimeSeriesSealedStore.MERGE_TEMP_SUFFIX).toPath());
    }
  }

  private Outcome run(final Rewrite rewrite, final Leftover leftover) throws IOException {
    final String basePath = ROOT + "/" + rewrite + "_" + leftover + "/sealed";
    new File(basePath).getParentFile().mkdirs();

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(basePath, columns)) {
      for (int k = 1; k <= BLOCKS; k++)
        appendBlock(store, T0 + k * 1_000L);
      plant(store, basePath, leftover);

      rewrite.apply(store);

      // the rewrite's own temp file is consumed by the swap (the merge has a path of its own; the planted copy at the other one
      // is deleted by the next open)
      assertThat(new File(basePath + (rewrite == Rewrite.MERGE ? TimeSeriesSealedStore.MERGE_TEMP_SUFFIX : ".ts.sealed.tmp")))
          .as("the swap consumes the temp file").doesNotExist();
    }

    // what a restart sees: the directory is rebuilt by scanning the file, whatever its header says
    try (final TimeSeriesSealedStore reopened = new TimeSeriesSealedStore(basePath, columns)) {
      final List<Long> timestamps = new ArrayList<>();
      for (final Object[] row : reopened.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null))
        timestamps.add(((Number) row[0]).longValue());
      return new Outcome(reopened.getBlockCount(), reopened.getTotalSampleCount(), reopened.getFileSizeBytes(), timestamps,
          reopened.checkIntegrity(TimeSeriesIntegrity.Options.REPORT_ONLY).problems().stream().map(Object::toString).toList());
    }
  }

  @ParameterizedTest
  @EnumSource(Rewrite.class)
  void aLeftoverTempFileDoesNotChangeWhatARewriteProduces(final Rewrite rewrite) throws IOException {
    final Outcome control = run(rewrite, Leftover.NONE);
    assertThat(control.problems()).as("the control must be a sound store").isEmpty();
    assertThat(control.blocks()).isPositive();

    for (final Leftover leftover : Leftover.values()) {
      if (leftover == Leftover.NONE)
        continue;
      final Outcome outcome = run(rewrite, leftover);
      assertThat(outcome.problems()).as("%s over a %s leftover", rewrite, leftover).isEmpty();
      assertThat(outcome.blocks()).as("%s over a %s leftover: blocks", rewrite, leftover).isEqualTo(control.blocks());
      assertThat(outcome.samples()).as("%s over a %s leftover: samples", rewrite, leftover).isEqualTo(control.samples());
      assertThat(outcome.fileSize()).as("%s over a %s leftover: bytes on disk", rewrite, leftover).isEqualTo(control.fileSize());
      assertThat(outcome.timestamps()).as("%s over a %s leftover: rows served", rewrite, leftover).isEqualTo(control.timestamps());
    }
  }

  /** The reachable case: a retention pass over the temp file of a compaction that died after Phase 3. */
  @Test
  void retentionOverAnAbandonedCompactionDoesNotResurrectTheBlocksItDropped() throws IOException {
    final String basePath = ROOT + "/retention/sealed";
    new File(basePath).getParentFile().mkdirs();

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(basePath, columns)) {
      for (int k = 1; k <= BLOCKS; k++)
        appendBlock(store, T0 + k * 1_000L);
      store.writeTempCompactionFile(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), null);
      assertThat(new File(basePath + ".ts.sealed.tmp")).exists();

      store.truncateBefore(T0 + 4_000);

      assertThat(store.getBlockCount()).isEqualTo(3);
      assertThat(store.getTotalSampleCount()).isEqualTo(3L * PER_BLOCK);
      // the bytes the HA layer ships to every follower are the whole file: it must not carry the abandoned attempt
      assertThat(store.readWholeSealedFile()).hasSize((int) store.getFileSizeBytes());
    }

    try (final TimeSeriesSealedStore reopened = new TimeSeriesSealedStore(basePath, columns)) {
      assertThat(reopened.getBlockCount()).as("the blocks retention dropped must stay dropped").isEqualTo(3);
      assertThat(reopened.getTotalSampleCount()).isEqualTo(3L * PER_BLOCK);
      assertThat(reopened.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(3 * PER_BLOCK);
      assertThat(reopened.checkIntegrity(TimeSeriesIntegrity.Options.REPORT_ONLY).problems()).isEmpty();
    }
  }

  /**
   * The signal when two rewrites collided used to be a JDK buffer bounds error out of a header read from an empty file that
   * {@code new RandomAccessFile(path, "rw")} had just re-created. The temp file Phase 4b appends to has to exist: a missing one
   * is refused, by name, and nothing is created in its place.
   */
  @Test
  void appendingToATempFileThatVanishedIsRefusedByNameAndCreatesNothing() throws IOException {
    final String basePath = ROOT + "/vanished/sealed";
    new File(basePath).getParentFile().mkdirs();

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(basePath, columns)) {
      appendBlock(store, T0 + 1_000L);
      final long[] timestamps = { T0 + 5_000L };
      final List<byte[][]> compressed = new ArrayList<>();
      compressed.add(new byte[][] { DeltaOfDeltaCodec.encode(timestamps), GorillaXORCodec.encode(new double[] { 1 }) });
      final List<long[]> meta = List.of(new long[] { T0 + 5_000L, T0 + 5_000L, 1 });
      final List<TimeSeriesSealedStore.BlockStats> stats = List.of(
          new TimeSeriesSealedStore.BlockStats(new double[] { Double.NaN, 1 }, new double[] { Double.NaN, 1 },
              new double[] { Double.NaN, 1 }, new long[] { 0, 1 }));

      assertThatThrownBy(() -> store.appendBlocksToTempFile(compressed, meta, stats, null, new ArrayList<>()))
          .isInstanceOf(IOException.class).hasMessageContaining("ts.sealed.tmp").hasMessageContaining("another rewrite");
      assertThat(new File(basePath + ".ts.sealed.tmp")).as("a refused append must not create the file it wanted").doesNotExist();
    }
  }
}
