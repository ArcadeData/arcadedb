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

/**
 * Issue #7973: a sealed block the walk of issue #7897 holds a snapshot of is re-resolved against the live
 * directory by its own IDENTITY, not by the {@code (minTimestamp, maxTimestamp, sampleCount)} triple that a
 * rewrite happens to preserve.
 * <p>
 * The triple is not an identifier. Two blocks may carry all three values and still hold different rows - nothing
 * in the writer forbids it - and the walk would then read one of them twice and the other never. That the tree
 * does not produce such a pair today is a statement about what the writers happen to do, not an invariant the
 * store enforces, and since #7897 the resolution is load-bearing for CORRECTNESS rather than for an optimisation.
 * <p>
 * The pair below is built directly through {@link TimeSeriesSealedStore#appendBlock} for exactly that reason: the
 * point is what the type guarantees, so the test states the guarantee rather than reaching for a writer that
 * happens to violate it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7973SealedBlockIdentityTest {

  private static final String            BASE_DIR  = "target/databases/Issue7973SealedBlockIdentityTest";
  private static final String            TEST_PATH = BASE_DIR + "/sealed";
  private static final double[]          NO_STATS  = { Double.NaN, Double.NaN, Double.NaN };
  private static final long[]            NO_COUNTS = { 0, 0, 0 };

  private              List<ColumnDefinition> columns;

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
   * The defect: two blocks sharing the whole triple, walked over a snapshot taken BEFORE a rewrite replaced every
   * directory entry with a fresh object. The reference-identity fast path cannot answer either of them - which is
   * the ordinary state of affairs after a compaction, not a contrived one - so both go through the search, and a
   * search by the triple hands the first block back for both.
   */
  @Test
  void twoBlocksSharingTheTripleAreStillTwoBlocks() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      appendBlock(store, "A", 1.0, 2.0);
      appendBlock(store, "B", 3.0, 4.0);
      assertThat(store.getBlockCount()).isEqualTo(2);

      final TimeSeriesSealedStore.BlockDirectorySnapshot snapshot =
          store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);
      assertThat(snapshot.blocks()).hasSize(2);

      rewriteDirectoryInPlace(store);

      assertThat(sensorsOf(store, snapshot))
          .as("each snapshot entry resolves to ITS OWN block, not to the first one that matches the triple")
          .containsExactly("A", "A", "B", "B");
    }
  }

  /**
   * The rewrite really is what breaks reference identity, so the assertion above is about the search and not about
   * a fast path that quietly answered everything. Without this the test would pass on a store where nothing was
   * ever rewritten, which is the one case the fix does not touch.
   */
  @Test
  void aRewriteReplacesEveryDirectoryEntryObject() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      appendBlock(store, "A", 1.0, 2.0);
      appendBlock(store, "B", 3.0, 4.0);

      final TimeSeriesSealedStore.BlockDirectorySnapshot before =
          store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);
      rewriteDirectoryInPlace(store);
      final TimeSeriesSealedStore.BlockDirectorySnapshot after =
          store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);

      for (int i = 0; i < before.blocks().size(); i++) {
        assertThat(after.blocks().get(i))
            .as("entry %d is a fresh object after the rewrite", i)
            .isNotSameAs(before.blocks().get(i));
        assertThat(after.blocks().get(i).blockId)
            .as("a block copied verbatim keeps the id it was born with")
            .isEqualTo(before.blocks().get(i).blockId);
      }
    }
  }

  /**
   * Every block ever built gets its own id, whichever path built it.
   * <p>
   * Uniqueness and nothing more: the ids stopped being a monotone sequence with issue #8043, which put them in the
   * file so they survive a directory reload. Nothing ORDERS blocks by this field - the directory is ordered by
   * {@code minTimestamp} and {@code resolveLiveBlock} tests it for equality - so an ordering assertion would only
   * pin the way they happen to be generated.
   */
  @Test
  void everyBlockIsBornWithItsOwnId() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      appendBlock(store, "A", 1.0, 2.0);
      appendBlock(store, "B", 3.0, 4.0);
      appendBlock(store, "C", 5.0, 6.0);

      final List<Long> blockIds = new ArrayList<>();
      for (final TimeSeriesSealedStore.BlockEntry entry :
          store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks())
        blockIds.add(entry.blockId);

      assertThat(blockIds).doesNotHaveDuplicates().doesNotContain(0L);
    }
  }

  /**
   * A block the rewrite DROPPED must resolve to nothing rather than to whichever survivor shares its triple: the
   * rows it held are no longer in the store, and reading a survivor in its place would hand them over twice.
   */
  @Test
  void aDroppedBlockResolvesToNothing() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      appendBlock(store, "A", 1.0, 2.0);
      appendBlock(store, "B", 3.0, 4.0);

      final TimeSeriesSealedStore.BlockDirectorySnapshot snapshot =
          store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);

      // Drops the last block and rebuilds every remaining entry, so the survivor is a fresh object too.
      store.truncateToBlockCount(1);

      assertThat(sensorsOf(store, snapshot))
          .as("the survivor answers once; the dropped block answers not at all")
          .containsExactly("A", "A");
    }
  }

  // --- helpers ---

  /**
   * Two samples at 1000 and 2000 under one sensor name: every block this builds carries the SAME
   * {@code (minTimestamp, maxTimestamp, sampleCount)} and is told apart only by the values it holds.
   */
  private void appendBlock(final TimeSeriesSealedStore store, final String sensor, final double first,
      final double second) throws Exception {
    final long[] timestamps = { 1000L, 2000L };
    final String[] sensors = { sensor, sensor };
    final double[] temperatures = { first, second };

    store.appendBlock(2, 1000L, 2000L, new byte[][] {
        DeltaOfDeltaCodec.encode(timestamps),
        DictionaryCodec.encode(sensors),
        GorillaXORCodec.encode(temperatures)
    }, NO_STATS, NO_STATS, NO_STATS, NO_COUNTS, null);
  }

  /**
   * Rewrites the sealed file with the very blocks it already holds - what a {@code compact()} that seals nothing
   * new does - so every directory entry is replaced by a fresh object while the store's contents are unchanged.
   */
  private void rewriteDirectoryInPlace(final TimeSeriesSealedStore store) throws Exception {
    store.commitTempCompactionFile(
        store.writeTempCompactionFile(List.of(), List.of(), List.of(), List.of()));
  }

  /** The sensor column of every row the walk hands over, in order. */
  private List<String> sensorsOf(final TimeSeriesSealedStore store,
      final TimeSeriesSealedStore.BlockDirectorySnapshot snapshot) throws Exception {
    final List<String> sensors = new ArrayList<>();
    store.forEachRow(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, row -> {
      sensors.add((String) row[1]);
      return true;
    });
    return sensors;
  }
}
