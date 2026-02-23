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
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesSealedStoreTest {

  private static final String TEST_PATH = "target/databases/TimeSeriesSealedStoreTest/sealed";
  private List<ColumnDefinition> columns;

  // Stats arrays: ts(NaN), sensor_id(NaN), temperature(has stats)
  private static final double[] NO_MINS = { Double.NaN, Double.NaN, Double.NaN };
  private static final double[] NO_MAXS = { Double.NaN, Double.NaN, Double.NaN };
  private static final double[] NO_SUMS = { Double.NaN, Double.NaN, Double.NaN };

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File("target/databases/TimeSeriesSealedStoreTest"));
    new File("target/databases/TimeSeriesSealedStoreTest").mkdirs();

    columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor_id", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File("target/databases/TimeSeriesSealedStoreTest"));
  }

  @Test
  void testCreateEmptyStore() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      assertThat(store.getBlockCount()).isEqualTo(0);
    }
  }

  @Test
  void testAppendAndReadBlock() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      final long[] timestamps = { 1000L, 2000L, 3000L, 4000L, 5000L };
      final String[] sensorIds = { "A", "B", "A", "C", "B" };
      final double[] temperatures = { 20.0, 21.5, 22.0, 19.5, 23.0 };

      final byte[][] compressed = {
          DeltaOfDeltaCodec.encode(timestamps),
          DictionaryCodec.encode(sensorIds),
          GorillaXORCodec.encode(temperatures)
      };

      store.appendBlock(5, 1000L, 5000L, compressed,
          new double[] { Double.NaN, Double.NaN, 19.5 },
          new double[] { Double.NaN, Double.NaN, 23.0 },
          new double[] { Double.NaN, Double.NaN, 106.0 }, null);

      assertThat(store.getBlockCount()).isEqualTo(1);
      assertThat(store.getGlobalMinTimestamp()).isEqualTo(1000L);
      assertThat(store.getGlobalMaxTimestamp()).isEqualTo(5000L);

      // Read back
      final List<Object[]> results = store.scanRange(1000L, 5000L, null, null);
      assertThat(results).hasSize(5);

      assertThat((long) results.get(0)[0]).isEqualTo(1000L);
      assertThat((String) results.get(0)[1]).isEqualTo("A");
      assertThat((double) results.get(0)[2]).isEqualTo(20.0);

      assertThat((long) results.get(4)[0]).isEqualTo(5000L);
      assertThat((String) results.get(4)[1]).isEqualTo("B");
      assertThat((double) results.get(4)[2]).isEqualTo(23.0);
    }
  }

  @Test
  void testRangeFilter() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      final long[] timestamps = { 1000L, 2000L, 3000L, 4000L, 5000L };
      final String[] sensorIds = { "A", "B", "A", "C", "B" };
      final double[] temperatures = { 20.0, 21.5, 22.0, 19.5, 23.0 };

      final byte[][] compressed = {
          DeltaOfDeltaCodec.encode(timestamps),
          DictionaryCodec.encode(sensorIds),
          GorillaXORCodec.encode(temperatures)
      };
      store.appendBlock(5, 1000L, 5000L, compressed, NO_MINS, NO_MAXS, NO_SUMS, null);

      // Query subset
      final List<Object[]> results = store.scanRange(2000L, 4000L, null, null);
      assertThat(results).hasSize(3);
      assertThat((long) results.get(0)[0]).isEqualTo(2000L);
      assertThat((long) results.get(2)[0]).isEqualTo(4000L);
    }
  }

  @Test
  void testMultipleBlocks() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      // Block 1: timestamps 1000-3000
      store.appendBlock(3, 1000L, 3000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L, 2000L, 3000L }),
          DictionaryCodec.encode(new String[] { "A", "A", "A" }),
          GorillaXORCodec.encode(new double[] { 10.0, 11.0, 12.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, null);

      // Block 2: timestamps 4000-6000
      store.appendBlock(3, 4000L, 6000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 4000L, 5000L, 6000L }),
          DictionaryCodec.encode(new String[] { "B", "B", "B" }),
          GorillaXORCodec.encode(new double[] { 20.0, 21.0, 22.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, null);

      assertThat(store.getBlockCount()).isEqualTo(2);
      assertThat(store.getGlobalMinTimestamp()).isEqualTo(1000L);
      assertThat(store.getGlobalMaxTimestamp()).isEqualTo(6000L);

      // Query spanning both blocks
      final List<Object[]> results = store.scanRange(2000L, 5000L, null, null);
      assertThat(results).hasSize(4);
    }
  }

  @Test
  void testBlockSkipping() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(2, 1000L, 2000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L, 2000L }),
          DictionaryCodec.encode(new String[] { "A", "A" }),
          GorillaXORCodec.encode(new double[] { 10.0, 11.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, null);

      store.appendBlock(2, 5000L, 6000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 5000L, 6000L }),
          DictionaryCodec.encode(new String[] { "B", "B" }),
          GorillaXORCodec.encode(new double[] { 20.0, 21.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, null);

      // Query only block 2
      final List<Object[]> results = store.scanRange(5000L, 6000L, null, null);
      assertThat(results).hasSize(2);
      assertThat((String) results.get(0)[1]).isEqualTo("B");
    }
  }

  /**
   * Regression test: scanRange must apply per-row tag filtering for SLOW_PATH blocks
   * (blocks containing multiple distinct tag values where only some match the filter).
   */
  @Test
  void testTagFilterSlowPathScanRange() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      // Single block with mixed tag values — triggers SLOW_PATH in blockMatchesTagFilter
      final long[] timestamps = { 1000L, 2000L, 3000L, 4000L, 5000L };
      final String[] sensorIds = { "A", "B", "A", "C", "B" };
      final double[] temperatures = { 20.0, 21.5, 22.0, 19.5, 23.0 };

      final String[][] tagDV = new String[3][];
      tagDV[1] = new String[] { "A", "B", "C" }; // mixed → SLOW_PATH

      final byte[][] compressed = {
          DeltaOfDeltaCodec.encode(timestamps),
          DictionaryCodec.encode(sensorIds),
          GorillaXORCodec.encode(temperatures)
      };
      store.appendBlock(5, 1000L, 5000L, compressed,
          new double[] { Double.NaN, Double.NaN, 19.5 },
          new double[] { Double.NaN, Double.NaN, 23.0 },
          new double[] { Double.NaN, Double.NaN, 106.0 }, tagDV);

      // Filter for sensor_id == "A" only — should return rows at t=1000 and t=3000
      final TagFilter filterA = TagFilter.eq(0, "A");
      final List<Object[]> results = store.scanRange(1000L, 5000L, null, filterA);
      assertThat(results).hasSize(2);
      assertThat((long) results.get(0)[0]).isEqualTo(1000L);
      assertThat((String) results.get(0)[1]).isEqualTo("A");
      assertThat((long) results.get(1)[0]).isEqualTo(3000L);
      assertThat((String) results.get(1)[1]).isEqualTo("A");
    }
  }

  /**
   * Regression test: iterateRange must apply per-row tag filtering for SLOW_PATH blocks.
   */
  @Test
  void testTagFilterSlowPathIterateRange() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      final long[] timestamps = { 1000L, 2000L, 3000L };
      final String[] sensorIds = { "X", "Y", "X" };
      final double[] temperatures = { 10.0, 20.0, 30.0 };

      final String[][] tagDV = new String[3][];
      tagDV[1] = new String[] { "X", "Y" }; // mixed → SLOW_PATH

      store.appendBlock(3, 1000L, 3000L, new byte[][] {
          DeltaOfDeltaCodec.encode(timestamps),
          DictionaryCodec.encode(sensorIds),
          GorillaXORCodec.encode(temperatures)
      }, new double[] { Double.NaN, Double.NaN, 10.0 },
          new double[] { Double.NaN, Double.NaN, 30.0 },
          new double[] { Double.NaN, Double.NaN, 60.0 }, tagDV);

      final TagFilter filterX = TagFilter.eq(0, "X");
      final java.util.Iterator<Object[]> iter = store.iterateRange(1000L, 3000L, null, filterX);
      final List<Object[]> results = new java.util.ArrayList<>();
      while (iter.hasNext())
        results.add(iter.next());

      assertThat(results).hasSize(2);
      assertThat((String) results.get(0)[1]).isEqualTo("X");
      assertThat((String) results.get(1)[1]).isEqualTo("X");
    }
  }

  /**
   * Regression: buildTagMetadata must throw when a tag value's UTF-8 encoding exceeds 32767 bytes.
   * Previously it silently truncated the value via (short) val.length, causing data corruption.
   */
  @Test
  void testTagValueTooLongRejected() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      // 'ß' encodes to 2 UTF-8 bytes, so 16384 repetitions = 32768 bytes > 32767 limit
      final String longValue = "ß".repeat(16384);
      final String[][] tagDV = new String[3][];
      tagDV[1] = new String[] { longValue };

      assertThatThrownBy(() -> store.appendBlock(1, 1000L, 1000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L }),
          DictionaryCodec.encode(new String[] { longValue }),
          GorillaXORCodec.encode(new double[] { 1.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, tagDV))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("too long");
    }
  }

  /**
   * Regression: loadDirectory must throw when the file's column count does not match the schema.
   * Previously the mismatch was silently ignored, potentially causing incorrect reads.
   */
  @Test
  void testColumnCountMismatchOnReopen() throws Exception {
    // Write a store with 3 columns
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(1, 1000L, 1000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L }),
          DictionaryCodec.encode(new String[] { "A" }),
          GorillaXORCodec.encode(new double[] { 1.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, null);
    }

    // Try to reopen with a different schema (2 columns instead of 3)
    final List<ColumnDefinition> wrongSchema = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );
    assertThatThrownBy(() -> new TimeSeriesSealedStore(TEST_PATH, wrongSchema))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Column count mismatch");
  }

  /**
   * Regression: rewriteWithBlocks must write blocks in ascending minTimestamp order on disk.
   * Previously retained (newer) blocks were written first, then downsampled (older) blocks second.
   * After a restart, loadDirectory reads in file order — if the file is not sorted,
   * binary search in iterateRange fails to find blocks.
   */
  @Test
  void testDownsamplePreservesAscendingOrderOnDisk() throws Exception {
    final List<ColumnDefinition> numericCols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );
    final double[] noMins2 = { Double.NaN, Double.NaN };
    final double[] noMaxs2 = { Double.NaN, Double.NaN };
    final double[] noSums2 = { Double.NaN, Double.NaN };

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, numericCols)) {
      // Older block: t=6000..8000 — will be downsampled; bucket = (6000/5000)*5000 = 5000
      store.appendBlock(3, 6_000L, 8_000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 6_000L, 7_000L, 8_000L }),
          GorillaXORCodec.encode(new double[] { 1.0, 2.0, 3.0 })
      }, new double[] { Double.NaN, 1.0 }, new double[] { Double.NaN, 3.0 }, new double[] { Double.NaN, 6.0 }, null);

      // Newer block: t=100_000..102_000 — retained (beyond cutoff)
      store.appendBlock(3, 100_000L, 102_000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 100_000L, 101_000L, 102_000L }),
          GorillaXORCodec.encode(new double[] { 10.0, 20.0, 30.0 })
      }, new double[] { Double.NaN, 10.0 }, new double[] { Double.NaN, 30.0 }, new double[] { Double.NaN, 60.0 }, null);

      // Downsample blocks older than t=10_000 to 5_000ms granularity
      store.downsampleBlocks(10_000L, 5_000L, 0,
          List.of(),            // no tag columns
          List.of(1));          // numeric column at index 1
    }

    // Reopen to force loadDirectory — verifies on-disk ordering is correct
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, numericCols)) {
      // iterateRange uses binary search — requires ascending on-disk block order.
      // Downsampled block lands at bucket t=5000; query 5000..9000 to find it.
      final Iterator<Object[]> iter = store.iterateRange(5_000L, 9_000L, null, null);
      final List<Object[]> old = new java.util.ArrayList<>();
      while (iter.hasNext())
        old.add(iter.next());

      // Key assertion: iterateRange must find rows even after reopen (binary search correctness)
      assertThat(old).isNotEmpty();

      // Newer block should also be retrievable
      final Iterator<Object[]> iterNew = store.iterateRange(100_000L, 102_000L, null, null);
      assertThat(iterNew.hasNext()).isTrue();
    }
  }

  @Test
  void testTruncateBefore() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(2, 1000L, 2000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L, 2000L }),
          DictionaryCodec.encode(new String[] { "A", "A" }),
          GorillaXORCodec.encode(new double[] { 10.0, 11.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, null);

      store.appendBlock(2, 5000L, 6000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 5000L, 6000L }),
          DictionaryCodec.encode(new String[] { "B", "B" }),
          GorillaXORCodec.encode(new double[] { 20.0, 21.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, null);

      // Truncate old data
      store.truncateBefore(3000L);
      assertThat(store.getBlockCount()).isEqualTo(1);

      final List<Object[]> results = store.scanRange(0L, 10000L, null, null);
      assertThat(results).hasSize(2);
      assertThat((long) results.get(0)[0]).isEqualTo(5000L);
    }
  }
}
