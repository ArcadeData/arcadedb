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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.codec.DictionaryCodec;
import com.arcadedb.engine.codec.GorillaXORCodec;
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore.BlockDirectorySnapshot;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
  private static final long[]   NO_COUNTS = { 0, 0, 0 };

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
  void createEmptyStore() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      assertThat(store.getBlockCount()).isEqualTo(0);
    }
  }

  @Test
  void appendAndReadBlock() throws Exception {
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
          new double[] { Double.NaN, Double.NaN, 106.0 }, new long[] { 0, 0, 5 }, null);

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
  void rangeFilter() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      final long[] timestamps = { 1000L, 2000L, 3000L, 4000L, 5000L };
      final String[] sensorIds = { "A", "B", "A", "C", "B" };
      final double[] temperatures = { 20.0, 21.5, 22.0, 19.5, 23.0 };

      final byte[][] compressed = {
          DeltaOfDeltaCodec.encode(timestamps),
          DictionaryCodec.encode(sensorIds),
          GorillaXORCodec.encode(temperatures)
      };
      store.appendBlock(5, 1000L, 5000L, compressed, NO_MINS, NO_MAXS, NO_SUMS, NO_COUNTS, null);

      // Query subset
      final List<Object[]> results = store.scanRange(2000L, 4000L, null, null);
      assertThat(results).hasSize(3);
      assertThat((long) results.get(0)[0]).isEqualTo(2000L);
      assertThat((long) results.get(2)[0]).isEqualTo(4000L);
    }
  }

  @Test
  void multipleBlocks() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      // Block 1: timestamps 1000-3000
      store.appendBlock(3, 1000L, 3000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L, 2000L, 3000L }),
          DictionaryCodec.encode(new String[] { "A", "A", "A" }),
          GorillaXORCodec.encode(new double[] { 10.0, 11.0, 12.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, NO_COUNTS, null);

      // Block 2: timestamps 4000-6000
      store.appendBlock(3, 4000L, 6000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 4000L, 5000L, 6000L }),
          DictionaryCodec.encode(new String[] { "B", "B", "B" }),
          GorillaXORCodec.encode(new double[] { 20.0, 21.0, 22.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, NO_COUNTS, null);

      assertThat(store.getBlockCount()).isEqualTo(2);
      assertThat(store.getGlobalMinTimestamp()).isEqualTo(1000L);
      assertThat(store.getGlobalMaxTimestamp()).isEqualTo(6000L);

      // Query spanning both blocks
      final List<Object[]> results = store.scanRange(2000L, 5000L, null, null);
      assertThat(results).hasSize(4);
    }
  }

  @Test
  void blockSkipping() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(2, 1000L, 2000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L, 2000L }),
          DictionaryCodec.encode(new String[] { "A", "A" }),
          GorillaXORCodec.encode(new double[] { 10.0, 11.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, NO_COUNTS, null);

      store.appendBlock(2, 5000L, 6000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 5000L, 6000L }),
          DictionaryCodec.encode(new String[] { "B", "B" }),
          GorillaXORCodec.encode(new double[] { 20.0, 21.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, NO_COUNTS, null);

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
  void tagFilterSlowPathScanRange() throws Exception {
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
          new double[] { Double.NaN, Double.NaN, 106.0 }, new long[] { 0, 0, 5 }, tagDV);

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
  void tagFilterSlowPathIterateRange() throws Exception {
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
          new double[] { Double.NaN, Double.NaN, 60.0 }, new long[] { 0, 0, 3 }, tagDV);

      final TagFilter filterX = TagFilter.eq(0, "X");
      final Iterator<Object[]> iter = store.iterateRange(1000L, 3000L, null, filterX);
      final List<Object[]> results = new ArrayList<>();
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
  void tagValueTooLongRejected() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      // 'ß' encodes to 2 UTF-8 bytes, so 16384 repetitions = 32768 bytes > 32767 limit
      final String longValue = "ß".repeat(16384);
      final String[][] tagDV = new String[3][];
      tagDV[1] = new String[] { longValue };

      assertThatThrownBy(() -> store.appendBlock(1, 1000L, 1000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L }),
          DictionaryCodec.encode(new String[] { longValue }),
          GorillaXORCodec.encode(new double[] { 1.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, NO_COUNTS, tagDV))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("too long");
    }
  }

  /**
   * Regression: loadDirectory must throw when the file's column count does not match the schema.
   * Previously the mismatch was silently ignored, potentially causing incorrect reads.
   */
  @Test
  void columnCountMismatchOnReopen() throws Exception {
    // Write a store with 3 columns
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(1, 1000L, 1000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L }),
          DictionaryCodec.encode(new String[] { "A" }),
          GorillaXORCodec.encode(new double[] { 1.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, NO_COUNTS, null);
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
  void downsamplePreservesAscendingOrderOnDisk() throws Exception {
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
      }, new double[] { Double.NaN, 1.0 }, new double[] { Double.NaN, 3.0 }, new double[] { Double.NaN, 6.0 }, new long[] { 0, 3 }, null);

      // Newer block: t=100_000..102_000 — retained (beyond cutoff)
      store.appendBlock(3, 100_000L, 102_000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 100_000L, 101_000L, 102_000L }),
          GorillaXORCodec.encode(new double[] { 10.0, 20.0, 30.0 })
      }, new double[] { Double.NaN, 10.0 }, new double[] { Double.NaN, 30.0 }, new double[] { Double.NaN, 60.0 }, new long[] { 0, 3 }, null);

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
      final List<Object[]> old = new ArrayList<>();
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
  void truncateBefore() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(2, 1000L, 2000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L, 2000L }),
          DictionaryCodec.encode(new String[] { "A", "A" }),
          GorillaXORCodec.encode(new double[] { 10.0, 11.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, NO_COUNTS, null);

      store.appendBlock(2, 5000L, 6000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 5000L, 6000L }),
          DictionaryCodec.encode(new String[] { "B", "B" }),
          GorillaXORCodec.encode(new double[] { 20.0, 21.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS, NO_COUNTS, null);

      // Truncate old data
      store.truncateBefore(3000L);
      assertThat(store.getBlockCount()).isEqualTo(1);

      final List<Object[]> results = store.scanRange(0L, 10000L, null, null);
      assertThat(results).hasSize(2);
      assertThat((long) results.get(0)[0]).isEqualTo(5000L);
    }
  }

  /**
   * Regression for HA sealed-store replication (issue #4382): a sealed file captured on the leader via
   * {@link TimeSeriesSealedStore#readWholeSealedFile()} can be installed verbatim on another node via
   * {@link TimeSeriesSealedStore#installSealedFileBytes(byte[])}, the install is idempotent (re-applying
   * the same bytes is a no-op), and installing a newer (larger) image overwrites the older one.
   */
  @Test
  void installSealedFileBytesIsIdempotentAndOverwrites() throws Exception {
    final String sourcePath = "target/databases/TimeSeriesSealedStoreTest/source";
    final byte[] sealedV1;
    final byte[] sealedV2;

    // Build a "leader" sealed store with one block, capture its bytes (V1), then add a second block (V2).
    try (final TimeSeriesSealedStore source = new TimeSeriesSealedStore(sourcePath, columns)) {
      source.appendBlock(2, 1000L, 2000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L, 2000L }),
          DictionaryCodec.encode(new String[] { "A", "B" }),
          GorillaXORCodec.encode(new double[] { 1.0, 2.0 })
      }, new double[] { Double.NaN, Double.NaN, 1.0 }, new double[] { Double.NaN, Double.NaN, 2.0 },
          new double[] { Double.NaN, Double.NaN, 3.0 }, new long[] { 0, 0, 2 }, null);
      source.flushHeader();
      sealedV1 = source.readWholeSealedFile();

      source.appendBlock(2, 3000L, 4000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 3000L, 4000L }),
          DictionaryCodec.encode(new String[] { "C", "D" }),
          GorillaXORCodec.encode(new double[] { 3.0, 4.0 })
      }, new double[] { Double.NaN, Double.NaN, 3.0 }, new double[] { Double.NaN, Double.NaN, 4.0 },
          new double[] { Double.NaN, Double.NaN, 7.0 }, new long[] { 0, 0, 2 }, null);
      source.flushHeader();
      sealedV2 = source.readWholeSealedFile();
    }

    // A "follower" store that starts empty, then installs V1.
    try (final TimeSeriesSealedStore follower = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      assertThat(follower.getBlockCount()).isZero();

      follower.installSealedFileBytes(sealedV1);
      assertThat(follower.getBlockCount()).isEqualTo(1);
      assertThat(follower.scanRange(0L, 10000L, null, null)).hasSize(2);
      assertThat(follower.readWholeSealedFile()).isEqualTo(sealedV1);

      // Idempotent re-apply of the same image: state unchanged.
      follower.installSealedFileBytes(sealedV1);
      assertThat(follower.getBlockCount()).isEqualTo(1);
      assertThat(follower.scanRange(0L, 10000L, null, null)).hasSize(2);

      // Installing the newer image overwrites it (ordered Raft replay ships V2 after V1).
      follower.installSealedFileBytes(sealedV2);
      assertThat(follower.getBlockCount()).isEqualTo(2);
      assertThat(follower.scanRange(0L, 10000L, null, null)).hasSize(4);
      assertThat(follower.readWholeSealedFile()).isEqualTo(sealedV2);
    }
  }

  /**
   * Issue #6283 (item 2): the header accessors of {@link TimeSeriesBucket} used to disagree with each other -
   * {@code getSampleCount()} and {@code getDataPageCount()} short-circuited on {@code getTotalPages() == 0} while
   * the timestamp and compaction readers went straight to {@code tx.getPage()}, which resolves with
   * {@code createIfNotExists=true} and therefore FABRICATES a zero-filled page 0, caches it, and reads zeros out
   * of it. Zero is a legal timestamp, so that answer is indistinguishable from a real one.
   * <p>
   * All of them now go through one non-fabricating header read that is authoritative on the magic in page 0, the
   * way {@code TimeSeriesTagDictionary.readStoredHeader()} is (issue #6198).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BucketHeaderAccessors extends TestHelper {

    private List<ColumnDefinition> columns() {
      return List.of(
          new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
          new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
    }

    /**
     * A bucket whose file carries no header page yet: the constructor deliberately does not write one, the shard
     * calls {@code initHeaderPage()} afterwards. Every reader must report emptiness rather than invent a page.
     */
    // #6283
    @Test
    void aBucketWithoutAHeaderPageReportsEmptyAndLeavesNoPhantomPage() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      database.begin();
      final TimeSeriesBucket bucket = new TimeSeriesBucket(db, "issue6283_noheader",
          db.getDatabasePath() + "/issue6283_noheader", columns());
      ((LocalSchema) db.getSchema()).registerFile(bucket);

      assertThat(bucket.getSampleCount()).isZero();
      assertThat(bucket.getDataPageCount()).isZero();
      // The empty markers initHeaderPage() writes, NOT the 0 a fabricated page would have read back.
      assertThat(bucket.getMinTimestamp()).isEqualTo(Long.MAX_VALUE);
      assertThat(bucket.getMaxTimestamp()).isEqualTo(Long.MIN_VALUE);
      assertThat(bucket.isCompactionInProgress()).isFalse();
      assertThat(bucket.getCompactionWatermark()).isZero();

      // Nothing was cached for a page that does not exist: asking the page manager without createIfNotExists
      // still finds nothing.
      assertThat(db.getPageManager().getImmutablePage(new PageId(db, bucket.getFileId(), 0), bucket.getPageSize(), true, false))
          .isNull();
      database.rollback();

      dropBucket(db, bucket);
    }

    /**
     * The counters written inside the current transaction - by {@code initHeaderPage()}, which adds page 0, and by
     * the append that follows - must be the ones the readers see, before any commit.
     */
    // #6283
    @Test
    void headerWrittenInTheCurrentTransactionIsVisibleToTheReaders() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      database.begin();
      final TimeSeriesBucket bucket = new TimeSeriesBucket(db, "issue6283_txlocal",
          db.getDatabasePath() + "/issue6283_txlocal", columns());
      ((LocalSchema) db.getSchema()).registerFile(bucket);
      bucket.initHeaderPage();

      assertThat(bucket.getSampleCount()).isZero();
      assertThat(bucket.getMinTimestamp()).isEqualTo(Long.MAX_VALUE);
      assertThat(bucket.getMaxTimestamp()).isEqualTo(Long.MIN_VALUE);

      bucket.appendSamples(new long[] { 1000L, 3000L }, new Object[] { 20.0, 22.0 });
      assertThat(bucket.getSampleCount()).isEqualTo(2);
      assertThat(bucket.getMinTimestamp()).isEqualTo(1000L);
      assertThat(bucket.getMaxTimestamp()).isEqualTo(3000L);
      assertThat(bucket.getDataPageCount()).isEqualTo(1);

      bucket.setCompactionInProgress(true);
      bucket.setCompactionWatermark(4096L);
      assertThat(bucket.isCompactionInProgress()).isTrue();
      assertThat(bucket.getCompactionWatermark()).isEqualTo(4096L);
      database.commit();

      // ...and again once committed, when the readers resolve the page through the page manager instead.
      database.begin();
      assertThat(bucket.getSampleCount()).isEqualTo(2);
      assertThat(bucket.getMinTimestamp()).isEqualTo(1000L);
      assertThat(bucket.getMaxTimestamp()).isEqualTo(3000L);
      assertThat(bucket.isCompactionInProgress()).isTrue();
      assertThat(bucket.getCompactionWatermark()).isEqualTo(4096L);
      database.commit();

      dropBucket(db, bucket);
    }

    /**
     * Issue #6937 added {@code getMinMaxTimestamps()}, which answers both bounds from ONE header read so the pair
     * is a consistent snapshot. It has to agree with the two single-value readers in every state, empty included.
     */
    // #6283
    @Test
    void combinedMinMaxReaderAgreesWithTheSingleValueReaders() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      database.begin();
      final TimeSeriesBucket bucket = new TimeSeriesBucket(db, "issue6937_minmax",
          db.getDatabasePath() + "/issue6937_minmax", columns());
      ((LocalSchema) db.getSchema()).registerFile(bucket);

      // No header page yet.
      assertThat(bucket.getMinMaxTimestamps()).containsExactly(Long.MAX_VALUE, Long.MIN_VALUE);

      // Header page written, still no samples.
      bucket.initHeaderPage();
      assertThat(bucket.getMinMaxTimestamps()).containsExactly(Long.MAX_VALUE, Long.MIN_VALUE);
      assertThat(bucket.getMinMaxTimestamps()).containsExactly(bucket.getMinTimestamp(), bucket.getMaxTimestamp());

      bucket.appendSamples(new long[] { -5_000L, 1000L, 3000L }, new Object[] { 19.0, 20.0, 22.0 });
      assertThat(bucket.getMinMaxTimestamps()).containsExactly(-5_000L, 3000L);
      assertThat(bucket.getMinMaxTimestamps()).containsExactly(bucket.getMinTimestamp(), bucket.getMaxTimestamp());
      database.commit();

      // ...and once committed, when the reader resolves the page through the page manager instead.
      database.begin();
      assertThat(bucket.getMinMaxTimestamps()).containsExactly(bucket.getMinTimestamp(), bucket.getMaxTimestamp());
      assertThat(bucket.getMinMaxTimestamps()).containsExactly(-5_000L, 3000L);
      database.commit();

      dropBucket(db, bucket);
    }

    /**
     * The buckets built here are registered by hand and belong to no TimeSeries type, so they have to be removed
     * before the integrity check that closes the test, which reports any file the schema does not claim.
     */
    private void dropBucket(final DatabaseInternal db, final TimeSeriesBucket bucket) throws IOException {
      ((LocalSchema) db.getSchema()).removeFile(bucket.getFileId());
      db.getPageManager().deleteFile(db, bucket.getFileId());
      db.getFileManager().dropFile(bucket.getFileId());
    }
  }

  /**
   * #8813 at the level of the sealed store: blocks appended out of timestamp order (what a compaction does with the
   * late-arriving samples it finds after merging the rest) must be served in timestamp order, survive a reopen, and be
   * handled by the checks that walk the file.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SealedStoreDirectoryOrder {
    private static final String ROOT      = "target/databases/Issue8813SealedStoreDirectoryOrderTest";
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

    /** A wide late block first on disk, then narrow ones: appended in the order a compaction's Phase 4 would. */
    private void load(final TimeSeriesSealedStore store) throws IOException {
      appendBlock(store, T0 + 1_000, T0 + 1_099);
      appendBlock(store, T0 + 2_000, T0 + 2_099);
      appendBlock(store, T0 + 3_000, T0 + 3_099);
      // late-arriving samples: older than everything above, written after it
      appendBlock(store, T0, T0 + 9);
      // and one that starts early and ends late, so maxTimestamp is not monotonic along the directory
      appendBlock(store, T0 + 500, T0 + 3_500);
    }

    private static long oldest(final TimeSeriesSealedStore store, final long fromTs, final int limit) throws IOException {
      final List<Object[]> rows = store.scanRangeAscending(fromTs, Long.MAX_VALUE, null, null, limit, null);
      return rows.isEmpty() ? -1 : (long) rows.getFirst()[0];
    }

    private void assertServedInOrder(final TimeSeriesSealedStore store) throws IOException {
      assertThat(oldest(store, Long.MIN_VALUE, 1)).isEqualTo(T0);
      // a lower bound inside the wide block, past blocks that end before it: only a running max finds the wide one
      assertThat(oldest(store, T0 + 3_200, 1)).isEqualTo(T0 + 3_200);
      assertThat(store.scanRange(T0 + 3_200, T0 + 3_300, null, null)).hasSize(101);
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.REPORT_ONLY).problems()).isEmpty();
    }

    // #8813
    @Test
    void outOfOrderBlocksAreServedInTimestampOrderAndSurviveReopen() throws IOException {
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_PATH, columns)) {
        load(store);
        assertServedInOrder(store);
      }
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_PATH, columns)) {
        assertServedInOrder(store);
      }
    }

    // #8813
    @Test
    void truncateToBlockCountKeepsTheFirstBlocksInFileOrder() throws IOException {
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_PATH, columns)) {
        load(store);
        // the first three blocks WRITTEN are the T0+1000, T0+2000 and T0+3000 ones, not the three oldest
        store.truncateToBlockCount(3);
        assertThat(store.getBlockCount()).isEqualTo(3);
        assertThat(oldest(store, Long.MIN_VALUE, 1)).isEqualTo(T0 + 1_000);
        assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.REPORT_ONLY).problems()).isEmpty();
      }
    }
  }

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
  @Nested
  class SealedBlockIdentity {

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
    // #7973
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
    // #7973
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
    // #7973
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
    // #7973
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

  /**
   * #8794: a continuous feed slower than one full block per maintenance pass sealed one small block per shard per
   * pass, and nothing ever merged them: 26 to 234 samples per block, a directory that grew with wall-clock time
   * instead of with data volume, and a retention pass that had that many blocks to copy.
   * <p>
   * {@link TimeSeriesEngine#mergeSmallBlocks()} is the missing merge. It must be LOSSLESS (same rows, same order,
   * same statistics), it must leave a block that already sits in one compaction bucket in that bucket, and a read
   * that is walking the old blocks while it lands must be answered whole (issue #9488; it used to be refused).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MergeSmallSealedBlocks extends TestHelper {
    private static final long T0 = 1_700_000_000_000L;

    private TimeSeriesEngine engine(final String type) {
      return ((LocalTimeSeriesType) database.getSchema().getType(type)).getEngine();
    }

    /** One maintenance pass worth of samples, sealed by itself: exactly what the 60 s scheduler does to a slow feed. */
    private void feedAndCompact(final TimeSeriesEngine engine, final int passes, final int samplesPerPass, final long stepMs)
        throws Exception {
      long ts = T0;
      for (int p = 0; p < passes; p++) {
        final long[] timestamps = new long[samplesPerPass];
        final Object[] tags = new Object[samplesPerPass];
        final Object[] values = new Object[samplesPerPass];
        for (int i = 0; i < samplesPerPass; i++) {
          timestamps[i] = ts;
          tags[i] = "series-" + (i % 7);
          values[i] = (double) (p * samplesPerPass + i);
          ts += stepMs;
        }
        engine.appendSamples(timestamps, tags, values);
        engine.compactAll();
      }
    }

    private int sealedBlocks(final TimeSeriesEngine engine) {
      int blocks = 0;
      for (int s = 0; s < engine.getShardCount(); s++)
        blocks += engine.getShard(s).getSealedStore().getBlockCount();
      return blocks;
    }

    private List<Object[]> allRows(final TimeSeriesEngine engine) throws Exception {
      return engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    }

    // #8794
    @Test
    void smallBlocksAreMergedWithoutLosingARow() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Slow");
      feedAndCompact(engine, 60, 25, 1_000L);

      assertThat(sealedBlocks(engine)).as("one small block per pass").isEqualTo(60);
      final List<Object[]> before = allRows(engine);
      assertThat(before).hasSize(1_500);

      engine.mergeSmallBlocks();

      assertThat(sealedBlocks(engine)).as("1500 samples fit in one block").isEqualTo(1);
      final List<Object[]> after = allRows(engine);
      assertThat(after).hasSize(before.size());
      for (int i = 0; i < before.size(); i++)
        assertThat(after.get(i)).as("row " + i).containsExactly(before.get(i));

      // the numbers the aggregation push-down answers from the block header must still be those of the data
      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      assertThat(sealed.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
      assertThat(engine.checkIntegrity().problems()).isEmpty();
    }

    // #8794
    @Test
    void theMergedLayoutSurvivesAReopen() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      feedAndCompact(engine("Slow"), 40, 30, 500L);
      final List<Object[]> before = allRows(engine("Slow"));
      engine("Slow").mergeSmallBlocks();
      assertThat(sealedBlocks(engine("Slow"))).isEqualTo(1);

      reopenDatabase();

      assertThat(sealedBlocks(engine("Slow"))).isEqualTo(1);
      final List<Object[]> after = allRows(engine("Slow"));
      assertThat(after).hasSameSizeAs(before);
      for (int i = 0; i < before.size(); i++)
        assertThat(after.get(i)).containsExactly(before.get(i));
    }

    // #8794
    @Test
    void aSecondMergeHasNothingToDo() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Slow");
      feedAndCompact(engine, 10, 20, 1_000L);
      engine.mergeSmallBlocks();

      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      final long idBefore = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks().getFirst().blockId;
      engine.mergeSmallBlocks();

      assertThat(sealedBlocks(engine)).isEqualTo(1);
      assertThat(sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks().getFirst().blockId)
          .as("a merge with nothing to merge must not rewrite the store").isEqualTo(idBefore);
    }

    // #8794
    @Test
    void theNextPassFoldsIntoTheTailBlockInsteadOfAddingOne() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Slow");

      long ts = T0;
      for (int p = 0; p < 12; p++) {
        final long[] timestamps = new long[40];
        final Object[] tags = new Object[40];
        final Object[] values = new Object[40];
        for (int i = 0; i < 40; i++) {
          timestamps[i] = ts;
          tags[i] = "s" + (i % 3);
          values[i] = (double) ts;
          ts += 1_000L;
        }
        engine.appendSamples(timestamps, tags, values);
        engine.compactAll();
        engine.mergeSmallBlocks();
        assertThat(sealedBlocks(engine)).as("after pass " + p).isEqualTo(1);
      }
      assertThat(allRows(engine)).hasSize(480);
    }

    // #8794
    @Test
    void nanNullAndExtremeValuesSurviveTheMerge() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Odd TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE, n LONG) SHARDS 1");
      final TimeSeriesEngine engine = engine("Odd");

      final double[][] vals = { { Double.NaN, 1.5 }, { 2.5, Double.MAX_VALUE }, { -0.0, Double.MIN_VALUE } };
      for (int p = 0; p < 3; p++) {
        engine.appendSamples(new long[] { T0 + p * 10, T0 + p * 10 + 1 }, new Object[] { "a", "" },
            new Object[] { vals[p][0], vals[p][1] }, new Object[] { -(1L << 40) + p, (1L << 40) - p });
        engine.compactAll();
      }
      final List<Object[]> before = allRows(engine);
      assertThat(sealedBlocks(engine)).isEqualTo(3);

      engine.mergeSmallBlocks();

      assertThat(sealedBlocks(engine)).isEqualTo(1);
      final List<Object[]> after = allRows(engine);
      assertThat(after).hasSameSizeAs(before);
      for (int i = 0; i < before.size(); i++)
        assertThat(after.get(i)).as("row " + i).containsExactly(before.get(i));
      assertThat(engine.getShard(0).getSealedStore().checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }

    // #8794
    @Test
    void aBlockNeverCrossesACompactionBucketWhenTheTypeAligns() throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE Aligned TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1 COMPACTION_INTERVAL 1 HOURS");
      final TimeSeriesEngine engine = engine("Aligned");
      final long hour = 3_600_000L;
      final long base = Math.floorDiv(T0, hour) * hour;

      // 6 passes of 10 samples in each of 3 consecutive hours
      for (int h = 0; h < 3; h++)
        for (int p = 0; p < 6; p++) {
          final long[] timestamps = new long[10];
          final Object[] tags = new Object[10];
          final Object[] values = new Object[10];
          for (int i = 0; i < 10; i++) {
            timestamps[i] = base + h * hour + p * 60_000L + i * 1_000L;
            tags[i] = "x";
            values[i] = (double) i;
          }
          engine.appendSamples(timestamps, tags, values);
          engine.compactAll();
        }
      assertThat(sealedBlocks(engine)).isGreaterThanOrEqualTo(18);

      engine.mergeSmallBlocks();

      final BlockDirectorySnapshot snapshot = engine.getShard(0).getSealedStore().snapshotBlockDirectory(Long.MIN_VALUE,
          Long.MAX_VALUE);
      assertThat(snapshot.blocks()).as("one block per hour").hasSize(3);
      snapshot.blocks().forEach(
          b -> assertThat(Math.floorDiv(b.minTimestamp, hour)).as("block stays inside its hour").isEqualTo(Math.floorDiv(b.maxTimestamp, hour)));
      assertThat(allRows(engine)).hasSize(180);
    }

    // #8794
    @Test
    void aWalkThatCrossesAMergeIsAnsweredWholeNotShort() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Slow");
      feedAndCompact(engine, 5, 20, 1_000L);

      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      final BlockDirectorySnapshot snapshot = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);
      assertThat(snapshot.blocks()).hasSize(5);

      engine.mergeSmallBlocks();

      // a merge keeps every row, so the walk follows it to the merged block instead of being refused (issue #9488)
      final List<Object[]> rows = new ArrayList<>();
      sealed.forEachRow(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, null, null, new AggregationMetrics(), rows::add);
      assertThat(rows).as("the walk is whole").hasSize(100);
      assertThat(allRows(engine)).as("a fresh read is whole").hasSize(100);
    }

    // #8794
    @Test
    void theSchedulerPassMergesToo() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("Slow");
      final TimeSeriesEngine engine = type.getEngine();
      feedAndCompact(engine, 20, 15, 1_000L);
      assertThat(sealedBlocks(engine)).isEqualTo(20);

      TimeSeriesMaintenanceScheduler.runMaintenance(database, type, "Slow");

      assertThat(sealedBlocks(engine)).isEqualTo(1);
      assertThat(allRows(engine)).hasSize(300);
    }

    // #8794
    @Test
    void fullBlocksAreLeftAlone() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Big TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Big");
      // two passes that each seal a full block (SEALED_BLOCK_SIZE rows) plus a small tail
      final int rows = TimeSeriesShard.SEALED_BLOCK_SIZE;
      for (int p = 0; p < 2; p++) {
        final long[] timestamps = new long[rows];
        final Object[] tags = new Object[rows];
        final Object[] values = new Object[rows];
        for (int i = 0; i < rows; i++) {
          timestamps[i] = T0 + (long) p * rows + i;
          tags[i] = "x";
          values[i] = (double) i;
        }
        engine.appendSamples(timestamps, tags, values);
        engine.compactAll();
      }
      final int blocks = sealedBlocks(engine);
      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      final long firstId = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks().getFirst().blockId;

      engine.mergeSmallBlocks();

      assertThat(sealedBlocks(engine)).isEqualTo(blocks);
      assertThat(sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks().getFirst().blockId).isEqualTo(firstId);
    }

    // #8794
    @Test
    void theSchedulerWaitsUntilAMergeIsWorthAFileRewrite() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("Slow");
      final TimeSeriesEngine engine = type.getEngine();
      feedAndCompact(engine, 5, 15, 1_000L);

      TimeSeriesMaintenanceScheduler.runMaintenance(database, type, "Slow");
      assertThat(sealedBlocks(engine)).as("5 blocks to save 4 is below the scheduler's threshold").isEqualTo(5);

      engine.mergeSmallBlocks(4);
      assertThat(sealedBlocks(engine)).isEqualTo(1);
    }

    // #8794
    @Test
    void aMergeAndARetentionPassRunningTogetherLeaveAConsistentStore() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine("Slow");
      feedAndCompact(engine, 40, 25, 1_000L);
      final long cutoff = T0 + 10 * 25 * 1_000L;

      final Thread retention = new Thread(() -> {
        try {
          for (int i = 0; i < 20; i++)
            engine.applyRetention(cutoff);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
      retention.start();
      for (int i = 0; i < 20; i++)
        engine.mergeSmallBlocks();
      retention.join();
      engine.mergeSmallBlocks();

      // retention drops whole blocks, so how many rows survive depends on how far the merge got: never more than were
      // fed, never fewer than those at or after the cutoff
      final List<Object[]> rows = allRows(engine);
      assertThat(rows.size()).isBetween(750, 1_000);
      final int survivors = rows.size();
      assertThat(engine.checkIntegrity().problems()).isEmpty();
      reopenDatabase();
      assertThat(allRows(engine("Slow"))).hasSize(survivors);
    }
  }
}
