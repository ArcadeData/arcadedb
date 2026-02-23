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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.timeseries.codec.DictionaryCodec;
import com.arcadedb.engine.timeseries.codec.GorillaXORCodec;
import com.arcadedb.engine.timeseries.codec.Simple8bCodec;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Pairs a mutable TimeSeriesBucket with a sealed TimeSeriesSealedStore.
 * Implements crash-safe compaction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesShard implements AutoCloseable {

  private final int                    shardIndex;
  private final DatabaseInternal       database;
  private final List<ColumnDefinition> columns;
  private final long                   compactionBucketIntervalMs;
  private final TimeSeriesBucket       mutableBucket;
  private final TimeSeriesSealedStore  sealedStore;
  // Read lock: held by scan/iterate (concurrent reads allowed).
  // Write lock: held by compact() to prevent queries from seeing data twice
  // during the window where sealed blocks are written but mutable not yet cleared.
  private final ReadWriteLock          compactionLock = new ReentrantReadWriteLock();

  public TimeSeriesShard(final DatabaseInternal database, final String baseName, final int shardIndex,
      final List<ColumnDefinition> columns) throws IOException {
    this(database, baseName, shardIndex, columns, 0);
  }

  public TimeSeriesShard(final DatabaseInternal database, final String baseName, final int shardIndex,
      final List<ColumnDefinition> columns, final long compactionBucketIntervalMs) throws IOException {
    this.shardIndex = shardIndex;
    this.database = database;
    this.columns = columns;
    this.compactionBucketIntervalMs = compactionBucketIntervalMs;

    final String shardName = baseName + "_shard_" + shardIndex;
    final String shardPath = database.getDatabasePath() + "/" + shardName;
    final LocalSchema schema = (LocalSchema) database.getSchema();

    // Check if the bucket was already loaded by the component factory (cold open)
    final com.arcadedb.engine.Component existing = schema.getFileByName(shardName);
    if (existing instanceof TimeSeriesBucket tsb) {
      this.mutableBucket = tsb;
      this.mutableBucket.setColumns(columns);
    } else {
      // First-time creation
      this.mutableBucket = new TimeSeriesBucket(database, shardName, shardPath, columns);
      schema.registerFile(mutableBucket);
    }

    this.sealedStore = new TimeSeriesSealedStore(shardPath, columns);

    // Crash recovery: if a compaction was interrupted, truncate any partial sealed blocks
    database.begin();
    try {
      if (mutableBucket.isCompactionInProgress()) {
        final long watermark = mutableBucket.getCompactionWatermark();
        sealedStore.truncateToBlockCount(watermark);
        mutableBucket.setCompactionInProgress(false);
        database.commit();
      } else {
        database.rollback();
      }
    } catch (final Exception e) {
      if (database.isTransactionActive())
        database.rollback();
      throw e instanceof IOException ? (IOException) e : new IOException("Crash recovery failed for shard " + shardIndex, e);
    }
  }

  /**
   * Appends samples to the mutable bucket.
   * No compactionLock needed: ArcadeDB's MVCC guarantees that if appendSamples() commits
   * concurrently with compact(), the page-version conflict is detected at commit time and the
   * losing transaction retries — preventing both data loss and double-counting.
   */
  public void appendSamples(final long[] timestamps, final Object[]... columnValues) throws IOException {
    mutableBucket.appendSamples(timestamps, columnValues);
  }

  /**
   * Scans both sealed and mutable layers, merging results by timestamp.
   * Holds the read lock to prevent concurrent {@link #compact()} from clearing the
   * mutable bucket after sealing, which would cause double-counting.
   */
  public List<Object[]> scanRange(final long fromTs, final long toTs, final int[] columnIndices,
      final TagFilter tagFilter) throws IOException {
    compactionLock.readLock().lock();
    try {
      final List<Object[]> results = new ArrayList<>();

      // Sealed layer first (already filtered by tagFilter inside sealedStore)
      final List<Object[]> sealedResults = sealedStore.scanRange(fromTs, toTs, columnIndices, tagFilter);
      results.addAll(sealedResults);

      // Then mutable layer
      final List<Object[]> mutableResults = mutableBucket.scanRange(fromTs, toTs, columnIndices);
      addFiltered(results, mutableResults, tagFilter, columnIndices);

      return results;
    } finally {
      compactionLock.readLock().unlock();
    }
  }

  /**
   * Returns a lazy iterator over both sealed and mutable layers.
   * Sealed data is iterated first, then mutable. Tag filter is applied inline.
   * Both underlying iterators eagerly collect matching rows under the read lock,
   * preventing concurrent {@link #compact()} from clearing the mutable bucket
   * while data collection is in progress.
   */
  public Iterator<Object[]> iterateRange(final long fromTs, final long toTs, final int[] columnIndices,
      final TagFilter tagFilter) throws IOException {
    final Iterator<Object[]> sealedIter;
    final Iterator<Object[]> mutableIter;
    compactionLock.readLock().lock();
    try {
      sealedIter = sealedStore.iterateRange(fromTs, toTs, columnIndices, tagFilter);
      mutableIter = mutableBucket.iterateRange(fromTs, toTs, columnIndices);
    } finally {
      compactionLock.readLock().unlock();
    }

    // Chain sealed then mutable, with inline tag filtering.
    // Data is already in memory (both iterators are eager), so no lock needed here.
    return new Iterator<>() {
      private Iterator<Object[]> current = sealedIter;
      private boolean            switchedToMutable = false;
      private Object[]           nextRow = null;

      {
        advance();
      }

      private void advance() {
        nextRow = null;
        while (true) {
          if (current.hasNext()) {
            final Object[] row = current.next();
            // Use matchesMapped() so the filter works correctly when columnIndices is a subset.
            if (tagFilter == null || tagFilter.matchesMapped(row, columnIndices)) {
              nextRow = row;
              return;
            }
          } else if (!switchedToMutable) {
            current = mutableIter;
            switchedToMutable = true;
          } else
            return;
        }
      }

      @Override
      public boolean hasNext() {
        return nextRow != null;
      }

      @Override
      public Object[] next() {
        if (nextRow == null)
          throw new NoSuchElementException();
        final Object[] result = nextRow;
        advance();
        return result;
      }
    };
  }

  /** Maximum number of samples per sealed block. Keeps decompression cost bounded. */
  static final int SEALED_BLOCK_SIZE = 65_536;

  /**
   * Compacts mutable data into sealed columnar storage.
   * Data is written in chunks of {@link #SEALED_BLOCK_SIZE} rows to keep
   * individual sealed blocks small for fast decompression during queries.
   * <p>
   * Follows the same lock-free pattern as LSMTree compaction:
   * <ol>
   *   <li>Heavy work (read, sort, compress, write temp file) runs WITHOUT the write lock so
   *       concurrent reads proceed unimpeded.</li>
   *   <li>The write lock is held only for the brief atomic-swap window:
   *       temp-file rename + mutable-bucket clear + commit.</li>
   * </ol>
   * Crash-safe: the {@code compactionInProgress} flag is persisted inside the same transaction
   * as {@code clearDataPages}, so crash recovery via {@link #TimeSeriesShard} constructor
   * correctly handles an interrupted swap.
   */
  public void compact() throws IOException {
    // Capture the initial block count before writing any new blocks.
    // Used for truncation if an exception occurs mid-compaction.
    final long initialBlockCount = sealedStore.getBlockCount();

    // ── Phase 1 (lock-free): read mutable data inside a transaction ──────────────────────────
    database.begin();
    try {
      if (mutableBucket.getSampleCount() == 0) {
        database.rollback();
        return;
      }
    } catch (final Exception e) {
      if (database.isTransactionActive())
        database.rollback();
      throw e instanceof IOException ? (IOException) e : new IOException("Compaction failed", e);
    }

    final Object[] allData;
    try {
      // Set crash-recovery flag before doing any sealed-store writes so that a crash
      // between the file swap and the mutable-clear commit can be detected on restart.
      mutableBucket.setCompactionInProgress(true);
      mutableBucket.setCompactionWatermark(initialBlockCount);

      allData = mutableBucket.readAllForCompaction();
      if (allData == null) {
        mutableBucket.setCompactionInProgress(false);
        database.commit();
        return;
      }
    } catch (final Exception e) {
      if (database.isTransactionActive())
        database.rollback();
      throw e instanceof IOException ? (IOException) e : new IOException("Compaction failed", e);
    }
    // Transaction stays open through the sort/compress/file-write phases so that
    // any concurrent appendSamples() commit is detected via MVCC at our commit time.

    // ── Phase 2 (lock-free): sort + compress ─────────────────────────────────────────────────
    final long[] timestamps = (long[]) allData[0];
    final int totalSamples = timestamps.length;

    final int[] sortedIndices = sortIndices(timestamps);
    final long[] sortedTs = applyOrder(timestamps, sortedIndices);

    final int colCount = columns.size();
    final Object[][] sortedColArrays = new Object[colCount][];
    int nonTsIdx = 0;
    for (int c = 0; c < colCount; c++) {
      if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        sortedColArrays[c] = null;
      else {
        sortedColArrays[c] = applyOrderObjects((Object[]) allData[nonTsIdx + 1], sortedIndices);
        nonTsIdx++;
      }
    }

    // Build lists of compressed blocks (written to the temp file in Phase 3).
    // When bucket-aligned compaction is configured, split at bucket boundaries
    // so each block fits entirely within one time bucket (enabling fast-path aggregation).
    final List<byte[][]> newCompressedList = new ArrayList<>();
    final List<long[]> newMetaList = new ArrayList<>();    // [minTs, maxTs, sampleCount]
    final List<double[]> newMinsList = new ArrayList<>();
    final List<double[]> newMaxsList = new ArrayList<>();
    final List<double[]> newSumsList = new ArrayList<>();
    final List<String[][]> newTagDVList = new ArrayList<>();

    int chunkStart = 0;
    while (chunkStart < totalSamples) {
      int chunkEnd;
      if (compactionBucketIntervalMs > 0) {
        final long bucketStart = (sortedTs[chunkStart] / compactionBucketIntervalMs) * compactionBucketIntervalMs;
        final long bucketEnd = bucketStart + compactionBucketIntervalMs;
        chunkEnd = chunkStart;
        final int maxEnd = Math.min(chunkStart + SEALED_BLOCK_SIZE, totalSamples);
        while (chunkEnd < maxEnd && sortedTs[chunkEnd] < bucketEnd)
          chunkEnd++;
        if (chunkEnd == chunkStart)
          chunkEnd = chunkStart + 1;
      } else {
        chunkEnd = Math.min(chunkStart + SEALED_BLOCK_SIZE, totalSamples);
      }

      chunkEnd = adjustChunkForDictionaryLimit(chunkStart, chunkEnd, colCount, sortedColArrays);

      final int chunkLen = chunkEnd - chunkStart;
      final long[] chunkTs = Arrays.copyOfRange(sortedTs, chunkStart, chunkEnd);

      final double[] mins = new double[colCount];
      final double[] maxs = new double[colCount];
      final double[] sums = new double[colCount];
      Arrays.fill(mins, Double.NaN);
      Arrays.fill(maxs, Double.NaN);

      final byte[][] compressedCols = new byte[colCount][];
      for (int c = 0; c < colCount; c++) {
        if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP) {
          compressedCols[c] = DeltaOfDeltaCodec.encode(chunkTs);
        } else {
          final Object[] chunkValues = Arrays.copyOfRange(sortedColArrays[c], chunkStart, chunkEnd);
          compressedCols[c] = compressColumn(columns.get(c), chunkValues);

          final TimeSeriesCodec codec = columns.get(c).getCompressionHint();
          if (codec == TimeSeriesCodec.GORILLA_XOR || codec == TimeSeriesCodec.SIMPLE8B) {
            double min = Double.MAX_VALUE, max = -Double.MAX_VALUE, sum = 0;
            for (final Object v : chunkValues) {
              final double d = v != null ? ((Number) v).doubleValue() : 0.0;
              if (d < min)
                min = d;
              if (d > max)
                max = d;
              sum += d;
            }
            mins[c] = min;
            maxs[c] = max;
            sums[c] = sum;
          }
        }
      }

      final String[][] chunkTagDistinctValues = new String[colCount][];
      for (int c = 0; c < colCount; c++) {
        if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TAG && sortedColArrays[c] != null) {
          final LinkedHashSet<String> distinctSet = new LinkedHashSet<>();
          for (int i = chunkStart; i < chunkEnd; i++) {
            final Object val = sortedColArrays[c][i];
            distinctSet.add(val != null ? val.toString() : "");
          }
          chunkTagDistinctValues[c] = distinctSet.toArray(new String[0]);
        }
      }

      newCompressedList.add(compressedCols);
      newMetaList.add(new long[] { chunkTs[0], chunkTs[chunkLen - 1], chunkLen });
      newMinsList.add(mins);
      newMaxsList.add(maxs);
      newSumsList.add(sums);
      newTagDVList.add(chunkTagDistinctValues);
      chunkStart = chunkEnd;
    }

    // ── Phase 3 (lock-free): write existing + new blocks to a temp file ──────────────────────
    // Concurrent queries still read from the CURRENT sealed file — no double-counting possible.
    final List<TimeSeriesSealedStore.BlockEntry> newBlockDirectory;
    try {
      newBlockDirectory = sealedStore.writeTempCompactionFile(
          newCompressedList, newMetaList, newMinsList, newMaxsList, newSumsList, newTagDVList);
    } catch (final Exception e) {
      if (database.isTransactionActive())
        database.rollback();
      sealedStore.deleteTempFileIfExists();
      throw e instanceof IOException ? (IOException) e : new IOException("Compaction failed writing temp file", e);
    }

    // ── Phase 4 (brief write lock): atomic file swap + clear mutable ─────────────────────────
    // The write lock prevents queries from seeing both the new sealed blocks AND the mutable
    // data simultaneously during the brief swap + clear window.
    compactionLock.writeLock().lock();
    try {
      try {
        // Atomically swap temp file into the sealed store; updates in-memory blockDirectory.
        sealedStore.commitTempCompactionFile(newBlockDirectory);
      } catch (final Exception e) {
        if (database.isTransactionActive())
          database.rollback();
        // Temp file may have been partially swapped — restore sealed store to initial state.
        try {
          sealedStore.truncateToBlockCount(initialBlockCount);
        } catch (final IOException te) {
          throw new IOException("Compaction failed and sealed store rollback also failed: " + te.getMessage(), e);
        }
        throw e instanceof IOException ? (IOException) e : new IOException("Compaction failed during file swap", e);
      }

      try {
        // Clear mutable bucket and persist the crash-recovery flag (both in one commit so
        // MVCC detects any concurrent appendSamples() that committed between Phase 1 and now).
        mutableBucket.clearDataPages();
        mutableBucket.setCompactionInProgress(false);
        database.commit();
      } catch (final Exception e) {
        if (database.isTransactionActive())
          database.rollback();
        // The sealed file was already swapped. Truncate it back so the next run
        // re-compacts from a clean state (crash-recovery flag is set, so restart also works).
        try {
          sealedStore.truncateToBlockCount(initialBlockCount);
        } catch (final IOException te) {
          throw new IOException("Compaction failed and sealed store rollback also failed: " + te.getMessage(), e);
        }
        throw e instanceof IOException ? (IOException) e : new IOException("Compaction failed clearing mutable", e);
      }
    } finally {
      compactionLock.writeLock().unlock();
    }
  }

  public TimeSeriesBucket getMutableBucket() {
    return mutableBucket;
  }

  public TimeSeriesSealedStore getSealedStore() {
    return sealedStore;
  }

  public int getShardIndex() {
    return shardIndex;
  }

  @Override
  public void close() throws IOException {
    mutableBucket.close();
    sealedStore.close();
  }

  // --- Private helpers ---

  private static void addFiltered(final List<Object[]> results, final List<Object[]> source, final TagFilter filter,
      final int[] columnIndices) {
    if (filter == null)
      results.addAll(source);
    else
      for (final Object[] row : source)
        // Use matchesMapped() so the filter works correctly when columnIndices is a subset.
        if (filter.matchesMapped(row, columnIndices))
          results.add(row);
  }

  private static int[] sortIndices(final long[] timestamps) {
    final int n = timestamps.length;
    final int[] indices = new int[n];
    for (int i = 0; i < n; i++)
      indices[i] = i;
    // Merge sort on primitive int[] to avoid Integer boxing
    mergeSort(indices, new int[n], 0, n, timestamps);
    return indices;
  }

  private static void mergeSort(final int[] arr, final int[] temp, final int from, final int to, final long[] keys) {
    if (to - from <= 1)
      return;
    final int mid = (from + to) >>> 1;
    mergeSort(arr, temp, from, mid, keys);
    mergeSort(arr, temp, mid, to, keys);
    // Merge
    int i = from, j = mid, k = from;
    while (i < mid && j < to) {
      if (keys[arr[i]] <= keys[arr[j]])
        temp[k++] = arr[i++];
      else
        temp[k++] = arr[j++];
    }
    while (i < mid)
      temp[k++] = arr[i++];
    while (j < to)
      temp[k++] = arr[j++];
    System.arraycopy(temp, from, arr, from, to - from);
  }

  private static long[] applyOrder(final long[] data, final int[] indices) {
    final long[] result = new long[data.length];
    for (int i = 0; i < indices.length; i++)
      result[i] = data[indices[i]];
    return result;
  }

  private static Object[] applyOrderObjects(final Object[] data, final int[] indices) {
    final Object[] result = new Object[data.length];
    for (int i = 0; i < indices.length; i++)
      result[i] = data[indices[i]];
    return result;
  }

  private byte[] compressColumn(final ColumnDefinition col, final Object[] values) {
    final TimeSeriesCodec codec = col.getCompressionHint();
    return switch (codec) {
      case GORILLA_XOR -> {
        final double[] doubles = new double[values.length];
        for (int i = 0; i < values.length; i++)
          doubles[i] = values[i] != null ? ((Number) values[i]).doubleValue() : 0.0;
        yield GorillaXORCodec.encode(doubles);
      }
      case SIMPLE8B -> {
        final long[] longs = new long[values.length];
        for (int i = 0; i < values.length; i++)
          longs[i] = values[i] != null ? ((Number) values[i]).longValue() : 0L;
        yield Simple8bCodec.encode(longs);
      }
      case DICTIONARY -> {
        final String[] strings = new String[values.length];
        for (int i = 0; i < values.length; i++)
          strings[i] = values[i] != null ? values[i].toString() : "";
        yield DictionaryCodec.encode(strings);
      }
      default -> throw new IllegalStateException("Unknown compression codec: " + codec);
    };
  }

  /**
   * If any DICTIONARY column in [chunkStart, chunkEnd) exceeds {@link DictionaryCodec#MAX_DICTIONARY_SIZE},
   * shrinks chunkEnd until all dictionary columns fit. Guarantees at least one row per chunk.
   */
  private int adjustChunkForDictionaryLimit(final int chunkStart, int chunkEnd,
      final int colCount, final Object[][] sortedColArrays) {
    for (int c = 0; c < colCount; c++) {
      if (columns.get(c).getCompressionHint() != TimeSeriesCodec.DICTIONARY || sortedColArrays[c] == null)
        continue;
      final HashSet<Object> distinct = new HashSet<>();
      for (int i = chunkStart; i < chunkEnd; i++) {
        distinct.add(sortedColArrays[c][i] != null ? sortedColArrays[c][i] : "");
        if (distinct.size() > DictionaryCodec.MAX_DICTIONARY_SIZE) {
          // Shrink chunk to i (exclusive) — the last row that still fits
          chunkEnd = Math.max(chunkStart + 1, i);
          break;
        }
      }
    }
    return chunkEnd;
  }
}
