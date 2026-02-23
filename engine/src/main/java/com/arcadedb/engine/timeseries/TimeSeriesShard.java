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
   * Holds the read lock to prevent {@link #compact()} from clearing newly appended
   * samples that arrive between {@code readAllForCompaction()} and {@code clearDataPages()}.
   */
  public void appendSamples(final long[] timestamps, final Object[]... columnValues) throws IOException {
    compactionLock.readLock().lock();
    try {
      mutableBucket.appendSamples(timestamps, columnValues);
    } finally {
      compactionLock.readLock().unlock();
    }
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
   * Crash-safe: uses a flag to detect incomplete compactions.
   */
  public void compact() throws IOException {
    // Write lock prevents queries from seeing data twice during the window between
    // sealing mutable data and clearing the mutable bucket.
    compactionLock.writeLock().lock();
    try {
    // Capture the initial block count before writing any new blocks.
    // If an exception occurs mid-compaction (non-crash), we truncate back here
    // to prevent duplicate blocks on the next compaction run.
    final long initialBlockCount = sealedStore.getBlockCount();
    database.begin();
    try {
      if (mutableBucket.getSampleCount() == 0) {
        database.rollback();
        return;
      }

      // Phase 1: Set compaction flag
      mutableBucket.setCompactionInProgress(true);
      final long watermark = initialBlockCount;
      mutableBucket.setCompactionWatermark(watermark);

      // Phase 2: Read all mutable data
      final Object[] allData = mutableBucket.readAllForCompaction();
      if (allData == null) {
        mutableBucket.setCompactionInProgress(false);
        database.commit();
        return;
      }

      final long[] timestamps = (long[]) allData[0];
      final int totalSamples = timestamps.length;

      // Sort by timestamp
      final int[] sortedIndices = sortIndices(timestamps);
      final long[] sortedTs = applyOrder(timestamps, sortedIndices);

      // Build sorted column arrays once
      final int colCount = columns.size();
      final Object[][] sortedColArrays = new Object[colCount][];
      int nonTsIdx = 0;
      for (int c = 0; c < colCount; c++) {
        if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
          sortedColArrays[c] = null; // timestamps handled separately
        else {
          sortedColArrays[c] = applyOrderObjects((Object[]) allData[nonTsIdx + 1], sortedIndices);
          nonTsIdx++;
        }
      }

      // Phase 3: Write sealed blocks in chunks with per-column stats.
      // When bucket-aligned compaction is configured, split at bucket boundaries
      // so each block fits entirely within one time bucket (enabling 100% fast-path aggregation).
      int chunkStart = 0;
      while (chunkStart < totalSamples) {
        int chunkEnd;
        if (compactionBucketIntervalMs > 0) {
          // Find the bucket for the first sample in this chunk
          final long bucketStart = (sortedTs[chunkStart] / compactionBucketIntervalMs) * compactionBucketIntervalMs;
          final long bucketEnd = bucketStart + compactionBucketIntervalMs;

          // Find where the bucket ends (first sample >= bucketEnd) or cap at SEALED_BLOCK_SIZE
          chunkEnd = chunkStart;
          final int maxEnd = Math.min(chunkStart + SEALED_BLOCK_SIZE, totalSamples);
          while (chunkEnd < maxEnd && sortedTs[chunkEnd] < bucketEnd)
            chunkEnd++;

          // Safety: ensure at least one sample per chunk to avoid infinite loop
          if (chunkEnd == chunkStart)
            chunkEnd = chunkStart + 1;
        } else {
          chunkEnd = Math.min(chunkStart + SEALED_BLOCK_SIZE, totalSamples);
        }

        // Shrink chunk if any DICTIONARY column exceeds max distinct values
        chunkEnd = adjustChunkForDictionaryLimit(chunkStart, chunkEnd, colCount, sortedColArrays);

        final int chunkLen = chunkEnd - chunkStart;
        final long[] chunkTs = Arrays.copyOfRange(sortedTs, chunkStart, chunkEnd);

        // Compute per-column stats for numeric columns
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

            // Compute stats for numeric columns (GORILLA_XOR / SIMPLE8B)
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

        // Collect distinct tag values for this chunk
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

        sealedStore.appendBlock(chunkLen, chunkTs[0], chunkTs[chunkLen - 1], compressedCols, mins, maxs, sums,
            chunkTagDistinctValues);
        chunkStart = chunkEnd;
      }

      // Flush sealed store header after all blocks have been written
      sealedStore.flushHeader();

      // Phase 4: Clear mutable pages
      mutableBucket.clearDataPages();
      mutableBucket.setCompactionInProgress(false);
      database.commit();

    } catch (final Exception e) {
      if (database.isTransactionActive())
        database.rollback();
      // Truncate any sealed blocks written during this failed attempt so the
      // next compaction run starts from a clean state and avoids duplicates.
      try {
        sealedStore.truncateToBlockCount(initialBlockCount);
      } catch (final IOException te) {
        // Log but do not suppress the original exception
        throw new IOException("Compaction failed and sealed store rollback also failed: " + te.getMessage(), e);
      }
      throw e instanceof IOException ? (IOException) e : new IOException("Compaction failed", e);
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
