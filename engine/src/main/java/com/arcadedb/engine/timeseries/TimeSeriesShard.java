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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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
  private final TimeSeriesBucket       mutableBucket;
  private final TimeSeriesSealedStore  sealedStore;

  public TimeSeriesShard(final DatabaseInternal database, final String baseName, final int shardIndex,
      final List<ColumnDefinition> columns) throws IOException {
    this.shardIndex = shardIndex;
    this.database = database;
    this.columns = columns;

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
  }

  /**
   * Appends samples to the mutable bucket.
   */
  public void appendSamples(final long[] timestamps, final Object[]... columnValues) throws IOException {
    mutableBucket.appendSamples(timestamps, columnValues);
  }

  /**
   * Scans both sealed and mutable layers, merging results by timestamp.
   */
  public List<Object[]> scanRange(final long fromTs, final long toTs, final int[] columnIndices,
      final TagFilter tagFilter) throws IOException {
    final List<Object[]> results = new ArrayList<>();

    // Sealed layer first
    final List<Object[]> sealedResults = sealedStore.scanRange(fromTs, toTs, columnIndices);
    addFiltered(results, sealedResults, tagFilter);

    // Then mutable layer
    final List<Object[]> mutableResults = mutableBucket.scanRange(fromTs, toTs, columnIndices);
    addFiltered(results, mutableResults, tagFilter);

    return results;
  }

  /**
   * Returns a lazy iterator over both sealed and mutable layers.
   * Sealed data is iterated first, then mutable. Tag filter is applied inline.
   * Memory usage: O(blockSize) for sealed, O(pageSize) for mutable.
   */
  public Iterator<Object[]> iterateRange(final long fromTs, final long toTs, final int[] columnIndices,
      final TagFilter tagFilter) throws IOException {
    final Iterator<Object[]> sealedIter = sealedStore.iterateRange(fromTs, toTs, columnIndices);
    final Iterator<Object[]> mutableIter = mutableBucket.iterateRange(fromTs, toTs, columnIndices);

    // Chain sealed then mutable, with inline tag filtering
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
            if (tagFilter == null || tagFilter.matches(row)) {
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
    database.begin();
    try {
      if (mutableBucket.getSampleCount() == 0) {
        database.commit();
        return;
      }

      // Phase 1: Set compaction flag
      mutableBucket.setCompactionInProgress(true);
      final long watermark = sealedStore.getBlockCount();
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

      // Phase 3: Write sealed blocks in chunks with per-column stats
      for (int chunkStart = 0; chunkStart < totalSamples; chunkStart += SEALED_BLOCK_SIZE) {
        final int chunkEnd = Math.min(chunkStart + SEALED_BLOCK_SIZE, totalSamples);
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

        sealedStore.appendBlock(chunkLen, chunkTs[0], chunkTs[chunkLen - 1], compressedCols, mins, maxs, sums);
      }

      // Phase 4: Clear mutable pages
      mutableBucket.clearDataPages();
      mutableBucket.setCompactionInProgress(false);
      database.commit();

    } catch (final Exception e) {
      if (database.isTransactionActive())
        database.rollback();
      throw e instanceof IOException ? (IOException) e : new IOException("Compaction failed", e);
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

  private static void addFiltered(final List<Object[]> results, final List<Object[]> source, final TagFilter filter) {
    if (filter == null)
      results.addAll(source);
    else
      for (final Object[] row : source)
        if (filter.matches(row))
          results.add(row);
  }

  private static int[] sortIndices(final long[] timestamps) {
    final Integer[] indices = new Integer[timestamps.length];
    for (int i = 0; i < indices.length; i++)
      indices[i] = i;
    Arrays.sort(indices, (a, b) -> Long.compare(timestamps[a], timestamps[b]));
    final int[] result = new int[indices.length];
    for (int i = 0; i < indices.length; i++)
      result[i] = indices[i];
    return result;
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
      default -> new byte[0];
    };
  }
}
