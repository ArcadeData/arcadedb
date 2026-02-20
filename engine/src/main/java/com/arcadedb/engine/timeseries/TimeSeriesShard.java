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
import java.util.List;

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

    final String shardPath = database.getDatabasePath() + "/" + baseName + "_shard_" + shardIndex;
    this.mutableBucket = new TimeSeriesBucket(database, baseName + "_shard_" + shardIndex,
        shardPath, columns);
    ((LocalSchema) database.getSchema()).registerFile(mutableBucket);

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
   * Compacts mutable data into sealed columnar storage.
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

      // Sort by timestamp
      final int[] sortedIndices = sortIndices(timestamps);
      final long[] sortedTs = applyOrder(timestamps, sortedIndices);

      // Phase 3: Compress per-column and write sealed block
      final byte[][] compressedCols = new byte[columns.size()][];
      int tsIdx = 0;
      int colIdx = 0;
      for (int c = 0; c < columns.size(); c++) {
        if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP) {
          compressedCols[c] = DeltaOfDeltaCodec.encode(sortedTs);
          tsIdx = c;
        } else {
          final Object[] colValues = (Object[]) allData[colIdx + 1];
          final Object[] sortedColValues = applyOrderObjects(colValues, sortedIndices);
          compressedCols[c] = compressColumn(columns.get(c), sortedColValues);
          colIdx++;
        }
      }

      sealedStore.appendBlock(sortedTs.length, sortedTs[0], sortedTs[sortedTs.length - 1], compressedCols);

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
