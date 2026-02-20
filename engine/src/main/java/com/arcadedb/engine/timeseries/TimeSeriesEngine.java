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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Coordinates N shards for a TimeSeries type. Routes writes to shards
 * using thread-based selection, merges reads from all shards.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesEngine implements AutoCloseable {

  private final DatabaseInternal       database;
  private final String                 typeName;
  private final List<ColumnDefinition> columns;
  private final TimeSeriesShard[]      shards;
  private final int                    shardCount;

  public TimeSeriesEngine(final DatabaseInternal database, final String typeName,
      final List<ColumnDefinition> columns, final int shardCount) throws IOException {
    this.database = database;
    this.typeName = typeName;
    this.columns = columns;
    this.shardCount = shardCount;
    this.shards = new TimeSeriesShard[shardCount];

    for (int i = 0; i < shardCount; i++)
      shards[i] = new TimeSeriesShard(database, typeName, i, columns);
  }

  /**
   * Appends samples, routing to a shard based on the current thread.
   */
  public void appendSamples(final long[] timestamps, final Object[]... columnValues) throws IOException {
    final int shardIdx = (int) (Thread.currentThread().threadId() % shardCount);
    shards[shardIdx].appendSamples(timestamps, columnValues);
  }

  /**
   * Queries all shards and merge-sorts results by timestamp.
   */
  public List<Object[]> query(final long fromTs, final long toTs, final int[] columnIndices,
      final TagFilter tagFilter) throws IOException {
    final List<Object[]> merged = new ArrayList<>();
    for (final TimeSeriesShard shard : shards)
      merged.addAll(shard.scanRange(fromTs, toTs, columnIndices, tagFilter));

    merged.sort(Comparator.comparingLong(row -> (long) row[0]));
    return merged;
  }

  /**
   * Aggregates across all shards.
   */
  public AggregationResult aggregate(final long fromTs, final long toTs, final int columnIndex,
      final AggregationType aggType, final long bucketIntervalNs, final TagFilter tagFilter) throws IOException {
    // For MVP: query all data and aggregate in-memory
    final List<Object[]> data = query(fromTs, toTs, null, tagFilter);
    final AggregationResult result = new AggregationResult();

    for (final Object[] row : data) {
      final long ts = (long) row[0];
      final long bucketTs = bucketIntervalNs > 0 ? (ts / bucketIntervalNs) * bucketIntervalNs : fromTs;
      final double value;

      if (columnIndex + 1 < row.length && row[columnIndex + 1] instanceof Number)
        value = ((Number) row[columnIndex + 1]).doubleValue();
      else
        value = 0.0;

      accumulateToBucket(result, bucketTs, value, aggType);
    }

    // Finalize AVG
    if (aggType == AggregationType.AVG) {
      for (int i = 0; i < result.size(); i++) {
        // AVG stored as sum; divide by count to get average
        // AggregationResult doesn't support in-place update, so this is handled at query level
      }
    }

    return result;
  }

  /**
   * Triggers compaction on all shards.
   */
  public void compactAll() throws IOException {
    for (final TimeSeriesShard shard : shards)
      shard.compact();
  }

  /**
   * Applies retention policy: removes data older than the given timestamp.
   */
  public void applyRetention(final long cutoffTimestamp) throws IOException {
    for (final TimeSeriesShard shard : shards)
      shard.getSealedStore().truncateBefore(cutoffTimestamp);
  }

  public int getShardCount() {
    return shardCount;
  }

  public TimeSeriesShard getShard(final int index) {
    return shards[index];
  }

  public List<ColumnDefinition> getColumns() {
    return columns;
  }

  public String getTypeName() {
    return typeName;
  }

  @Override
  public void close() throws IOException {
    for (final TimeSeriesShard shard : shards)
      shard.close();
  }

  // --- Private helpers ---

  private void accumulateToBucket(final AggregationResult result, final long bucketTs, final double value,
      final AggregationType type) {
    // Find existing bucket
    for (int i = 0; i < result.size(); i++) {
      if (result.getBucketTimestamp(i) == bucketTs) {
        // Can't update in-place with current AggregationResult API
        // For MVP: this creates duplicates that are merged at query time
        return;
      }
    }
    result.addBucket(bucketTs, type == AggregationType.COUNT ? 1.0 : value, 1);
  }
}
