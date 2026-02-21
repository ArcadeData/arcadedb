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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

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
   * Returns a lazy merge-sorted iterator across all shards.
   * Uses a min-heap to merge shard iterators by timestamp.
   * Memory usage: O(shardCount * max(blockSize, pageSize)) instead of O(totalRows).
   */
  public Iterator<Object[]> iterateQuery(final long fromTs, final long toTs, final int[] columnIndices,
      final TagFilter tagFilter) throws IOException {
    final PriorityQueue<PeekableIterator> heap = new PriorityQueue<>(
        Math.max(1, shardCount), Comparator.comparingLong(it -> (long) it.peek()[0]));

    for (final TimeSeriesShard shard : shards) {
      final Iterator<Object[]> it = shard.iterateRange(fromTs, toTs, columnIndices, tagFilter);
      if (it.hasNext())
        heap.add(new PeekableIterator(it));
    }

    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return !heap.isEmpty();
      }

      @Override
      public Object[] next() {
        if (heap.isEmpty())
          throw new NoSuchElementException();
        final PeekableIterator min = heap.poll();
        final Object[] row = min.next();
        if (min.hasNext())
          heap.add(min);
        return row;
      }
    };
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

    // Finalize AVG: divide accumulated sums by counts
    if (aggType == AggregationType.AVG) {
      for (int i = 0; i < result.size(); i++)
        result.updateValue(i, result.getValue(i) / result.getCount(i));
    }

    return result;
  }

  /**
   * Aggregates multiple columns in a single pass, bucketed by time interval.
   * Returns only the aggregated buckets instead of all raw rows.
   */
  public MultiColumnAggregationResult aggregateMulti(final long fromTs, final long toTs,
      final List<MultiColumnAggregationRequest> requests, final long bucketIntervalMs,
      final TagFilter tagFilter) throws IOException {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult();
    final Iterator<Object[]> it = iterateQuery(fromTs, toTs, null, tagFilter);

    while (it.hasNext()) {
      final Object[] row = it.next();
      final long ts = (long) row[0];
      final long bucketTs = bucketIntervalMs > 0 ? (ts / bucketIntervalMs) * bucketIntervalMs : fromTs;

      for (final MultiColumnAggregationRequest req : requests) {
        final double value;
        if (req.type() == AggregationType.COUNT) {
          value = 1.0;
        } else if (req.columnIndex() < row.length && row[req.columnIndex()] instanceof Number n) {
          value = n.doubleValue();
        } else {
          value = 0.0;
        }
        result.accumulate(bucketTs, req.alias(), value, req.type());
      }
    }

    result.finalizeAvg();
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

  /**
   * Returns the total number of samples across all shards (sealed + mutable).
   * O(shardCount * blockCount), all data already in memory.
   */
  public long countSamples() throws IOException {
    long total = 0;
    for (final TimeSeriesShard shard : shards) {
      total += shard.getSealedStore().getTotalSampleCount();
      total += shard.getMutableBucket().getSampleCount();
    }
    return total;
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

  private static final class PeekableIterator implements Iterator<Object[]> {
    private final Iterator<Object[]> delegate;
    private       Object[]           peeked;

    PeekableIterator(final Iterator<Object[]> delegate) {
      this.delegate = delegate;
      this.peeked = delegate.hasNext() ? delegate.next() : null;
    }

    Object[] peek() {
      return peeked;
    }

    @Override
    public boolean hasNext() {
      return peeked != null;
    }

    @Override
    public Object[] next() {
      if (peeked == null)
        throw new NoSuchElementException();
      final Object[] result = peeked;
      peeked = delegate.hasNext() ? delegate.next() : null;
      return result;
    }
  }

  private void accumulateToBucket(final AggregationResult result, final long bucketTs, final double value,
      final AggregationType type) {
    final int idx = result.findBucketIndex(bucketTs);
    if (idx >= 0) {
      final double existing = result.getValue(idx);
      final long count = result.getCount(idx);
      final double merged = switch (type) {
        case SUM -> existing + value;
        case COUNT -> existing + 1;
        case AVG -> existing + value; // accumulate sum, divide by count later
        case MIN -> Math.min(existing, value);
        case MAX -> Math.max(existing, value);
      };
      result.updateValue(idx, merged);
      result.updateCount(idx, count + 1);
    } else {
      result.addBucket(bucketTs, type == AggregationType.COUNT ? 1.0 : value, 1);
    }
  }
}
