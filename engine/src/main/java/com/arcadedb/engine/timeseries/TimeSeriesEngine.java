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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinates N shards for a TimeSeries type. Routes sync writes to shards
 * using round-robin selection, merges reads from all shards.
 * <p>
 * Shard count defaults to the number of async worker threads so that when
 * using the async API each slot owns exactly one shard (1:1 affinity).
 * When running on a machine with fewer or more cores than the one where
 * the type was created, the async executor's {@code getSlot(shardIdx)}
 * mapping still guarantees contention-free writes: each shard always
 * maps to the same slot.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesEngine implements AutoCloseable {

  private final DatabaseInternal       database;
  private final String                 typeName;
  private final List<ColumnDefinition> columns;
  private final TimeSeriesShard[]      shards;
  private final int                    shardCount;
  private final long                   compactionBucketIntervalMs;
  private final ExecutorService        shardExecutor;
  private final AtomicLong             appendCounter = new AtomicLong();

  public TimeSeriesEngine(final DatabaseInternal database, final String typeName,
      final List<ColumnDefinition> columns, final int shardCount) throws IOException {
    this(database, typeName, columns, shardCount, 0);
  }

  public TimeSeriesEngine(final DatabaseInternal database, final String typeName,
      final List<ColumnDefinition> columns, final int shardCount,
      final long compactionBucketIntervalMs) throws IOException {
    this.database = database;
    this.typeName = typeName;
    this.columns = columns;
    this.shardCount = shardCount;
    this.compactionBucketIntervalMs = compactionBucketIntervalMs;
    this.shards = new TimeSeriesShard[shardCount];
    final AtomicInteger threadCounter = new AtomicInteger(0);
    this.shardExecutor = Executors.newFixedThreadPool(shardCount, r -> {
      final Thread t = new Thread(r, "ArcadeDB-TS-Shard-" + typeName + "-" + threadCounter.getAndIncrement());
      t.setDaemon(true);
      return t;
    });

    try {
      for (int i = 0; i < shardCount; i++)
        shards[i] = new TimeSeriesShard(database, typeName, i, columns, compactionBucketIntervalMs);
    } catch (final Exception e) {
      shardExecutor.shutdownNow();
      // Close any shards that were successfully created
      for (final TimeSeriesShard shard : shards) {
        if (shard != null) {
          try {
            shard.close();
          } catch (final IOException ignored) {
          }
        }
      }
      throw e instanceof IOException ? (IOException) e : new IOException("Failed to initialize shards for " + typeName, e);
    }
  }

  /**
   * Appends samples, routing to a shard using round-robin distribution.
   * <p>
   * Note: this method is not synchronized. When multiple threads call it concurrently,
   * they may be routed to the same shard. For contention-free writes, use the async API
   * which provides 1:1 slot-to-shard affinity.
   * <p>
   * <b>Dictionary column constraint:</b> columns using {@code DICTIONARY} compression
   * (typically TAG columns) must not exceed {@link com.arcadedb.engine.timeseries.codec.DictionaryCodec#MAX_DICTIONARY_SIZE}
   * distinct values per sealed block. This is validated at compaction time; data that violates
   * the limit will cause compaction to fail. Plan tag cardinality accordingly.
   */
  public void appendSamples(final long[] timestamps, final Object[]... columnValues) throws IOException {
    final int shardIdx = (int) Math.floorMod(appendCounter.getAndIncrement(), (long) shardCount);
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
   *
   * @param columnIndex 0-based index among non-timestamp columns (i.e. column 0 = first non-ts column).
   *                    This differs from {@link MultiColumnAggregationRequest#columnIndex()} which uses
   *                    the full schema index (including the timestamp column).
   */
  public AggregationResult aggregate(final long fromTs, final long toTs, final int columnIndex,
      final AggregationType aggType, final long bucketIntervalMs, final TagFilter tagFilter) throws IOException {
    // Use lazy iteration to avoid loading all data into memory
    final Iterator<Object[]> iter = iterateQuery(fromTs, toTs, null, tagFilter);
    final AggregationResult result = new AggregationResult();

    while (iter.hasNext()) {
      final Object[] row = iter.next();
      final long ts = (long) row[0];
      final long bucketTs = bucketIntervalMs > 0 ? (ts / bucketIntervalMs) * bucketIntervalMs : fromTs;
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
   * Uses block-level aggregation on sealed stores (decompresses arrays directly, no Object[] boxing).
   * Falls back to row iteration only for the small mutable bucket.
   */
  public MultiColumnAggregationResult aggregateMulti(final long fromTs, final long toTs,
      final List<MultiColumnAggregationRequest> requests, final long bucketIntervalMs,
      final TagFilter tagFilter) throws IOException {
    return aggregateMulti(fromTs, toTs, requests, bucketIntervalMs, tagFilter, null);
  }

  /**
   * Aggregates multiple columns in a single pass, bucketed by time interval.
   * Optionally populates an {@link AggregationMetrics} with timing breakdown.
   */
  public MultiColumnAggregationResult aggregateMulti(final long fromTs, final long toTs,
      final List<MultiColumnAggregationRequest> requests, final long bucketIntervalMs,
      final TagFilter tagFilter, final AggregationMetrics metrics) throws IOException {
    final int reqCount = requests.size();

    // Determine actual data range to size flat arrays correctly
    long actualMin = Long.MAX_VALUE;
    long actualMax = Long.MIN_VALUE;
    final boolean useFlatMode = bucketIntervalMs > 0;
    if (useFlatMode) {
      for (final TimeSeriesShard shard : shards) {
        final TimeSeriesSealedStore ss = shard.getSealedStore();
        if (ss.getBlockCount() > 0) {
          if (ss.getGlobalMinTimestamp() < actualMin)
            actualMin = ss.getGlobalMinTimestamp();
          if (ss.getGlobalMaxTimestamp() > actualMax)
            actualMax = ss.getGlobalMaxTimestamp();
        }
      }
      // Clamp to query range
      if (fromTs != Long.MIN_VALUE && fromTs > actualMin)
        actualMin = fromTs;
      if (toTs != Long.MAX_VALUE && toTs < actualMax)
        actualMax = toTs;
    }

    final long firstBucket;
    final int maxBuckets;
    if (useFlatMode && actualMin <= actualMax) {
      firstBucket = (actualMin / bucketIntervalMs) * bucketIntervalMs;
      final long computedBuckets = (actualMax - firstBucket) / bucketIntervalMs + 2;
      if (computedBuckets > MultiColumnAggregationResult.MAX_FLAT_BUCKETS)
        // Will trigger map-mode fallback in MultiColumnAggregationResult constructor
        maxBuckets = MultiColumnAggregationResult.MAX_FLAT_BUCKETS + 1;
      else
        maxBuckets = (int) computedBuckets;
    } else {
      firstBucket = 0;
      maxBuckets = 0;
    }

    // Pre-extract column indices and types for mutable bucket iteration
    final int[] columnIndices = new int[reqCount];
    final boolean[] isCount = new boolean[reqCount];
    for (int r = 0; r < reqCount; r++) {
      columnIndices[r] = requests.get(r).columnIndex();
      isCount[r] = requests.get(r).type() == AggregationType.COUNT;
    }

    // Process sealed stores in parallel when there are multiple shards with data
    if (shardCount > 1 && maxBuckets > 0) {
      @SuppressWarnings("unchecked")
      final CompletableFuture<MultiColumnAggregationResult>[] futures = new CompletableFuture[shardCount];

      // Acquire all shard compaction read locks on the calling thread (which has the database
      // transaction context) before dispatching futures. This prevents compaction from
      // completing between the sealed reads (in futures) and the mutable reads (on the calling
      // thread), which would cause data loss: sealed would see the old state and mutable would
      // be empty after compaction cleared it.
      // Worker threads only read sealed stores (no transaction required); mutable reads happen
      // on the calling thread after futures complete.
      for (int s = 0; s < shardCount; s++)
        shards[s].getCompactionLock().readLock().lock();
      try {
        final AggregationMetrics[] shardMetricsArr = metrics != null ? new AggregationMetrics[shardCount] : null;
        for (int s = 0; s < shardCount; s++) {
          final TimeSeriesShard shard = shards[s];
          final AggregationMetrics shardMetrics = metrics != null ? new AggregationMetrics() : null;
          if (shardMetricsArr != null)
            shardMetricsArr[s] = shardMetrics;
          futures[s] = CompletableFuture.supplyAsync(() -> {
            try {
              final MultiColumnAggregationResult shardResult =
                  new MultiColumnAggregationResult(requests, firstBucket, bucketIntervalMs, maxBuckets);
              shard.getSealedStore().aggregateMultiBlocks(fromTs, toTs, requests, bucketIntervalMs, shardResult, shardMetrics, tagFilter);
              return shardResult;
            } catch (final IOException e) {
              throw new CompletionException(e);
            }
          }, shardExecutor);
        }

        // Wait for all sealed reads to complete
        try {
          CompletableFuture.allOf(futures).join();
        } catch (final CompletionException e) {
          if (e.getCause() instanceof IOException ioe)
            throw ioe;
          throw new IOException("Parallel shard aggregation failed", e.getCause());
        }

        // Merge metrics after all futures have completed (avoids race condition)
        if (shardMetricsArr != null)
          for (final AggregationMetrics sm : shardMetricsArr)
            metrics.mergeFrom(sm);

        final MultiColumnAggregationResult result = futures[0].join();
        for (int s = 1; s < shardCount; s++)
          result.mergeFrom(futures[s].join());

        // Process mutable buckets on the calling thread (has both transaction context and the
        // compaction read locks acquired above, so compaction cannot clear mutable data now)
        final double[] rowValues = new double[reqCount];
        for (final TimeSeriesShard shard : shards) {
          final Iterator<Object[]> mutableIter = shard.getMutableBucket().iterateRange(fromTs, toTs, null);
          while (mutableIter.hasNext()) {
            final Object[] row = mutableIter.next();
            if (tagFilter != null && !tagFilter.matches(row))
              continue;
            final long ts = (long) row[0];
            final long bucketTs = (ts / bucketIntervalMs) * bucketIntervalMs;
            for (int r = 0; r < reqCount; r++) {
              if (isCount[r])
                rowValues[r] = 1.0;
              else if (columnIndices[r] < row.length && row[columnIndices[r]] instanceof Number n)
                rowValues[r] = n.doubleValue();
              else
                rowValues[r] = 0.0;
            }
            result.accumulateRow(bucketTs, rowValues);
          }
        }

        result.finalizeAvg();
        return result;
      } finally {
        for (int s = 0; s < shardCount; s++)
          shards[s].getCompactionLock().readLock().unlock();
      }

    } else {
      // Sequential path: single shard or no flat mode
      final MultiColumnAggregationResult result = maxBuckets > 0
          ? new MultiColumnAggregationResult(requests, firstBucket, bucketIntervalMs, maxBuckets)
          : new MultiColumnAggregationResult(requests);

      final double[] rowValues = new double[reqCount];

      for (final TimeSeriesShard shard : shards) {
        // Hold the compaction read lock for sealed+mutable reads to prevent data loss
        // if compaction completes between reading the two layers.
        shard.getCompactionLock().readLock().lock();
        try {
          shard.getSealedStore().aggregateMultiBlocks(fromTs, toTs, requests, bucketIntervalMs, result, metrics, tagFilter);

          final Iterator<Object[]> mutableIter = shard.getMutableBucket().iterateRange(fromTs, toTs, null);
          while (mutableIter.hasNext()) {
            final Object[] row = mutableIter.next();
            if (tagFilter != null && !tagFilter.matches(row))
              continue;
            final long ts = (long) row[0];
            final long bucketTs = bucketIntervalMs > 0 ? (ts / bucketIntervalMs) * bucketIntervalMs : fromTs;

            for (int r = 0; r < reqCount; r++) {
              if (isCount[r])
                rowValues[r] = 1.0;
              else if (columnIndices[r] < row.length && row[columnIndices[r]] instanceof Number n)
                rowValues[r] = n.doubleValue();
              else
                rowValues[r] = 0.0;
            }

            result.accumulateRow(bucketTs, rowValues);
          }
        } finally {
          shard.getCompactionLock().readLock().unlock();
        }
      }

      result.finalizeAvg();
      return result;
    }
  }

  /**
   * Triggers compaction on all shards.
   */
  public void compactAll() throws IOException {
    for (final TimeSeriesShard shard : shards)
      shard.compact();
  }

  /**
   * Applies retention policy: removes sealed blocks older than the given timestamp.
   * Note: this only truncates sealed stores. To ensure mutable bucket data is also
   * covered, call {@link #compactAll()} before this method.
   */
  public void applyRetention(final long cutoffTimestamp) throws IOException {
    for (final TimeSeriesShard shard : shards)
      shard.getSealedStore().truncateBefore(cutoffTimestamp);
  }

  /**
   * Applies downsampling tiers to sealed data. For each tier (sorted by afterMs ascending),
   * blocks older than (nowMs - tier.afterMs) are reduced to tier.granularityMs resolution
   * by averaging numeric fields per time bucket. Tag columns are preserved as group keys.
   * The density check provides idempotency: blocks already at or coarser than the target
   * resolution are left untouched.
   */
  public void applyDownsampling(final List<DownsamplingTier> tiers, final long nowMs) throws IOException {
    if (tiers == null || tiers.isEmpty())
      return;

    // Identify column roles
    final int tsColIdx = findTimestampColumnIndex();
    final List<Integer> tagColIndices = new ArrayList<>();
    final List<Integer> numericColIndices = new ArrayList<>();
    for (int c = 0; c < columns.size(); c++) {
      if (c == tsColIdx)
        continue;
      if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TAG)
        tagColIndices.add(c);
      else
        numericColIndices.add(c);
    }

    for (final DownsamplingTier tier : tiers) {
      final long cutoffTs = nowMs - tier.afterMs();
      for (final TimeSeriesShard shard : shards)
        shard.getSealedStore().downsampleBlocks(cutoffTs, tier.granularityMs(), tsColIdx, tagColIndices, numericColIndices);
    }
  }

  private int findTimestampColumnIndex() {
    for (int i = 0; i < columns.size(); i++)
      if (columns.get(i).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        return i;
    return 0;
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
    shardExecutor.shutdown();
    try {
      if (!shardExecutor.awaitTermination(30, TimeUnit.SECONDS))
        shardExecutor.shutdownNow();
    } catch (final InterruptedException e) {
      shardExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
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
