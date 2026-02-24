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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds multi-column aggregation results bucketed by timestamp.
 * Supports two modes:
 * <ul>
 *   <li><b>Flat mode</b>: pre-allocated arrays indexed by {@code (bucketTs - firstBucketTs) / bucketIntervalMs}.
 *       Zero HashMap overhead per sample. Used when bucket interval and data range are known.</li>
 *   <li><b>Map mode</b>: HashMap-based fallback for unknown ranges or zero-interval queries.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class MultiColumnAggregationResult {

  /** Maximum number of buckets allowed in flat mode before falling back to map mode. */
  static final int MAX_FLAT_BUCKETS = 10_000_000;

  private final int               requestCount;
  private final AggregationType[] types;

  // --- Map mode (fallback) ---
  private final Map<Long, double[]> valuesByBucket;
  private final Map<Long, long[]>   countsByBucket;
  private final List<Long>          orderedBuckets;

  // --- Flat mode ---
  private final boolean   flatMode;
  private final long      firstBucketTs;
  private final long      bucketIntervalMs;
  private final int       maxBuckets;
  private       double[][] flatValues;   // [bucketIdx][requestIdx]
  private       long[][]   flatCounts;   // [bucketIdx][requestIdx]
  private       boolean[]  bucketUsed;   // whether this bucket has been touched
  private       List<Long> cachedBucketTimestamps; // cached result for flat mode

  /**
   * Map-mode constructor (original behavior).
   */
  public MultiColumnAggregationResult(final List<MultiColumnAggregationRequest> requests) {
    this.requestCount = requests.size();
    this.types = new AggregationType[requestCount];
    for (int i = 0; i < requestCount; i++)
      types[i] = requests.get(i).type();
    this.valuesByBucket = new HashMap<>();
    this.countsByBucket = new HashMap<>();
    this.orderedBuckets = new ArrayList<>();
    this.flatMode = false;
    this.firstBucketTs = 0;
    this.bucketIntervalMs = 0;
    this.maxBuckets = 0;
  }

  /**
   * Flat-mode constructor. Pre-allocates arrays for direct-index access.
   * If {@code maxBuckets} exceeds {@link #MAX_FLAT_BUCKETS}, falls back to map mode
   * to avoid excessive memory allocation.
   *
   * @param requests         aggregation request definitions
   * @param firstBucketTs    timestamp of the first bucket (aligned to interval)
   * @param bucketIntervalMs bucket width in ms (must be > 0)
   * @param maxBuckets       number of buckets to pre-allocate
   */
  public MultiColumnAggregationResult(final List<MultiColumnAggregationRequest> requests,
      final long firstBucketTs, final long bucketIntervalMs, final int maxBuckets) {
    this.requestCount = requests.size();
    this.types = new AggregationType[requestCount];
    for (int i = 0; i < requestCount; i++)
      types[i] = requests.get(i).type();

    if (maxBuckets > MAX_FLAT_BUCKETS) {
      // Fall back to map mode to avoid OOM
      this.flatMode = false;
      this.firstBucketTs = 0;
      this.bucketIntervalMs = 0;
      this.maxBuckets = 0;
      this.valuesByBucket = new HashMap<>();
      this.countsByBucket = new HashMap<>();
      this.orderedBuckets = new ArrayList<>();
    } else {
      this.flatMode = true;
      this.firstBucketTs = firstBucketTs;
      this.bucketIntervalMs = bucketIntervalMs;
      this.maxBuckets = maxBuckets;
      this.flatValues = new double[maxBuckets][];
      this.flatCounts = new long[maxBuckets][];
      this.bucketUsed = new boolean[maxBuckets];
      this.valuesByBucket = null;
      this.countsByBucket = null;
      this.orderedBuckets = null;
    }
  }

  /**
   * Returns whether this result uses flat array mode.
   */
  public boolean isFlatMode() {
    return flatMode;
  }

  // ---- Accumulation methods ----

  /**
   * Accumulates a value for request at the given index into the given bucket.
   */
  public void accumulate(final long bucketTs, final int requestIndex, final double value) {
    if (flatMode) {
      final int idx = flatIndex(bucketTs);
      if (!ensureFlatBucket(idx))
        return;
      accumulateInPlace(flatValues[idx], flatCounts[idx], requestIndex, value);
    } else {
      double[] vals = valuesByBucket.get(bucketTs);
      if (vals == null) {
        vals = newInitializedValues();
        final long[] counts = new long[requestCount];
        valuesByBucket.put(bucketTs, vals);
        countsByBucket.put(bucketTs, counts);
        orderedBuckets.add(bucketTs);
      }
      accumulateInPlace(vals, countsByBucket.get(bucketTs), requestIndex, value);
    }
  }

  /**
   * Batch accumulate for all requests in a single row.
   */
  public void accumulateRow(final long bucketTs, final double[] values) {
    if (flatMode) {
      final int idx = flatIndex(bucketTs);
      if (!ensureFlatBucket(idx))
        return;
      final double[] vals = flatValues[idx];
      final long[] counts = flatCounts[idx];
      for (int i = 0; i < requestCount; i++)
        accumulateInPlace(vals, counts, i, values[i]);
    } else {
      double[] vals = valuesByBucket.get(bucketTs);
      long[] counts;
      if (vals == null) {
        vals = newInitializedValues();
        counts = new long[requestCount];
        valuesByBucket.put(bucketTs, vals);
        countsByBucket.put(bucketTs, counts);
        orderedBuckets.add(bucketTs);
      } else {
        counts = countsByBucket.get(bucketTs);
      }
      for (int i = 0; i < requestCount; i++)
        accumulateInPlace(vals, counts, i, values[i]);
    }
  }

  /**
   * Accumulates block-level statistics for all requests in a single call.
   */
  public void accumulateBlockStats(final long bucketTs, final double[] values, final int sampleCount) {
    if (flatMode) {
      final int idx = flatIndex(bucketTs);
      if (!ensureFlatBucket(idx))
        return;
      accumulateBlockStatsInPlace(flatValues[idx], flatCounts[idx], values, sampleCount);
    } else {
      double[] vals = valuesByBucket.get(bucketTs);
      long[] counts;
      if (vals == null) {
        vals = newInitializedValues();
        counts = new long[requestCount];
        valuesByBucket.put(bucketTs, vals);
        countsByBucket.put(bucketTs, counts);
        orderedBuckets.add(bucketTs);
      } else {
        counts = countsByBucket.get(bucketTs);
      }
      accumulateBlockStatsInPlace(vals, counts, values, sampleCount);
    }
  }

  /**
   * Accumulates a single statistic result for one request at the given bucket.
   * Used by vectorized (SIMD) segment accumulation where each aggregation type
   * is computed separately per segment.
   *
   * @param bucketTs     aligned bucket timestamp
   * @param requestIndex which aggregation request this applies to
   * @param value        the aggregated value (sum, min, max, or count)
   * @param count        number of samples that produced this value
   */
  public void accumulateSingleStat(final long bucketTs, final int requestIndex,
      final double value, final long count) {
    if (flatMode) {
      final int idx = flatIndex(bucketTs);
      if (!ensureFlatBucket(idx))
        return;
      accumulateStatInPlace(flatValues[idx], flatCounts[idx], requestIndex, value, count);
    } else {
      double[] vals = valuesByBucket.get(bucketTs);
      long[] counts;
      if (vals == null) {
        vals = newInitializedValues();
        counts = new long[requestCount];
        valuesByBucket.put(bucketTs, vals);
        countsByBucket.put(bucketTs, counts);
        orderedBuckets.add(bucketTs);
      } else {
        counts = countsByBucket.get(bucketTs);
      }
      accumulateStatInPlace(vals, counts, requestIndex, value, count);
    }
  }

  // ---- Finalize & query ----

  /**
   * Finalizes AVG accumulators by dividing accumulated sums by their counts.
   */
  public void finalizeAvg() {
    if (flatMode) {
      for (int i = 0; i < requestCount; i++) {
        if (types[i] == AggregationType.AVG) {
          for (int b = 0; b < maxBuckets; b++) {
            if (bucketUsed[b] && flatCounts[b][i] > 0)
              flatValues[b][i] = flatValues[b][i] / flatCounts[b][i];
          }
        }
      }
    } else {
      for (int i = 0; i < requestCount; i++) {
        if (types[i] == AggregationType.AVG) {
          for (final Map.Entry<Long, double[]> entry : valuesByBucket.entrySet()) {
            final long[] counts = countsByBucket.get(entry.getKey());
            if (counts[i] > 0)
              entry.getValue()[i] = entry.getValue()[i] / counts[i];
          }
        }
      }
    }
  }

  /**
   * Returns bucket timestamps in order.
   */
  public List<Long> getBucketTimestamps() {
    if (flatMode) {
      if (cachedBucketTimestamps == null) {
        final List<Long> result = new ArrayList<>();
        for (int b = 0; b < maxBuckets; b++)
          if (bucketUsed[b])
            result.add(firstBucketTs + (long) b * bucketIntervalMs);
        cachedBucketTimestamps = result;
      }
      return cachedBucketTimestamps;
    }
    return orderedBuckets;
  }

  public double getValue(final long bucketTs, final int requestIndex) {
    if (flatMode) {
      final int idx = flatIndex(bucketTs);
      if (idx >= 0 && idx < maxBuckets && bucketUsed[idx])
        return flatValues[idx][requestIndex];
      return 0.0;
    }
    final double[] vals = valuesByBucket.get(bucketTs);
    return vals != null ? vals[requestIndex] : 0.0;
  }

  public long getCount(final long bucketTs, final int requestIndex) {
    if (flatMode) {
      final int idx = flatIndex(bucketTs);
      if (idx >= 0 && idx < maxBuckets && bucketUsed[idx])
        return flatCounts[idx][requestIndex];
      return 0;
    }
    final long[] counts = countsByBucket.get(bucketTs);
    return counts != null ? counts[requestIndex] : 0;
  }

  public int size() {
    if (flatMode) {
      int count = 0;
      for (int b = 0; b < maxBuckets; b++)
        if (bucketUsed[b])
          count++;
      return count;
    }
    return valuesByBucket.size();
  }

  /**
   * Merges another result into this one. Both must use flat mode with
   * the same firstBucketTs, bucketIntervalMs, and maxBuckets.
   */
  public void mergeFrom(final MultiColumnAggregationResult other) {
    if (flatMode && other.flatMode) {
      if (firstBucketTs != other.firstBucketTs || bucketIntervalMs != other.bucketIntervalMs || maxBuckets != other.maxBuckets)
        throw new IllegalArgumentException(
            "Cannot merge incompatible flat-mode results: firstBucketTs=" + firstBucketTs + "/" + other.firstBucketTs
                + " bucketIntervalMs=" + bucketIntervalMs + "/" + other.bucketIntervalMs
                + " maxBuckets=" + maxBuckets + "/" + other.maxBuckets);
      for (int b = 0; b < other.maxBuckets; b++) {
        if (!other.bucketUsed[b])
          continue;
        ensureFlatBucket(b);
        final double[] oVals = other.flatValues[b];
        final long[] oCounts = other.flatCounts[b];
        final double[] tVals = flatValues[b];
        final long[] tCounts = flatCounts[b];
        for (int i = 0; i < requestCount; i++) {
          switch (types[i]) {
          case MIN:
            if (oVals[i] < tVals[i])
              tVals[i] = oVals[i];
            break;
          case MAX:
            if (oVals[i] > tVals[i])
              tVals[i] = oVals[i];
            break;
          case SUM:
          case AVG:
          case COUNT:
            tVals[i] += oVals[i];
            break;
          }
          tCounts[i] += oCounts[i];
        }
      }
    } else {
      // Fallback: merge map-mode results
      for (final long bucketTs : other.getBucketTimestamps()) {
        for (int i = 0; i < requestCount; i++) {
          final double oVal = other.getValue(bucketTs, i);
          final long oCount = other.getCount(bucketTs, i);
          accumulateStatInPlaceByTs(bucketTs, i, oVal, oCount);
        }
      }
    }
  }

  // ---- Internal helpers ----

  int getRequestCount() {
    return requestCount;
  }

  AggregationType[] getTypes() {
    return types;
  }

  private int flatIndex(final long bucketTs) {
    final long idx = (bucketTs - firstBucketTs) / bucketIntervalMs;
    if (idx < 0 || idx >= maxBuckets)
      return -1;
    return (int) idx; // safe: idx < maxBuckets which is an int
  }

  private boolean ensureFlatBucket(final int idx) {
    if (idx < 0 || idx >= maxBuckets)
      return false;
    if (!bucketUsed[idx]) {
      bucketUsed[idx] = true;
      flatValues[idx] = newInitializedValues();
      flatCounts[idx] = new long[requestCount];
      cachedBucketTimestamps = null; // invalidate cache
    }
    return true;
  }

  private double[] newInitializedValues() {
    final double[] vals = new double[requestCount];
    for (int i = 0; i < requestCount; i++) {
      switch (types[i]) {
      case MIN:
        vals[i] = Double.MAX_VALUE;
        break;
      case MAX:
        vals[i] = -Double.MAX_VALUE;
        break;
      default:
        vals[i] = 0.0;
        break;
      }
    }
    return vals;
  }

  private void accumulateInPlace(final double[] vals, final long[] counts, final int idx, final double value) {
    switch (types[idx]) {
    case SUM:
    case AVG:
      vals[idx] += value;
      break;
    case COUNT:
      vals[idx] += 1;
      break;
    case MIN:
      if (value < vals[idx])
        vals[idx] = value;
      break;
    case MAX:
      if (value > vals[idx])
        vals[idx] = value;
      break;
    }
    counts[idx]++;
  }

  private void accumulateBlockStatsInPlace(final double[] vals, final long[] counts,
      final double[] values, final int sampleCount) {
    for (int i = 0; i < requestCount; i++) {
      switch (types[i]) {
      case MIN:
        if (values[i] < vals[i])
          vals[i] = values[i];
        break;
      case MAX:
        if (values[i] > vals[i])
          vals[i] = values[i];
        break;
      case SUM:
      case AVG:
        vals[i] += values[i];
        break;
      case COUNT:
        vals[i] += values[i];
        break;
      }
      counts[i] += sampleCount;
    }
  }

  private void accumulateStatInPlace(final double[] vals, final long[] counts,
      final int requestIndex, final double value, final long count) {
    switch (types[requestIndex]) {
    case MIN:
      if (value < vals[requestIndex])
        vals[requestIndex] = value;
      break;
    case MAX:
      if (value > vals[requestIndex])
        vals[requestIndex] = value;
      break;
    case SUM:
    case AVG:
      vals[requestIndex] += value;
      break;
    case COUNT:
      vals[requestIndex] += value;
      break;
    }
    counts[requestIndex] += count;
  }

  private void accumulateStatInPlaceByTs(final long bucketTs, final int requestIndex,
      final double value, final long count) {
    if (flatMode) {
      final int idx = flatIndex(bucketTs);
      if (!ensureFlatBucket(idx))
        return;
      accumulateStatInPlace(flatValues[idx], flatCounts[idx], requestIndex, value, count);
    } else {
      double[] vals = valuesByBucket.get(bucketTs);
      long[] counts;
      if (vals == null) {
        vals = newInitializedValues();
        counts = new long[requestCount];
        valuesByBucket.put(bucketTs, vals);
        countsByBucket.put(bucketTs, counts);
        orderedBuckets.add(bucketTs);
      } else {
        counts = countsByBucket.get(bucketTs);
      }
      accumulateStatInPlace(vals, counts, requestIndex, value, count);
    }
  }
}
