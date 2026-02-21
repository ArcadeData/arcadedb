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
 * Uses flat arrays indexed by request position for minimal per-row overhead.
 * Each bucket stores a double[] (values) and long[] (counts) with one slot per aggregation request.
 */
public final class MultiColumnAggregationResult {

  private final int                          requestCount;
  private final AggregationType[]            types;
  private final Map<Long, double[]>          valuesByBucket = new HashMap<>();
  private final Map<Long, long[]>            countsByBucket = new HashMap<>();
  private final List<Long>                   orderedBuckets = new ArrayList<>();

  public MultiColumnAggregationResult(final List<MultiColumnAggregationRequest> requests) {
    this.requestCount = requests.size();
    this.types = new AggregationType[requestCount];
    for (int i = 0; i < requestCount; i++)
      types[i] = requests.get(i).type();
  }

  /**
   * Accumulates a value for request at the given index into the given bucket.
   * Designed for hot-loop performance: single HashMap lookup per bucket per row.
   */
  public void accumulate(final long bucketTs, final int requestIndex, final double value) {
    double[] vals = valuesByBucket.get(bucketTs);
    if (vals == null) {
      vals = new double[requestCount];
      final long[] counts = new long[requestCount];
      // Initialize MIN to MAX_VALUE, MAX to -MAX_VALUE
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
      valuesByBucket.put(bucketTs, vals);
      countsByBucket.put(bucketTs, counts);
      orderedBuckets.add(bucketTs);
    }
    accumulateInPlace(vals, countsByBucket.get(bucketTs), requestIndex, value);
  }

  /**
   * Batch accumulate for all requests in a single row.
   * Minimizes HashMap lookups: one lookup per row instead of one per request.
   */
  public void accumulateRow(final long bucketTs, final double[] values) {
    double[] vals = valuesByBucket.get(bucketTs);
    long[] counts;
    if (vals == null) {
      vals = new double[requestCount];
      counts = new long[requestCount];
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
      valuesByBucket.put(bucketTs, vals);
      countsByBucket.put(bucketTs, counts);
      orderedBuckets.add(bucketTs);
    } else {
      counts = countsByBucket.get(bucketTs);
    }
    for (int i = 0; i < requestCount; i++)
      accumulateInPlace(vals, counts, i, values[i]);
  }

  /**
   * Finalizes AVG accumulators by dividing accumulated sums by their counts.
   */
  public void finalizeAvg() {
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

  /**
   * Returns bucket timestamps in insertion order.
   */
  public List<Long> getBucketTimestamps() {
    return orderedBuckets;
  }

  public double getValue(final long bucketTs, final int requestIndex) {
    final double[] vals = valuesByBucket.get(bucketTs);
    return vals != null ? vals[requestIndex] : 0.0;
  }

  public long getCount(final long bucketTs, final int requestIndex) {
    final long[] counts = countsByBucket.get(bucketTs);
    return counts != null ? counts[requestIndex] : 0;
  }

  public int size() {
    return valuesByBucket.size();
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
}
