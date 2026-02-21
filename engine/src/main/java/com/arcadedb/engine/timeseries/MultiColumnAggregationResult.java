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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds multi-column aggregation results bucketed by timestamp.
 * Each bucket maps alias -> AccumulatorEntry which tracks value, count, and aggregation type.
 */
public final class MultiColumnAggregationResult {

  private final LinkedHashMap<Long, Map<String, AccumulatorEntry>> buckets = new LinkedHashMap<>();

  /**
   * Accumulates a value into the given bucket for the given alias.
   */
  public void accumulate(final long bucketTs, final String alias, final double value, final AggregationType type) {
    final Map<String, AccumulatorEntry> bucket = buckets.computeIfAbsent(bucketTs, k -> new LinkedHashMap<>());
    final AccumulatorEntry entry = bucket.get(alias);
    if (entry == null) {
      bucket.put(alias, new AccumulatorEntry(type == AggregationType.COUNT ? 1.0 : value, 1, type));
    } else {
      entry.accumulate(value);
    }
  }

  /**
   * Finalizes AVG accumulators by dividing accumulated sums by their counts.
   */
  public void finalizeAvg() {
    for (final Map<String, AccumulatorEntry> bucket : buckets.values())
      for (final AccumulatorEntry entry : bucket.values())
        if (entry.type == AggregationType.AVG)
          entry.value = entry.value / entry.count;
  }

  /**
   * Returns bucket timestamps in insertion order.
   */
  public List<Long> getBucketTimestamps() {
    return List.copyOf(buckets.keySet());
  }

  public double getValue(final long bucketTs, final String alias) {
    final Map<String, AccumulatorEntry> bucket = buckets.get(bucketTs);
    if (bucket == null)
      return 0.0;
    final AccumulatorEntry entry = bucket.get(alias);
    return entry != null ? entry.value : 0.0;
  }

  public long getCount(final long bucketTs, final String alias) {
    final Map<String, AccumulatorEntry> bucket = buckets.get(bucketTs);
    if (bucket == null)
      return 0;
    final AccumulatorEntry entry = bucket.get(alias);
    return entry != null ? entry.count : 0;
  }

  public int size() {
    return buckets.size();
  }

  static final class AccumulatorEntry {
    double              value;
    long                count;
    final AggregationType type;

    AccumulatorEntry(final double value, final long count, final AggregationType type) {
      this.value = value;
      this.count = count;
      this.type = type;
    }

    void accumulate(final double newValue) {
      switch (type) {
      case SUM:
        value += newValue;
        break;
      case COUNT:
        value += 1;
        break;
      case AVG:
        value += newValue; // accumulate sum, finalize later
        break;
      case MIN:
        value = Math.min(value, newValue);
        break;
      case MAX:
        value = Math.max(value, newValue);
        break;
      }
      count++;
    }
  }
}
