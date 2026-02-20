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
import java.util.List;

/**
 * Holds time-bucketed aggregation results.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class AggregationResult {

  private final List<Long>   bucketTimestamps = new ArrayList<>();
  private final List<Double> values           = new ArrayList<>();
  private final List<Long>   counts           = new ArrayList<>();

  public void addBucket(final long timestamp, final double value, final long count) {
    bucketTimestamps.add(timestamp);
    values.add(value);
    counts.add(count);
  }

  public int size() {
    return bucketTimestamps.size();
  }

  public long getBucketTimestamp(final int index) {
    return bucketTimestamps.get(index);
  }

  public double getValue(final int index) {
    return values.get(index);
  }

  public long getCount(final int index) {
    return counts.get(index);
  }

  /**
   * Merges another result into this one. Used for combining partial results from multiple shards.
   * Assumes both results have matching bucket timestamps.
   */
  public void merge(final AggregationResult other, final AggregationType type) {
    if (bucketTimestamps.isEmpty()) {
      bucketTimestamps.addAll(other.bucketTimestamps);
      values.addAll(other.values);
      counts.addAll(other.counts);
      return;
    }

    for (int i = 0; i < other.size(); i++) {
      final long otherTs = other.getBucketTimestamp(i);
      final int idx = findBucket(otherTs);
      if (idx >= 0) {
        final double merged = mergeValue(values.get(idx), counts.get(idx), other.getValue(i), other.getCount(i), type);
        values.set(idx, merged);
        counts.set(idx, counts.get(idx) + other.getCount(i));
      } else {
        bucketTimestamps.add(otherTs);
        values.add(other.getValue(i));
        counts.add(other.getCount(i));
      }
    }
  }

  private int findBucket(final long timestamp) {
    for (int i = 0; i < bucketTimestamps.size(); i++)
      if (bucketTimestamps.get(i) == timestamp)
        return i;
    return -1;
  }

  private static double mergeValue(final double v1, final long c1, final double v2, final long c2,
      final AggregationType type) {
    return switch (type) {
      case SUM, COUNT -> v1 + v2;
      case AVG -> (v1 * c1 + v2 * c2) / (c1 + c2);
      case MIN -> Math.min(v1, v2);
      case MAX -> Math.max(v1, v2);
    };
  }
}
