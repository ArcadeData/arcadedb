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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * A multi-column aggregation grouped by the values of one or more TAG columns and, within each group, bucketed by time
 * (issue #9489): one {@link MultiColumnAggregationResult} per distinct tag combination.
 * <p>
 * Every group shares the bucket window of the query, so a group's result is exactly what
 * {@link TimeSeriesEngine#aggregateMulti} would have answered for the samples of that combination alone, and two
 * partial results of the same query (one per shard) can be merged group by group.
 * <p>
 * A group's buckets are pre-allocated as a flat array only when the window is small: that array is paid for per group,
 * and a query grouping a thousand series over a year of one-minute buckets would otherwise allocate gigabytes of
 * empty slots to answer a handful of rows per series.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GroupedAggregationResult {
  /** The largest bucket window a group still gets as a flat array. */
  static final int MAX_FLAT_WINDOW_PER_GROUP = 8_192;

  private final List<MultiColumnAggregationRequest>   requests;
  private final long                                  firstBucket;
  private final long                                  bucketIntervalMs;
  private final int                                   window;
  private final Map<GroupKey, MultiColumnAggregationResult> groups = new LinkedHashMap<>();
  private       int                                   bucketCeiling;

  /**
   * @param window the number of buckets of the query window, {@code 0} when it is unknown or the query is not bucketed
   *               by time; a window too wide for a flat array per group makes every group a map-mode result
   */
  GroupedAggregationResult(final List<MultiColumnAggregationRequest> requests, final long firstBucket, final long bucketIntervalMs,
      final int window) {
    this.requests = requests;
    this.firstBucket = firstBucket;
    this.bucketIntervalMs = bucketIntervalMs;
    this.window = window > 0 && window <= MAX_FLAT_WINDOW_PER_GROUP ? window : 0;
  }

  /** The tag values of one group, in the order the query listed the grouping columns. */
  private record GroupKey(String[] values) {
    @Override
    public boolean equals(final Object o) {
      return o instanceof GroupKey other && Arrays.equals(values, other.values);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }
  }

  /**
   * The result of the group with these tag values, created on first use. {@code values} is kept as the group's identity,
   * so the caller must not reuse the array afterwards.
   */
  MultiColumnAggregationResult groupFor(final String[] values) {
    final GroupKey key = new GroupKey(values);
    MultiColumnAggregationResult result = groups.get(key);
    if (result == null) {
      result = window > 0 ? new MultiColumnAggregationResult(requests, firstBucket, bucketIntervalMs, window)
          : new MultiColumnAggregationResult(requests);
      groups.put(key, result);
    }
    return result;
  }

  /** How many distinct tag combinations were seen. */
  public int getGroupCount() {
    return groups.size();
  }

  /**
   * The buckets of every group together, which is the number of rows the answer has.
   */
  public int getUsedBucketCount() {
    int total = 0;
    for (final MultiColumnAggregationResult group : groups.values())
      total += group.getUsedBucketCount();
    return total;
  }

  /** See {@link MultiColumnAggregationResult#setBucketCeiling(int)}: the ceiling bounds the rows of the whole answer. */
  void setBucketCeiling(final int bucketCeiling) {
    this.bucketCeiling = bucketCeiling;
  }

  boolean isOverBucketCeiling() {
    return bucketCeiling > 0 && getUsedBucketCount() > bucketCeiling;
  }

  void finalizeAvg() {
    for (final MultiColumnAggregationResult group : groups.values())
      group.finalizeAvg();
  }

  /** Folds another partial result of the same query in, group by group. */
  void mergeFrom(final GroupedAggregationResult other) {
    for (final Map.Entry<GroupKey, MultiColumnAggregationResult> entry : other.groups.entrySet()) {
      final MultiColumnAggregationResult mine = groups.get(entry.getKey());
      if (mine == null)
        groups.put(entry.getKey(), entry.getValue());
      else
        mine.mergeFrom(entry.getValue());
    }
  }

  /**
   * Visits the groups in the order they were first seen. {@code tagValues} is the group's tag combination as stored (the
   * text of each value), {@code buckets} its time-bucketed aggregates.
   */
  public void forEachGroup(final BiConsumer<String[], MultiColumnAggregationResult> visitor) {
    for (final Map.Entry<GroupKey, MultiColumnAggregationResult> entry : groups.entrySet())
      visitor.accept(entry.getKey().values(), entry.getValue());
  }
}
