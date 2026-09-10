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
package com.arcadedb.remote.timeseries;

import java.util.Arrays;

/**
 * One fixed-interval bucket of an aggregated time-series answer (issue #7305). {@code values} carries one
 * entry per requested aggregate, in the order the aggregates were requested and named by
 * {@link TimeSeriesQueryResult#aggregations()}; a bucket in which an aggregate had nothing to measure carries
 * {@code null} there rather than zero.
 *
 * @param timestampMs start of the bucket, epoch milliseconds
 * @param values      the aggregate values, one per requested aggregation
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record TimeSeriesBucket(long timestampMs, Object[] values) {

  public TimeSeriesBucket {
    values = values == null ? new Object[0] : values.clone();
  }

  @Override
  public Object[] values() {
    // Defensive copy on the way out too: a record's accessor hands back the field itself, and an array field
    // would otherwise let a caller mutate a value another caller is still reading.
    return values.clone();
  }

  /** The aggregate at {@code index}, or {@code null} when that aggregate had nothing to measure here. */
  public Object value(final int index) {
    return values[index];
  }

  @Override
  public String toString() {
    return "TimeSeriesBucket[" + timestampMs + ", " + Arrays.toString(values) + "]";
  }
}
