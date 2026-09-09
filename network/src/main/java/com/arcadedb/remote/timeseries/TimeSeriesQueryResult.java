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

import java.util.List;

/**
 * The answer to a {@link TimeSeriesQuery} (issue #7305). A raw query fills {@link #columns()} and
 * {@link #rows()}; an aggregated one fills {@link #aggregations()} and {@link #buckets()}. The other pair is
 * empty, which {@link #isAggregated()} reports without the caller having to guess.
 * <p>
 * A value that stands for "no measurement" - an absent MIN/MAX, a non-finite sample - is {@code null} in a row
 * or bucket, on both protocols: JSON {@code null} over HTTP and an unset {@code GrpcValue} over gRPC.
 *
 * @param type         the queried type
 * @param columns      names of the values in each row, in order; empty for an aggregated answer
 * @param rows         raw sample rows; empty for an aggregated answer
 * @param aggregations aliases of the values in each bucket, in order; empty for a raw answer
 * @param buckets      aggregation buckets; empty for a raw answer
 * @param truncated    whether a limit cut the answer short, so more data exists beyond what is here
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record TimeSeriesQueryResult(String type, List<String> columns, List<Object[]> rows,
                                    List<String> aggregations, List<TimeSeriesBucket> buckets, boolean truncated) {

  public TimeSeriesQueryResult {
    columns = columns == null ? List.of() : List.copyOf(columns);
    rows = rows == null ? List.of() : List.copyOf(rows);
    aggregations = aggregations == null ? List.of() : List.copyOf(aggregations);
    buckets = buckets == null ? List.of() : List.copyOf(buckets);
  }

  public boolean isAggregated() {
    return !aggregations.isEmpty();
  }

  /** Rows for a raw answer, buckets for an aggregated one. */
  public int count() {
    return isAggregated() ? buckets.size() : rows.size();
  }

  /** The index of {@code columnName} among the values of each row, or {@code -1} when it is not projected. */
  public int columnIndex(final String columnName) {
    return columns.indexOf(columnName);
  }
}
