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
package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.ContinuousAggregate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class FetchFromSchemaContinuousAggregatesStep extends AbstractExecutionStep {

  private final List<ResultInternal> result = new ArrayList<>();

  private int cursor = 0;

  public FetchFromSchemaContinuousAggregatesStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (cursor == 0) {
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        final ContinuousAggregate[] aggregates = context.getDatabase().getSchema().getContinuousAggregates();

        final List<ContinuousAggregate> ordered = Arrays.stream(aggregates)
            .sorted(Comparator.comparing(ContinuousAggregate::getName, String::compareToIgnoreCase))
            .collect(Collectors.toList());

        for (final ContinuousAggregate ca : ordered) {
          final ResultInternal r = new ResultInternal(context.getDatabase());
          result.add(r);

          r.setProperty("name", ca.getName());
          r.setProperty("query", ca.getQuery());
          r.setProperty("backingType", ca.getBackingType().getName());
          r.setProperty("sourceType", ca.getSourceTypeName());
          r.setProperty("bucketIntervalMs", ca.getBucketIntervalMs());
          r.setProperty("bucketColumn", ca.getBucketColumn());
          r.setProperty("timestampColumn", ca.getTimestampColumn());
          r.setProperty("watermarkTs", ca.getWatermarkTs());
          r.setProperty("lastRefreshTime", ca.getLastRefreshTime());
          r.setProperty("status", ca.getStatus());

          // Runtime metrics
          r.setProperty("refreshCount", ca.getRefreshCount());
          r.setProperty("refreshTotalTimeMs", ca.getRefreshTotalTimeMs());
          r.setProperty("refreshMinTimeMs", ca.getRefreshMinTimeMs());
          r.setProperty("refreshMaxTimeMs", ca.getRefreshMaxTimeMs());
          final long count = ca.getRefreshCount();
          r.setProperty("refreshAvgTimeMs", count > 0 ? ca.getRefreshTotalTimeMs() / count : 0L);
          r.setProperty("errorCount", ca.getErrorCount());
          r.setProperty("lastRefreshDurationMs", ca.getLastRefreshDurationMs());

          context.setVariable("current", r);
        }
      } finally {
        if (context.isProfiling())
          cost += (System.nanoTime() - begin);
      }
    }
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return cursor < result.size();
      }

      @Override
      public Result next() {
        return result.get(cursor++);
      }

      @Override
      public void close() {
      }

      @Override
      public void reset() {
        cursor = 0;
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA CONTINUOUS AGGREGATES";
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }
}
