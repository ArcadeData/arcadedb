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

import com.arcadedb.engine.timeseries.AggregationMetrics;
import com.arcadedb.engine.timeseries.MultiColumnAggregationRequest;
import com.arcadedb.engine.timeseries.MultiColumnAggregationResult;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.LocalTimeSeriesType;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Push-down execution step that performs aggregation directly in the TimeSeries engine.
 * Replaces the combination of FetchFromTimeSeriesStep + ProjectionCalculationStep + AggregateProjectionCalculationStep
 * for eligible queries with ts.timeBucket GROUP BY and simple aggregate functions.
 */
public class AggregateFromTimeSeriesStep extends AbstractExecutionStep {

  private final LocalTimeSeriesType                tsType;
  private final long                               fromTs;
  private final long                               toTs;
  private final List<MultiColumnAggregationRequest> requests;
  private final long                               bucketIntervalMs;
  private final String                             timeBucketAlias;
  private final Map<String, String>                requestAliasToOutputAlias;
  private final TagFilter                          tagFilter;
  private       Iterator<ResultInternal>           resultIterator;
  private       boolean                            fetched = false;
  private       AggregationMetrics                 aggregationMetrics;

  public AggregateFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final List<MultiColumnAggregationRequest> requests, final long bucketIntervalMs, final String timeBucketAlias,
      final Map<String, String> requestAliasToOutputAlias, final CommandContext context) {
    this(tsType, fromTs, toTs, requests, bucketIntervalMs, timeBucketAlias, requestAliasToOutputAlias, null, context);
  }

  public AggregateFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final List<MultiColumnAggregationRequest> requests, final long bucketIntervalMs, final String timeBucketAlias,
      final Map<String, String> requestAliasToOutputAlias, final TagFilter tagFilter, final CommandContext context) {
    super(context);
    this.tsType = tsType;
    this.fromTs = fromTs;
    this.toTs = toTs;
    this.requests = requests;
    this.bucketIntervalMs = bucketIntervalMs;
    this.timeBucketAlias = timeBucketAlias;
    this.requestAliasToOutputAlias = requestAliasToOutputAlias;
    this.tagFilter = tagFilter;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      if (!fetched) {
        try {
          final TimeSeriesEngine engine = tsType.getEngine();
          if (engine == null)
            throw new CommandExecutionException(
                "TimeSeries engine for type '" + tsType.getName() + "' is not initialized");
          if (context.isProfiling())
            aggregationMetrics = new AggregationMetrics();
          final MultiColumnAggregationResult aggResult = engine.aggregateMulti(fromTs, toTs, requests, bucketIntervalMs, tagFilter, aggregationMetrics);

          // Lazy conversion: wrap the bucket timestamp iterator instead of materializing all rows
          final Iterator<Long> bucketIterator = aggResult.getBucketTimestamps().iterator();
          resultIterator = new Iterator<>() {
            @Override
            public boolean hasNext() {
              return bucketIterator.hasNext();
            }

            @Override
            public ResultInternal next() {
              final long bucketTs = bucketIterator.next();
              final ResultInternal row = new ResultInternal(context.getDatabase());
              row.setProperty(timeBucketAlias, new Date(bucketTs));
              for (int i = 0; i < requests.size(); i++) {
                final MultiColumnAggregationRequest req = requests.get(i);
                final String outputAlias = requestAliasToOutputAlias.getOrDefault(req.alias(), req.alias());
                row.setProperty(outputAlias, aggResult.getValue(bucketTs, i));
              }
              rowCount++;
              return row;
            }
          };
          fetched = true;
        } catch (final CommandExecutionException e) {
          throw e;
        } catch (final IOException e) {
          throw new CommandExecutionException("Error in TimeSeries push-down aggregation", e);
        }
      }

      return new ResultSet() {
        private int count = 0;

        @Override
        public boolean hasNext() {
          return count < nRecords && resultIterator.hasNext();
        }

        @Override
        public Result next() {
          if (!hasNext())
            throw new IllegalStateException("No more results");
          count++;
          return resultIterator.next();
        }

        @Override
        public void close() {
          // no-op
        }
      };
    } finally {
      if (context.isProfiling())
        cost += (System.nanoTime() - begin);
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder sb = new StringBuilder();
    sb.append(spaces).append("+ AGGREGATE FROM TIMESERIES ").append(tsType.getName());
    sb.append(" [").append(fromTs).append(" - ").append(toTs).append("] bucket=").append(bucketIntervalMs).append("ms");
    sb.append("\n").append(spaces).append("    ");
    for (int i = 0; i < requests.size(); i++) {
      if (i > 0)
        sb.append(", ");
      final MultiColumnAggregationRequest req = requests.get(i);
      sb.append(req.type().name().toLowerCase()).append("(col").append(req.columnIndex()).append(")");
    }
    if (context.isProfiling()) {
      sb.append("\n").append(spaces).append("    (").append(getCostFormatted()).append(", ").append(getRowCountFormatted()).append(")");
      if (aggregationMetrics != null)
        sb.append("\n").append(spaces).append("    ").append(aggregationMetrics);
    }
    return sb.toString();
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new AggregateFromTimeSeriesStep(tsType, fromTs, toTs, requests, bucketIntervalMs, timeBucketAlias,
        requestAliasToOutputAlias, tagFilter, context);
  }
}
