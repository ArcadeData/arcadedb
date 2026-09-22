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
import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.engine.timeseries.MultiColumnAggregationRequest;
import com.arcadedb.engine.timeseries.MultiColumnAggregationResult;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesNaN;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.utility.DateUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
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
          // Gated accessor: the push-down aggregation reads the very same samples a plain scan would, so it has
          // to apply the same per-type read check (a TimeSeries type owns no bucket to gate by file id).
          final TimeSeriesEngine engine = tsType.getEngine(SecurityDatabaseUser.ACCESS.READ_RECORD);
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
              // Issue #4385: expose the bucket timestamp as a LocalDateTime (the engine's standard
              // DATETIME representation) rather than a java.util.Date, so the result JSON serializer
              // keeps the full date-time instead of truncating to the day.
              row.setProperty(timeBucketAlias,
                  DateUtils.dateTime(context.getDatabase(), bucketTs, ChronoUnit.MILLIS, LocalDateTime.class, ChronoUnit.MILLIS));
              for (int i = 0; i < requests.size(); i++) {
                final MultiColumnAggregationRequest req = requests.get(i);
                final String outputAlias = requestAliasToOutputAlias.getOrDefault(req.alias(), req.alias());
                final double value = aggResult.getValue(bucketTs, i);
                // The absent marker becomes SQL NULL at the SQL boundary, exactly as it does on the row path
                // (FetchFromTimeSeriesStep, issue #7743). The push-down and the generic aggregation answer the
                // same query, so they cannot spell "this bucket measured nothing" differently: the generic path
                // has always answered NULL for an AVG or MIN with nothing to average, and a client reading NaN
                // from one plan and NULL from the other is reading the PLAN, not the data.
                //
                // The JSON boundary is unaffected: JSONObject.put(String, Object) already routed the NaN to its
                // NaN-aware overload and wrote null (issue #7584). This makes the embedded SQL caller see the
                // same thing the HTTP one always did, without depending on that coupling.
                //
                // The COUNT of real contributors is what separates the two NaNs, and the accumulator keys on it
                // for the same reason (TimeSeriesNaN.sum): a SUM over +Infinity and -Infinity is NaN with real
                // samples behind it - an undefined TOTAL, which IEEE keeps and so do we - while an absent bucket
                // is NaN with nothing behind it. Only the second is NULL (CodeRabbit on PR #7747).
                row.setProperty(outputAlias,
                    TimeSeriesNaN.isAbsent(value) && aggResult.getCount(bucketTs, i) == 0 ? null : value);
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
        cost += System.nanoTime() - begin;
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
      sb.append(req.type().name().toLowerCase(Locale.ROOT));
      // COUNT's request names no column - it counts rows on both halves of the push-down - so it prints as the
      // query spelled it rather than as the placeholder index it carries (issue #8140). Every other request
      // prints the ENGINE ROW position it aggregates, which is what the record holds.
      if (req.type() == AggregationType.COUNT)
        sb.append("(*)");
      else
        sb.append("(col").append(req.columnIndex()).append(")");
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
