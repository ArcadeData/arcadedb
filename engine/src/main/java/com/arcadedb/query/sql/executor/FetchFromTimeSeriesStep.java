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

import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.LocalTimeSeriesType;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Execution step that fetches data from a TimeSeries engine.
 * Supports profiling via the standard {@code context.isProfiling()} mechanism.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromTimeSeriesStep extends AbstractExecutionStep {

  private final LocalTimeSeriesType tsType;
  private final long                fromTs;
  private final long                toTs;
  private final TagFilter           tagFilter;
  private       Iterator<Object[]>  resultIterator;
  private       boolean             fetched = false;

  public FetchFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final CommandContext context) {
    this(tsType, fromTs, toTs, null, context);
  }

  public FetchFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final TagFilter tagFilter, final CommandContext context) {
    super(context);
    this.tsType = tsType;
    this.fromTs = fromTs;
    this.toTs = toTs;
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
          resultIterator = engine.iterateQuery(fromTs, toTs, null, tagFilter);
          fetched = true;
        } catch (final CommandExecutionException e) {
          throw e;
        } catch (final IOException e) {
          throw new CommandExecutionException("Error querying TimeSeries engine", e);
        }
      }

      final List<ColumnDefinition> columns = tsType.getTsColumns();

      return new ResultSet() {
        private int count = 0;

        @Override
        public boolean hasNext() {
          final long begin1 = context.isProfiling() ? System.nanoTime() : 0;
          try {
            return count < nRecords && resultIterator.hasNext();
          } finally {
            if (context.isProfiling())
              cost += (System.nanoTime() - begin1);
          }
        }

        @Override
        public Result next() {
          final long begin1 = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (!hasNext())
              throw new IllegalStateException("No more results");

            count++;
            final Object[] row = resultIterator.next();
            final ResultInternal result = new ResultInternal(context.getDatabase());

            for (int i = 0; i < columns.size() && i < row.length; i++) {
              final ColumnDefinition col = columns.get(i);
              Object value = row[i];

              // Convert timestamp long to Date for SQL compatibility
              if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP && value instanceof Long)
                value = new Date((Long) value);

              result.setProperty(col.getName(), value);
            }

            rowCount++;
            return result;
          } finally {
            if (context.isProfiling())
              cost += (System.nanoTime() - begin1);
          }
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
    sb.append(spaces).append("+ FETCH FROM TIMESERIES ").append(tsType.getName());
    sb.append(" [").append(fromTs).append(" - ").append(toTs).append("]");
    if (context.isProfiling())
      sb.append(" (").append(getCostFormatted()).append(", ").append(getRowCountFormatted()).append(")");
    return sb.toString();
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new FetchFromTimeSeriesStep(tsType, fromTs, toTs, tagFilter, context);
  }
}
