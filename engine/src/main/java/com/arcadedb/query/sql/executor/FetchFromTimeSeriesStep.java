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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromTimeSeriesStep extends AbstractExecutionStep {

  private final LocalTimeSeriesType tsType;
  private final long                fromTs;
  private final long                toTs;
  private       Iterator<Object[]>  resultIterator;
  private       boolean             fetched = false;

  public FetchFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final CommandContext context) {
    super(context);
    this.tsType = tsType;
    this.fromTs = fromTs;
    this.toTs = toTs;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (!fetched) {
      try {
        final TimeSeriesEngine engine = tsType.getEngine();
        final List<Object[]> rows = engine.query(fromTs, toTs, null, null);
        resultIterator = rows.iterator();
        fetched = true;
      } catch (final IOException e) {
        throw new CommandExecutionException("Error querying TimeSeries engine", e);
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
        final Object[] row = resultIterator.next();
        final ResultInternal result = new ResultInternal(context.getDatabase());

        final List<ColumnDefinition> columns = tsType.getTsColumns();
        for (int i = 0; i < columns.size() && i < row.length; i++) {
          final ColumnDefinition col = columns.get(i);
          Object value = row[i];

          // Convert timestamp long to Date for SQL compatibility
          if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP && value instanceof Long)
            value = new Date((Long) value);

          result.setProperty(col.getName(), value);
        }

        return result;
      }

      @Override
      public void close() {
        // no-op
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ FETCH FROM TIMESERIES " + tsType.getName() + " [" + fromTs + " - " + toTs + "]";
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new FetchFromTimeSeriesStep(tsType, fromTs, toTs, context);
  }
}
