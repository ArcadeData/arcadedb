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
import com.arcadedb.engine.timeseries.FieldFilter;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesGateway;
import com.arcadedb.engine.timeseries.TimeSeriesNaN;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.security.SecurityDatabaseUser;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Execution step that fetches data from a TimeSeries engine.
 * Supports profiling via the standard {@code context.isProfiling()} mechanism.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromTimeSeriesStep extends AbstractExecutionStep {

  private static final long MILLIS_PER_DAY = 86_400_000L;

  private final LocalTimeSeriesType tsType;
  private final long                fromTs;
  private final long                toTs;
  private final TagFilter           tagFilter;
  /**
   * Issue #9612: range predicates on numeric FIELD columns the engine evaluates on block statistics and on the primitive
   * columns, so a sample the WHERE clause drops is never turned into a row. Only on the unbounded ascending read; the
   * residual filter above this step still runs over the rows it keeps.
   */
  private final FieldFilter         fieldFilter;
  /**
   * Issue #5414: when true the engine is scanned newest-first, so a last-point query
   * ({@code ORDER BY <ts> DESC LIMIT n}, {@code ts.last()}) stops after the newest blocks instead of
   * walking the whole series.
   */
  private final boolean             descending;
  /**
   * Row cap pushed into the scan, in whichever direction {@link #descending} selects; {@code <= 0} means
   * unlimited. Only set when the planner proved that every WHERE condition is consumed by the push-down and that
   * nothing downstream can discard a row, so the rows the engine stops at really are the answer.
   * <p>
   * Ascending is the later of the two (issue #7663). The descending cap came first (issue #5414) because a
   * last-point query was the obvious O(series) read; the ascending one is the same defect on
   * {@code ORDER BY ts ASC LIMIT n} and on a bare {@code LIMIT n}, whose surplus rows used to be decompressed,
   * boxed and held before {@code LimitExecutionStep} dropped them.
   */
  private final int                 limit;
  /**
   * Issue #10324: the columns of a projection made only of plain column references, by output name and by column name, when the
   * planner proved nothing between this step and the projection reads another column. The row is then built here as the
   * projection would make it, instead of a row of every column that the projection step copies into a second one. {@code null}
   * builds the row of every column.
   */
  private       String[]            projectedAliases;
  private       String[]            projectedColumns;
  private       Iterator<Object[]>  resultIterator;
  private       boolean             fetched = false;
  // THE DATE OF THE LAST TIMESTAMP TURNED INTO A DATE-TIME: THE ROWS COME IN TIMESTAMP ORDER, SO CONSECUTIVE ONES SHARE THEIR DAY AND
  // ITS CALENDAR COMPUTATION IS DONE ONCE PER DAY INSTEAD OF ONCE PER ROW (issue #10324)
  private       long                cachedEpochDay = Long.MIN_VALUE;
  private       LocalDate           cachedDate;

  public FetchFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final CommandContext context) {
    this(tsType, fromTs, toTs, null, context);
  }

  public FetchFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final TagFilter tagFilter, final CommandContext context) {
    this(tsType, fromTs, toTs, tagFilter, false, 0, context);
  }

  public FetchFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final TagFilter tagFilter, final boolean descending, final int limit, final CommandContext context) {
    this(tsType, fromTs, toTs, tagFilter, null, descending, limit, context);
  }

  /**
   * @param fieldFilter range predicates on numeric FIELD columns (issue #9612), or {@code null}; only honoured by the
   *                    unbounded ascending read, so it must be {@code null} when {@code descending} or {@code limit > 0}
   */
  public FetchFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final TagFilter tagFilter, final FieldFilter fieldFilter, final boolean descending, final int limit, final CommandContext context) {
    super(context);
    if (fieldFilter != null && (descending || limit > 0))
      throw new IllegalArgumentException("A field filter is only pushed into the unbounded ascending TimeSeries read");
    this.tsType = tsType;
    this.fromTs = fromTs;
    this.toTs = toTs;
    this.tagFilter = tagFilter;
    this.fieldFilter = fieldFilter;
    this.descending = descending;
    this.limit = limit;
  }

  /**
   * Builds each row as the projection {@code SELECT <columns[0]> AS <aliases[0]>, ...} makes it (issue #10324). Only for a
   * projection of plain column references, and only when nothing the plan runs between this step and the projection reads a column
   * left out: the planner then plans no projection step of its own.
   */
  void setProjection(final String[] aliases, final String[] columns) {
    this.projectedAliases = aliases;
    this.projectedColumns = columns;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      if (!fetched) {
        try {
          // A TimeSeries type stores its rows in its own engine, not in a bucket LocalBucket's read check could
          // gate, so the gated accessor applies the equivalent per-type read check.
          final TimeSeriesEngine engine = tsType.getEngine(SecurityDatabaseUser.ACCESS.READ_RECORD);
          if (engine == null)
            throw new CommandExecutionException(
                "TimeSeries engine for type '" + tsType.getName() + "' is not initialized");
          // The bound belongs to the FETCH in both directions (issues #5414, #7663). iterateQuery is the genuinely
          // unbounded ascending read: it streams the sealed layer one block per shard (issue #9420), so a SELECT with
          // no LIMIT holds a block per shard and the mutable buckets, not the series.
          resultIterator = descending
              ? engine.queryDescending(fromTs, toTs, null, tagFilter, limit, null).iterator()
              : limit > 0
                  ? engine.queryAscending(fromTs, toTs, null, tagFilter, limit, null).iterator()
                  : engine.iterateQuery(fromTs, toTs, null, tagFilter, fieldFilter, null);
          fetched = true;
        } catch (final CommandExecutionException e) {
          throw e;
        } catch (final IOException e) {
          throw new CommandExecutionException("Error querying TimeSeries engine", e);
        }
      }

      // The engine row is { timestamp, non-TIMESTAMP columns in schema order... } whatever position the
      // TIMESTAMP column occupies in the declaration - TimeSeriesBucket.readRow writes position 0 from the
      // row's own timestamp slot and then walks the schema SKIPPING that column. Mapping row[i] onto
      // getTsColumns().get(i) is therefore only right while the TIMESTAMP column is declared first, which
      // issue #7702 stopped being true of every declaration the grammar accepts: a plain SELECT * answered a
      // TAG's value under the timestamp's name and the timestamp under the TAG's (issue #7899). The same
      // helper every wire surface names its columns from, so SQL and HTTP cannot disagree, and resolved once
      // per pull rather than once per row.
      final List<ColumnDefinition> columns = TimeSeriesGateway.selectedColumns(tsType.getTsColumns(), null);
      // the row positions and the output names of the columns each row carries: every column under its own name, or the projected
      // ones under their aliases, in projection order
      final int[] positions;
      final String[] names;
      if (projectedColumns != null) {
        positions = new int[projectedColumns.length];
        for (int p = 0; p < projectedColumns.length; p++) {
          positions[p] = -1;
          for (int i = 0; i < columns.size(); i++)
            if (columns.get(i).getName().equals(projectedColumns[p])) {
              positions[p] = i;
              break;
            }
          if (positions[p] < 0)
            throw new CommandExecutionException(
                "Column '" + projectedColumns[p] + "' is not a column of the TIMESERIES type '" + tsType.getName() + "'");
        }
        names = projectedAliases;
      } else {
        positions = new int[columns.size()];
        names = new String[columns.size()];
        for (int i = 0; i < positions.length; i++) {
          positions[i] = i;
          names[i] = columns.get(i).getName();
        }
      }

      return new ResultSet() {
        private int count = 0;

        @Override
        public boolean hasNext() {
          final long begin1 = context.isProfiling() ? System.nanoTime() : 0;
          try {
            return count < nRecords && resultIterator.hasNext();
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin1;
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

            for (int p = 0; p < positions.length; p++) {
              final int i = positions[p];
              // never with the full rows this step reads (no column subset is asked of the engine): kept as the bound the loop
              // always had, so a shorter row leaves the property out rather than failing the scan
              if (i >= row.length)
                continue;
              final ColumnDefinition col = columns.get(i);
              Object value = row[i];

              // Issue #4385: expose the stored millisecond timestamp as a LocalDateTime (the engine's
              // standard DATETIME representation) instead of a java.util.Date. A java.util.Date is
              // formatted by the result JSON serializer with the date-only pattern, truncating the
              // value to the day; a temporal keeps the full date-time with its sub-second precision.
              if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP && value instanceof Long epochMillis)
                value = dateTimeOf(epochMillis);
              // Issue #9725: every other DATE/DATETIME column holds an epoch count in its own unit, handed back as the object a
              // document property of the same type reads as. As the raw count it could not be written back by INSERT, which
              // reads a number as epoch millis, nor matched by the literal that wrote it
              else if (value instanceof Long epoch && col.isTemporal())
                value = col.temporalOf(context.getDatabase(), epoch);

              // The absent marker becomes SQL NULL at the SQL boundary (issue #7743). NaN is what the storage
              // layers use for "no measurement here" - it is what a null field value is stored as on a
              // floating-point column - and NULL is what SQL calls the same thing: sum() and avg() skip it,
              // min() does not return it, and count(*) still counts the row. Handing the NaN through instead
              // would make a plain SELECT sum(value) answer NaN for a series with one gap in it, while the
              // aggregation push-down over the very same rows answers the total of the real samples. The HTTP
              // read paths already encode it this way (AbstractServerHttpHandler.putSampleValue).
              //
              // Only NaN, not every non-finite: an infinity IS a value in SQL arithmetic, and the JSON layer
              // folds it in only because JSON cannot write one. Asked of TimeSeriesNaN rather than spelled out
              // here, so the two SQL boundaries cannot drift apart from the storage layer's own definition of
              // absence (code review on PR #7747). A non-floating Number can never be NaN, so the widened
              // instanceof costs nothing but covers Float without a second arm.
              if (value instanceof Number n && TimeSeriesNaN.isAbsent(n.doubleValue()))
                value = null;

              result.setProperty(names[p], value);
            }

            rowCount++;
            return result;
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin1;
          }
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

  /**
   * The UTC date-time of a millisecond timestamp, the value {@code DateUtils.dateTime} answers for it, with the date of the previous
   * row reused while the day does not change (issue #10324).
   */
  private LocalDateTime dateTimeOf(final long epochMillis) {
    final long epochDay = Math.floorDiv(epochMillis, MILLIS_PER_DAY);
    if (epochDay != cachedEpochDay) {
      cachedDate = LocalDate.ofEpochDay(epochDay);
      cachedEpochDay = epochDay;
    }
    return LocalDateTime.of(cachedDate, LocalTime.ofNanoOfDay((epochMillis - epochDay * MILLIS_PER_DAY) * 1_000_000L));
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder sb = new StringBuilder();
    sb.append(spaces).append("+ FETCH FROM TIMESERIES ");
    if (descending)
      sb.append("DESC ");
    sb.append(tsType.getName());
    sb.append(" [").append(fromTs).append(" - ").append(toTs).append("]");
    // Issue #5416: the tag filter is pushed into the fetch, so the plan has to show it. Without this
    // the only visible tag predicate is the redundant FILTER step above and the plan reads as if no
    // push-down happened.
    if (tagFilter != null)
      sb.append(" TAGS ").append(tagFilter.describe(nonTsColumnNames()));
    if (fieldFilter != null)
      sb.append(" FIELDS ").append(fieldFilter.describe());
    // Shown for either direction: the ascending cap is as much a plan decision as the descending one, and a plan
    // that hides it reads as if no push-down happened (issue #7663).
    if (limit > 0)
      sb.append(" TOP ").append(limit);
    if (projectedAliases != null) {
      sb.append(" PROJECTED ");
      for (int i = 0; i < projectedAliases.length; i++) {
        if (i > 0)
          sb.append(", ");
        sb.append(projectedColumns[i]);
        if (!projectedColumns[i].equals(projectedAliases[i]))
          sb.append(" AS ").append(projectedAliases[i]);
      }
    }
    if (context.isProfiling())
      sb.append(" (").append(getCostFormatted()).append(", ").append(getRowCountFormatted()).append(")");
    return sb.toString();
  }

  /**
   * Names of the non-timestamp columns in schema order, i.e. the indexing a {@link TagFilter} uses.
   */
  private String[] nonTsColumnNames() {
    final List<ColumnDefinition> columns = tsType.getTsColumns();
    final String[] names = new String[columns.size()];
    int nonTsIdx = 0;
    for (final ColumnDefinition col : columns)
      if (col.getRole() != ColumnDefinition.ColumnRole.TIMESTAMP)
        names[nonTsIdx++] = col.getName();
    return Arrays.copyOf(names, nonTsIdx);
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    final FetchFromTimeSeriesStep copy = new FetchFromTimeSeriesStep(tsType, fromTs, toTs, tagFilter, fieldFilter, descending, limit,
        context);
    copy.setProjection(projectedAliases, projectedColumns);
    return copy;
  }
}
