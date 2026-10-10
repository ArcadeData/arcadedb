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
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.FieldFilter;
import com.arcadedb.engine.timeseries.GroupedAggregationResult;
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
import java.util.ArrayList;
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
  private final long                               bucketOffsetMs;
  private final String                             timeBucketAlias;
  private final Map<String, String>                requestAliasToOutputAlias;
  private final TagFilter                          tagFilter;
  private final FieldFilter                        fieldFilter;
  /** An ungrouped aggregate (issue #9612): exactly one row, whatever the range holds. */
  private final boolean                            singleRow;
  private final int[]                              groupColumns;
  private final String[]                           groupTagNames;
  private final List<OutputColumn>                 outputs;
  private       Iterator<ResultInternal>           resultIterator;
  private       boolean                            fetched = false;
  private       AggregationMetrics                 aggregationMetrics;
  /** The largest magnitude up to which a double holds every integer: 2^53. */
  private static final double MAX_EXACT_INTEGRAL_DOUBLE = 0x1p53;
  /** The non-timestamp columns in schema order, the numbering an engine row uses past its timestamp; built on first use. */
  private       ColumnDefinition[]                 nonTsColumns;

  public AggregateFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final List<MultiColumnAggregationRequest> requests, final long bucketIntervalMs, final String timeBucketAlias,
      final Map<String, String> requestAliasToOutputAlias, final CommandContext context) {
    this(tsType, fromTs, toTs, requests, bucketIntervalMs, timeBucketAlias, requestAliasToOutputAlias, null, context);
  }

  public AggregateFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final List<MultiColumnAggregationRequest> requests, final long bucketIntervalMs, final String timeBucketAlias,
      final Map<String, String> requestAliasToOutputAlias, final TagFilter tagFilter, final CommandContext context) {
    this(tsType, fromTs, toTs, requests, bucketIntervalMs, 0L, timeBucketAlias, requestAliasToOutputAlias, tagFilter, context);
  }

  /**
   * @param bucketOffsetMs where the bucket grid starts, as {@code TimeBucketGrid.normalizeOffset} returns it (issue
   *                       #8798); {@code 0} for the epoch-aligned grid
   */
  public AggregateFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final List<MultiColumnAggregationRequest> requests, final long bucketIntervalMs, final long bucketOffsetMs,
      final String timeBucketAlias, final Map<String, String> requestAliasToOutputAlias, final TagFilter tagFilter,
      final CommandContext context) {
    this(tsType, fromTs, toTs, requests, bucketIntervalMs, bucketOffsetMs, timeBucketAlias, requestAliasToOutputAlias, tagFilter, null,
        null, null, null, false, context);
  }

  /**
   * The push-down of an ungrouped aggregate, {@code SELECT count(*), avg(uu) FROM T WHERE ts >= ? AND ts < ?} (issue #9612):
   * one row, as SQL answers an aggregate with no GROUP BY even over no rows (a COUNT of 0, every other aggregate NULL). The
   * engine aggregates the whole range as one bucket, answering every sealed block wholly inside it from its statistics.
   *
   * @param outputs the aggregates of the answer in projection order
   */
  public AggregateFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final List<MultiColumnAggregationRequest> requests, final TagFilter tagFilter, final FieldFilter fieldFilter,
      final List<OutputColumn> outputs, final CommandContext context) {
    this(tsType, fromTs, toTs, requests, 0L, 0L, null, null, tagFilter, fieldFilter, null, null, outputs, true, context);
  }

  /**
   * What one column of a grouped answer is, in the order the query projected them (issue #9489).
   *
   * @param name  the property the row carries it under
   * @param kind  what the column holds
   * @param index for a {@link Kind#TAG}, the position in the grouping columns; for an {@link Kind#AGGREGATE}, the position in the requests
   */
  public record OutputColumn(String name, Kind kind, int index) {
    public enum Kind {
      BUCKET, TAG, AGGREGATE
    }
  }

  /**
   * The push-down of a query that also groups by TAG columns (issue #9489).
   *
   * @param groupColumns  the grouping columns as NON-timestamp column indices, in the order of {@code groupTagNames}
   * @param groupTagNames the names of the grouping columns
   * @param outputs       the columns of the answer in projection order; {@code timeBucketAlias} may be {@code null} (a query grouping by
   *                      tags alone, answered as one bucket per group, with {@code bucketIntervalMs} 0)
   */
  public AggregateFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final List<MultiColumnAggregationRequest> requests, final long bucketIntervalMs, final long bucketOffsetMs,
      final String timeBucketAlias, final Map<String, String> requestAliasToOutputAlias, final TagFilter tagFilter,
      final int[] groupColumns, final String[] groupTagNames, final List<OutputColumn> outputs, final CommandContext context) {
    this(tsType, fromTs, toTs, requests, bucketIntervalMs, bucketOffsetMs, timeBucketAlias, requestAliasToOutputAlias, tagFilter, null,
        groupColumns, groupTagNames, outputs, false, context);
  }

  /**
   * Any of the above with a field filter (issue #9612): range predicates on numeric FIELD columns the engine evaluates, so the
   * WHERE clause can carry them and still be consumed by the push-down.
   *
   * @param fieldFilter may be {@code null}; {@code groupColumns}, {@code groupTagNames} and {@code outputs} are {@code null} for
   *                    a query grouped by the bucket alone
   */
  public AggregateFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final List<MultiColumnAggregationRequest> requests, final long bucketIntervalMs, final long bucketOffsetMs,
      final String timeBucketAlias, final Map<String, String> requestAliasToOutputAlias, final TagFilter tagFilter,
      final FieldFilter fieldFilter, final int[] groupColumns, final String[] groupTagNames, final List<OutputColumn> outputs,
      final CommandContext context) {
    this(tsType, fromTs, toTs, requests, bucketIntervalMs, bucketOffsetMs, timeBucketAlias, requestAliasToOutputAlias, tagFilter, fieldFilter,
        groupColumns, groupTagNames, outputs, false, context);
  }

  private AggregateFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs,
      final List<MultiColumnAggregationRequest> requests, final long bucketIntervalMs, final long bucketOffsetMs,
      final String timeBucketAlias, final Map<String, String> requestAliasToOutputAlias, final TagFilter tagFilter,
      final FieldFilter fieldFilter, final int[] groupColumns, final String[] groupTagNames, final List<OutputColumn> outputs,
      final boolean singleRow, final CommandContext context) {
    super(context);
    this.fieldFilter = fieldFilter;
    this.singleRow = singleRow;
    this.groupColumns = groupColumns;
    this.groupTagNames = groupTagNames;
    this.outputs = outputs;
    this.tsType = tsType;
    this.fromTs = fromTs;
    this.toTs = toTs;
    this.requests = requests;
    this.bucketIntervalMs = bucketIntervalMs;
    this.bucketOffsetMs = bucketOffsetMs;
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
          // THE ROWS OF THE ANSWER ARE HELD IN HEAP LIKE THOSE OF ANY OTHER IN-HEAP OPERATION, SO THEY ARE BOUNDED BY THE SAME
          // CAP: CARRIED INTO THE SCAN, WHICH STOPS ONCE PAST IT, RATHER THAN CHECKED ON A RESULT ALREADY BUILT (ISSUE #7476)
          final OperationHeapLimit limit = OperationHeapLimit.of(context, "buckets", "time series aggregation");
          // A NON-POSITIVE CAP MEANS NO LIMIT, WHICH IS ALSO WHAT A CEILING OF 0 MEANS TO THE ENGINE
          final int ceiling = (int) Math.min(Math.max(limit.getMaxElements(), 0L), Integer.MAX_VALUE);
          if (groupColumns != null) {
            final GroupedAggregationResult grouped = engine.aggregateGrouped(fromTs, toTs, requests, bucketIntervalMs, bucketOffsetMs,
                groupColumns, tagFilter, fieldFilter, aggregationMetrics, ceiling);
            limit.check(grouped.getUsedBucketCount());
            resultIterator = groupedRows(grouped, context);
            fetched = true;
            return resultSet(nRecords);
          }
          final MultiColumnAggregationResult aggResult = engine.aggregateMulti(fromTs, toTs, requests, bucketIntervalMs, bucketOffsetMs,
              tagFilter, fieldFilter, aggregationMetrics, ceiling);
          // THE SCAN STOPS ONE BLOCK PAST THE CEILING AT MOST, AND WHAT COMES BACK OVER IT IS REFUSED HERE
          limit.check(aggResult.getUsedBucketCount());

          if (singleRow) {
            // The one bucket the engine filled, or none when no sample matched: the result then answers a COUNT of 0 and the
            // absent marker for every other request, which aggregateValue turns into the NULL the generic plan answers
            final List<Long> buckets = aggResult.getBucketTimestamps();
            final long bucketTs = buckets.isEmpty() ? Long.MIN_VALUE : buckets.getFirst();
            final ResultInternal row = new ResultInternal(context.getDatabase());
            for (final OutputColumn output : outputs)
              row.setProperty(output.name(), aggregateValue(aggResult, requests.get(output.index()), bucketTs, output.index()));
            rowCount++;
            resultIterator = List.of(row).iterator();
            fetched = true;
            return resultSet(nRecords);
          }

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
                row.setProperty(outputAlias, aggregateValue(aggResult, req, bucketTs, i));
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

      return resultSet(nRecords);
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  private ResultSet resultSet(final int nRecords) {
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
  }

  /**
   * The value of request {@code i} in a bucket, as the SQL boundary spells it.
   */
  private Object aggregateValue(final MultiColumnAggregationResult aggResult, final MultiColumnAggregationRequest req,
      final long bucketTs, final int i) {
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
    if (req.type() == AggregationType.COUNT)
      // a Long, as SQLFunctionCount answers on the generic path (issue #8915)
      return (long) value;
    if (TimeSeriesNaN.isAbsent(value) && aggResult.getCount(bucketTs, i) == 0)
      return null;
    return typedLikeTheGenericPlan(req, value);
  }

  /**
   * The value in the Java type the generic aggregation answers for the same column (issue #9612): the engine accumulates
   * every aggregate as a double, while {@code min}/{@code max} hand back a sample of the column, so its declared type, and
   * {@code sum} adds samples with {@code Type.increment}, which keeps an integral total integral and a FLOAT total a FLOAT.
   * An ungrouped aggregate used to run the generic plan, so this is what its clients have always read. {@code avg} is a
   * Double on both plans.
   */
  private Object typedLikeTheGenericPlan(final MultiColumnAggregationRequest req, final double value) {
    if (Double.isNaN(value) || Double.isInfinite(value) || (req.type() != AggregationType.MIN && req.type() != AggregationType.MAX
        && req.type() != AggregationType.SUM))
      return value;
    final ColumnDefinition column = requestColumn(req);
    if (column == null)
      return value;
    return switch (column.getDataType()) {
      case LONG -> integralOrDouble(value, false);
      case INTEGER, SHORT, BYTE -> req.type() == AggregationType.SUM
          // a total of ints is an int until it overflows, then a long, as Type.increment widens it
          ? integralOrDouble(value, true)
          // a sample of the column, so it fits the column's type
          : column.boxRaw((long) value);
      case FLOAT -> (float) value;
      default -> value;
    };
  }

  /**
   * The integral value an engine double stands for, as an Integer when {@code preferInt} and it fits one, else as a Long - or the
   * double itself past 2^53 in magnitude, where a double no longer holds every integer: the engine accumulates in doubles, so a
   * total that large may have been rounded, and handing it back as a Long would dress a rounded total up as an exact one.
   */
  private static Object integralOrDouble(final double value, final boolean preferInt) {
    if (Math.abs(value) > MAX_EXACT_INTEGRAL_DOUBLE)
      return value;
    if (preferInt && value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE)
      return (int) value;
    return (long) value;
  }

  /** The column a request aggregates: its index is a position in the engine row, where 0 is the timestamp. */
  private ColumnDefinition requestColumn(final MultiColumnAggregationRequest req) {
    if (nonTsColumns == null)
      nonTsColumns = nonTsColumnDefinitions();
    final int nonTsIdx = req.columnIndex() - 1;
    return nonTsIdx >= 0 && nonTsIdx < nonTsColumns.length ? nonTsColumns[nonTsIdx] : null;
  }

  private ColumnDefinition[] nonTsColumnDefinitions() {
    final List<ColumnDefinition> columns = new ArrayList<>();
    for (final ColumnDefinition column : tsType.getTsColumns())
      if (column.getRole() != ColumnDefinition.ColumnRole.TIMESTAMP)
        columns.add(column);
    return columns.toArray(new ColumnDefinition[0]);
  }

  /**
   * The rows of a grouped answer, group by group and bucket by bucket, built as the caller pulls them (issue #9489). The
   * columns come in the order the query projected them.
   */
  private Iterator<ResultInternal> groupedRows(final GroupedAggregationResult grouped, final CommandContext context) {
    final List<String[]> tagValues = new ArrayList<>(grouped.getGroupCount());
    final List<MultiColumnAggregationResult> results = new ArrayList<>(grouped.getGroupCount());
    grouped.forEachGroup((tags, result) -> {
      tagValues.add(tags);
      results.add(result);
    });

    final List<ColumnDefinition> columns = tsType.getTsColumns();
    final ColumnDefinition[] groupDefinitions = new ColumnDefinition[groupTagNames.length];
    for (int g = 0; g < groupDefinitions.length; g++)
      for (final ColumnDefinition column : columns)
        if (column.getName().equals(groupTagNames[g]))
          groupDefinitions[g] = column;

    return new Iterator<>() {
      private int            group   = 0;
      private Iterator<Long> buckets = results.isEmpty() ? null : results.getFirst().getBucketTimestamps().iterator();

      @Override
      public boolean hasNext() {
        while (buckets != null && !buckets.hasNext()) {
          if (++group >= results.size())
            buckets = null;
          else
            buckets = results.get(group).getBucketTimestamps().iterator();
        }
        return buckets != null;
      }

      @Override
      public ResultInternal next() {
        if (!hasNext())
          throw new IllegalStateException("No more results");
        final long bucketTs = buckets.next();
        final MultiColumnAggregationResult aggResult = results.get(group);
        final String[] tags = tagValues.get(group);
        final ResultInternal row = new ResultInternal(context.getDatabase());
        for (final OutputColumn output : outputs)
          switch (output.kind()) {
          case BUCKET ->
              row.setProperty(output.name(), DateUtils.dateTime(context.getDatabase(), bucketTs, ChronoUnit.MILLIS, LocalDateTime.class, ChronoUnit.MILLIS));
          case TAG -> row.setProperty(output.name(), groupDefinitions[output.index()].boxString(tags[output.index()]));
          case AGGREGATE -> row.setProperty(output.name(), aggregateValue(aggResult, requests.get(output.index()), bucketTs, output.index()));
          }
        rowCount++;
        return row;
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder sb = new StringBuilder();
    sb.append(spaces).append("+ AGGREGATE FROM TIMESERIES ").append(tsType.getName());
    sb.append(" [").append(fromTs).append(" - ").append(toTs).append("]");
    if (singleRow)
      sb.append(" ungrouped");
    else
      sb.append(" bucket=").append(bucketIntervalMs).append("ms");
    if (tagFilter != null)
      sb.append(" TAGS ").append(tagFilter.describe(nonTsColumnNames()));
    if (fieldFilter != null)
      sb.append(" FIELDS ").append(fieldFilter.describe());
    if (groupTagNames != null)
      sb.append(" group by ").append(String.join(", ", groupTagNames));
    if (bucketOffsetMs != 0)
      sb.append(" offset=").append(bucketOffsetMs).append("ms");
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

  /** Names of the non-timestamp columns in schema order, the numbering a {@link TagFilter} uses. */
  private String[] nonTsColumnNames() {
    final ColumnDefinition[] columns = nonTsColumnDefinitions();
    final String[] names = new String[columns.length];
    for (int i = 0; i < columns.length; i++)
      names[i] = columns[i].getName();
    return names;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new AggregateFromTimeSeriesStep(tsType, fromTs, toTs, requests, bucketIntervalMs, bucketOffsetMs, timeBucketAlias,
        requestAliasToOutputAlias, tagFilter, fieldFilter, groupColumns, groupTagNames, outputs, singleRow, context);
  }
}
