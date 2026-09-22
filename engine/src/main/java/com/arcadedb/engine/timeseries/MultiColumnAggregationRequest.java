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

import java.util.List;

/**
 * Describes a single aggregation request within a multi-column push-down aggregation.
 * <p>
 * <b>{@code columnIndex} is a ROW index, never a schema index.</b> An engine row is
 * {@code [timestamp, non-TIMESTAMP columns in schema order...]}, so position 0 is the timestamp and a value
 * column sits at {@code 1 + <its ordinal among the non-TIMESTAMP columns>}. The two numbers coincide only
 * while the TIMESTAMP column is declared FIRST, which issue #7702 stopped being a property of every
 * declaration {@code CREATE TIMESERIES TYPE} can spell; every producer supplied the schema index and only the
 * sealed half read it that way, so the same samples answered one number before compaction and another after it
 * (issue #8140). {@link TimeSeriesGateway#aggregationRowIndex} is the conversion, and every surface that
 * resolves a caller-supplied field name applies it.
 * <p>
 * A {@link AggregationType#COUNT} request reads no column at all - both halves count rows - so its
 * {@code columnIndex} is {@link #NO_COLUMN}. Build every request through {@link #of} or {@link #count} rather
 * than through the canonical constructor: that is what makes the rule hold at every producer instead of being
 * remembered at four call sites (code review on PR #8192).
 * <p>
 * {@code TimeSeriesEngine#aggregate}, the single-column path, counts NON-TIMESTAMP columns instead (0 = the
 * first non-timestamp column) and says so in its own javadoc. It is not reachable from any wire protocol or
 * from the SQL push-down.
 *
 * @param columnIndex index into the engine row (0 = timestamp, 1+ = value columns in schema order)
 * @param type        the aggregation type (AVG, MAX, MIN, SUM, COUNT)
 * @param alias       the output alias for this aggregation
 */
public record MultiColumnAggregationRequest(int columnIndex, AggregationType type, String alias) {

  /**
   * The {@code columnIndex} of a request that names no column. Position 0 of an engine row is the timestamp, so
   * it cannot double as "no column"; a negative index cannot be a row position at all, and both halves of the
   * push-down look at {@link AggregationType#COUNT} before they look at the index, so it is never dereferenced.
   */
  public static final int NO_COLUMN = -1;

  /**
   * A request over the column at {@code schemaIndex}, converting that schema index into the ROW index this
   * record carries - and answering {@link #NO_COLUMN} for a {@link AggregationType#COUNT}, which reads no
   * column on either half of the push-down.
   * <p>
   * Every surface that resolves a caller-supplied field name builds its requests here, so the two rules a
   * producer has to get right live in one place rather than at each call site (issue #8140).
   *
   * @param schemaIndex the column's index in {@code columns}, as {@link TimeSeriesGateway#findColumnIndex}
   *                    returns it. Ignored for a COUNT, which may therefore be asked with any value
   */
  public static MultiColumnAggregationRequest of(final List<ColumnDefinition> columns, final int schemaIndex,
      final AggregationType type, final String alias) {
    if (type == AggregationType.COUNT)
      return count(alias);
    return new MultiColumnAggregationRequest(TimeSeriesGateway.aggregationRowIndex(columns, schemaIndex), type, alias);
  }

  /**
   * A COUNT request, for the producer that never resolves a column at all - {@code COUNT(*)} in the SQL
   * push-down names no field to resolve.
   */
  public static MultiColumnAggregationRequest count(final String alias) {
    return new MultiColumnAggregationRequest(NO_COLUMN, AggregationType.COUNT, alias);
  }
}
