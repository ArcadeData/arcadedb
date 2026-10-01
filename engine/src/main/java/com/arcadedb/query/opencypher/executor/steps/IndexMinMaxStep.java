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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.executor.operators.NodeIndexRangeScan;
import com.arcadedb.query.opencypher.optimizer.RangePredicate;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;

import java.util.List;

/**
 * Answers {@code MATCH (n:Label) RETURN min(n.prop)} and {@code max(n.prop)} from one end of the index on the property,
 * the way SQL's {@code MIN FROM INDEX} does, instead of reading every vertex of the label (issue #8666).
 * <p>
 * The planner only builds it where the index holds exactly the values the aggregate looks at: an ordered index on that
 * property alone, defined on the label itself, which skips the vertices with no value (what {@code min} and {@code max}
 * ignore anyway), and orders its keys the way Cypher orders the values. The answer is read from the vertex the first
 * entry points to, so it is the value the scan would have returned, not the index's own copy of the key. {@code DISTINCT} is irrelevant to a minimum or a maximum, so it does not stop the shortcut.
 * <p>
 * With a WHERE that is a range of the same property ({@code WHERE v.a > 500000}, issue #8812) the walk starts at the end
 * of that range instead: the first row of the range scan that passes the WHERE is the answer, so the range is never
 * read past it. The WHERE is evaluated on each row the scan returns because the bounds the scan pushes into the index
 * are not always exact (a bound of another type is dropped, two bounds on one side keep the last), exactly as the
 * ordinary plan keeps the filter above the same scan.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class IndexMinMaxStep extends AbstractExecutionStep {
  private final String  typeName;
  private final String  propertyName;
  private final boolean max;
  private final String  outputAlias;
  /** The MATCH variable and the range of the WHERE, or null for the whole label. */
  private final String               variable;
  private final List<RangePredicate> range;
  private final BooleanExpression    filter;
  private       boolean              executed = false;

  public IndexMinMaxStep(final String typeName, final String propertyName, final boolean max, final String outputAlias,
      final CommandContext context) {
    this(typeName, propertyName, max, outputAlias, null, null, null, context);
  }

  /**
   * @param variable the variable the MATCH binds, the one the range and the filter are about
   * @param range    the bounds of the WHERE, pushed into the index
   * @param filter   the whole WHERE, evaluated on each row the range returns
   */
  public IndexMinMaxStep(final String typeName, final String propertyName, final boolean max, final String outputAlias,
      final String variable, final List<RangePredicate> range, final BooleanExpression filter, final CommandContext context) {
    super(context);
    this.typeName = typeName;
    this.propertyName = propertyName;
    this.max = max;
    this.outputAlias = outputAlias;
    this.variable = variable;
    this.range = range;
    this.filter = filter;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (executed)
      return new InternalResultSet();
    executed = true;

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    final Object value;
    try {
      value = readEnd(context);
      if (context.isProfiling())
        rowCount++;
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }

    final ResultInternal result = new ResultInternal();
    result.setProperty(outputAlias, value);
    final InternalResultSet resultSet = new InternalResultSet();
    resultSet.add(result);
    return resultSet;
  }

  private Object readEnd(final CommandContext context) {
    final DocumentType type = context.getDatabase().getSchema().getType(typeName);
    // Read per execution, not at planning: the plan is cached, and the index can be dropped in between. An empty answer
    // would pass for a label with no values, so fail instead
    final TypeIndex index = type.getIndexByProperties(propertyName);
    if (!(index instanceof RangeIndex))
      throw new CommandExecutionException(
          "The index on '" + typeName + "." + propertyName + "' is no longer available: re-plan the query");
    // The planner refuses a case-insensitive index, whose key is the folded value (issue #8698): so must a plan that
    // predates one
    if (index.getMetadata() != null && index.getMetadata().isCaseInsensitive(0))
      throw new CommandExecutionException(
          "The index on '" + typeName + "." + propertyName + "' is case-insensitive now: re-plan the query");

    if (filter != null)
      return readEndOfRange(context, index);

    final IndexCursor cursor = index.iterator(!max);
    try {
      while (cursor.hasNext()) {
        try {
          final Object value = cursor.next().asVertex().get(propertyName);
          if (value != null)
            return value;
        } catch (final RecordNotFoundException e) {
          // deleted since the index answered: the next entry is the answer
        }
      }
      return null;
    } finally {
      cursor.close();
    }
  }

  /**
   * The first value, from the end the aggregate looks at, of a row of the range that passes the WHERE.
   */
  private Object readEndOfRange(final CommandContext context, final TypeIndex index) {
    final NodeIndexRangeScan scan = new NodeIndexRangeScan(variable, typeName, propertyName, range, index.getName(),
        List.of(propertyName), 0, 0);
    scan.setIndexOrder(!max, NodeIndexRangeScan.NullKeys.NONE);
    final ResultSet rows = scan.execute(context, 1);
    try {
      while (rows.hasNext()) {
        final Result row = rows.next();
        try {
          if (!Boolean.TRUE.equals(filter.evaluateTernary(row, context)))
            continue;
          final Object value = row.<Vertex>getProperty(variable).get(propertyName);
          if (value != null)
            return value;
        } catch (final RecordNotFoundException e) {
          // deleted since the index answered: the next row is the answer
        }
      }
      return null;
    } finally {
      rows.close();
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    builder.append("  ".repeat(Math.max(0, depth * indent)));
    builder.append(max ? "+ MAX" : "+ MIN").append(" FROM INDEX ").append(typeName).append('[').append(propertyName).append(']');
    if (range != null)
      builder.append(" WHERE ").append(filter.getText());
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }
}
