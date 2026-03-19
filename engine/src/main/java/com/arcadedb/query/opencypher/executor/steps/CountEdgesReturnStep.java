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

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Optimized step that replaces MatchRelationshipStep + GroupByAggregationStep for queries like:
 * <pre>
 * MATCH (p:Person)-[:KNOWS]->(friend)
 * RETURN p.name AS name, count(friend) AS friend_count
 * </pre>
 * <p>
 * Instead of materializing all 50K edges and then grouping/counting, this step:
 * 1. For each source vertex, calls vertex.countEdges() (no target vertex loading)
 * 2. Evaluates grouping expressions (e.g., p.name)
 * 3. Aggregates counts for groups with the same key
 * 4. Skips vertices with 0 matching edges (MATCH semantics, not OPTIONAL MATCH)
 * <p>
 * This eliminates N vertex loads where N = total edge count.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CountEdgesReturnStep extends AbstractExecutionStep {
  private final String              sourceVariable;
  private final Vertex.DIRECTION    direction;
  private final String[]            edgeTypes;
  private final String              countAlias;
  private final Expression[]        groupingExpressions;
  private final String[]            groupingAliases;
  private final ExpressionEvaluator evaluator;

  /**
   * @param sourceVariable      variable name of the source vertex in the MATCH pattern
   * @param direction           edge traversal direction
   * @param edgeTypes           edge type filter (null or empty for all types)
   * @param countAlias          output alias for the count aggregation
   * @param groupingExpressions expressions for grouping (e.g., p.name)
   * @param groupingAliases     output aliases for grouping expressions
   * @param context             command context
   * @param functionFactory     function factory for expression evaluation
   */
  public CountEdgesReturnStep(final String sourceVariable, final Vertex.DIRECTION direction,
      final String[] edgeTypes, final String countAlias,
      final Expression[] groupingExpressions, final String[] groupingAliases,
      final CommandContext context, final CypherFunctionFactory functionFactory) {
    super(context);
    this.sourceVariable = sourceVariable;
    this.direction = direction;
    this.edgeTypes = edgeTypes;
    this.countAlias = countAlias;
    this.groupingExpressions = groupingExpressions;
    this.groupingAliases = groupingAliases;
    this.evaluator = functionFactory != null ? new ExpressionEvaluator(functionFactory) : null;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet prevResult = checkForPrevious("CountEdgesReturnStep").syncPull(context, nRecords);

    if (groupingExpressions.length == 1) {
      // Single-key fast path: use raw Object as map key
      final Map<Object, Long> groups = new LinkedHashMap<>();

      while (prevResult.hasNext()) {
        final Result inputRow = prevResult.next();
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          if (context.isProfiling())
            rowCount++;

          final Object vertexObj = inputRow.getProperty(sourceVariable);
          if (!(vertexObj instanceof Vertex))
            continue;

          final long count = ((Vertex) vertexObj).countEdges(direction, edgeTypes);
          if (count == 0)
            continue; // MATCH semantics: no edges = no match

          final Object key = evaluator != null
              ? evaluator.evaluate(groupingExpressions[0], inputRow, context)
              : inputRow.getProperty(groupingAliases[0]);

          groups.merge(key, count, Long::sum);
        } finally {
          if (context.isProfiling())
            cost += (System.nanoTime() - begin);
        }
      }

      // Build output
      final List<Result> results = new ArrayList<>(groups.size());
      for (final Map.Entry<Object, Long> entry : groups.entrySet()) {
        final ResultInternal result = new ResultInternal();
        result.setProperty(groupingAliases[0], entry.getKey());
        result.setProperty(countAlias, entry.getValue());
        results.add(result);
      }
      return new IteratorResultSet(results.iterator());
    }

    // Multi-key path
    final Map<GroupKey, Long> groups = new LinkedHashMap<>();
    while (prevResult.hasNext()) {
      final Result inputRow = prevResult.next();
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        if (context.isProfiling())
          rowCount++;

        final Object vertexObj = inputRow.getProperty(sourceVariable);
        if (!(vertexObj instanceof Vertex))
          continue;

        final long count = ((Vertex) vertexObj).countEdges(direction, edgeTypes);
        if (count == 0)
          continue;

        final Object[] keys = new Object[groupingExpressions.length];
        for (int i = 0; i < groupingExpressions.length; i++)
          keys[i] = evaluator != null
              ? evaluator.evaluate(groupingExpressions[i], inputRow, context)
              : inputRow.getProperty(groupingAliases[i]);

        groups.merge(new GroupKey(keys), count, Long::sum);
      } finally {
        if (context.isProfiling())
          cost += (System.nanoTime() - begin);
      }
    }

    final List<Result> results = new ArrayList<>(groups.size());
    for (final Map.Entry<GroupKey, Long> entry : groups.entrySet()) {
      final ResultInternal result = new ResultInternal();
      final Object[] keys = entry.getKey().values;
      for (int i = 0; i < groupingAliases.length; i++)
        result.setProperty(groupingAliases[i], keys[i]);
      result.setProperty(countAlias, entry.getValue());
      results.add(result);
    }
    return new IteratorResultSet(results.iterator());
  }

  private static final class GroupKey {
    final Object[] values;
    private final int hash;

    GroupKey(final Object[] values) {
      this.values = values;
      int h = 1;
      for (final Object v : values)
        h = 31 * h + (v == null ? 0 : v.hashCode());
      this.hash = h;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof GroupKey other)) return false;
      if (values.length != other.values.length) return false;
      for (int i = 0; i < values.length; i++) {
        if (values[i] == null) {
          if (other.values[i] != null) return false;
        } else if (!values[i].equals(other.values[i]))
          return false;
      }
      return true;
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    final StringBuilder builder = new StringBuilder();
    builder.append(ind);
    builder.append("+ COUNT EDGES RETURN (").append(sourceVariable);
    builder.append(" ").append(direction);
    if (edgeTypes != null && edgeTypes.length > 0) {
      builder.append(" [");
      for (int i = 0; i < edgeTypes.length; i++) {
        if (i > 0) builder.append(", ");
        builder.append(edgeTypes[i]);
      }
      builder.append("]");
    }
    builder.append(" -> ").append(countAlias).append(")");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }
}
