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
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Optimized execution step that replaces OPTIONAL MATCH + count() aggregation
 * with a direct Vertex.countEdges() call.
 * <p>
 * Instead of materializing all edges/vertices via OptionalMatchStep and then
 * grouping+counting via GroupByAggregationStep, this step directly calls
 * vertex.countEdges(direction, edgeTypes) which iterates edge metadata
 * without materializing Edge objects.
 * <p>
 * Pattern detected:
 * OPTIONAL MATCH (x)-[r:TYPE]->(y) ... WITH y, count(x) AS cnt
 * where x is only used for counting.
 */
public final class CountEdgesStep extends AbstractExecutionStep {
  private final String boundVertexVariable;
  private final Vertex.DIRECTION direction;
  private final String[] edgeTypes;
  private final String countOutputAlias;
  private final Map<String, String> passThroughAliases;

  /**
   * @param boundVertexVariable variable name of the already-bound vertex
   * @param direction           edge traversal direction relative to the bound vertex
   * @param edgeTypes           edge type filter (null or empty for all types)
   * @param countOutputAlias    output property name for the count
   * @param passThroughAliases  maps WITH output alias to input variable name for grouping keys
   * @param context             command context
   */
  public CountEdgesStep(final String boundVertexVariable, final Vertex.DIRECTION direction,
      final String[] edgeTypes, final String countOutputAlias,
      final Map<String, String> passThroughAliases, final CommandContext context) {
    super(context);
    this.boundVertexVariable = boundVertexVariable;
    this.direction = direction;
    this.edgeTypes = edgeTypes;
    this.countOutputAlias = countOutputAlias;
    this.passThroughAliases = passThroughAliases;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet prevResult = checkForPrevious("CountEdgesStep").syncPull(context, nRecords);

    final List<Result> results = new ArrayList<>();
    while (prevResult.hasNext()) {
      final Result inputRow = prevResult.next();

      final ResultInternal result = new ResultInternal();

      // Copy pass-through properties with their WITH aliases
      for (final Map.Entry<String, String> entry : passThroughAliases.entrySet())
        result.setProperty(entry.getKey(), inputRow.getProperty(entry.getValue()));

      // Get the vertex and count edges
      final Object vertexObj = inputRow.getProperty(boundVertexVariable);
      final long count;
      if (vertexObj instanceof Vertex) {
        final Vertex vertex = (Vertex) vertexObj;
        count = vertex.countEdges(direction, edgeTypes);
      } else
        count = 0L; // NULL vertex = LEFT OUTER JOIN semantics

      result.setProperty(countOutputAlias, count);
      results.add(result);
    }

    return new IteratorResultSet(results.iterator());
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    final StringBuilder builder = new StringBuilder();
    builder.append(ind);
    builder.append("+ COUNT EDGES OPTIMIZATION (").append(boundVertexVariable);
    builder.append(" ").append(direction);
    if (edgeTypes != null && edgeTypes.length > 0) {
      builder.append(" [");
      for (int i = 0; i < edgeTypes.length; i++) {
        if (i > 0)
          builder.append(", ");
        builder.append(edgeTypes[i]);
      }
      builder.append("]");
    }
    builder.append(" -> ").append(countOutputAlias).append(")");
    if (context.isProfiling())
      builder.append(" (").append(getCostFormatted()).append(")");
    return builder.toString();
  }
}
