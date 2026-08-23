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

import com.arcadedb.database.Database;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.Vertex;
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
 * Optimized execution step that replaces OPTIONAL MATCH + count() aggregation
 * with a direct edge-count call. Uses GAV/CSR when available for O(1) counting.
 * <p>
 * Pattern detected:
 * OPTIONAL MATCH (x)-[r:TYPE]->(y) ... WITH y, count(x) AS cnt
 * where x is only used for counting.
 * <p>
 * A row-multiplying clause upstream (UNWIND, an earlier fan-out MATCH hop) can feed this step
 * more than one input row per distinct grouping-key combination - each still needs its own edge
 * count, but the WITH aggregation boundary collapses them into one output row per group rather
 * than passing every input row through (issue #6629). This step therefore groups by the
 * pass-through key values and sums the per-row edge counts within each group.
 */
public final class CountEdgesStep extends AbstractExecutionStep {
  private final String boundVertexVariable;
  private final Vertex.DIRECTION direction;
  private final String[] edgeTypes;
  private final String countOutputAlias;
  private final Map<String, String> passThroughAliases;

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

    // Try GAV provider for accelerated counting
    final Database db = context.getDatabase();
    final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(db, edgeTypes);

    final String[] aliasOutputNames = passThroughAliases.keySet().toArray(new String[0]);
    final String[] aliasVarNames = passThroughAliases.values().toArray(new String[0]);

    // One accumulated count per distinct grouping-key combination (LinkedHashMap to keep the
    // first-seen group order, matching GroupByAggregationStep).
    final Map<GroupKeyValues, long[]> groups = new LinkedHashMap<>();

    while (prevResult.hasNext()) {
      final Result inputRow = prevResult.next();
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        if (context.isProfiling())
          rowCount++;

        final Object[] keyValues = new Object[aliasVarNames.length];
        for (int i = 0; i < aliasVarNames.length; i++)
          keyValues[i] = inputRow.getProperty(aliasVarNames[i]);
        final GroupKeyValues groupKey = new GroupKeyValues(keyValues);

        // Get the vertex and count edges for this row
        final Object vertexObj = inputRow.getProperty(boundVertexVariable);
        final long count;
        if (vertexObj instanceof Vertex) {
          final Vertex vertex = (Vertex) vertexObj;
          if (provider != null) {
            // GAV/CSR path: O(1) count from offset arrays
            final int nodeId = provider.getNodeId(vertex.getIdentity());
            count = nodeId >= 0 ? provider.countEdges(nodeId, direction, edgeTypes) : vertex.countEdges(direction, edgeTypes);
          } else
            count = vertex.countEdges(direction, edgeTypes);
        } else
          count = 0L; // NULL vertex = LEFT OUTER JOIN semantics

        final long[] accumulator = groups.computeIfAbsent(groupKey, k -> new long[1]);
        accumulator[0] += count;
      } finally {
        if (context.isProfiling())
          cost += System.nanoTime() - begin;
      }
    }

    final List<Result> results = new ArrayList<>(groups.size());
    for (final Map.Entry<GroupKeyValues, long[]> entry : groups.entrySet()) {
      final ResultInternal result = new ResultInternal();
      final Object[] keyValues = entry.getKey().values;
      for (int i = 0; i < aliasOutputNames.length; i++)
        result.setProperty(aliasOutputNames[i], keyValues[i]);
      result.setProperty(countOutputAlias, entry.getValue()[0]);
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
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }
}
