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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Optimized execution step for chained OPTIONAL MATCH + count() pattern.
 * Uses GAV/CSR when available for O(1) neighbor lookup and edge counting.
 * <p>
 * Handles pattern:
 * OPTIONAL MATCH (bound)-[r1:TYPE1]->(intermediate)
 * OPTIONAL MATCH (target)-[r2:TYPE2]->(intermediate)
 * WITH bound, count(target) AS cnt
 * <p>
 * A row-multiplying clause upstream (UNWIND, an earlier fan-out MATCH hop) can feed this step
 * more than one input row per distinct grouping-key combination - each still needs its own count,
 * but the WITH aggregation boundary collapses them into one output row per group rather than
 * passing every input row through (issue #6629). This step therefore groups by the pass-through
 * key values and sums the per-row counts within each group.
 */
public final class CountChainedEdgesStep extends AbstractExecutionStep {
  private final String boundVertexVariable;
  private final Vertex.DIRECTION firstHopDirection;
  private final String[] firstHopTypes;
  private final Vertex.DIRECTION secondHopDirection;
  private final String[] secondHopTypes;
  private final String countOutputAlias;
  private final Map<String, String> passThroughAliases;

  public CountChainedEdgesStep(final String boundVertexVariable,
      final Vertex.DIRECTION firstHopDirection,
      final String[] firstHopTypes,
      final Vertex.DIRECTION secondHopDirection,
      final String[] secondHopTypes,
      final String countOutputAlias,
      final Map<String, String> passThroughAliases,
      final CommandContext context) {
    super(context);
    this.boundVertexVariable = boundVertexVariable;
    this.firstHopDirection = firstHopDirection;
    this.firstHopTypes = firstHopTypes;
    this.secondHopDirection = secondHopDirection;
    this.secondHopTypes = secondHopTypes;
    this.countOutputAlias = countOutputAlias;
    this.passThroughAliases = passThroughAliases;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet prevResult = checkForPrevious("CountChainedEdgesStep").syncPull(context, nRecords);

    // Try GAV provider for accelerated traversal and counting
    final Database db = context.getDatabase();
    // Need a provider that covers both first-hop and second-hop edge types
    final String[] allTypes = mergeEdgeTypes(firstHopTypes, secondHopTypes);
    final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(db, allTypes);

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

        // Get the bound vertex and count through the chain for this row
        final Object vertexObj = inputRow.getProperty(boundVertexVariable);
        final long totalCount;

        if (vertexObj instanceof Vertex) {
          final Vertex boundVertex = (Vertex) vertexObj;

          if (provider != null) {
            // GAV/CSR path: array lookups instead of linked list traversal
            final int nodeId = provider.getNodeId(boundVertex.getIdentity());
            if (nodeId >= 0) {
              // First hop: get intermediate neighbors via CSR
              final int[] intermediateIds = provider.getNeighborIds(nodeId, firstHopDirection, firstHopTypes);
              // Second hop: count edges for each intermediate via CSR
              long count = 0;
              for (final int intermediateId : intermediateIds)
                count += provider.countEdges(intermediateId, secondHopDirection, secondHopTypes);
              totalCount = count;
            } else
              totalCount = countOLTP(boundVertex);
          } else
            totalCount = countOLTP(boundVertex);
        } else {
          totalCount = 0L; // NULL vertex = LEFT OUTER JOIN semantics
        }

        final long[] accumulator = groups.computeIfAbsent(groupKey, k -> new long[1]);
        accumulator[0] += totalCount;
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

  /**
   * OLTP fallback for vertices not in the GAV mapping.
   */
  private long countOLTP(final Vertex boundVertex) {
    final Iterator<Vertex> intermediates = firstHopTypes == null || firstHopTypes.length == 0 ?
        boundVertex.getVertices(firstHopDirection).iterator() :
        boundVertex.getVertices(firstHopDirection, firstHopTypes).iterator();

    long count = 0;
    while (intermediates.hasNext()) {
      final Vertex intermediate = intermediates.next();
      count += intermediate.countEdges(secondHopDirection, secondHopTypes);
    }
    return count;
  }

  private static String[] mergeEdgeTypes(final String[] a, final String[] b) {
    if ((a == null || a.length == 0) && (b == null || b.length == 0))
      return null;
    final int lenA = a != null ? a.length : 0;
    final int lenB = b != null ? b.length : 0;
    final String[] merged = new String[lenA + lenB];
    if (lenA > 0)
      System.arraycopy(a, 0, merged, 0, lenA);
    if (lenB > 0)
      System.arraycopy(b, 0, merged, lenA, lenB);
    return merged;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    final StringBuilder builder = new StringBuilder();
    builder.append(ind);
    builder.append("+ COUNT CHAINED EDGES OPTIMIZATION (").append(boundVertexVariable);
    builder.append(" ").append(firstHopDirection);
    if (firstHopTypes != null && firstHopTypes.length > 0) {
      builder.append(" [");
      for (int i = 0; i < firstHopTypes.length; i++) {
        if (i > 0)
          builder.append(", ");
        builder.append(firstHopTypes[i]);
      }
      builder.append("]");
    }
    builder.append(" -> intermediate ").append(secondHopDirection);
    if (secondHopTypes != null && secondHopTypes.length > 0) {
      builder.append(" [");
      for (int i = 0; i < secondHopTypes.length; i++) {
        if (i > 0)
          builder.append(", ");
        builder.append(secondHopTypes[i]);
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
