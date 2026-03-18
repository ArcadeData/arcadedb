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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Optimized execution step for counting paths through a linear chain pattern.
 * <p>
 * Instead of materializing all intermediate rows and counting at the end (O(paths) memory),
 * this step propagates path counts level-by-level through the CSR adjacency structure
 * using two {@code long[]} arrays indexed by dense node IDs (O(nodes) memory).
 * <p>
 * Algorithm: breadth-first count propagation
 * <pre>
 *   counts[node] = number of distinct paths reaching this node from previous levels
 *
 *   1. Initialize: for each vertex of the anchor type, counts[nodeId] = 1
 *   2. For each hop (edgeType, direction):
 *      nextCounts[neighbor] += counts[node]  for each neighbor of each active node
 *      Filter nextCounts by target type if label specified
 *   3. Total = sum of counts at the last level
 * </pre>
 * <p>
 * For Q1 (221M paths, 3.9M nodes): allocates ~31MB instead of ~8GB of Result objects.
 * For Q6 (1.6B paths): same ~31MB instead of ~60GB.
 * <p>
 * Requires a GAV provider covering all edge types in the chain. Falls back to OLTP
 * vertex iteration when GAV is unavailable.
 */
public final class CountChainPathsStep extends AbstractExecutionStep {
  private final String[] nodeLabels;           // label for each node (length = hops + 1)
  private final String[] edgeTypes;            // edge type for each hop
  private final Vertex.DIRECTION[] directions; // direction for each hop
  private final String countAlias;             // output property name

  public CountChainPathsStep(final String[] nodeLabels, final String[] edgeTypes,
      final Vertex.DIRECTION[] directions, final String countAlias,
      final CommandContext context) {
    super(context);
    this.nodeLabels = nodeLabels;
    this.edgeTypes = edgeTypes;
    this.directions = directions;
    this.countAlias = countAlias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final Database db = context.getDatabase();
      final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(db, edgeTypes);

      final long count;
      if (provider != null)
        count = countWithCSR(provider, db);
      else
        count = countWithOLTP(db);

      if (context.isProfiling()) {
        cost = System.nanoTime() - begin;
        rowCount = 1;
        context.setVariable(CommandContext.CSR_ACCELERATED_VAR, provider != null);
      }

      final ResultInternal result = new ResultInternal();
      result.setProperty(countAlias, count);
      return new IteratorResultSet(List.of((Result) result).iterator());
    } finally {
      if (context.isProfiling() && cost < 0)
        cost = System.nanoTime() - begin;
    }
  }

  /**
   * CSR-accelerated count propagation using NeighborView for zero-copy access.
   */
  private long countWithCSR(final GraphTraversalProvider provider, final Database db) {
    final int nodeCount = provider.getNodeCount();
    final int hops = edgeTypes.length;

    // Pre-compute valid bucket sets for type filtering at each level
    final Set<Integer>[] validBuckets = new Set[hops + 1];
    for (int i = 0; i <= hops; i++) {
      if (nodeLabels[i] != null && db.getSchema().existsType(nodeLabels[i])) {
        validBuckets[i] = new HashSet<>();
        for (final var b : db.getSchema().getType(nodeLabels[i]).getBuckets(true))
          validBuckets[i].add(b.getFileId());
      }
    }

    // Initialize counts for the anchor type (first node in the chain)
    long[] current = new long[nodeCount];
    final String anchorLabel = nodeLabels[0];
    if (anchorLabel != null && db.getSchema().existsType(anchorLabel)) {
      for (final Iterator<? extends Identifiable> it = db.iterateType(anchorLabel, true); it.hasNext(); ) {
        final RID rid = it.next().getIdentity();
        final int nodeId = provider.getNodeId(rid);
        if (nodeId >= 0)
          current[nodeId] = 1;
      }
    }

    // Propagate counts through each hop
    for (int hop = 0; hop < hops; hop++) {
      final long[] next = new long[nodeCount];

      // Try zero-copy NeighborView first
      final NeighborView view = provider.getNeighborView(directions[hop], edgeTypes[hop]);
      if (view != null) {
        final int[] nbrs = view.neighbors();
        for (int v = 0; v < nodeCount; v++) {
          if (current[v] == 0)
            continue;
          final long pathCount = current[v];
          for (int j = view.offset(v), end = view.offsetEnd(v); j < end; j++)
            next[nbrs[j]] += pathCount;
        }
      } else {
        // Fall back to per-node neighbor lookup
        for (int v = 0; v < nodeCount; v++) {
          if (current[v] == 0)
            continue;
          final long pathCount = current[v];
          final int[] neighbors = provider.getNeighborIds(v, directions[hop], edgeTypes[hop]);
          for (final int neighbor : neighbors)
            next[neighbor] += pathCount;
        }
      }

      // Filter by target type label if specified
      final Set<Integer> targetBuckets = validBuckets[hop + 1];
      if (targetBuckets != null && !targetBuckets.isEmpty()) {
        for (int v = 0; v < nodeCount; v++) {
          if (next[v] > 0) {
            final RID rid = provider.getRID(v);
            if (!targetBuckets.contains(rid.getBucketId()))
              next[v] = 0;
          }
        }
      }

      current = next;
    }

    // Sum all remaining counts
    long total = 0;
    for (int v = 0; v < nodeCount; v++)
      total += current[v];
    return total;
  }

  /**
   * OLTP fallback: iterates vertices using the standard graph API.
   * Still uses count propagation but via vertex iteration instead of CSR arrays.
   */
  private long countWithOLTP(final Database db) {
    // For OLTP, use a simple recursive counting approach
    // Start with all vertices of the anchor type
    final String anchorLabel = nodeLabels[0];
    if (anchorLabel == null || !db.getSchema().existsType(anchorLabel))
      return 0;

    long total = 0;
    for (final Iterator<? extends Identifiable> it = db.iterateType(anchorLabel, true); it.hasNext(); ) {
      final Vertex v = it.next().asVertex();
      total += countPathsFrom(v, 0, db);
    }
    return total;
  }

  /**
   * Recursively counts paths from a vertex through the remaining hops.
   */
  private long countPathsFrom(final Vertex vertex, final int hopIndex, final Database db) {
    if (hopIndex >= edgeTypes.length)
      return 1; // Reached the end of the chain

    final String targetLabel = nodeLabels[hopIndex + 1];
    long count = 0;

    final Iterator<Vertex> neighbors;
    neighbors = vertex.getVertices(directions[hopIndex], edgeTypes[hopIndex]).iterator();

    while (neighbors.hasNext()) {
      final Vertex neighbor = neighbors.next();
      // Filter by target type
      if (targetLabel != null && !neighbor.getType().instanceOf(targetLabel))
        continue;
      count += countPathsFrom(neighbor, hopIndex + 1, db);
    }
    return count;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder sb = new StringBuilder();
    sb.append("  ".repeat(Math.max(0, depth * indent)));
    sb.append("+ COUNT CHAIN PATHS (CSR count-push-down)\n");
    sb.append("  ".repeat(Math.max(0, depth * indent)));
    sb.append("  chain: ");
    for (int i = 0; i < edgeTypes.length; i++) {
      if (i > 0)
        sb.append(" → ");
      sb.append("(").append(nodeLabels[i] != null ? nodeLabels[i] : "?").append(")");
      sb.append(directions[i] == Vertex.DIRECTION.OUT ? "-[:" : "<-[:");
      sb.append(edgeTypes[i]);
      sb.append(directions[i] == Vertex.DIRECTION.OUT ? "]->" : "]-");
    }
    sb.append("(").append(nodeLabels[edgeTypes.length] != null ? nodeLabels[edgeTypes.length] : "?").append(")");
    if (context.isProfiling()) {
      sb.append("\n").append("  ".repeat(Math.max(0, depth * indent)));
      sb.append("  (").append(getCostFormatted()).append(")");
    }
    return sb.toString();
  }
}
