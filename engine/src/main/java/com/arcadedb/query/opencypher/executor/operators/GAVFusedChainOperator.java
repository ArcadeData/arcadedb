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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GAVVertex;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Fused multi-hop GAV traversal operator — zero intermediate object allocation.
 * <p>
 * Replaces a chain of GAVExpandAll operators with a single operator that traverses
 * the entire path using only {@code int} nodeIds from CSR arrays. No intermediate
 * {@link ResultInternal}, {@link Vertex}, or HashMap allocations.
 * <p>
 * Only variables referenced in downstream expressions (WHERE/WITH/RETURN) are
 * materialized as Vertex objects in the final output. Intermediate variables
 * that are only used for traversal are never loaded from OLTP.
 * <p>
 * Memory: O(max_fanout) per source vertex for the traversal stack.
 * GC pressure: near-zero (only int[] arrays reused from CSR slices).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GAVFusedChainOperator extends AbstractPhysicalOperator {
  private final GraphTraversalProvider provider;

  // Source variable (bound by the child operator, e.g., NodeByLabelScan)
  private final String sourceVariable;

  // Chain hops: [0] = first expand, [n-1] = last expand
  private final Vertex.DIRECTION[] hopDirections;
  private final String[][] hopEdgeTypes;
  private final String[] hopTargetVariables;

  // Target label filter per hop: bucket IDs for type checking (null = no filter)
  private final int[][] hopTargetBucketIds;

  // Which variables need to be materialized as Vertex in the output
  private final boolean[] materializeVariable; // [0]=source, [1..n]=hop targets

  // Optional pushed-down filter predicate (evaluated via column store before emitting)
  private BooleanExpression pushedFilter;

  public GAVFusedChainOperator(final PhysicalOperator child,
      final GraphTraversalProvider provider,
      final String sourceVariable,
      final Vertex.DIRECTION[] hopDirections,
      final String[][] hopEdgeTypes,
      final String[] hopTargetVariables,
      final int[][] hopTargetBucketIds,
      final boolean[] materializeVariable,
      final double estimatedCost,
      final long estimatedCardinality) {
    super(child, estimatedCost, estimatedCardinality);
    this.provider = provider;
    this.sourceVariable = sourceVariable;
    this.hopDirections = hopDirections;
    this.hopEdgeTypes = hopEdgeTypes;
    this.hopTargetVariables = hopTargetVariables;
    this.hopTargetBucketIds = hopTargetBucketIds;
    this.materializeVariable = materializeVariable;
  }

  /**
   * Pushes a WHERE filter into the fused chain. The filter is evaluated via GAVVertex
   * (column store access) before creating output objects, avoiding ResultInternal
   * allocations for rows that will be immediately discarded.
   */
  public void setPushedFilter(final BooleanExpression filter) {
    this.pushedFilter = filter;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final ResultSet inputResults = child.execute(context, nRecords);
    final Database db = context.getDatabase();
    final int chainLength = hopDirections.length;

    // Pre-compute the output variable names array (shared across all rows — zero per-row allocation)
    final String[] outputNames = buildOutputNames();

    return new ResultSet() {
      // Per-source traversal state (stack-based DFS through the chain)
      private Result currentInputResult = null;
      private int sourceNodeId = -1;

      // DFS stack: nodeId at each level, index into neighbor array at each level
      private final int[] stackNodeId = new int[chainLength + 1]; // [0]=source, [1..n]=hop targets
      private final int[][] stackNeighbors = new int[chainLength][];
      private final int[] stackIdx = new int[chainLength];
      private int stackDepth = -1; // -1 = need new source

      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;
        if (finished)
          return false;
        fetchMore(nRecords > 0 ? nRecords : 100);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int batchSize) {
        buffer.clear();
        bufferIndex = 0;

        while (buffer.size() < batchSize) {
          // Need a new source vertex?
          if (stackDepth < 0) {
            if (!inputResults.hasNext()) {
              finished = true;
              break;
            }
            currentInputResult = inputResults.next();
            final Object sourceObj = currentInputResult.getProperty(sourceVariable);

            if (sourceObj instanceof GAVVertex)
              sourceNodeId = ((GAVVertex) sourceObj).getNodeId();
            else if (sourceObj instanceof Vertex)
              sourceNodeId = provider.getNodeId(((Vertex) sourceObj).getIdentity());
            else {
              sourceNodeId = -1;
              continue;
            }

            if (sourceNodeId < 0)
              continue; // not in GAV

            stackNodeId[0] = sourceNodeId;
            // Initialize first hop
            stackNeighbors[0] = provider.getNeighborIds(sourceNodeId, hopDirections[0], hopEdgeTypes[0]);
            stackIdx[0] = 0;
            stackDepth = 0;
          }

          // DFS traversal through the chain
          while (stackDepth >= 0) {
            if (stackIdx[stackDepth] >= stackNeighbors[stackDepth].length) {
              // Exhausted this level — backtrack
              stackDepth--;
              if (stackDepth >= 0)
                stackIdx[stackDepth]++;
              continue;
            }

            final int neighborId = stackNeighbors[stackDepth][stackIdx[stackDepth]];

            // Target label filter: check bucket ID without loading vertex
            if (hopTargetBucketIds[stackDepth] != null) {
              final RID rid = provider.getRID(neighborId);
              if (rid == null || !matchesBuckets(rid.getBucketId(), hopTargetBucketIds[stackDepth])) {
                stackIdx[stackDepth]++;
                continue;
              }
            }

            stackNodeId[stackDepth + 1] = neighborId;

            if (stackDepth == chainLength - 1) {
              // Reached the end of the chain — emit result
              emitResult(db);
              stackIdx[stackDepth]++;
              if (buffer.size() >= batchSize)
                break;
            } else {
              // Go deeper
              stackDepth++;
              stackNeighbors[stackDepth] = provider.getNeighborIds(neighborId, hopDirections[stackDepth], hopEdgeTypes[stackDepth]);
              stackIdx[stackDepth] = 0;
            }
          }

          if (stackDepth < 0)
            stackDepth = -1; // signal: need new source
        }
      }

      private void emitResult(final Database database) {
        // Build values array (one allocation per row — no HashMap, no Entry objects)
        final Object[] values = new Object[outputNames.length];
        int slot = 0;

        // Source variable
        if (materializeVariable[0])
          values[slot++] = makeReference(stackNodeId[0], database);
        else {
          // Pass through input properties into the first slots
          for (final String prop : currentInputResult.getPropertyNames())
            values[slot++] = currentInputResult.getProperty(prop);
        }

        // Hop target variables
        for (int i = 0; i < hopTargetVariables.length; i++)
          if (hopTargetVariables[i] != null && materializeVariable[i + 1])
            values[slot++] = makeReference(stackNodeId[i + 1], database);

        final GAVResult result = new GAVResult(outputNames, values);

        // Evaluate pushed filter (column store access, no OLTP) before buffering
        if (pushedFilter != null) {
          final Object filterResult = pushedFilter.evaluate(result, context);
          if (!Boolean.TRUE.equals(filterResult))
            return;
        }

        buffer.add(result);
      }

      private GAVVertex makeReference(final int nodeId, final Database database) {
        final RID rid = provider.getRID(nodeId);
        return rid != null ? new GAVVertex(rid, nodeId, provider, database) : null;
      }

      @Override
      public void close() {
        inputResults.close();
      }
    };
  }

  /**
   * Pre-computes the output variable names array. Shared across all rows (interned).
   */
  private String[] buildOutputNames() {
    final List<String> names = new ArrayList<>();
    if (materializeVariable[0])
      names.add(sourceVariable);
    // When source is not materialized, we pass through input properties — those names come at runtime
    // For now, add source variable name as placeholder
    else
      names.add(sourceVariable);

    for (int i = 0; i < hopTargetVariables.length; i++)
      if (hopTargetVariables[i] != null && materializeVariable[i + 1])
        names.add(hopTargetVariables[i]);

    return names.toArray(new String[0]);
  }

  private static boolean matchesBuckets(final int bucketId, final int[] targetBuckets) {
    for (final int tb : targetBuckets)
      if (tb == bucketId)
        return true;
    return false;
  }

  @Override
  public String getOperatorType() {
    return "GAVFusedChain";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ GAVFusedChain(").append(sourceVariable).append(")");
    for (int i = 0; i < hopDirections.length; i++) {
      sb.append("-[");
      if (hopEdgeTypes[i] != null && hopEdgeTypes[i].length > 0)
        sb.append(":").append(String.join("|", hopEdgeTypes[i]));
      sb.append("]-");
      sb.append(hopDirections[i] == Vertex.DIRECTION.OUT ? ">" : hopDirections[i] == Vertex.DIRECTION.IN ? "<" : "");
      sb.append("(").append(hopTargetVariables[i] != null ? hopTargetVariables[i] : "?");
      if (!materializeVariable[i + 1])
        sb.append("*"); // asterisk marks deferred variables
      sb.append(")");
    }
    sb.append(" [provider=").append(provider.getName());
    sb.append(", hops=").append(hopDirections.length);
    sb.append(", cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");

    if (child != null)
      sb.append(child.explain(depth + 1));

    return sb.toString();
  }

  public GraphTraversalProvider getProvider() {
    return provider;
  }

  public int getChainLength() {
    return hopDirections.length;
  }
}
