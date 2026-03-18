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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Iterator;
import java.util.List;

/**
 * Optimized execution step for star-join patterns with {@code RETURN count(*)}.
 * <p>
 * Handles queries where multiple patterns share a single central node and all non-central
 * nodes are anonymous. For each central node, the path count is the product of degrees along
 * each arm. OPTIONAL MATCH arms use {@code max(1, degree)} instead of bare degree.
 * <p>
 * Example (Q4):
 * <pre>
 *   MATCH (:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(:Person),
 *         (m)<-[:LIKES]-(:Person),
 *         (m)<-[:REPLY_OF]-(:Comment)
 *   RETURN count(*) AS count
 * </pre>
 * For each Message m: count += hasTag_in(m) * hasCreator_out(m) * likes_in(m) * replyOf_in(m).
 * <p>
 * Complexity: O(|central type| * arms) with CSR degree lookups.
 */
public final class CountStarJoinStep extends AbstractExecutionStep {
  private final String centralLabel;
  private final Arm[] arms;
  private final String countAlias;

  /**
   * A single arm extending from the central node. Each arm is a sequence of hops.
   * For single-hop arms (the common case), degree is computed directly via countEdges().
   * For multi-hop arms, count propagation through the arm's chain is used.
   */
  public static final class Arm {
    final String[] edgeTypes;
    final Vertex.DIRECTION[] directions;
    final boolean optional; // OPTIONAL MATCH arm: use max(1, count)

    public Arm(final String[] edgeTypes, final Vertex.DIRECTION[] directions, final boolean optional) {
      this.edgeTypes = edgeTypes;
      this.directions = directions;
      this.optional = optional;
    }
  }

  public CountStarJoinStep(final String centralLabel, final Arm[] arms,
      final String countAlias, final CommandContext context) {
    super(context);
    this.centralLabel = centralLabel;
    this.arms = arms;
    this.countAlias = countAlias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final Database db = context.getDatabase();

      // Collect all edge types across all arms for provider lookup
      int totalEdgeTypes = 0;
      for (final Arm arm : arms)
        totalEdgeTypes += arm.edgeTypes.length;
      final String[] allEdgeTypes = new String[totalEdgeTypes];
      int idx = 0;
      for (final Arm arm : arms)
        for (final String et : arm.edgeTypes)
          allEdgeTypes[idx++] = et;

      final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(db, allEdgeTypes);

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

  private long countWithCSR(final GraphTraversalProvider provider, final Database db) {
    long total = 0;

    for (final Iterator<? extends Identifiable> it = db.iterateType(centralLabel, true); it.hasNext(); ) {
      final RID rid = it.next().getIdentity();
      final int nodeId = provider.getNodeId(rid);
      if (nodeId < 0)
        continue;

      long product = 1;
      for (final Arm arm : arms) {
        final long armCount;
        if (arm.edgeTypes.length == 1) {
          // Single-hop arm: direct degree lookup (most common case)
          armCount = provider.countEdges(nodeId, arm.directions[0], arm.edgeTypes[0]);
        } else {
          // Multi-hop arm: propagate through the arm chain starting from this node
          armCount = propagateArm(provider, nodeId, arm);
        }

        if (arm.optional) {
          product *= Math.max(1, armCount);
        } else {
          if (armCount == 0) {
            product = 0;
            break;
          }
          product *= armCount;
        }
      }
      total += product;
    }
    return total;
  }

  /**
   * Propagates count through a multi-hop arm starting from a single central node.
   * Returns the total number of paths from the central node through all hops.
   */
  private long propagateArm(final GraphTraversalProvider provider, final int startNodeId, final Arm arm) {
    // Start with just the central node
    int[] currentNodes = new int[]{startNodeId};
    for (int hop = 0; hop < arm.edgeTypes.length; hop++) {
      // Expand to next level
      int nextSize = 0;
      // First pass: count to allocate
      for (final int nid : currentNodes)
        nextSize += provider.getNeighborIds(nid, arm.directions[hop], arm.edgeTypes[hop]).length;
      if (nextSize == 0)
        return 0;

      final int[] nextNodes = new int[nextSize];
      int pos = 0;
      for (final int nid : currentNodes) {
        final int[] neighbors = provider.getNeighborIds(nid, arm.directions[hop], arm.edgeTypes[hop]);
        System.arraycopy(neighbors, 0, nextNodes, pos, neighbors.length);
        pos += neighbors.length;
      }
      currentNodes = nextNodes;
    }
    return currentNodes.length;
  }

  private long countWithOLTP(final Database db) {
    long total = 0;
    for (final Iterator<? extends Identifiable> it = db.iterateType(centralLabel, true); it.hasNext(); ) {
      final Vertex v = it.next().asVertex();
      long product = 1;
      for (final Arm arm : arms) {
        long armCount = 0;
        if (arm.edgeTypes.length == 1) {
          armCount = v.countEdges(arm.directions[0], arm.edgeTypes[0]);
        } else {
          // Multi-hop: recursive count
          armCount = countArmOLTP(v, arm, 0);
        }
        if (arm.optional)
          product *= Math.max(1, armCount);
        else {
          if (armCount == 0) {
            product = 0;
            break;
          }
          product *= armCount;
        }
      }
      total += product;
    }
    return total;
  }

  private long countArmOLTP(final Vertex vertex, final Arm arm, final int hopIndex) {
    if (hopIndex >= arm.edgeTypes.length)
      return 1;
    long count = 0;
    final Iterator<Vertex> neighbors = vertex.getVertices(arm.directions[hopIndex], arm.edgeTypes[hopIndex]).iterator();
    while (neighbors.hasNext())
      count += countArmOLTP(neighbors.next(), arm, hopIndex + 1);
    return count;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder sb = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    sb.append(ind).append("+ COUNT STAR JOIN (CSR degree product)\n");
    sb.append(ind).append("  central: ").append(centralLabel).append(", arms: ").append(arms.length);
    for (int i = 0; i < arms.length; i++) {
      sb.append("\n").append(ind).append("  arm ").append(i).append(arms[i].optional ? " [OPTIONAL]" : "").append(": ");
      for (int j = 0; j < arms[i].edgeTypes.length; j++) {
        if (j > 0) sb.append(" → ");
        sb.append(arms[i].directions[j] == Vertex.DIRECTION.OUT ? "-[:" : "<-[:");
        sb.append(arms[i].edgeTypes[j]);
        sb.append(arms[i].directions[j] == Vertex.DIRECTION.OUT ? "]->" : "]-");
      }
    }
    if (context.isProfiling())
      sb.append("\n").append(ind).append("  (").append(getCostFormatted()).append(")");
    return sb.toString();
  }
}
