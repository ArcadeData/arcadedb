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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Optimized execution step for two comma-separated patterns sharing two endpoint variables,
 * combined with {@code RETURN count(*)}.
 * <p>
 * The build side has a central start node with two arms branching to the shared endpoints.
 * <p>
 * Example (Q2):
 * <pre>
 *   MATCH (p1:Person)-[:KNOWS]-(p2:Person),
 *         (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2)
 *   RETURN count(*) AS count
 * </pre>
 * Build from Comment c: arm1 = c→p1 (HAS_CREATOR), arm2 = c→po→p2 (REPLY_OF,HAS_CREATOR).
 * Probe: iterate KNOWS edges, look up pairCount[(p1,p2)].
 * <p>
 * Complexity: O(|buildNodes| × arm_degrees + |probeEdges|) with HashMap lookup.
 */
public final class CountPairJoinStep extends AbstractExecutionStep {
  private final String buildStartLabel;
  // Two arms from the build start node, each reaching one shared endpoint
  private final String[] arm1EdgeTypes;
  private final Vertex.DIRECTION[] arm1Directions;
  private final String[] arm2EdgeTypes;
  private final Vertex.DIRECTION[] arm2Directions;

  private final String probeEdgeType;
  private final Vertex.DIRECTION probeDirection;
  private final String countAlias;

  public CountPairJoinStep(final String buildStartLabel,
      final String[] arm1EdgeTypes, final Vertex.DIRECTION[] arm1Directions,
      final String[] arm2EdgeTypes, final Vertex.DIRECTION[] arm2Directions,
      final String probeEdgeType, final Vertex.DIRECTION probeDirection,
      final String countAlias, final CommandContext context) {
    super(context);
    this.buildStartLabel = buildStartLabel;
    this.arm1EdgeTypes = arm1EdgeTypes;
    this.arm1Directions = arm1Directions;
    this.arm2EdgeTypes = arm2EdgeTypes;
    this.arm2Directions = arm2Directions;
    this.probeEdgeType = probeEdgeType;
    this.probeDirection = probeDirection;
    this.countAlias = countAlias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final Database db = context.getDatabase();

      // Collect all edge types
      final java.util.HashSet<String> allTypesSet = new java.util.HashSet<>();
      for (final String et : arm1EdgeTypes) allTypesSet.add(et);
      for (final String et : arm2EdgeTypes) allTypesSet.add(et);
      allTypesSet.add(probeEdgeType);

      final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(db,
          allTypesSet.toArray(new String[0]));

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
    final HashMap<Long, Long> pairCounts = new HashMap<>();

    // BUILD PHASE: for each start node, walk arm1 to get endpoint1 IDs, arm2 to get endpoint2 IDs
    for (final Iterator<? extends Identifiable> it = db.iterateType(buildStartLabel, true); it.hasNext(); ) {
      final int startId = provider.getNodeId(it.next().getIdentity());
      if (startId < 0)
        continue;

      // Get all endpoint1 values via arm1
      final int[] ep1Ids = walkArm(provider, startId, arm1EdgeTypes, arm1Directions);
      if (ep1Ids.length == 0)
        continue;

      // Get all endpoint2 values via arm2
      final int[] ep2Ids = walkArm(provider, startId, arm2EdgeTypes, arm2Directions);
      if (ep2Ids.length == 0)
        continue;

      // Record all (ep1, ep2) pairs
      for (final int ep1 : ep1Ids)
        for (final int ep2 : ep2Ids)
          pairCounts.merge(packPair(ep1, ep2), 1L, Long::sum);
    }

    // PROBE PHASE: iterate probe edges, look up pair counts
    final NeighborView probeView = provider.getNeighborView(probeDirection, probeEdgeType);
    final int nodeCount = provider.getNodeCount();
    long total = 0;

    if (probeView != null) {
      final int[] probeNbrs = probeView.neighbors();
      for (int p1 = 0; p1 < nodeCount; p1++) {
        for (int j = probeView.offset(p1), end = probeView.offsetEnd(p1); j < end; j++) {
          final Long cnt = pairCounts.get(packPair(p1, probeNbrs[j]));
          if (cnt != null)
            total += cnt;
        }
      }
    } else {
      for (int p1 = 0; p1 < nodeCount; p1++) {
        final int[] neighbors = provider.getNeighborIds(p1, probeDirection, probeEdgeType);
        for (final int p2 : neighbors) {
          final Long cnt = pairCounts.get(packPair(p1, p2));
          if (cnt != null)
            total += cnt;
        }
      }
    }
    return total;
  }

  /**
   * Walks an arm from a start node through multiple hops, returning all reachable endpoint IDs.
   * For single-hop arms, returns direct neighbors. For multi-hop, expands recursively.
   */
  private int[] walkArm(final GraphTraversalProvider provider, final int startId,
      final String[] edgeTypes, final Vertex.DIRECTION[] directions) {
    int[] current = new int[]{startId};
    for (int hop = 0; hop < edgeTypes.length; hop++) {
      int totalNext = 0;
      for (final int nid : current)
        totalNext += provider.getNeighborIds(nid, directions[hop], edgeTypes[hop]).length;
      if (totalNext == 0)
        return new int[0];
      final int[] next = new int[totalNext];
      int pos = 0;
      for (final int nid : current) {
        final int[] neighbors = provider.getNeighborIds(nid, directions[hop], edgeTypes[hop]);
        System.arraycopy(neighbors, 0, next, pos, neighbors.length);
        pos += neighbors.length;
      }
      current = next;
    }
    return current;
  }

  private static long packPair(final int a, final int b) {
    return ((long) a << 32) | (b & 0xFFFFFFFFL);
  }

  private long countWithOLTP(final Database db) {
    final HashMap<String, Long> pairCounts = new HashMap<>();

    for (final Iterator<? extends Identifiable> it = db.iterateType(buildStartLabel, true); it.hasNext(); ) {
      final Vertex start = it.next().asVertex();
      final java.util.List<RID> ep1List = walkArmOLTP(start, arm1EdgeTypes, arm1Directions);
      final java.util.List<RID> ep2List = walkArmOLTP(start, arm2EdgeTypes, arm2Directions);
      for (final RID ep1 : ep1List)
        for (final RID ep2 : ep2List)
          pairCounts.merge(ep1 + "|" + ep2, 1L, Long::sum);
    }

    long total = 0;
    for (final com.arcadedb.schema.DocumentType dt : db.getSchema().getTypes()) {
      if (!(dt instanceof com.arcadedb.schema.VertexType))
        continue;
      for (final Iterator<? extends Identifiable> it = db.iterateType(dt.getName(), false); it.hasNext(); ) {
        final Vertex p1 = it.next().asVertex();
        for (final Iterator<Vertex> vIt = p1.getVertices(probeDirection, probeEdgeType).iterator(); vIt.hasNext(); ) {
          final Vertex p2 = vIt.next();
          final Long cnt = pairCounts.get(p1.getIdentity() + "|" + p2.getIdentity());
          if (cnt != null)
            total += cnt;
        }
      }
    }
    return total;
  }

  private java.util.List<RID> walkArmOLTP(final Vertex start, final String[] edgeTypes,
      final Vertex.DIRECTION[] directions) {
    java.util.List<Vertex> current = java.util.List.of(start);
    for (int hop = 0; hop < edgeTypes.length; hop++) {
      final java.util.List<Vertex> next = new java.util.ArrayList<>();
      for (final Vertex v : current)
        for (final Iterator<Vertex> it = v.getVertices(directions[hop], edgeTypes[hop]).iterator(); it.hasNext(); )
          next.add(it.next());
      current = next;
    }
    final java.util.List<RID> result = new java.util.ArrayList<>(current.size());
    for (final Vertex v : current)
      result.add(v.getIdentity());
    return result;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder sb = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    sb.append(ind).append("+ COUNT PAIR JOIN (build-probe hash join)\n");
    sb.append(ind).append("  build: ").append(buildStartLabel);
    sb.append(", arm1[");
    for (int i = 0; i < arm1EdgeTypes.length; i++) {
      if (i > 0) sb.append(",");
      sb.append(arm1EdgeTypes[i]);
    }
    sb.append("], arm2[");
    for (int i = 0; i < arm2EdgeTypes.length; i++) {
      if (i > 0) sb.append(",");
      sb.append(arm2EdgeTypes[i]);
    }
    sb.append("], probe: ").append(probeEdgeType);
    if (context.isProfiling())
      sb.append("\n").append(ind).append("  (").append(getCostFormatted()).append(")");
    return sb.toString();
  }
}
