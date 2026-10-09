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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Record;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.Iterator;

/**
 * Counts the relationships one hop between two unconstrained nodes matches, {@code MATCH ()-[e:T]->() RETURN count(e)} and
 * its untyped, multi-typed and undirected forms (issue #9600).
 * <p>
 * A directed hop matches every edge of the types once. An undirected one matches an edge once from each end, and a self
 * loop, which has one end, once: {@code 2E - L}, as the row pipeline and Neo4j count it (issues #8750, #9540).
 * <p>
 * Not from the type's record counter, which the SQL {@code SELECT count(*) FROM T} reads: a light edge keeps no record, and
 * light edges exist in types that never declared {@code LIGHTWEIGHT} too (issue #9389), so that counter is the number of
 * edge <i>records</i>, not of relationships. Off a provider a directed count is the total its slices hold, in O(types), and
 * an undirected one the size of its merged view with each self loop kept once; while it serves committed changes from an
 * overlay both are summed per node. Off the records it is one walk of every vertex's lists, counting entries without
 * loading an edge or a neighbour.
 * <p>
 * A directed edge is counted at its source, so the written direction does not matter, and an edge type declared
 * unidirectional, whose edges are stored on the outgoing side only, is counted like any other. An undirected hop over such
 * a type is not given to this operator: its incoming side is not stored.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class EdgeCountOp implements CountOp {
  private final String[] edgeTypes;
  private final boolean  undirected;

  /**
   * @param edgeTypes  the types to count, sub-types included, or null for every edge type. The caller removes a type that is
   *                   a sub-type of another one listed, so no family is named twice
   * @param undirected true for {@code ()-[e]-()}, false for either directed spelling
   */
  public EdgeCountOp(final String[] edgeTypes, final boolean undirected) {
    this.edgeTypes = edgeTypes;
    this.undirected = undirected;
  }

  /** Null asks the registry for a provider covering every edge type. */
  @Override
  public String[] edgeTypes() {
    return edgeTypes;
  }

  /** Both ends are every vertex: a provider over a subset of the vertex types misses the edges that leave it. */
  @Override
  public boolean requiresFullVertexCoverage() {
    return true;
  }

  @Override
  public long execute(final GraphTraversalProvider provider, final Database db, final WorkGuard guard) {
    if (undirected) {
      final NeighborView view = CSRCountUtils.patternView(provider, Vertex.DIRECTION.BOTH, edgeTypes);
      if (view != null)
        return view.edgeCount();
    } else {
      final long total = provider.countAllEdges(edgeTypes);
      if (total >= 0)
        return total;
    }

    final Vertex.DIRECTION direction = undirected ? Vertex.DIRECTION.BOTH : Vertex.DIRECTION.OUT;
    long sum = 0;
    final int nodeIdUpperBound = provider.getNodeIdUpperBound();
    for (int v = 0; v < nodeIdUpperBound; v++) {
      guard.checkPeriodically(v);
      if (provider.isNodeLive(v))
        sum += CSRCountUtils.hopDegree(provider, v, direction, edgeTypes);
    }
    return sum;
  }

  @Override
  public long executeOLTP(final Database db, final WorkGuard guard) {
    final GraphEngine graphEngine = ((DatabaseInternal) db).getGraphEngine();
    long sum = 0;
    int visited = 0;
    for (final Iterator<Record> it = Labels.iterateMatchingVertices(db, null, false); it.hasNext(); ) {
      guard.checkPeriodically(visited++);
      final VertexInternal vertex = (VertexInternal) it.next().asVertex();
      sum += undirected ? graphEngine.countUndirectedEdges(vertex, edgeTypes) : vertex.countEdges(Vertex.DIRECTION.OUT, edgeTypes);
    }
    return sum;
  }

  @Override
  public String describe(final int depth, final int indent) {
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    return ind + "+ COUNT EDGES (" + (edgeTypes == null ? "every edge type" : String.join("|", edgeTypes))
        + (undirected ? ", undirected" : "") + ": the provider's totals, or one walk of the edge lists)";
  }
}
