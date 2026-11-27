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
package com.arcadedb.gremlin;

import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.utility.IntIntHashMap;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Fused multi-hop GAV-accelerated step. Replaces 2+ consecutive {@link ArcadeGAVVertexStep}
 * instances with a single step that traverses the entire chain using only {@code int[]} nodeId
 * arrays from CSR, eliminating all intermediate Vertex materialization.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * <p>
 * For example, {@code g.V().out('A').out('B').out('C')} becomes a single fused DFS
 * that only materializes vertices at the final hop.
 */
class ArcadeGAVFusedStep extends FlatMapStep<org.apache.tinkerpop.gremlin.structure.Vertex, org.apache.tinkerpop.gremlin.structure.Vertex> {
  private final GraphTraversalProvider  provider;
  private final ArcadeGraph             graph;
  private final Vertex.DIRECTION[]      directions;
  private final String[][]              edgeLabelsPerHop;
  private final int                     hopCount;

  ArcadeGAVFusedStep(final Traversal.Admin<?, ?> traversal, final ArcadeGraph graph,
      final GraphTraversalProvider provider, final List<ArcadeGAVVertexStep> steps) {
    super(traversal);
    this.graph = graph;
    this.provider = provider;
    this.hopCount = steps.size();
    this.directions = new Vertex.DIRECTION[hopCount];
    this.edgeLabelsPerHop = new String[hopCount][];
    for (int i = 0; i < hopCount; i++) {
      final ArcadeGAVVertexStep step = steps.get(i);
      directions[i] = ArcadeGraph.mapDirection(step.getDirection());
      edgeLabelsPerHop[i] = step.getEdgeLabels();
    }
  }

  @Override
  protected Iterator<org.apache.tinkerpop.gremlin.structure.Vertex> flatMap(
      final Traverser.Admin<org.apache.tinkerpop.gremlin.structure.Vertex> traverser) {
    final org.apache.tinkerpop.gremlin.structure.Vertex gremlinVertex = traverser.get();

    final RID rid;
    if (gremlinVertex instanceof ArcadeVertex arcadeVertex)
      rid = arcadeVertex.getBaseElement().getIdentity();
    else
      rid = null;

    if (rid != null && ArcadeGAVVertexStep.viewAnswers(graph)) {
      final int startNodeId = provider.getNodeId(rid);
      if (startNodeId >= 0) {
        final List<org.apache.tinkerpop.gremlin.structure.Vertex> results = new ArrayList<>();
        // DFS traversal using int[] arrays — zero intermediate Vertex allocation
        dfs(startNodeId, 0, results, new IntIntHashMap());
        return results.iterator();
      }
    }

    // OLTP fallback: execute each hop sequentially
    return oltpFallback(gremlinVertex);
  }

  /**
   * Depth-first traversal through the CSR chain. Only materializes vertices at the final hop.
   * <p>
   * An intermediate vertex is not materialized, but it is still asked whether it exists (issue #11636): the view may be stale
   * (a {@code gavUseWhenStale} view, or a commit whose change has not reached the overlay yet), and the unfused chain drops a
   * vertex deleted since the build before it feeds the next hop, so walking through it would answer what the unfused chain does
   * not. {@code liveness} remembers the answer per node of this walk (1 live, 0 gone), so a node reached by several paths is
   * asked once.
   */
  private void dfs(final int nodeId, final int hop, final List<org.apache.tinkerpop.gremlin.structure.Vertex> results,
      final IntIntHashMap liveness) {
    final int[] neighbors = edgeLabelsPerHop[hop].length == 0
        ? provider.getNeighborIds(nodeId, directions[hop])
        : provider.getNeighborIds(nodeId, directions[hop], edgeLabelsPerHop[hop]);

    if (hop == hopCount - 1) {
      // Final hop: materialize vertices
      for (final int neighborId : neighbors) {
        final RID neighborRid = provider.getRID(neighborId);
        if (neighborRid != null) {
          try {
            results.add(new ArcadeVertex(graph, neighborRid.asVertex()));
          } catch (final RecordNotFoundException e) {
            // deleted since CSR build — skip
          }
        }
      }
    } else {
      // Intermediate hop: recurse with raw int IDs — no Vertex allocation
      for (final int neighborId : neighbors)
        if (exists(neighborId, liveness))
          dfs(neighborId, hop + 1, results, liveness);
    }
  }

  /** Whether the record of {@code nodeId} still exists, read from the bucket's slot table without loading it. */
  private boolean exists(final int nodeId, final IntIntHashMap liveness) {
    final int known = liveness.get(nodeId, -1);
    if (known >= 0)
      return known == 1;
    final RID rid = provider.getRID(nodeId);
    final boolean exists = rid != null && graph.getDatabase().existsRecord(rid);
    liveness.put(nodeId, exists ? 1 : 0);
    return exists;
  }

  /**
   * OLTP fallback: execute each hop through normal Gremlin vertex traversal.
   */
  private Iterator<org.apache.tinkerpop.gremlin.structure.Vertex> oltpFallback(
      org.apache.tinkerpop.gremlin.structure.Vertex start) {
    List<org.apache.tinkerpop.gremlin.structure.Vertex> current = List.of(start);
    for (int hop = 0; hop < hopCount; hop++) {
      final Direction dir = switch (directions[hop]) {
        case OUT -> Direction.OUT;
        case IN -> Direction.IN;
        case BOTH -> Direction.BOTH;
      };
      final List<org.apache.tinkerpop.gremlin.structure.Vertex> next = new ArrayList<>();
      for (final org.apache.tinkerpop.gremlin.structure.Vertex v : current) {
        final Iterator<org.apache.tinkerpop.gremlin.structure.Vertex> neighbors =
            v.vertices(dir, edgeLabelsPerHop[hop]);
        while (neighbors.hasNext())
          next.add(neighbors.next());
      }
      current = next;
    }
    return current.iterator();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GAVFused[");
    for (int i = 0; i < hopCount; i++) {
      if (i > 0) sb.append(" → ");
      sb.append(directions[i]).append("(").append(String.join(",", edgeLabelsPerHop[i])).append(")");
    }
    sb.append("]");
    return StringFactory.stepString(this, sb.toString());
  }
}
