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
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * GAV-accelerated replacement for TinkerPop's VertexStep. Uses CSR arrays via
 * {@link GraphTraversalProvider} for O(1) neighbor lookup instead of OLTP edge
 * linked-list traversal.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * <p>
 * Injected by {@link ArcadeTraversalStrategy} when a GAV provider is available
 * and covers the required edge labels. Falls back to OLTP if a vertex is not
 * in the CSR mapping (created after last GAV build).
 */
class ArcadeGAVVertexStep extends FlatMapStep<org.apache.tinkerpop.gremlin.structure.Vertex, org.apache.tinkerpop.gremlin.structure.Vertex> {
  private final GraphTraversalProvider provider;
  private final ArcadeGraph            graph;
  private final Direction              direction;
  private final String[]               edgeLabels;

  ArcadeGAVVertexStep(final ArcadeGraph graph, final FlatMapStep<org.apache.tinkerpop.gremlin.structure.Vertex, ?> original,
      final GraphTraversalProvider provider, final Direction direction, final String... edgeLabels) {
    super(original.getTraversal());
    this.graph = graph;
    this.provider = provider;
    this.direction = direction;
    this.edgeLabels = edgeLabels;
  }

  @Override
  protected Iterator<org.apache.tinkerpop.gremlin.structure.Vertex> flatMap(
      final Traverser.Admin<org.apache.tinkerpop.gremlin.structure.Vertex> traverser) {
    final org.apache.tinkerpop.gremlin.structure.Vertex gremlinVertex = traverser.get();

    // Extract the ArcadeDB RID from the Gremlin vertex
    final RID rid;
    if (gremlinVertex instanceof ArcadeVertex arcadeVertex)
      rid = arcadeVertex.getBaseElement().getIdentity();
    else
      rid = null;

    if (rid != null) {
      final int nodeId = provider.getNodeId(rid);
      if (nodeId >= 0) {
        final Vertex.DIRECTION dir = ArcadeGraph.mapDirection(direction);
        final int[] neighborIds = edgeLabels.length == 0
            ? provider.getNeighborIds(nodeId, dir)
            : provider.getNeighborIds(nodeId, dir, edgeLabels);
        final List<org.apache.tinkerpop.gremlin.structure.Vertex> result = new ArrayList<>(neighborIds.length);
        for (final int neighborId : neighborIds) {
          final RID neighborRid = provider.getRID(neighborId);
          if (neighborRid != null) {
            try {
              result.add(new ArcadeVertex(graph, neighborRid.asVertex()));
            } catch (final RecordNotFoundException e) {
              // vertex deleted since CSR was built — skip
            }
          }
        }
        return result.iterator();
      }
    }

    // OLTP fallback: delegate to ArcadeVertex.vertices()
    return gremlinVertex.vertices(direction, edgeLabels);
  }

  public Direction getDirection() {
    return direction;
  }

  public String[] getEdgeLabels() {
    return edgeLabels;
  }

  public GraphTraversalProvider getProvider() {
    return provider;
  }

  @Override
  public String toString() {
    return StringFactory.stepString(this, direction, String.join(",", edgeLabels), "GAV");
  }
}
