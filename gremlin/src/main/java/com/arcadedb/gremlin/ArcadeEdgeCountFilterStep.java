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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.function.Predicate;

/**
 * GAV-accelerated filter step that replaces the pattern
 * {@code where(outE('X').count().is(predicate))} with O(1) degree lookup via
 * {@link GraphTraversalProvider#countEdges}.
 * <p>
 * Instead of materializing all edges to count them, this step calls
 * {@code provider.countEdges(nodeId, direction, edgeLabels)} which is a simple
 * offset subtraction on the CSR arrays.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ArcadeEdgeCountFilterStep extends FilterStep<org.apache.tinkerpop.gremlin.structure.Vertex> {
  private final GraphTraversalProvider provider;
  private final Vertex.DIRECTION       direction;
  private final String[]               edgeLabels;
  private final Predicate<Long>        predicate;

  ArcadeEdgeCountFilterStep(final Traversal.Admin<?, ?> traversal, final GraphTraversalProvider provider,
      final Direction direction, final String[] edgeLabels, final Predicate<Long> predicate) {
    super(traversal);
    this.provider = provider;
    this.direction = ArcadeGraph.mapDirection(direction);
    this.edgeLabels = edgeLabels;
    this.predicate = predicate;
  }

  @Override
  protected boolean filter(final Traverser.Admin<org.apache.tinkerpop.gremlin.structure.Vertex> traverser) {
    final org.apache.tinkerpop.gremlin.structure.Vertex gremlinVertex = traverser.get();

    final RID rid;
    if (gremlinVertex instanceof ArcadeVertex arcadeVertex)
      rid = arcadeVertex.getBaseElement().getIdentity();
    else
      rid = null;

    if (rid != null) {
      final int nodeId = provider.getNodeId(rid);
      if (nodeId >= 0) {
        final long count = edgeLabels.length == 0
            ? provider.countEdges(nodeId, direction)
            : provider.countEdges(nodeId, direction, edgeLabels);
        return predicate.test(count);
      }
    }

    // OLTP fallback: iterate edges and count
    long count = 0;
    final var edgeIter = gremlinVertex.edges(
        switch (direction) {
          case OUT -> Direction.OUT;
          case IN -> Direction.IN;
          case BOTH -> Direction.BOTH;
        }, edgeLabels);
    while (edgeIter.hasNext()) {
      edgeIter.next();
      count++;
    }
    return predicate.test(count);
  }

  @Override
  public String toString() {
    return StringFactory.stepString(this, direction, String.join(",", edgeLabels), predicate, "GAV");
  }
}
