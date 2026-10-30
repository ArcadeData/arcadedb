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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.NodeEdgeWeights;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The CSR build of issue #11011 fills the forward slices straight from the scan and derives the backward ones by
 * transposing them, instead of sorting both from a list of (source, target) pairs. Whatever the order the scan meets
 * the vertices in, every slice must hold what the graph holds: each neighbour with its multiplicity (parallel edges and
 * self-loops), sorted, in both directions, with the edge properties of each edge carried to the same neighbour whether
 * it is read forward or backward.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11011CSRBuildMatchesGraphTest extends TestHelper {
  private static final int VERTICES = 1_500;

  @Test
  void everySliceHoldsTheEdgesOfTheGraphInBothDirections() {
    final List<RID> vertices = createGraph();

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("eq").withVertexTypes("V")
        .withEdgeTypes("A", "B").withEdgeProperties("w", "i").build();
    try {
      assertThat(view.getNodeCount()).isEqualTo(vertices.size());

      for (final String type : new String[] { "A", "B" }) {
        final CSRAdjacencyIndex csr = view.getCSRIndex(type);
        for (int node = 0; node < csr.getNodeCount(); node++) {
          assertSortedSlice(csr, node, true);
          assertSortedSlice(csr, node, false);
        }
      }

      database.transaction(() -> {
        for (final RID rid : vertices) {
          final Vertex vertex = rid.asVertex();
          final int node = view.getNodeId(rid);
          for (final String type : new String[] { "A", "B" }) {
            assertThat(neighbours(view, node, Vertex.DIRECTION.OUT, type)).as("OUT %s of %s", type, rid)
                .isEqualTo(graphNeighbours(vertex, Vertex.DIRECTION.OUT, type));
            assertThat(neighbours(view, node, Vertex.DIRECTION.IN, type)).as("IN %s of %s", type, rid)
                .isEqualTo(graphNeighbours(vertex, Vertex.DIRECTION.IN, type));
          }
          assertThat(weightedNeighbours(view, node, Vertex.DIRECTION.OUT)).as("OUT weights of %s", rid)
              .isEqualTo(graphWeightedNeighbours(vertex, Vertex.DIRECTION.OUT));
          assertThat(weightedNeighbours(view, node, Vertex.DIRECTION.IN)).as("IN weights of %s", rid)
              .isEqualTo(graphWeightedNeighbours(vertex, Vertex.DIRECTION.IN));
        }
      });
    } finally {
      view.drop();
    }
  }

  private List<RID> createGraph() {
    database.getSchema().buildVertexType().withName("V").withTotalBuckets(3).create();
    database.getSchema().createEdgeType("A").createProperty("w", Type.DOUBLE);
    database.getSchema().getType("A").createProperty("i", Type.INTEGER);
    database.getSchema().createEdgeType("B");

    final List<RID> vertices = new ArrayList<>(VERTICES);
    final Random random = new Random(11011);
    database.transaction(() -> {
      final List<MutableVertex> created = new ArrayList<>(VERTICES);
      for (int i = 0; i < VERTICES; i++) {
        final MutableVertex v = database.newVertex("V").set("id", i).save();
        created.add(v);
        vertices.add(v.getIdentity());
      }
      for (int e = 0; e < 12_000; e++) {
        final MutableVertex from = created.get(random.nextInt(VERTICES));
        // A QUARTER OF THE EDGES GO TO A SMALL SET OF HUBS, SO PARALLEL EDGES AND LONG SLICES ARE COMMON; A FEW ARE SELF-LOOPS
        final MutableVertex to = e % 50 == 0 ? from : created.get(e % 4 == 0 ? random.nextInt(20) : random.nextInt(VERTICES));
        if (e % 7 == 0)
          from.newEdge("A", to, "i", e); // NO WEIGHT: READ BACK AS THE DEFAULT
        else
          from.newEdge("A", to, "w", random.nextInt(1000) / 10.0, "i", e);
      }
      for (int e = 0; e < 6_000; e++)
        created.get(random.nextInt(VERTICES)).newLightEdge("B", created.get(random.nextInt(VERTICES)));
    });
    return vertices;
  }

  private static void assertSortedSlice(final CSRAdjacencyIndex csr, final int node, final boolean outgoing) {
    final int from = outgoing ? csr.outOffset(node) : csr.inOffset(node);
    final int to = outgoing ? csr.outOffsetEnd(node) : csr.inOffsetEnd(node);
    for (int k = from + 1; k < to; k++) {
      final int previous = outgoing ? csr.outNeighbor(node, k - from - 1) : csr.inNeighbor(node, k - from - 1);
      final int current = outgoing ? csr.outNeighbor(node, k - from) : csr.inNeighbor(node, k - from);
      assertThat(current).isGreaterThanOrEqualTo(previous);
    }
  }

  private static List<String> neighbours(final GraphAnalyticalView view, final int node, final Vertex.DIRECTION direction,
      final String type) {
    final List<String> result = new ArrayList<>();
    for (final int neighbour : view.getNeighborIdsOfSlice(node, direction, type))
      result.add(view.getRID(neighbour).toString());
    Collections.sort(result);
    return result;
  }

  private static List<String> graphNeighbours(final Vertex vertex, final Vertex.DIRECTION direction, final String type) {
    final List<String> result = new ArrayList<>();
    for (final Vertex neighbour : vertex.getVertices(direction, type))
      result.add(neighbour.getIdentity().toString());
    Collections.sort(result);
    return result;
  }

  private static List<String> weightedNeighbours(final GraphAnalyticalView view, final int node, final Vertex.DIRECTION direction) {
    final NodeEdgeWeights weights = view.edgeWeightsForSlice(node, direction, "A", "w", -1.0, null);
    final List<String> result = new ArrayList<>();
    for (int k = 0; k < weights.neighbors().length; k++)
      result.add(view.getRID(weights.neighbors()[k]) + "=" + weights.weights()[k]);
    Collections.sort(result);
    return result;
  }

  private static List<String> graphWeightedNeighbours(final Vertex vertex, final Vertex.DIRECTION direction) {
    final List<String> result = new ArrayList<>();
    for (final Edge edge : vertex.getEdges(direction, "A")) {
      final RID neighbour = direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
      final Object w = edge.get("w");
      result.add(neighbour + "=" + (w != null ? ((Number) w).doubleValue() : -1.0));
    }
    Collections.sort(result);
    return result;
  }
}
