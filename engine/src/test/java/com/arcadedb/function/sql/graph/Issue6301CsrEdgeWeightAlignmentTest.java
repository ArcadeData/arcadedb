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
package com.arcadedb.function.sql.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6301 in the two SQL graph functions that read edge weights out of a Graph
 * Analytical View: {@code astar()} and {@code bellmanFord()}.
 * <p>
 * Both paired {@link com.arcadedb.graph.GraphTraversalProvider#getNeighborIds} with
 * {@link com.arcadedb.graph.GraphTraversalProvider#getEdgeProperty} by hand, which is wrong in two independent
 * ways and silently so - a wrong weight produces a wrong path, never an exception:
 * <ul>
 *   <li><b>a multi-type neighbour list is merged and sorted across types</b>, while the property columns are per
 *       type, so position {@code j} in the list is not position {@code j} in any one column;</li>
 *   <li><b>{@code BOTH} has no column at all.</b> A provider resolves {@code OUT} and {@code IN} only, so a
 *       {@code BOTH} lookup answers {@code null} for every edge - and {@code bellmanFord}'s direction argument
 *       <em>defaults</em> to {@code BOTH}, which made the plain three-argument call unit-weight the whole graph
 *       the moment a view existed.</li>
 * </ul>
 * Both now go through {@code GraphTraversalProvider.edgeWeightsOf}, which walks one slice per (type, direction)
 * and is the same helper the {@code algo.*} procedures use, so the three cannot drift apart again.
 * <p>
 * The fixture is the discriminator: {@code A-B-C-D} at 1.0 per hop against a direct {@code A-C} at 100.0. By
 * weight the shortest path from A to D is the four-vertex {@code A-B-C-D}; by hop count - which is what unit
 * weights collapse to - it is the three-vertex {@code A-C-D}. So a test that gets the weights wrong does not
 * merely report a wrong number, it returns a different path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6301CsrEdgeWeightAlignmentTest {
  private Database        database;
  private MutableVertex[] nodes;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6301-csr-edge-weights");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("node");
    // Declared, so a view built over it actually materialises the edge property column - without the declaration
    // the columnar path is never taken and this test would silently pin the OLTP one twice.
    database.getSchema().createEdgeType("road").createProperty("weight", Type.DOUBLE);

    database.transaction(() -> {
      nodes = new MutableVertex[4];
      for (int i = 0; i < 4; i++)
        nodes[i] = database.newVertex("node").set("name", String.valueOf((char) ('A' + i))).save();
      nodes[0].newEdge("road", nodes[1], true, new Object[] { "weight", 1.0 }).save();
      nodes[1].newEdge("road", nodes[2], true, new Object[] { "weight", 1.0 }).save();
      nodes[2].newEdge("road", nodes[3], true, new Object[] { "weight", 1.0 }).save();
      nodes[0].newEdge("road", nodes[2], true, new Object[] { "weight", 100.0 }).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  @Test
  void bellmanFordKeepsItsWeightsOnACsrBackedGraph() {
    assertThat(bellmanFordPath()).as("OLTP: A-B-C-D costs 3.0, the direct A-C hop alone costs 100.0")
        .containsExactly(rid(0), rid(1), rid(2), rid(3));

    final GraphAnalyticalView view = weightedView("bellman-csr-view");
    try {
      assertThat(bellmanFordPath())
          .as("the same call with a view present: BOTH has no property column, so this used to unit-weight "
              + "every edge and return the two-hop A-C-D instead")
          .containsExactly(rid(0), rid(1), rid(2), rid(3));
    } finally {
      view.drop();
    }
  }

  @Test
  void astarKeepsItsWeightsOnACsrBackedGraph() {
    // Directions are exercised both ways round: OUT is the direction whose column exists, BOTH is the one that
    // has none and used to price every edge at MIN - which for astar is 0.0, i.e. free, so every path tied and
    // the first one found won.
    for (final String direction : new String[] { "OUT", "BOTH" }) {
      assertThat(astarPath(direction)).as("OLTP, direction=%s", direction)
          .containsExactly(rid(0), rid(1), rid(2), rid(3));
    }

    final GraphAnalyticalView view = weightedView("astar-csr-view");
    try {
      for (final String direction : new String[] { "OUT", "BOTH" }) {
        assertThat(astarPath(direction)).as("view present, direction=%s", direction)
            .containsExactly(rid(0), rid(1), rid(2), rid(3));
      }
    } finally {
      view.drop();
    }
  }

  @Test
  void aViewMaterialisingAnotherPropertyDoesNotUnweightTheGraph() {
    // The other half of the same question: the view holds `other`, the call asks for `weight`. Every
    // getEdgeProperty then answers null, which is indistinguishable from "this edge has no value", so the only
    // safe reading is that the provider cannot serve the request at all.
    database.getSchema().getType("road").createProperty("other", Type.DOUBLE);
    database.transaction(() -> {
      for (final var edge : nodes[0].getEdges(Vertex.DIRECTION.OUT))
        edge.asEdge().modify().set("other", 1.0).save();
    });

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("other-property-view")
        .withVertexTypes("node")
        .withEdgeTypes("road")
        .withEdgeProperties("other")
        .build();
    try {
      assertThat(bellmanFordPath()).as("`other` is materialised, `weight` is not, so the records answer")
          .containsExactly(rid(0), rid(1), rid(2), rid(3));
    } finally {
      view.drop();
    }
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  private GraphAnalyticalView weightedView(final String name) {
    return GraphAnalyticalView.builder(database)
        .withName(name)
        .withVertexTypes("node")
        .withEdgeTypes("road")
        .withEdgeProperties("weight")
        .build();
  }

  /** {@code bellmanFord(A, D, 'weight')} - three arguments, so the direction defaults to BOTH. */
  @SuppressWarnings("unchecked")
  private List<RID> bellmanFordPath() {
    database.begin();
    try {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      return (List<RID>) new SQLFunctionBellmanFord().execute(null, null, null,
          new Object[] { nodes[0], nodes[3], "'weight'" }, context);
    } finally {
      database.rollback();
    }
  }

  private List<RID> astarPath(final String direction) {
    database.begin();
    try {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final Map<String, Object> options = new HashMap<>();
      options.put("direction", direction);
      options.put("edgeTypeNames", new String[] { "road" });
      return new SQLFunctionAstar().execute(null, null, null,
          new Object[] { nodes[0], nodes[3], "'weight'", options }, context);
    } finally {
      database.rollback();
    }
  }

  private RID rid(final int index) {
    return nodes[index].getIdentity();
  }
}
