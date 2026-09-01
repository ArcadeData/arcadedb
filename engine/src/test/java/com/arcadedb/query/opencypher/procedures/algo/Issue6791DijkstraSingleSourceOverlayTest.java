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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6791: {@code algo.dijkstra.singleSource} was the one weighted procedure that still
 * abandoned the CSR path and fell all the way back to the edge records on every commit against a
 * {@code SYNCHRONOUS} Graph Analytical View - the state such a view is in after every single commit - instead
 * of resolving the delta overlay the way {@code algo.mst}, {@code algo.steinerTree}, {@code astar()} and
 * {@code bellmanFord()} already do through {@link com.arcadedb.graph.GraphTraversalProvider#edgeWeightsOf}
 * (issue #6315).
 * <p>
 * {@link com.arcadedb.graph.olap.GraphAlgorithms#dijkstraSingleSource} now keeps popping nodes off its CSR-based
 * heap while an overlay is active, resolving each popped node's neighbours and weights through the provider
 * instead of indexing the raw arrays - and sizes its result against the overlay's own id-space upper bound
 * rather than the base node mapping, since an added vertex's dense id sits above it (issue #6792).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6791DijkstraSingleSourceOverlayTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6791-dijkstra-overlay");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("ROAD").createProperty("w", Type.DOUBLE);
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
  void staysCsrAcceleratedAcrossACommitAndAnswersForAddedAndDeletedEdges() {
    // A -1-> B -2-> C, plus a redundant direct A -10-> C that will be deleted.
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("name", "A").save();
      final MutableVertex b = database.newVertex("N").set("name", "B").save();
      final MutableVertex c = database.newVertex("N").set("name", "C").save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
      b.newEdge("ROAD", c, true, new Object[] { "w", 2.0 }).save();
      a.newEdge("ROAD", c, true, new Object[] { "w", 10.0 }).save();
    });

    final GraphAnalyticalView view = syncView("dijkstra-overlay-basic");
    try {
      // Delete the redundant direct edge, add a new vertex E and an edge from C to it - both live only in the
      // overlay: the redundant edge's column slot still exists but must not be walked, and E's dense id sits
      // above the base mapping entirely.
      database.transaction(() -> {
        for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if ("C".equals(edge.getInVertex().get("name")))
            edge.delete();
        final MutableVertex e = database.newVertex("N").set("name", "E").save();
        vertex("C").newEdge("ROAD", e, true, new Object[] { "w", 5.0 }).save();
      });
      assertThat(view.hasPendingChanges()).as("every commit leaves a SYNCHRONOUS view with an active overlay").isTrue();

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final Map<String, Double> costs = new HashMap<>();
      for (final Result row : collect(new AlgoDijkstraSingleSource().execute(
          new Object[] { vertex("A"), "ROAD", "w", "OUT" }, null, context))) {
        final Vertex node = ((RID) row.getProperty("node")).asVertex();
        costs.put((String) node.get("name"), ((Number) row.getProperty("cost")).doubleValue());
      }

      assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
          .as("the overlay must not send this call back to the edge records")
          .isEqualTo(true);
      // B at 1, C via A-B-C at 3 (the direct 10-weight edge was deleted), E via A-B-C-E at 8.
      assertThat(costs).containsExactlyInAnyOrderEntriesOf(Map.of("B", 1.0, "C", 3.0, "E", 8.0));
    } finally {
      view.drop();
    }
  }

  @Test
  void fallsBackToOltpWhenTheOverlayCannotResolveAnAmbiguousParallelDeletion() {
    // Three parallel A->B edges of distinct weight.
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("name", "A").save();
      final MutableVertex b = database.newVertex("N").set("name", "B").save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 2.0 }).save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 3.0 }).save();
    });

    final GraphAnalyticalView view = syncView("dijkstra-overlay-ambiguous");
    try {
      // Delete the 2.0 edge and add a 4.0 one between the same pair: which of the surviving column slots
      // belongs to which remaining edge is not recoverable from a per-pair deletion count (issue #6315).
      database.transaction(() -> {
        for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if (Double.valueOf(2.0).equals(edge.get("w")))
            edge.delete();
        vertex("A").newEdge("ROAD", vertex("B"), true, new Object[] { "w", 4.0 }).save();
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final List<Result> rows = collect(new AlgoDijkstraSingleSource().execute(
          new Object[] { vertex("A"), "ROAD", "w", "OUT" }, null, context));

      assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
          .as("an unresolvable node must send the whole call back to the edge records, not half of it")
          .isNotEqualTo(true);
      assertThat(rows).hasSize(1);
      // The cheapest surviving edge (1.0) must win, not a plausible wrong weight from the ambiguous merge.
      assertThat(((Number) rows.getFirst().getProperty("cost")).doubleValue()).isEqualTo(1.0);
    } finally {
      view.drop();
    }
  }

  private GraphAnalyticalView syncView(final String name) {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName(name)
        .withVertexTypes("N")
        .withEdgeTypes("ROAD")
        .withEdgeProperties("w")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();
    assertThat(view.hasEdgeProperty("ROAD", "w")).isTrue();
    return view;
  }

  private Vertex vertex(final String name) {
    return database.query("sql", "SELECT FROM N WHERE name = ?", name).next().getRecord().get().asVertex();
  }

  private static List<Result> collect(final Stream<Result> rows) {
    final List<Result> collected = new ArrayList<>();
    rows.forEach(collected::add);
    return collected;
  }
}
