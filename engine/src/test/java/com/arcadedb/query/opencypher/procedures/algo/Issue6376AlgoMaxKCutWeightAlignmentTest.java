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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6376 - {@code algo.maxKCut} paired an edge weight with the neighbour
 * it was <em>iterated at</em> rather than with the neighbour it belongs to, the same defect class
 * #6301 fixed for {@code algo.apsp}, {@code algo.bellmanford}, {@code algo.steinerTree} and the
 * kShortestPaths CSR path.
 * <p>
 * {@code buildWeightedAdj} filled a weight array positionally against
 * {@code graph.adjacency(dir, relTypes)} - CSR order (sorted by dense node id after the Graph
 * Analytical View's BFS/RCM cache-locality reordering) - by walking
 * {@code graph.getVertex(i).getEdges(dir, relTypes)}, which is always OLTP (edge-record) order.
 * When a view exists the two orders diverge - confirmed for the graph below by printing both
 * orders directly - so {@code adjW[i][j]} ends up being the weight of whatever edge OLTP
 * iteration happened to visit at position {@code j}, not the weight of the edge to
 * {@code adj[i][j]}.
 * <p>
 * H-A(1.0), H-B(10.0), H-C(100.0) plus one extra A-B(5.0) edge breaks the leaf symmetry a pure
 * star would have (under which a misassigned weight would just relabel an otherwise-identical
 * answer): the unique maximum 2-cut of this graph is 115.0, achieved by grouping {H, A} against
 * {B, C}. Empirically, the unfixed code returns a non-integer 110.5 through a Graph Analytical
 * View - itself a symptom of the bug: the cut sums each edge's weight from <em>both</em>
 * endpoints' adjacency rows and halves it, and misaligned rows disagree with each other - while
 * the OLTP path (no view, no misalignment) reliably returns the true 115.0, both independent of
 * seed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6376AlgoMaxKCutWeightAlignmentTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6376-maxkcut-weights");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    // The weight is a DECLARED property: a Graph Analytical View only materialises an edge property
    // column for a property the schema knows about, and without the column the view falls back to
    // the edge records - which would leave the columnar half of the fix untested.
    database.getSchema().createEdgeType("ROAD").createProperty("w", Type.DOUBLE);

    // Star hub H with three unequal leaf weights, plus one extra A-B edge that makes the leaves
    // structurally distinguishable (without it, permuting which leaf gets the heaviest edge would
    // just relabel an equally-valued answer and could never expose a misalignment).
    database.transaction(() -> {
      final MutableVertex h = database.newVertex("N").set("name", "H").save();
      final MutableVertex a = database.newVertex("N").set("name", "A").save();
      final MutableVertex b = database.newVertex("N").set("name", "B").save();
      final MutableVertex c = database.newVertex("N").set("name", "C").save();
      h.newEdge("ROAD", c, true, new Object[] { "w", 100.0 }).save();
      h.newEdge("ROAD", b, true, new Object[] { "w", 10.0 }).save();
      h.newEdge("ROAD", a, true, new Object[] { "w", 1.0 }).save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 5.0 }).save();
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
  void theAnalyticalViewDoesNotChangeTheMaxCut() {
    for (long seed = 0; seed < 10; seed++) {
      assertThat(maxKCutWeight(seed))
          .as("OLTP: the unique maximum 2-cut ({H,A} vs {B,C}) is 115.0, seed " + seed)
          .isEqualTo(115.0);
    }

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("maxkcut-view")
        .withVertexTypes("N")
        .withEdgeTypes("ROAD")
        .withEdgeProperties("w")
        .build();
    try {
      for (long seed = 0; seed < 10; seed++) {
        assertThat(maxKCutWeight(seed))
            .as("a Graph Analytical View is a transparent accelerator: same graph, same max-cut, seed " + seed)
            .isEqualTo(115.0);
      }
    } finally {
      view.drop();
    }
  }

  @Test
  void theViewIsActuallyExercised() {
    // The counterweight to the test above: if the view were silently ignored, both halves of it
    // would be the OLTP run and it would pin nothing. CSR_ACCELERATED_VAR is what loadGraph() sets
    // when it resolves a provider, so asserting on it is what makes the comparison meaningful.
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("maxkcut-view-probe")
        .withVertexTypes("N")
        .withEdgeTypes("ROAD")
        .withEdgeProperties("w")
        .build();
    try {
      database.begin();
      try {
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        final List<Result> rows = collect(new AlgoMaxKCut().execute(
            new Object[] { 2, Map.of("weightProperty", "w", "seed", 7L) }, null, context));

        assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
            .as("the view must actually back the call")
            .isEqualTo(true);
        assertThat(rows).hasSize(4);
        assertThat(((Number) rows.getFirst().getProperty("cutWeight")).doubleValue()).isEqualTo(115.0);
      } finally {
        database.rollback();
      }
    } finally {
      view.drop();
    }
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  /** The {@code cutWeight} of an {@code algo.maxKCut(2, ...)} run over the {@code w} property, for a fixed seed. */
  private double maxKCutWeight(final long seed) {
    database.begin();
    try {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final List<Result> rows = collect(new AlgoMaxKCut().execute(
          new Object[] { 2, Map.of("weightProperty", "w", "seed", seed) }, null, context));
      assertThat(rows).hasSize(4);
      return ((Number) rows.getFirst().getProperty("cutWeight")).doubleValue();
    } finally {
      database.rollback();
    }
  }

  private static List<Result> collect(final Stream<Result> rows) {
    final List<Result> collected = new ArrayList<>();
    rows.forEach(collected::add);
    return collected;
  }
}
