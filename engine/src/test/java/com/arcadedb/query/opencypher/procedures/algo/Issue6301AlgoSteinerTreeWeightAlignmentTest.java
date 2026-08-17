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
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6301 - {@code algo.steinerTree} paired an edge weight with the neighbour it was
 * <em>iterated at</em> rather than with the neighbour it belongs to.
 * <p>
 * The adjacency list was built with the {@code relTypes} filter applied and in the backing store's order; the
 * weight array beside it was filled positionally from an <em>unfiltered</em> {@code getEdges(BOTH)} walk in OLTP
 * order. Nothing reconciled the two, so {@code adjW[i][j]} was "the weight of the j-th edge I happened to see",
 * not "the weight of the edge to {@code adj[i][j]}". That array is what Dijkstra runs on in step 1, so the tree
 * itself moved, not merely the {@code weight} column of the result.
 * <p>
 * Two shapes make it visible, and both are pinned here:
 * <ul>
 *   <li>an edge type the caller excluded still lands its weight on an included edge;</li>
 *   <li>the same query answers differently depending on whether a {@link GraphAnalyticalView} happens to exist,
 *       because the CSR neighbour order need not match the OLTP one. A view is meant to be a transparent
 *       accelerator, so this is the worst failure mode it has.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6301AlgoSteinerTreeWeightAlignmentTest {
  private Database database;
  /** Whether the last {@link #apspDistance} call actually went through a provider rather than the OLTP records. */
  private boolean  csrAccelerated;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6301-steiner-weights");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    // The weight is a DECLARED property on both edge types: a Graph Analytical View only materialises an edge
    // property column for a property the schema knows about, and without the column the view falls back to the
    // edge records - which would leave the columnar half of the fix untested.
    database.getSchema().createEdgeType("ROAD").createProperty("w", Type.DOUBLE);
    database.getSchema().createEdgeType("NOISE").createProperty("w", Type.DOUBLE);
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
  void anExcludedEdgeTypeDoesNotLendItsWeightToAnIncludedEdge() {
    // X -ROAD(1)- Y -ROAD(1)- Z, plus one NOISE(999) edge hanging off X. `relTypes='ROAD'` excludes NOISE
    // entirely, so the tree connecting X and Z is X-Y-Z and costs 2.0.
    //
    // Positionally, the NOISE edge was the first edge X's unfiltered walk produced, so its 999.0 landed on
    // slot 0 of X's ROAD-only adjacency - the X-Y edge - and the answer came back 500x too expensive.
    database.transaction(() -> {
      final MutableVertex x = database.newVertex("N").set("name", "X").save();
      final MutableVertex y = database.newVertex("N").set("name", "Y").save();
      final MutableVertex z = database.newVertex("N").set("name", "Z").save();
      final MutableVertex noise = database.newVertex("N").set("name", "NOISE_TARGET").save();
      x.newEdge("ROAD", y, true, new Object[] { "w", 1.0 }).save();
      y.newEdge("ROAD", z, true, new Object[] { "w", 1.0 }).save();
      x.newEdge("NOISE", noise, true, new Object[] { "w", 999.0 }).save();
    });

    final List<Result> rows = steinerTree("X", "Z", "ROAD", "w");

    assertThat(rows).as("the tree connecting X and Z is X-Y-Z, two edges").hasSize(2);
    for (final Result row : rows) {
      assertThat(((Number) row.getProperty("weight")).doubleValue())
          .as("every ROAD edge of the tree weighs 1.0; 999.0 belongs to the excluded NOISE edge")
          .isEqualTo(1.0);
      assertThat(((Number) row.getProperty("totalWeight")).doubleValue()).isEqualTo(2.0);
    }
  }

  @Test
  void theAnalyticalViewDoesNotChangeTheTree() {
    // One edge type, no filter subtlety at all: X has two ROAD neighbours (Y at 1.0 and Z at 50.0) and Y-Z
    // costs 1.0, so the cheapest tree joining X and Z is X-Y-Z at 2.0.
    //
    // The OLTP walk produced X's edges in creation order and the CSR produces them in dense-id order, so the
    // positional pairing swapped 1.0 and 50.0 exactly when a view existed - and Dijkstra then preferred the
    // direct 50.0 hop, returning a 51.0 tree for a query that had returned 2.0 a moment earlier.
    database.transaction(() -> {
      final MutableVertex x = database.newVertex("N").set("name", "X").save();
      final MutableVertex y = database.newVertex("N").set("name", "Y").save();
      final MutableVertex z = database.newVertex("N").set("name", "Z").save();
      x.newEdge("ROAD", y, true, new Object[] { "w", 1.0 }).save();
      x.newEdge("ROAD", z, true, new Object[] { "w", 50.0 }).save();
      y.newEdge("ROAD", z, true, new Object[] { "w", 1.0 }).save();
    });

    final List<Result> oltp = steinerTree("X", "Z", "ROAD", "w");
    assertThat(totalWeightOf(oltp)).as("OLTP: the cheapest tree is X-Y-Z").isEqualTo(2.0);
    assertThat(oltp).hasSize(2);

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("steiner-view")
        .withVertexTypes("N")
        .withEdgeTypes("ROAD")
        .withEdgeProperties("w")
        .build();
    try {
      final List<Result> csr = steinerTree("X", "Z", "ROAD", "w");
      assertThat(totalWeightOf(csr))
          .as("a Graph Analytical View is a transparent accelerator: same query, same tree")
          .isEqualTo(2.0);
      assertThat(csr).hasSize(2);
      for (final Result row : csr)
        assertThat(((Number) row.getProperty("weight")).doubleValue()).isEqualTo(1.0);
    } finally {
      view.drop();
    }
  }

  @Test
  void theViewIsActuallyExercised() {
    // The counterweight to the test above: if the view were silently ignored, both halves of it would be the
    // OLTP run and it would pin nothing. CSR_ACCELERATED_VAR is what the procedure sets when loadGraph()
    // resolves a provider, so asserting on it is what makes the comparison meaningful.
    database.transaction(() -> {
      final MutableVertex x = database.newVertex("N").set("name", "X").save();
      final MutableVertex y = database.newVertex("N").set("name", "Y").save();
      x.newEdge("ROAD", y, true, new Object[] { "w", 3.0 }).save();
    });

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("steiner-view-probe")
        .withVertexTypes("N")
        .withEdgeTypes("ROAD")
        .withEdgeProperties("w")
        .build();
    try {
      database.begin();
      try {
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        final List<Result> rows = collect(new AlgoSteinerTree().execute(
            new Object[] { List.of(vertexNamed("X"), vertexNamed("Y")), "ROAD", "w" }, null, context));

        assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
            .as("the view must actually back the call")
            .isEqualTo(true);
        assertThat(rows).hasSize(1);
        assertThat(((Number) rows.getFirst().getProperty("weight")).doubleValue()).isEqualTo(3.0);
      } finally {
        database.rollback();
      }
    } finally {
      view.drop();
    }
  }

  @Test
  void parallelEdgesOfTheSamePairKeepTheirOwnWeights() {
    // Two ROAD edges join X and Y, at 7.0 and at 2.0. Whichever adjacency slot each lands in, the pair of
    // weights present must be {2.0, 7.0} - so the shortest path from X to Y costs 2.0 and not 7.0. Pairing by
    // neighbour rather than by position has to survive a neighbour appearing more than once.
    database.transaction(() -> {
      final MutableVertex x = database.newVertex("N").set("name", "X").save();
      final MutableVertex y = database.newVertex("N").set("name", "Y").save();
      x.newEdge("ROAD", y, true, new Object[] { "w", 7.0 }).save();
      x.newEdge("ROAD", y, true, new Object[] { "w", 2.0 }).save();
    });

    final List<Result> rows = steinerTree("X", "Y", "ROAD", "w");
    assertThat(rows).hasSize(1);
    assertThat(totalWeightOf(rows)).isEqualTo(2.0);
  }

  @Test
  void weightsSurviveAViewThatDidNotMaterialiseThem() {
    // algo.bellmanford took the CSR path on the strength of the adjacency alone and then, finding no property
    // column for `w`, used a unit weight for EVERY edge - silently, and only when a view existed. X-Y-Z costs
    // 2.0 and the direct X-Z hop costs 50.0, so unit weights invert the answer: the direct hop wins at 1.0.
    //
    // The view here deliberately materialises no edge properties, which is the configuration that triggered it.
    database.transaction(() -> {
      final MutableVertex x = database.newVertex("N").set("name", "X").save();
      final MutableVertex y = database.newVertex("N").set("name", "Y").save();
      final MutableVertex z = database.newVertex("N").set("name", "Z").save();
      x.newEdge("ROAD", y, true, new Object[] { "w", 1.0 }).save();
      y.newEdge("ROAD", z, true, new Object[] { "w", 1.0 }).save();
      x.newEdge("ROAD", z, true, new Object[] { "w", 50.0 }).save();
    });

    assertThat(bellmanFordWeight()).as("OLTP: X-Y-Z at 2.0 beats the direct 50.0 hop").isEqualTo(2.0);

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("bellman-view-no-edge-properties")
        .withVertexTypes("N")
        .withEdgeTypes("ROAD")
        .build();
    try {
      assertThat(bellmanFordWeight())
          .as("a view without the property column must send the weights back to the edge records, not to 1.0")
          .isEqualTo(2.0);
    } finally {
      view.drop();
    }
  }

  @Test
  void everyEdgeTypeIsWeightedWhenNoFilterIsGiven() {
    // With no relTypes the CSR weight lookup had no type to address its property columns by, and produced an
    // empty weight row against a non-empty neighbour row - an ArrayIndexOutOfBoundsException out of algo.apsp
    // for a query that worked perfectly without the view. Two edge types make the "all types" path the one
    // under test rather than the single-type shortcut.
    database.transaction(() -> {
      final MutableVertex x = database.newVertex("N").set("name", "X").save();
      final MutableVertex y = database.newVertex("N").set("name", "Y").save();
      final MutableVertex z = database.newVertex("N").set("name", "Z").save();
      x.newEdge("ROAD", y, true, new Object[] { "w", 2.0 }).save();
      y.newEdge("NOISE", z, true, new Object[] { "w", 3.0 }).save();
    });

    assertThat(apspDistance("X", "Z")).as("OLTP: 2.0 + 3.0 across the two edge types").isEqualTo(5.0);

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("apsp-view-all-types")
        .withVertexTypes("N")
        .withEdgeTypes("ROAD", "NOISE")
        .withEdgeProperties("w")
        .build();
    try {
      final double distance = apspDistance("X", "Z");
      assertThat(csrAccelerated).as("the view must actually back the call, or this pins the OLTP path twice")
          .isTrue();
      assertThat(distance)
          .as("the same distance, whichever backing answered and however many edge types are in play")
          .isEqualTo(5.0);
    } finally {
      view.drop();
    }
  }

  @Test
  void aViewThatMaterialisedADifferentPropertyDoesNotMakeTheGraphUnweighted() {
    // The narrower half of the same defect, and the one a coarse "does this view have edge properties?" gate
    // walks straight into: the view materialises `other`, the query asks for `w`, so every getEdgeProperty
    // returns null and every edge silently weighs 1.0 - which inverts the answer here, since X-Y-Z costs 2.0
    // and the direct X-Z hop costs 50.0. A null property value is also how "this edge has no value" is
    // reported, so the caller cannot tell the two apart and has to ask the sharper question up front.
    database.getSchema().getType("ROAD").createProperty("other", Type.DOUBLE);
    database.transaction(() -> {
      final MutableVertex x = database.newVertex("N").set("name", "X").save();
      final MutableVertex y = database.newVertex("N").set("name", "Y").save();
      final MutableVertex z = database.newVertex("N").set("name", "Z").save();
      x.newEdge("ROAD", y, true, new Object[] { "w", 1.0, "other", 1.0 }).save();
      y.newEdge("ROAD", z, true, new Object[] { "w", 1.0, "other", 1.0 }).save();
      x.newEdge("ROAD", z, true, new Object[] { "w", 50.0, "other", 1.0 }).save();
    });

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("steiner-view-other-property")
        .withVertexTypes("N")
        .withEdgeTypes("ROAD")
        .withEdgeProperties("other")
        .build();
    try {
      final List<Result> rows = steinerTree("X", "Z", "ROAD", "w");
      assertThat(totalWeightOf(rows))
          .as("the view holds `other`, not `w`, so the weights must come from the edge records")
          .isEqualTo(2.0);
    } finally {
      view.drop();
    }
  }

  @Test
  void anActiveOverlayDoesNotLendItsWeightsToTheWrongEdges() {
    // The overlay case, exercised through algo.bellmanford so the narrowing is pinned for a caller outside the
    // procedures rewritten here. Edge property columns are aligned with the base CSR's forward slots, while
    // getNeighborIds serves the overlay's view of the node - deletions dropped, additions merged, the list
    // re-sorted - so the n-th neighbour is no longer the n-th edge of the column store. The provider now says
    // it cannot serve edge properties in that state, and the weights come from the records instead.
    database.transaction(() -> {
      final MutableVertex x = database.newVertex("N").set("name", "X").save();
      final MutableVertex y = database.newVertex("N").set("name", "Y").save();
      final MutableVertex z = database.newVertex("N").set("name", "Z").save();
      x.newEdge("ROAD", y, true, new Object[] { "w", 1.0 }).save();
      y.newEdge("ROAD", z, true, new Object[] { "w", 1.0 }).save();
      x.newEdge("ROAD", z, true, new Object[] { "w", 50.0 }).save();
    });

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("overlay-view")
        .withVertexTypes("N")
        .withEdgeTypes("ROAD")
        .withEdgeProperties("w")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();
    try {
      assertThat(view.hasEdgeProperties()).as("no overlay yet, so the columns are addressable").isTrue();

      // A committed change the synchronous view absorbs into its delta overlay rather than by rebuilding.
      database.transaction(() -> {
        final MutableVertex extra = database.newVertex("N").set("name", "W").save();
        vertexOf("X").newEdge("ROAD", extra, true, new Object[] { "w", 7.0 }).save();
      });

      assertThat(view.hasEdgeProperties())
          .as("with an overlay active the positional mapping no longer holds, so the honest answer is no")
          .isFalse();
      assertThat(view.hasEdgeProperty("ROAD", "w")).isFalse();
      assertThat(bellmanFordWeight()).as("X-Y-Z at 2.0, read from the edge records").isEqualTo(2.0);
    } finally {
      view.drop();
    }
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  /** The weight of the {@code algo.bellmanford} path from X to Z over the {@code w} property. */
  private double bellmanFordWeight() {
    database.begin();
    try {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final List<Result> rows = collect(new AlgoBellmanFord().execute(
          new Object[] { vertexNamed("X"), vertexNamed("Z"), "ROAD", "w" }, null, context));
      assertThat(rows).hasSize(1);
      return ((Number) rows.getFirst().getProperty("weight")).doubleValue();
    } finally {
      database.rollback();
    }
  }

  /** The {@code algo.apsp} distance between two named vertices over the {@code w} property, all edge types. */
  private double apspDistance(final String from, final String to) {
    database.begin();
    try {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final RID fromRid = ((Vertex) vertexNamed(from)).getIdentity();
      final RID toRid = ((Vertex) vertexNamed(to)).getIdentity();
      final List<Result> rows = collect(new AlgoAPSP().execute(new Object[] { "w" }, null, context));
      csrAccelerated = Boolean.TRUE.equals(context.getVariable(CommandContext.CSR_ACCELERATED_VAR));
      for (final Result row : rows)
        if (fromRid.equals(row.getProperty("source")) && toRid.equals(row.getProperty("target")))
          return ((Number) row.getProperty("distance")).doubleValue();
      throw new AssertionError(from + " -> " + to + " is not in the apsp result");
    } finally {
      database.rollback();
    }
  }

  /**
   * Calls the procedure directly rather than through Cypher: the two terminals are looked up by name and handed
   * over as vertices, which is what the CALL form does anyway, and it keeps the assertions about the procedure
   * rather than about the planner. Requires an active transaction, like every direct use of the graph API.
   */
  private List<Result> steinerTree(final String from, final String to, final String relTypes,
      final String weightProperty) {
    database.begin();
    try {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      return collect(new AlgoSteinerTree().execute(
          new Object[] { List.of(vertexNamed(from), vertexNamed(to)), relTypes, weightProperty }, null, context));
    } finally {
      database.rollback();
    }
  }

  private Vertex vertexOf(final String name) {
    return (Vertex) vertexNamed(name);
  }

  private Object vertexNamed(final String name) {
    return database.query("sql", "SELECT FROM N WHERE name = ?", name).next().getElement().orElseThrow().asVertex();
  }

  private static List<Result> collect(final Stream<Result> rows) {
    final List<Result> collected = new ArrayList<>();
    rows.forEach(collected::add);
    return collected;
  }

  private static double totalWeightOf(final List<Result> rows) {
    assertThat(rows).isNotEmpty();
    double sum = 0.0;
    for (final Result row : rows)
      sum += ((Number) row.getProperty("weight")).doubleValue();
    // totalWeight repeats on every row and must agree with the edges actually returned.
    for (final Result row : rows)
      assertThat(((Number) row.getProperty("totalWeight")).doubleValue()).isEqualTo(sum);
    return sum;
  }

  @SuppressWarnings("unused")
  private static String nameOf(final Object rid) {
    return ((RID) rid).asVertex().getString("name");
  }
}
