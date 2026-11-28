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
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #12957: {@code algo.pagerank}, {@code algo.louvain} and {@code algo.maxKCut} read the weights of
 * {@code weightProperty} without checking them, so one NaN, infinite or negative edge weight silently turned every PageRank
 * score and every Louvain modularity into NaN and the {@code maxKCut} cut weight into Infinity (records) or -1.0 (view).
 * They now refuse such weights with the message {@code algo.personalizedPageRank} already used, on both the record path and
 * the Graph Analytical View path, and still answer valid weights (zero and negative zero included) as before.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12957AlgoEdgeWeightValidationTest {
  private static final String REFUSAL = "must be finite and non-negative";

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-12957-algo-edge-weights");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    // declared so that a Graph Analytical View materialises the weight as a column and the columnar read is exercised
    database.getSchema().createEdgeType("E").createProperty("w", Type.DOUBLE);
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  /** The reporter's graph, verbatim: every procedure that sums the weights answered it without an error. */
  @Test
  void reportersGraphIsRefusedByEveryProcedureThatSumsTheWeights() {
    database.transaction(() -> {
      final Vertex[] v = new Vertex[5];
      for (int i = 0; i < 5; i++)
        v[i] = database.newVertex("N").set("name", "n" + i).save();
      edge(v[0], v[1], 1.0);
      edge(v[1], v[2], Double.NaN);
      edge(v[0], v[2], Double.POSITIVE_INFINITY);
      edge(v[2], v[3], 1e308);
      edge(v[3], v[4], 1e308);
      edge(v[1], v[3], -1.0);
      edge(v[0], v[3], -0.0);
      edge(v[4], v[0], 5.0);
      edge(v[1], v[4], 2.0);
    });

    assertRefused("CALL algo.pagerank({weightProperty: 'w'}) YIELD node, score RETURN node, score", "algo.pagerank");
    assertRefused("CALL algo.louvain({weightProperty: 'w'}) YIELD node, modularity RETURN node, modularity", "algo.louvain");
    assertRefused("CALL algo.maxKCut(2, {weightProperty: 'w', seed: 1}) YIELD node, cutWeight RETURN node, cutWeight",
        "algo.maxKCut");
    // the procedure whose refusal the other three now share
    assertRefused(
        "MATCH (s:N {name: 'n0'}) CALL algo.personalizedPageRank(s, {weightProperty: 'w'}) YIELD nodeId, score RETURN nodeId, score",
        "algo.personalizedPageRank");
  }

  @ParameterizedTest
  @ValueSource(doubles = { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -1.0, -1e-300 })
  void oneBadWeightIsRefusedOnTheRecordPath(final double bad) {
    buildGraph(bad);

    final String got = "got " + bad;
    assertThatThrownBy(() -> drain(new AlgoPageRank().execute(new Object[] { Map.of("weightProperty", "w") }, null, newContext())))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("algo.pagerank()").hasMessageContaining(REFUSAL)
        .hasMessageContaining(got);
    assertThatThrownBy(() -> drain(new AlgoLouvain().execute(new Object[] { Map.of("weightProperty", "w") }, null, newContext())))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("algo.louvain()").hasMessageContaining(REFUSAL)
        .hasMessageContaining(got);
    assertThatThrownBy(() -> drain(new AlgoMaxKCut().execute(new Object[] { 2, Map.of("weightProperty", "w", "seed", 1) }, null,
        newContext()))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("algo.maxKCut()")
        .hasMessageContaining(REFUSAL).hasMessageContaining(got);
  }

  /**
   * The view path reads the weights from the view's columns rather than from the edge records, and it is the one where
   * {@code maxKCut} answered -1.0 with an all-zero partition instead of the record path's Infinity.
   */
  @ParameterizedTest
  @ValueSource(doubles = { Double.NaN, Double.POSITIVE_INFINITY, -1.0 })
  void oneBadWeightIsRefusedOnTheViewPath(final double bad) {
    buildGraph(bad);

    final GraphAnalyticalView view = buildView("issue-12957-bad-" + Math.abs(Double.hashCode(bad)));
    try {
      final String got = "got " + bad;

      final BasicCommandContext louvainContext = newContext();
      assertThatThrownBy(() -> drain(new AlgoLouvain().execute(new Object[] { Map.of("weightProperty", "w") }, null, louvainContext)))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(REFUSAL).hasMessageContaining(got);
      assertCSRAccelerated(louvainContext);

      final BasicCommandContext maxKCutContext = newContext();
      assertThatThrownBy(() -> drain(new AlgoMaxKCut().execute(new Object[] { 2, Map.of("weightProperty", "w", "seed", 1) }, null,
          maxKCutContext))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(REFUSAL).hasMessageContaining(got);
      assertCSRAccelerated(maxKCutContext);

      // weighted PageRank always reads the records; a view in place must not change that it refuses
      assertThatThrownBy(() -> drain(new AlgoPageRank().execute(new Object[] { Map.of("weightProperty", "w") }, null, newContext())))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(REFUSAL).hasMessageContaining(got);
    } finally {
      view.drop();
    }
  }

  /** Zero and negative zero are valid weights: the check must not refuse them, and the answers stay finite and view-independent. */
  @Test
  void validWeightsIncludingZeroAreStillAnswered() {
    buildGraph(0.0);
    database.transaction(() -> edge(named("A"), named("D"), -0.0));

    final Map<RID, Double> pageRank = pageRank(newContext());
    assertThat(pageRank).hasSize(4);
    assertThat(pageRank.values()).allSatisfy(s -> assertThat(Double.isFinite(s)).isTrue());
    assertThat(pageRank.values().stream().mapToDouble(Double::doubleValue).sum()).isCloseTo(1.0, within(1e-6));

    final BasicCommandContext louvainRecords = newContext();
    final Map<RID, Object[]> louvain = louvain(louvainRecords);
    assertThat(louvain).hasSize(4);
    assertThat(louvain.values()).allSatisfy(row -> assertThat(Double.isFinite((Double) row[1])).isTrue());

    final Map<RID, Object[]> maxKCut = maxKCut(newContext());
    assertThat(maxKCut).hasSize(4);
    // A->B 1, B->C 2, C->D 3, D->A 4, A->C 1.5, B->D 0, A->D -0: the cut weight can be at most the total, 11.5
    assertThat(maxKCut.values()).allSatisfy(row -> assertThat((Double) row[1]).isBetween(0.0, 11.5));

    final GraphAnalyticalView view = buildView("issue-12957-valid");
    try {
      final BasicCommandContext louvainView = newContext();
      final Map<RID, Object[]> louvainCsr = louvain(louvainView);
      assertCSRAccelerated(louvainView);
      assertThat(louvainCsr.keySet()).isEqualTo(louvain.keySet());
      for (final Map.Entry<RID, Object[]> e : louvain.entrySet())
        assertThat((Double) louvainCsr.get(e.getKey())[1]).isCloseTo((Double) e.getValue()[1], within(1e-12));

      final BasicCommandContext maxKCutView = newContext();
      final Map<RID, Object[]> maxKCutCsr = maxKCut(maxKCutView);
      assertCSRAccelerated(maxKCutView);
      assertThat(maxKCutCsr.keySet()).isEqualTo(maxKCut.keySet());
      assertThat(maxKCutCsr.values()).allSatisfy(row -> assertThat((Double) row[1]).isBetween(0.0, 11.5));
    } finally {
      view.drop();
    }
  }

  /** An unweighted call reads unit weights and never looks at the property, so a graph holding bad values is still answered. */
  @Test
  void unweightedCallsIgnoreTheBadValuesOfTheProperty() {
    buildGraph(Double.NaN);

    final Map<RID, Double> scores = new HashMap<>();
    drain(new AlgoPageRank().execute(new Object[0], null, newContext())).forEach(
        r -> scores.put(r.getProperty("node"), ((Number) r.getProperty("score")).doubleValue()));
    assertThat(scores).hasSize(4);
    assertThat(scores.values()).allSatisfy(s -> assertThat(Double.isFinite(s)).isTrue());

    assertThat(drain(new AlgoLouvain().execute(new Object[0], null, newContext()))).hasSize(4)
        .allSatisfy(r -> assertThat(Double.isFinite(((Number) r.getProperty("modularity")).doubleValue())).isTrue());
    assertThat(drain(new AlgoMaxKCut().execute(new Object[] { 2, Map.of("seed", 1) }, null, newContext()))).hasSize(4)
        .allSatisfy(r -> assertThat(Double.isFinite(((Number) r.getProperty("cutWeight")).doubleValue())).isTrue());
  }

  // ── fixtures ──────────────────────────────────────────────────────────────

  /** A 4-node graph of valid weights, plus the edge B->D weighing {@code value}. */
  private void buildGraph(final double value) {
    database.transaction(() -> {
      final Vertex a = node("A"), b = node("B"), c = node("C"), d = node("D");
      edge(a, b, 1.0);
      edge(b, c, 2.0);
      edge(c, d, 3.0);
      edge(d, a, 4.0);
      edge(a, c, 1.5);
      edge(b, d, value);
    });
  }

  private Vertex node(final String name) {
    return database.newVertex("N").set("name", name).save();
  }

  private Vertex named(final String name) {
    return database.query("sql", "SELECT FROM N WHERE name = ?", name).next().getElement().orElseThrow().asVertex();
  }

  private static void edge(final Vertex from, final Vertex to, final double weight) {
    from.newEdge("E", to, true, new Object[] { "w", weight }).save();
  }

  private GraphAnalyticalView buildView(final String name) {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName(name).withVertexTypes("N").withEdgeTypes("E")
        .withEdgeProperties("w").build();
    assertThat(view.awaitReady(10, TimeUnit.SECONDS)).isTrue();
    return view;
  }

  private BasicCommandContext newContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private static void assertCSRAccelerated(final CommandContext context) {
    assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
        .as("the view must actually back the call, or the view path is not the one tested").isEqualTo(true);
  }

  private void assertRefused(final String query, final String procedure) {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("opencypher", query)) {
        rs.stream().count();
      }
    }).as(procedure).hasStackTraceContaining(procedure + "(): the edge weights of 'w' " + REFUSAL);
  }

  private Map<RID, Double> pageRank(final CommandContext context) {
    final Map<RID, Double> scores = new HashMap<>();
    drain(new AlgoPageRank().execute(new Object[] { Map.of("weightProperty", "w") }, null, context)).forEach(
        r -> scores.put(r.getProperty("node"), ((Number) r.getProperty("score")).doubleValue()));
    return scores;
  }

  private Map<RID, Object[]> louvain(final CommandContext context) {
    final Map<RID, Object[]> rows = new HashMap<>();
    drain(new AlgoLouvain().execute(new Object[] { Map.of("weightProperty", "w") }, null, context)).forEach(
        r -> rows.put(r.getProperty("node"),
            new Object[] { r.getProperty("communityId"), ((Number) r.getProperty("modularity")).doubleValue() }));
    return rows;
  }

  private Map<RID, Object[]> maxKCut(final CommandContext context) {
    final Map<RID, Object[]> rows = new HashMap<>();
    drain(new AlgoMaxKCut().execute(new Object[] { 2, Map.of("weightProperty", "w", "seed", 1) }, null, context)).forEach(
        r -> rows.put(r.getProperty("node"),
            new Object[] { r.getProperty("community"), ((Number) r.getProperty("cutWeight")).doubleValue() }));
    return rows;
  }

  private static List<Result> drain(final Stream<Result> rows) {
    final List<Result> results = new ArrayList<>();
    for (final Iterator<Result> it = rows.iterator(); it.hasNext(); )
      results.add(it.next());
    return results;
  }
}
