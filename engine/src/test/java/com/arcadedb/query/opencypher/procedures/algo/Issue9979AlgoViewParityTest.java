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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #9979 (#9829: #9767, #9791): the answer of an {@code algo.*} procedure is a function of its arguments and of the
 * data, never of whether a Graph Analytical View happens to cover the types. Each procedure that has both an accelerated
 * and an OLTP path is run with and without a view, for every value of every option it accepts, and the two answers must
 * agree. {@code algo.labelpropagation} dropped {@code direction} on the accelerated path, {@code algo.pagerank} dropped
 * {@code tolerance}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9979AlgoViewParityTest extends TestHelper {
  private static final String   VIEW       = "parity";
  private static final String[] DIRECTIONS = { "OUT", "IN", "BOTH" };

  /**
   * The graph of #9767: 7 vertices, edges 0->1, 0->2, 0->3, 4->5, 6->4. Without a view the three directions answer three
   * different partitions, and the view must not turn OUT and IN into BOTH. Ties are broken on {@code k}: without a
   * tie-break property each path breaks them on its own internal node order, which the two need not share.
   */
  @Test
  void labelPropagationHonoursDirectionUnderAView() throws Exception {
    buildGraph(7, new int[][] { { 0, 1 }, { 0, 2 }, { 0, 3 }, { 4, 5 }, { 6, 4 } });

    final Map<String, Set<Set<Integer>>> expected = Map.of(//
        "BOTH", Set.of(Set.of(0), Set.of(1, 2, 3), Set.of(4), Set.of(5, 6)),//
        "OUT", Set.of(Set.of(0, 1), Set.of(2), Set.of(3), Set.of(4, 5, 6)),//
        "IN", Set.of(Set.of(0, 1, 2, 3), Set.of(4, 5, 6)));

    final Map<String, Set<Set<Integer>>> withoutView = new HashMap<>();
    for (final String direction : DIRECTIONS)
      withoutView.put(direction, partition(run("algo.labelpropagation", false, Map.of("direction", direction, "tieBreakProperty", "k"))));
    assertThat(withoutView).isEqualTo(expected);

    createView();
    for (final String direction : DIRECTIONS)
      assertThat(partition(run("algo.labelpropagation", true, Map.of("direction", direction, "tieBreakProperty", "k"))))
          .as("direction %s under a view", direction).isEqualTo(expected.get(direction));
  }

  /**
   * The graph of #9791: with {@code tolerance: 0.5} the OLTP path stops after one iteration, the accelerated path used to
   * run all of them whatever the tolerance.
   */
  @Test
  void pageRankHonoursToleranceUnderAView() throws Exception {
    buildGraph(8, new int[][] { { 0, 1 }, { 1, 2 }, { 2, 0 }, { 1, 3 }, { 3, 4 }, { 4, 5 }, { 5, 3 }, { 0, 5 }, { 2, 6 }, { 6, 1 }, { 3, 6 },
        { 6, 7 }, { 0, 7 } });

    final List<Map<String, Object>> configs = new ArrayList<>();
    for (final String direction : DIRECTIONS)
      for (final double tolerance : new double[] { 0.9, 0.5, 0.01, 0.0001, 0.0 })
        configs.add(Map.of("direction", direction, "tolerance", tolerance));
    configs.add(Map.of("direction", "OUT", "tolerance", 0.0, "maxIterations", 3));
    configs.add(Map.of("direction", "IN", "dampingFactor", 0.5, "tolerance", 0.001));
    configs.add(Map.of());

    final List<Map<Integer, Double>> withoutView = new ArrayList<>();
    for (final Map<String, Object> config : configs)
      withoutView.add(scores(run("algo.pagerank", false, config)));

    // the reporter's numbers: one iteration, three nodes tied for last
    final Map<Integer, Double> oneIteration = withoutView.getFirst();
    assertThat(oneIteration.get(0)).isCloseTo(0.08516, within(1e-5));
    assertThat(oneIteration.get(4)).isCloseTo(0.08516, within(1e-5));
    assertThat(oneIteration.get(3)).isCloseTo(0.19141, within(1e-5));

    createView();
    for (int i = 0; i < configs.size(); i++)
      assertScoresEqual(scores(run("algo.pagerank", true, configs.get(i))), withoutView.get(i), configs.get(i));
  }

  /** The in-package control of #9791: ArticleRank already forwarded its tolerance, and keeps doing so. */
  @Test
  void articleRankAgreesUnderAView() throws Exception {
    buildGraph(8, new int[][] { { 0, 1 }, { 1, 2 }, { 2, 0 }, { 1, 3 }, { 3, 4 }, { 4, 5 }, { 5, 3 }, { 0, 5 }, { 2, 6 }, { 6, 1 }, { 3, 6 },
        { 6, 7 }, { 0, 7 } });

    final List<Map<String, Object>> configs = List.of(Map.of("tolerance", 0.5), Map.of("tolerance", 0.0001),
        Map.of("tolerance", 0.0, "maxIterations", 4), Map.of());
    final List<Map<Integer, Double>> withoutView = new ArrayList<>();
    for (final Map<String, Object> config : configs)
      withoutView.add(scores(run("algo.articlerank", false, config)));

    createView();
    for (int i = 0; i < configs.size(); i++)
      assertScoresEqual(scores(run("algo.articlerank", true, configs.get(i))), withoutView.get(i), configs.get(i));
  }

  private static void assertScoresEqual(final Map<Integer, Double> actual, final Map<Integer, Double> expected, final Map<String, Object> config) {
    assertThat(actual.keySet()).as("%s", config).isEqualTo(expected.keySet());
    for (final Map.Entry<Integer, Double> entry : expected.entrySet())
      assertThat(actual.get(entry.getKey())).as("node %d, config %s", entry.getKey(), config).isCloseTo(entry.getValue(), within(1e-9));
  }

  private void buildGraph(final int vertices, final int[][] edges) {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE PROPERTY N.k INTEGER");
    database.command("sql", "CREATE EDGE TYPE R");
    database.transaction(() -> {
      final MutableVertex[] v = new MutableVertex[vertices];
      for (int i = 0; i < vertices; i++)
        v[i] = database.newVertex("N").set("k", i).save();
      for (final int[] edge : edges)
        v[edge[0]].newEdge("R", v[edge[1]]);
    });
  }

  private void createView() throws Exception {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + VIEW + " VERTEX TYPES (N) EDGE TYPES (R)");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW);
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
  }

  /** Runs the procedure, asserting it took the path the test expects, and returns its rows keyed by the vertex's k. */
  private Map<Integer, Object> run(final String name, final boolean expectAccelerated, final Map<String, Object> config) {
    final CypherProcedure procedure = CypherProcedureRegistry.get(name);
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final String valueField = procedure.getYieldFields().get(1);
    final Map<Integer, Object> byK = new TreeMap<>();
    try (final Stream<Result> rows = procedure.execute(new Object[] { config }, null, context)) {
      rows.forEach(row -> byK.put(database.lookupByRID(row.<RID>getProperty("node"), true).asVertex().getInteger("k"),
          row.getProperty(valueField)));
    }
    assertThat(Boolean.TRUE.equals(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))).as("%s %s accelerated", name, config)
        .isEqualTo(expectAccelerated);
    return byK;
  }

  private static Map<Integer, Double> scores(final Map<Integer, Object> rows) {
    final Map<Integer, Double> scores = new TreeMap<>();
    rows.forEach((k, v) -> scores.put(k, ((Number) v).doubleValue()));
    return scores;
  }

  /** Community ids differ between the two paths (each numbers nodes its own way): the partition is what must agree. */
  private static Set<Set<Integer>> partition(final Map<Integer, Object> rows) {
    final Map<Object, Set<Integer>> communities = new HashMap<>();
    rows.forEach((k, community) -> communities.computeIfAbsent(community, c -> new TreeSet<>()).add(k));
    return communities.values().stream().map(Set::copyOf).collect(Collectors.toSet());
  }
}
