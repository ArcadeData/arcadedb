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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11145: {@code algo.labelpropagation} had no way to pre-assign a label, although the docs list semi-supervised
 * classification among its uses, and it accepted any config key, so {@code {seedProperty: 'label'}} and
 * {@code {typoProperty: 'label'}} both answered an ordinary unseeded run. It now reads seed labels from {@code seedProperty},
 * keeps them for the whole run with {@code fixedSeeds}, and refuses a key it does not know, as every {@code algo.*} procedure
 * with a config map now does.
 * <p>
 * The graph of the report: two triangles 0-1-2 and 3-4-5 joined by the edge 2-3.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11145LabelPropagationSeedsTest extends TestHelper {

  @Test
  void unknownConfigKeysAreRefused() {
    buildGraph();
    for (final String key : new String[] { "typoProperty", "labelProperty", "maxIteration" })
      assertThatThrownBy(() -> rows("{" + key + ": 'label'}")).hasMessageContaining("unknown config key '" + key + "'")
          .hasMessageContaining("seedProperty");
  }

  @Test
  void otherAlgoProceduresRefuseUnknownConfigKeysToo() {
    buildGraph();
    assertThatThrownBy(() -> consume("CALL algo.pagerank({dampingFactr: 0.5}) YIELD node, score RETURN node"))
        .hasMessageContaining("unknown config key 'dampingFactr'");
    assertThatThrownBy(() -> consume("CALL algo.louvain({weightProprety: 'w'}) YIELD node, communityId RETURN node"))
        .hasMessageContaining("unknown config key 'weightProprety'");
    // Every key a procedure documents is still accepted
    consume("CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 5, tolerance: 0.001, direction: 'BOTH'}) YIELD node RETURN node");
  }

  @Test
  void fixedSeedsClassifyTheRestOfTheGraph() throws Exception {
    buildGraph();
    database.command("opencypher", "MATCH (n:N) WHERE n.k = 0 SET n.label = 1");
    database.command("opencypher", "MATCH (n:N) WHERE n.k = 5 SET n.label = 2");

    final Map<Integer, Object> expected = Map.of(0, 1L, 1, 1L, 2, 1L, 3, 2L, 4, 2L, 5, 2L);
    assertThat(rows("{seedProperty: 'label', fixedSeeds: true}")).isEqualTo(expected);

    // The same answer when a view serves the call
    createView();
    assertThat(rows("{seedProperty: 'label', fixedSeeds: true}")).isEqualTo(expected);
  }

  @Test
  void theReportedGraphPutsBothSeedsOfLabel7InOneCommunity() throws Exception {
    buildGraph();
    database.command("opencypher", "MATCH (n:N) WHERE n.k IN [0, 5] SET n.label = 7");

    final Map<Integer, Object> seeded = rows("{seedProperty: 'label', fixedSeeds: true}");
    assertThat(seeded.get(0)).isEqualTo(7L);
    assertThat(seeded.get(5)).isEqualTo(7L);
    assertThat(seeded.values()).containsOnly(7L);

    createView();
    assertThat(rows("{seedProperty: 'label', fixedSeeds: true}")).isEqualTo(seeded);
  }

  @Test
  void unfixedSeedsAreStartingLabelsOnlyAndUnseededCommunitiesNumberPastTheLargestSeed() throws Exception {
    buildGraph();
    database.command("opencypher", "MATCH (n:N) WHERE n.k = 0 SET n.label = 100");

    assertOneUnfixedIteration(rows("{seedProperty: 'label', maxIterations: 1}"));
    createView();
    assertOneUnfixedIteration(rows("{seedProperty: 'label', maxIterations: 1}"));
  }

  /**
   * One iteration: every node adopts the most frequent label of its neighbours, the smallest on a tie. Every unseeded node
   * starts on a label of its own past the largest seed (100), so 1 and 2, which both see the seed once and no label twice,
   * adopt 100; the seed of node 0 does not hold it, since it is not fixed: 0 sees two unseeded labels and adopts one.
   */
  private static void assertOneUnfixedIteration(final Map<Integer, Object> oneIteration) {
    assertThat(oneIteration.get(1)).isEqualTo(100L);
    assertThat(oneIteration.get(2)).isEqualTo(100L);
    assertThat((Long) oneIteration.get(0)).isGreaterThan(100L);
    for (final Object communityId : oneIteration.values())
      assertThat((Long) communityId).isGreaterThanOrEqualTo(100L);
  }

  @Test
  void anUnseededRunAnswersAsBefore() {
    buildGraph();
    final Map<Integer, Object> plain = rows("{}");
    for (final Object communityId : plain.values())
      assertThat(communityId).as("the dense node index, an int as it always was").isInstanceOf(Integer.class);
    // A seed property no vertex carries is an unseeded run
    final Map<Integer, Object> noSeed = rows("{seedProperty: 'missing'}");
    final Map<Integer, Object> asLong = new TreeMap<>();
    plain.forEach((k, v) -> asLong.put(k, ((Integer) v).longValue()));
    assertThat(noSeed).isEqualTo(asLong);
  }

  @Test
  void seedsCombineWithTheTieBreakProperty() throws Exception {
    buildGraph();
    database.command("opencypher", "MATCH (n:N) WHERE n.k = 0 SET n.label = 1");
    database.command("opencypher", "MATCH (n:N) WHERE n.k = 5 SET n.label = 2");
    database.command("opencypher", "MATCH (n:N) SET n.r = 5 - n.k");
    // A label ranks by the smallest tie-break value among the nodes starting on it: on k the seed of node 0 wins every tie
    // and the seed of node 5 loses every one, so label 1 also takes node 3 (three labels once each, the smallest is node
    // 2's, which already carries 1) and then node 4. On r = 5 - k it is the mirror image.
    final Map<Integer, Object> byK = Map.of(0, 1L, 1, 1L, 2, 1L, 3, 1L, 4, 1L, 5, 2L);
    final Map<Integer, Object> byR = Map.of(0, 1L, 1, 2L, 2, 2L, 3, 2L, 4, 2L, 5, 2L);
    assertThat(rows("{seedProperty: 'label', fixedSeeds: true, tieBreakProperty: 'k'}")).isEqualTo(byK);
    assertThat(rows("{seedProperty: 'label', fixedSeeds: true, tieBreakProperty: 'r'}")).isEqualTo(byR);
    createView();
    assertThat(rows("{seedProperty: 'label', fixedSeeds: true, tieBreakProperty: 'k'}")).isEqualTo(byK);
    assertThat(rows("{seedProperty: 'label', fixedSeeds: true, tieBreakProperty: 'r'}")).isEqualTo(byR);
  }

  @Test
  void seedValuesAreNotNodeIndexes() throws Exception {
    // A seed far larger than the node count, and a negative one: label ids are assigned per distinct value, so neither is
    // read as an index, and the unseeded nodes are numbered past the largest seed whatever its sign
    buildGraph();
    database.command("opencypher", "MATCH (n:N) WHERE n.k = 0 SET n.label = 1000000000000");
    database.command("opencypher", "MATCH (n:N) WHERE n.k = 5 SET n.label = -3");
    final Map<Integer, Object> expected = Map.of(0, 1000000000000L, 1, 1000000000000L, 2, 1000000000000L, 3, -3L, 4, -3L, 5,
        -3L);
    assertThat(rows("{seedProperty: 'label', fixedSeeds: true}")).isEqualTo(expected);
    createView();
    assertThat(rows("{seedProperty: 'label', fixedSeeds: true}")).isEqualTo(expected);
  }

  @Test
  void invalidSeedConfigurationsAreRefused() {
    buildGraph();
    assertThatThrownBy(() -> rows("{fixedSeeds: true}")).hasMessageContaining("fixedSeeds needs seedProperty");
    assertThatThrownBy(() -> rows("{seedProperty: 'label', fixedSeeds: 'yes'}")).hasMessageContaining("fixedSeeds must be a boolean");

    database.command("opencypher", "MATCH (n:N) WHERE n.k = 0 SET n.label = 'spam'");
    assertThatThrownBy(() -> rows("{seedProperty: 'label'}")).hasMessageContaining("must hold an integer");
    database.command("opencypher", "MATCH (n:N) WHERE n.k = 0 SET n.label = 1.5");
    assertThatThrownBy(() -> rows("{seedProperty: 'label'}")).hasMessageContaining("must hold an integer");
    // An integral value of a floating type is an integer
    database.command("opencypher", "MATCH (n:N) WHERE n.k = 0 SET n.label = 3.0");
    assertThat(rows("{seedProperty: 'label', fixedSeeds: true}").get(0)).isEqualTo(3L);
  }

  private void buildGraph() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE PROPERTY N.k INTEGER");
    database.command("sql", "CREATE EDGE TYPE E");
    database.transaction(() -> {
      final MutableVertex[] v = new MutableVertex[6];
      for (int i = 0; i < 6; i++)
        v[i] = database.newVertex("N").set("k", i).save();
      for (final int[] edge : new int[][] { { 0, 1 }, { 1, 2 }, { 2, 0 }, { 3, 4 }, { 4, 5 }, { 5, 3 }, { 2, 3 } })
        v[edge[0]].newEdge("E", v[edge[1]]);
    });
  }

  private void createView() throws Exception {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW lpa11145 VERTEX TYPES (N) EDGE TYPES (E)");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "lpa11145");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
  }

  /** communityId keyed by the vertex's k. */
  private Map<Integer, Object> rows(final String config) {
    final Map<Integer, Object> byK = new TreeMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.labelpropagation(" + config + ") YIELD node, communityId RETURN node.k AS k, communityId")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        byK.put(((Number) row.getProperty("k")).intValue(), row.getProperty("communityId"));
      }
    }
    return byK;
  }

  private void consume(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
