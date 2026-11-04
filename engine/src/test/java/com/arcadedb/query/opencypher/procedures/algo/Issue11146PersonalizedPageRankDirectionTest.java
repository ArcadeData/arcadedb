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
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #11146: {@code algo.personalizedPageRank} walked OUT only and read no edge weight, so a vertex nothing points at yet
 * (every record just written, linking to existing ones) scored 0 from any source, and a similarity graph could not weigh its
 * edges. A config map as second argument now takes {@code direction} and {@code weightProperty}, under the names and values
 * of {@code algo.pagerank}, with OUT as default so the positional form answers as before.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11146PersonalizedPageRankDirectionTest extends TestHelper {

  @Test
  void theReportedVertexWithOnlyOutgoingEdgesIsReachedWithBoth() throws Exception {
    // in -> source -> out
    buildGraph(new String[] { "in", "source", "out" }, new String[][] { { "in", "source", "1" }, { "source", "out", "1" } });
    for (final boolean view : new boolean[] { false, true }) {
      if (view)
        createView();

      final Map<String, Double> outDefault = scores("source", null);
      assertThat(outDefault.get("in")).as("OUT, the default, as before (view=%s)", view).isEqualTo(0.0);
      assertThat(outDefault.get("out")).isGreaterThan(0.0);
      assertThat(scores("source", "{direction: 'OUT'}")).isEqualTo(outDefault);

      final Map<String, Double> both = scores("source", "{direction: 'BOTH'}");
      assertThat(both.get("in")).as("view=%s", view).isGreaterThan(0.0);
      assertThat(both.get("in")).isCloseTo(both.get("out"), within(1e-9));
      assertThat(sum(both)).isCloseTo(1.0, within(1e-6));

      final Map<String, Double> in = scores("source", "{direction: 'in'}");
      assertThat(in.get("in")).as("view=%s", view).isGreaterThan(0.0);
      assertThat(in.get("out")).isEqualTo(0.0);
      assertThat(in.get("in")).isCloseTo(outDefault.get("out"), within(1e-9));
    }
  }

  @Test
  void edgeWeightsSplitTheScoreInProportion() throws Exception {
    // source -> a (3), source -> b (1)
    buildGraph(new String[] { "source", "a", "b" }, new String[][] { { "source", "a", "3" }, { "source", "b", "1" } });
    for (final boolean view : new boolean[] { false, true }) {
      if (view)
        createView();
      final Map<String, Double> unweighted = scores("source", "{}");
      assertThat(unweighted.get("a")).isCloseTo(unweighted.get("b"), within(1e-9));

      final Map<String, Double> weighted = scores("source", "{weightProperty: 'w'}");
      assertThat(weighted.get("a")).as("view=%s", view).isCloseTo(3.0 * weighted.get("b"), within(1e-9));
      assertThat(weighted.get("source")).isCloseTo(unweighted.get("source"), within(1e-9));
      assertThat(sum(weighted)).isCloseTo(1.0, within(1e-6));
    }
  }

  @Test
  void weightsCombineWithDirection() {
    // a -> source (3), b -> source (1): only reached walking backwards
    buildGraph(new String[] { "source", "a", "b" }, new String[][] { { "a", "source", "3" }, { "b", "source", "1" } });
    final Map<String, Double> out = scores("source", "{weightProperty: 'w'}");
    assertThat(out.get("a")).isEqualTo(0.0);
    final Map<String, Double> in = scores("source", "{weightProperty: 'w', direction: 'IN'}");
    assertThat(in.get("a")).isCloseTo(3.0 * in.get("b"), within(1e-9));
  }

  @Test
  void configMapCarriesEveryKnobOfThePositionalForm() {
    buildGraph(new String[] { "a", "b", "c", "d" }, new String[][] { { "a", "b", "1" }, { "b", "c", "1" }, { "c", "d", "1" } });
    assertThat(scores("a", "{relTypes: 'E', dampingFactor: 0.5, maxIterations: 7, tolerance: 0.0000001}"))
        .isEqualTo(scoresOf("MATCH (s:N {k: 'a'}) CALL algo.personalizedPageRank(s, 'E', 0.5, 7, 0.0000001) YIELD nodeId, score "
            + "RETURN nodeId.k AS k, score"));
  }

  @Test
  void invalidConfigurationsAreRefused() {
    buildGraph(new String[] { "a", "b" }, new String[][] { { "a", "b", "-1" } });
    assertThatThrownBy(() -> scores("a", "{directon: 'BOTH'}")).hasMessageContaining("unknown config key 'directon'");
    assertThatThrownBy(() -> scores("a", "{direction: 'INCOMING'}")).hasMessageContaining("unknown direction 'INCOMING'");
    assertThatThrownBy(() -> scores("a", "{weightProperty: 'w'}")).hasMessageContaining("finite and non-negative");
    assertThatThrownBy(() -> scores("a", "{dampingFactor: 'high'}")).hasMessageContaining("dampingFactor must be a number");
    assertThatThrownBy(() -> scoresOf("MATCH (s:N {k: 'a'}) CALL algo.personalizedPageRank(s, {direction: 'BOTH'}, 0.85) "
        + "YIELD nodeId, score RETURN nodeId.k AS k, score")).hasMessageContaining("there is no third one");
  }

  private void buildGraph(final String[] names, final String[][] edges) {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE E");
    database.transaction(() -> {
      final Map<String, MutableVertex> byName = new TreeMap<>();
      for (final String name : names)
        byName.put(name, database.newVertex("N").set("k", name).save());
      for (final String[] edge : edges)
        byName.get(edge[0]).newEdge("E", byName.get(edge[1]), "w", Double.parseDouble(edge[2]));
    });
  }

  private void createView() throws Exception {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW ppr11146 VERTEX TYPES (N) EDGE TYPES (E)");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "ppr11146");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
  }

  private Map<String, Double> scores(final String source, final String config) {
    return scoresOf("MATCH (s:N {k: '" + source + "'}) CALL algo.personalizedPageRank(s" + (config != null ? ", " + config : "")
        + ") YIELD nodeId, score RETURN nodeId.k AS k, score");
  }

  private Map<String, Double> scoresOf(final String cypher) {
    final Map<String, Double> byK = new TreeMap<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        byK.put(row.getProperty("k"), ((Number) row.getProperty("score")).doubleValue());
      }
    }
    return byK;
  }

  private static double sum(final Map<String, Double> scores) {
    return scores.values().stream().mapToDouble(Double::doubleValue).sum();
  }
}
