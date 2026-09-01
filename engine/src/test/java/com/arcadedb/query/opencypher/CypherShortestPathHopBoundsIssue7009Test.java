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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/7009
 * <p>
 * {@code shortestPath()} and {@code allShortestPaths()} must honour the {@code *min..max} hop bounds
 * declared on the pattern relationship. They used to be parsed and then dropped, so
 * {@code shortestPath((s)-[:LINK*..2]-(e))} happily returned a 4-hop path and answered exactly like the
 * unbounded {@code [:LINK*]} spelling.
 * <p>
 * The four evaluators that had to be fixed are all exercised here, because a fix in one is invisible to
 * the others: the unconstrained {@code MATCH p = shortestPath(...)} path (which delegates to
 * {@code SQLFunctionShortestPath}), the unconstrained {@code allShortestPaths()} layered BFS, the
 * edge-aware BFS both take when the relationship carries an inline property map or WHERE, and the
 * {@code RETURN shortestPath(...)} expression form.
 */
class CypherShortestPathHopBoundsIssue7009Test extends TestHelper {
  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("N").createProperty("k", com.arcadedb.schema.Type.STRING);
    database.getSchema().createEdgeType("LINK");

    // Graph (every LINK carries w=1, so an inline {w: 1} filter changes the evaluator without changing
    // reachability):
    //
    //   a -> n1 -> n2 -> n3 -> b        the only a..b path is 4 hops
    //   c -> d1 -> f, c -> d2 -> f      two co-shortest 2-hop c..f paths
    //   g -> h                          a single 1-hop path
    database.transaction(() -> {
      final MutableVertex a = node("a");
      final MutableVertex n1 = node("n1");
      final MutableVertex n2 = node("n2");
      final MutableVertex n3 = node("n3");
      final MutableVertex b = node("b");
      link(a, n1);
      link(n1, n2);
      link(n2, n3);
      link(n3, b);

      final MutableVertex c = node("c");
      final MutableVertex d1 = node("d1");
      final MutableVertex d2 = node("d2");
      final MutableVertex f = node("f");
      link(c, d1);
      link(d1, f);
      link(c, d2);
      link(d2, f);

      link(node("g"), node("h"));
    });
  }

  private MutableVertex node(final String key) {
    return database.newVertex("N").set("k", key).save();
  }

  private void link(final MutableVertex from, final MutableVertex to) {
    from.newEdge("LINK", to, true, new Object[] { "w", 1 }).save();
  }

  // ---------------------------------------------------------------------------------------------
  // shortestPath(), unconstrained relationship (SQLFunctionShortestPath path)
  // ---------------------------------------------------------------------------------------------

  @Test
  void shortestPathRejectsAPathLongerThanTheMaxHopBound() {
    assertThat(lengths("MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = shortestPath((s)-[:LINK*..2]-(e)) RETURN length(p) AS len"))
        .as("the only a..b path is 4 hops, so [:LINK*..2] must return nothing")
        .isEmpty();
  }

  @Test
  void shortestPathUnboundedControlStillReturnsTheFourHopPath() {
    assertThat(lengths("MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = shortestPath((s)-[:LINK*]-(e)) RETURN length(p) AS len"))
        .as("control: the unbounded spelling still finds the 4-hop path")
        .containsExactly(4);
  }

  @Test
  void shortestPathAcceptsAPathThatFitsTheMaxHopBound() {
    assertThat(lengths("MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = shortestPath((s)-[:LINK*..4]-(e)) RETURN length(p) AS len"))
        .as("a bound wide enough for the 4-hop path must not suppress it")
        .containsExactly(4);
    assertThat(lengths("MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = shortestPath((s)-[:LINK*..3]-(e)) RETURN length(p) AS len"))
        .as("one hop short of the only path is still no path")
        .isEmpty();
  }

  @Test
  void shortestPathRejectsAPathShorterThanTheMinHopBound() {
    assertThat(lengths("MATCH (s:N {k:'g'}), (e:N {k:'h'}), p = shortestPath((s)-[:LINK*3..]-(e)) RETURN length(p) AS len"))
        .as("the only g..h path is 1 hop, so [:LINK*3..] must return nothing")
        .isEmpty();
    assertThat(lengths("MATCH (s:N {k:'g'}), (e:N {k:'h'}), p = shortestPath((s)-[:LINK*]-(e)) RETURN length(p) AS len"))
        .as("control: the unbounded spelling still finds the 1-hop path")
        .containsExactly(1);
  }

  @Test
  void shortestPathHonoursAnExactHopCount() {
    assertThat(lengths("MATCH (s:N {k:'c'}), (e:N {k:'f'}), p = shortestPath((s)-[:LINK*2]-(e)) RETURN length(p) AS len"))
        .as("[:LINK*2] matches the 2-hop c..f path")
        .containsExactly(2);
    assertThat(lengths("MATCH (s:N {k:'c'}), (e:N {k:'f'}), p = shortestPath((s)-[:LINK*3]-(e)) RETURN length(p) AS len"))
        .as("[:LINK*3] does not, because the shortest c..f path is 2 hops")
        .isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // allShortestPaths(), unconstrained relationship (layered BFS path)
  // ---------------------------------------------------------------------------------------------

  @Test
  void allShortestPathsRejectsPathsLongerThanTheMaxHopBound() {
    assertThat(lengths("MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = allShortestPaths((s)-[:LINK*..2]-(e)) RETURN length(p) AS len"))
        .as("the only a..b path is 4 hops, so [:LINK*..2] must return nothing")
        .isEmpty();
    assertThat(lengths("MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = allShortestPaths((s)-[:LINK*]-(e)) RETURN length(p) AS len"))
        .as("control: the unbounded spelling still finds the 4-hop path")
        .containsExactly(4);
  }

  @Test
  void allShortestPathsKeepsEveryCoShortestPathThatFitsTheBound() {
    assertThat(lengths("MATCH (s:N {k:'c'}), (e:N {k:'f'}), p = allShortestPaths((s)-[:LINK*..2]-(e)) RETURN length(p) AS len"))
        .as("both 2-hop c..f paths fit a bound of 2")
        .containsExactly(2, 2);
    assertThat(lengths("MATCH (s:N {k:'c'}), (e:N {k:'f'}), p = allShortestPaths((s)-[:LINK*..1]-(e)) RETURN length(p) AS len"))
        .as("neither fits a bound of 1")
        .isEmpty();
  }

  @Test
  void allShortestPathsRejectsPathsShorterThanTheMinHopBound() {
    assertThat(lengths("MATCH (s:N {k:'g'}), (e:N {k:'h'}), p = allShortestPaths((s)-[:LINK*3..]-(e)) RETURN length(p) AS len"))
        .as("the only g..h path is 1 hop, so [:LINK*3..] must return nothing")
        .isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // Edge-aware BFS (an inline property map routes both forms away from SQLFunctionShortestPath)
  // ---------------------------------------------------------------------------------------------

  @Test
  void shortestPathWithAnInlineEdgeFilterAlsoHonoursTheMaxHopBound() {
    assertThat(lengths(
        "MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = shortestPath((s)-[:LINK*..2 {w: 1}]-(e)) RETURN length(p) AS len"))
        .as("the edge-aware BFS must apply the bound too")
        .isEmpty();
    assertThat(lengths(
        "MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = shortestPath((s)-[:LINK* {w: 1}]-(e)) RETURN length(p) AS len"))
        .as("control: every LINK carries w=1, so the unbounded filtered form still finds the 4-hop path")
        .containsExactly(4);
  }

  @Test
  void allShortestPathsWithAnInlineEdgeFilterAlsoHonoursTheMaxHopBound() {
    assertThat(lengths(
        "MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = allShortestPaths((s)-[:LINK*..2 {w: 1}]-(e)) RETURN length(p) AS len"))
        .isEmpty();
    assertThat(lengths(
        "MATCH (s:N {k:'c'}), (e:N {k:'f'}), p = allShortestPaths((s)-[:LINK*..2 {w: 1}]-(e)) RETURN length(p) AS len"))
        .as("control: both filtered co-shortest paths fit a bound of 2")
        .containsExactly(2, 2);
  }

  @Test
  void shortestPathWithAnInlineEdgeWhereAlsoHonoursTheMinHopBound() {
    assertThat(lengths(
        "MATCH (s:N {k:'g'}), (e:N {k:'h'}), p = shortestPath((s)-[r:LINK*3.. WHERE r.w = 1]-(e)) RETURN length(p) AS len"))
        .isEmpty();
    assertThat(lengths(
        "MATCH (s:N {k:'g'}), (e:N {k:'h'}), p = shortestPath((s)-[r:LINK* WHERE r.w = 1]-(e)) RETURN length(p) AS len"))
        .as("control: the unbounded filtered form still finds the 1-hop path")
        .containsExactly(1);
  }

  // ---------------------------------------------------------------------------------------------
  // Expression form: RETURN shortestPath(...)
  // ---------------------------------------------------------------------------------------------

  @Test
  void shortestPathExpressionHonoursTheMaxHopBound() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:N {k:'a'}), (e:N {k:'b'}) RETURN shortestPath((s)-[:LINK*..2]-(e)) AS p")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("p"))
          .as("a 4-hop path does not satisfy [:LINK*..2], so the expression yields null")
          .isNull();
    }

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:N {k:'a'}), (e:N {k:'b'}) RETURN shortestPath((s)-[:LINK*]-(e)) AS p")) {
      assertThat(rs.hasNext()).isTrue();
      final Object path = rs.next().getProperty("p");
      assertThat(path).as("control: the unbounded expression form still finds the path").isInstanceOf(List.class);
      // 5 vertices interleaved with 4 edges.
      assertThat((List<?>) path).hasSize(9);
    }
  }

  @Test
  void shortestPathExpressionWithAnInlineEdgeFilterHonoursTheMaxHopBound() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:N {k:'a'}), (e:N {k:'b'}) RETURN shortestPath((s)-[:LINK*..2 {w: 1}]-(e)) AS p")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("p")).isNull();
    }
  }

  @Test
  void allShortestPathsExpressionHonoursTheMaxHopBound() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:N {k:'a'}), (e:N {k:'b'}) RETURN allShortestPaths((s)-[:LINK*..2]-(e)) AS p")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<List<?>>getProperty("p"))
          .as("no path satisfies the bound, so the expression yields an empty list")
          .isEmpty();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Shapes that must keep working exactly as before
  // ---------------------------------------------------------------------------------------------

  @Test
  void aZeroLengthSelfPathIsStillReturned() {
    // The endpoints resolving to the same vertex short-circuits to the zero-length path before any hop
    // bound applies, which is what MATCH p = shortestPath((a)-[:KNOWS*]-(a)) has always answered.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = shortestPath((s)-[:LINK*]-(e)) RETURN length(p) AS len")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("len").intValue()).isZero();
    }
  }

  @Test
  void theMatchFormStillPlansAsAShortestPathStep() {
    // The optimizer declines every ShortestPathPattern and falls back to the traditional plan, so the
    // bound has exactly one evaluator to reach. Pin that, otherwise a future optimizer capability could
    // route these queries past the fix without any test noticing.
    try (final ResultSet rs = database.query("opencypher",
        "EXPLAIN MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = shortestPath((s)-[:LINK*..2]-(e)) RETURN p")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("executionPlanAsString").toString()).contains("SHORTEST PATH");
    }
  }

  private List<Integer> lengths(final String query) {
    final List<Integer> lengths = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Number len = row.getProperty("len");
        lengths.add(len == null ? null : len.intValue());
      }
    }
    return lengths;
  }
}
