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
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/7017, the shape issue #7009 left
 * outside its bound check on purpose.
 * <p>
 * When both endpoints of a {@code shortestPath()} resolve to the SAME vertex, the answer is the
 * zero-length path, and every evaluator produced it before consulting the {@code *min..max} bounds - so
 * {@code shortestPath((a)-[:LINK*3..5]-(a))} answered with a path of length 0, which is not in [3, 5],
 * and a declared minimum was silently unenforceable for exactly this one endpoint pair.
 * <p>
 * The rule, implemented by {@code ShortestPathStep.HopBounds.accepts(0)} for every evaluator: the zero-length path answers
 * only a pattern whose minimum is zero ({@code [*0..]}). Zero hops is not in the declared range of any other pattern, so it
 * has no answer for that pair, and the pattern is NOT re-interpreted as a request for the shortest cycle back to the
 * source - the same substitution the bound check already refuses for distinct endpoints, where a shortest path below the
 * declared minimum yields no row rather than a longer one that would fit.
 * <p>
 * Issue #7017 first kept the zero-length path for a minimum of one hop ({@code [*]}, {@code [*1..]}, an unquantified
 * relationship), on the belief that Neo4j answers those with it; Neo4j 5 refuses them instead ("The shortest path algorithm
 * does not work when the start and end nodes are the same"), and a path shorter than the declared minimum is not an
 * answer, so issue #11913 applies the minimum to them too.
 * <p>
 * The graph deliberately contains a 3-hop cycle through {@code a} and a self loop on {@code d}, so the "no answer" cases
 * below are choosing not to return a cycle that exists rather than failing to find one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherShortestPathSelfPathBoundsIssue7017Test extends TestHelper {
  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("N").createProperty("k", Type.STRING);
    database.getSchema().createEdgeType("LINK");

    // a -> b -> c -> a : a 3-hop cycle through every one of its vertices. Every LINK carries w=1, so an
    // inline {w: 1} filter routes the query to the edge-aware BFS without changing reachability.
    // d -> d : a real one-hop self loop, so the zero-length answer can be told apart from "found nothing".
    database.transaction(() -> {
      final MutableVertex a = node("a");
      final MutableVertex b = node("b");
      final MutableVertex c = node("c");
      link(a, b);
      link(b, c);
      link(c, a);

      final MutableVertex d = node("d");
      link(d, d);
    });
  }

  private MutableVertex node(final String key) {
    return database.newVertex("N").set("k", key).save();
  }

  private void link(final MutableVertex from, final MutableVertex to) {
    from.newEdge("LINK", to, true, new Object[] { "w", 1 }).save();
  }

  // ---------------------------------------------------------------------------------------------
  // shortestPath(), MATCH form
  // ---------------------------------------------------------------------------------------------

  @Test
  void aMinimumAboveOneHopRejectsTheZeroLengthSelfPath() {
    assertThat(selfLengths("MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = shortestPath((s)-[:LINK*3..5]-(e)) RETURN length(p) AS len"))
        .as("a zero-length path is not in [3, 5], and the 3-hop cycle is not a shortest path answer either")
        .isEmpty();
    assertThat(selfLengths("MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = shortestPath((s)-[:LINK*2..]-(e)) RETURN length(p) AS len"))
        .as("two hops is above the minimum, and the 3-hop cycle is not answered instead")
        .isEmpty();
  }

  @Test
  void anImplicitOrExplicitMinimumOfOneRejectsTheZeroLengthSelfPath() {
    assertThat(selfLengths("MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = shortestPath((s)-[:LINK*]-(e)) RETURN length(p) AS len"))
        .as("[*] is lowered to minHops = 1, and a zero-length path is shorter than that (issue #11913)")
        .isEmpty();
    assertThat(selfLengths("MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = shortestPath((s)-[:LINK*1..]-(e)) RETURN length(p) AS len"))
        .as("an explicitly written [*1..] is the same pattern as [*], so it must answer identically")
        .isEmpty();
    assertThat(selfLengths("MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = shortestPath((s)-[:LINK*0..]-(e)) RETURN length(p) AS len"))
        .as("a minimum of zero admits the zero-length path outright")
        .containsExactly(0);
  }

  @Test
  void anExactQuantifierAnswersByItsMinimumLikeAnyOther() {
    // An upper bound cannot exclude a zero-length path - it is at least zero hops long - so a quantifier is decided by
    // its minimum alone.
    assertThat(selfLengths("MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = shortestPath((s)-[:LINK*1..1]-(e)) RETURN length(p) AS len"))
        .isEmpty();
    assertThat(selfLengths("MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = shortestPath((s)-[:LINK]-(e)) RETURN length(p) AS len"))
        .as("an unquantified relationship declares the same single hop (issue #7009), so it answers the same")
        .isEmpty();
    assertThat(selfLengths("MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = shortestPath((s)-[:LINK*2..2]-(e)) RETURN length(p) AS len"))
        .as("a minimum of two rejects it, upper bound or not")
        .isEmpty();
    assertThat(selfLengths("MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = shortestPath((s)-[:LINK*0..1]-(e)) RETURN length(p) AS len"))
        .as("a minimum of zero admits it, upper bound or not")
        .containsExactly(0);
  }

  @Test
  void aRealSelfLoopIsNotAnsweredInstead() {
    // d carries a one-hop LINK to itself, so a 1-hop path from d to d exists. It is a cycle back to the source, not the
    // shortest path between the endpoints, so the patterns that reject the zero-length path have no answer rather than
    // that loop; a minimum of zero still answers the zero-length path, which is the shorter one.
    assertThat(selfLengths("MATCH (s:N {k:'d'}), (e:N {k:'d'}), p = shortestPath((s)-[:LINK]-(e)) RETURN length(p) AS len"))
        .isEmpty();
    assertThat(selfLengths("MATCH (s:N {k:'d'}), (e:N {k:'d'}), p = shortestPath((s)-[:LINK*]-(e)) RETURN length(p) AS len"))
        .isEmpty();
    assertThat(selfLengths("MATCH (s:N {k:'d'}), (e:N {k:'d'}), p = shortestPath((s)-[:LINK*2..]-(e)) RETURN length(p) AS len"))
        .isEmpty();
    assertThat(selfLengths("MATCH (s:N {k:'d'}), (e:N {k:'d'}), p = shortestPath((s)-[:LINK*0..]-(e)) RETURN length(p) AS len"))
        .containsExactly(0);
  }

  @Test
  void theExplainedPatternKeepsTheSpellingItWasWrittenIn() {
    // [*] is lowered to minHops = 1 internally; EXPLAIN must not render the commonest pattern of all as
    // "*1..", a spelling nobody writes it by.
    assertThat(explain("MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = shortestPath((s)-[:LINK*]-(e)) RETURN p"))
        .contains("[:LINK*]");
    assertThat(explain("MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = shortestPath((s)-[:LINK*2..3]-(e)) RETURN p"))
        .as("a pattern that really carries bounds still shows them")
        .contains("[:LINK*2..3]");
    assertThat(explain("MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = shortestPath((s)-[:LINK*0..]-(e)) RETURN p"))
        .as("a written zero minimum is a different pattern from [*] - the one that admits the zero-length "
            + "path outright - so it keeps its lower bound on show")
        .contains("[:LINK*0..]");
    assertThat(explain("MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = shortestPath((s)-[:LINK]-(e)) RETURN p"))
        .as("and one with no quantifier shows none")
        .contains("[:LINK]");
  }

  @Test
  void distinctEndpointsAreUnaffected() {
    assertThat(selfLengths("MATCH (s:N {k:'a'}), (e:N {k:'b'}), p = shortestPath((s)-[:LINK*]-(e)) RETURN length(p) AS len"))
        .as("control: the self-path rule must not touch a genuine two-endpoint search")
        .containsExactly(1);
  }

  // ---------------------------------------------------------------------------------------------
  // allShortestPaths(), MATCH form (layered BFS)
  // ---------------------------------------------------------------------------------------------

  @Test
  void allShortestPathsAppliesTheSameSelfPathRule() {
    assertThat(selfLengths(
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = allShortestPaths((s)-[:LINK*3..5]-(e)) RETURN length(p) AS len"))
        .isEmpty();
    assertThat(selfLengths(
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = allShortestPaths((s)-[:LINK*]-(e)) RETURN length(p) AS len"))
        .as("[*] declares at least one hop (issue #11913)")
        .isEmpty();
    assertThat(selfLengths(
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = allShortestPaths((s)-[:LINK*0..]-(e)) RETURN length(p) AS len"))
        .as("control: a minimum of zero still yields the single zero-length path")
        .containsExactly(0);
  }

  // ---------------------------------------------------------------------------------------------
  // Edge-aware BFS (an inline property map / WHERE routes both forms away from SQLFunctionShortestPath)
  // ---------------------------------------------------------------------------------------------

  @Test
  void theEdgeAwareBfsAppliesTheSameSelfPathRule() {
    assertThat(selfLengths(
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = shortestPath((s)-[:LINK*3..5 {w: 1}]-(e)) RETURN length(p) AS len"))
        .isEmpty();
    assertThat(selfLengths(
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = shortestPath((s)-[r:LINK* WHERE r.w = 1]-(e)) RETURN length(p) AS len"))
        .isEmpty();
    assertThat(selfLengths(
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = shortestPath((s)-[r:LINK*0.. WHERE r.w = 1]-(e)) RETURN length(p) AS len"))
        .as("control: a minimum of zero answers zero-length through the filtered BFS too")
        .containsExactly(0);
    assertThat(selfLengths(
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = allShortestPaths((s)-[:LINK*3..5 {w: 1}]-(e)) RETURN length(p) AS len"))
        .isEmpty();
    assertThat(selfLengths(
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = allShortestPaths((s)-[:LINK* {w: 1}]-(e)) RETURN length(p) AS len"))
        .isEmpty();
    assertThat(selfLengths(
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}), p = allShortestPaths((s)-[:LINK*0.. {w: 1}]-(e)) RETURN length(p) AS len"))
        .containsExactly(0);
  }

  // ---------------------------------------------------------------------------------------------
  // Expression form: RETURN shortestPath(...)
  // ---------------------------------------------------------------------------------------------

  @Test
  void theExpressionFormAppliesTheSameSelfPathRule() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}) RETURN shortestPath((s)-[:LINK*3..5]-(e)) AS p")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("p"))
          .as("the zero-length path does not satisfy [3, 5], so the expression yields null")
          .isNull();
    }

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}) RETURN shortestPath((s)-[:LINK*]-(e)) AS p")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("p"))
          .as("[*] declares at least one hop, so the expression yields null (issue #11913)")
          .isNull();
    }

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}) RETURN shortestPath((s)-[:LINK*0..]-(e)) AS p")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<TraversalPath>getProperty("p").getVertices())
          .as("control: a minimum of zero yields the one-vertex, zero-length path")
          .hasSize(1);
    }
  }

  @Test
  void theAllShortestPathsExpressionFormAppliesTheSameSelfPathRule() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:N {k:'a'}), (e:N {k:'a'}) RETURN allShortestPaths((s)-[:LINK*3..5]-(e)) AS p")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<List<?>>getProperty("p")).isEmpty();
    }
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().<Object>getProperty("executionPlanAsString").toString();
    }
  }

  private List<Integer> selfLengths(final String query) {
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
