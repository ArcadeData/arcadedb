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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #11904: a pattern with a node that has no label was always planned by the traditional step chain,
 * whose {@code MATCH NODE} seeks an index for an equality only, so a range or an IN list on an indexed property of the labelled
 * node scanned its whole label, and a pattern written from the unlabelled end scanned every vertex type. Such a pattern is now
 * planned by the optimizer when a labelled node offers it an index seek or range scan to start from, and the unlabelled node is
 * reached by an expansion that accepts any vertex. A pattern the optimizer would start from a scan of a whole label, or one with
 * no labelled node at all, keeps the traditional plan.
 * <p>
 * Differential: every statement is checked against the same statement after a leading {@code WITH 1 AS one}, which keeps it on
 * the traditional plan (a MATCH after a WITH), over a randomized graph with missing properties, values of several types and
 * bounds of several types.
 */
class CypherUnlabelledNodeOptimizerIssue11904Test extends TestHelper {
  private static final int VERTICES = 240;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE Q");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE L");
    database.command("sql", "CREATE PROPERTY P.id INTEGER");
    database.command("sql", "CREATE PROPERTY P.g INTEGER");
    database.command("sql", "CREATE PROPERTY P.name STRING");
    database.command("sql", "CREATE PROPERTY P.d DOUBLE");
    database.command("sql", "CREATE PROPERTY P.c1 INTEGER");
    database.command("sql", "CREATE PROPERTY P.c2 STRING");
    database.command("sql", "CREATE INDEX ON P (id) UNIQUE");
    database.command("sql", "CREATE INDEX ON P (g) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON P (name) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON P (d) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON P (c1, c2) NOTUNIQUE");

    final Random random = new Random(11904);
    final Object[] mixed = { 3, -7, 12L, 4.5, "4", "abc", "", true, null };
    final String[] names = { "a", "ab", "abc", "b", "ba", "10", "9", "Zed", "" };
    database.transaction(() -> {
      final List<Vertex> vertices = new ArrayList<>();
      for (int i = 0; i < VERTICES; i++) {
        final MutableVertex vertex = database.newVertex(i % 4 == 3 ? "Q" : "P");
        // a missing key on some vertices of each property
        if (i % 13 != 0)
          vertex.set("id", i);
        if (random.nextInt(6) != 0)
          vertex.set("g", random.nextInt(10));
        if (random.nextInt(5) != 0)
          vertex.set("name", names[random.nextInt(names.length)]);
        if (random.nextInt(5) != 0)
          vertex.set("d", random.nextInt(40) / 2.0);
        if (random.nextInt(4) != 0)
          vertex.set("c1", random.nextInt(8));
        vertex.set("c2", "k" + random.nextInt(3));
        // no declared type and no index: values of every kind, compared by the filter alone
        final Object x = mixed[random.nextInt(mixed.length)];
        if (x != null)
          vertex.set("x", x);
        vertex.save();
        vertices.add(vertex);
      }
      for (final Vertex from : vertices) {
        final int degree = random.nextInt(4);
        for (int k = 0; k < degree; k++)
          from.asVertex().newEdge(random.nextBoolean() ? "K" : "L", vertices.get(random.nextInt(vertices.size())));
      }
    });
  }

  /** The shapes of the issue: each is now planned by the optimizer, from the labelled node, through its index. */
  @Test
  void anUnlabelledNodeNoLongerKeepsThePatternFromItsIndex() {
    assertOptimized("MATCH (p:P)-[:K]->(q) WHERE p.id < 50 RETURN id(p) AS a, id(q) AS b", Map.of(), "NodeIndexRangeScan");
    assertOptimized("MATCH (p:P)-[:K]->(q)-[:K]->(r) WHERE p.id < 50 RETURN id(p) AS a, id(r) AS b", Map.of(), "NodeIndexRangeScan");
    assertOptimized("MATCH (q)-[:K]->(p:P) WHERE p.id < 50 RETURN id(p) AS a, id(q) AS b", Map.of(), "NodeIndexRangeScan");
    assertOptimized("MATCH (p:P) WHERE p.id < 50 MATCH (p)-[:K]->(q) RETURN id(p) AS a, id(q) AS b", Map.of(), "NodeIndexRangeScan");
    assertOptimized("MATCH (p:P)-[:K]->(q) WHERE p.id >= 10 AND p.id < 60 RETURN id(p) AS a, id(q) AS b", Map.of(),
        "NodeIndexRangeScan");
    assertOptimized("MATCH (p:P)-[:K]->(q) WHERE p.g IN [1, 2, 3] RETURN id(p) AS a, id(q) AS b", Map.of(), "NodeIndexSeek");
    assertOptimized("MATCH (p:P)-[:K]->(q) WHERE p.id = 5 RETURN id(p) AS a, id(q) AS b", Map.of(), "NodeIndexSeek");
    // an anonymous unlabelled node
    assertOptimized("MATCH (p:P)-[:K]->()-[:L]->(r) WHERE p.id < 80 RETURN id(p) AS a, id(r) AS b", Map.of(), "NodeIndexRangeScan");
  }

  /**
   * A pattern with no labelled node has nowhere to start but every vertex type, and a lone unlabelled node is a scan of every
   * vertex type: both keep the traditional plan. So do a pattern the optimizer would start from a scan of a whole label, which
   * walks every vertex of it on either plan, and a variable-length hop into an unlabelled node, whose step counts the distinct
   * ends of the walk while it walks (issue #10557).
   */
  @Test
  void aPatternWithNoLabelKeepsTheTraditionalPlan() {
    for (final String query : List.of("MATCH (a)-[:K]->(b) WHERE a.id < 50 RETURN id(a) AS a, id(b) AS b",
        "MATCH (a)-[:K]->(b) RETURN count(*) AS a", "MATCH (p:P)-[:K]->(q), (z) RETURN count(*) AS a",
        "MATCH (p:P)-[:K*1..2]->(q) WHERE p.id < 20 RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)-[:K]->(q)-[:K]->(r) RETURN id(p) AS a, id(r) AS b",
        "MATCH (p:P)-[:K]->(q) WHERE p.x < 3 RETURN id(p) AS a, id(q) AS b")) {
      assertThat(explainOf(query, Map.of())).as(query).doesNotContain("Cost-Based");
      assertSameRows(query, Map.of());
    }
  }

  @Test
  void theSameAnswersAsTheTraditionalPlan() {
    final List<String> shapes = List.of(
        "MATCH (p:P)-[:K]->(q) WHERE %s RETURN id(p) AS a, id(q) AS b",
        "MATCH (q)-[:K|L]->(p:P) WHERE %s RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)-[:K]->(q)-[:L]->(r) WHERE %s RETURN id(p) AS a, id(q) AS b, id(r) AS c",
        "MATCH (p:P) WHERE %s MATCH (p)-[]-(q) RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)<-[:K]-(q {g: 3}) WHERE %s RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)-[e]->(q) WHERE %s AND q.id < 120 RETURN id(p) AS a, id(e) AS b, labels(q) AS c",
        "MATCH (p:P)-[:K]->(q) WHERE %s RETURN count(*) AS a",
        "MATCH (p:P)-[:K]->(q) WHERE %s RETURN p.id AS a, q.x AS b ORDER BY a, b LIMIT 7");
    final List<String> predicates = List.of(
        "p.id < $v", "p.id <= $v", "p.id > $v", "p.id >= 3 AND p.id < $v", "$v > p.id", "p.id = $v",
        "p.g IN [1, 2, $v]", "p.g < $v", "p.d < $v", "p.d >= $v", "p.name < $v", "p.name STARTS WITH $s",
        "p.c1 < $v", "p.c1 = $v AND p.c2 >= 'k1'", "p.x < $v", "p.x >= $s", "p.id < $v AND p.x > 1");
    final List<Object> values = Arrays.asList(-1, 0, 5, 37, 150, 1000, 10.5, 7L, "zzz", "b", "10", true, null);
    final List<String> strings = List.of("a", "ab", "", "1", "Z");
    for (final String shape : shapes)
      for (final String predicate : predicates) {
        // the statement under test is the optimizer's where the predicate offers an index, so the comparison is between the
        // two plans; on the unindexed x it is the traditional plan on both sides
        final String plan = explainOf(String.format(shape, predicate), Map.of("v", 5, "s", "a"));
        if (predicate.startsWith("p.x"))
          assertThat(plan).as(shape + " " + predicate).doesNotContain("Cost-Based");
        else
          assertThat(plan).as(shape + " " + predicate).contains("Cost-Based");
        for (int i = 0; i < values.size(); i++) {
          final Map<String, Object> params = new HashMap<>();
          params.put("v", values.get(i));
          params.put("s", strings.get(i % strings.size()));
          assertSameRows(String.format(shape, predicate), params);
        }
      }
  }

  private void assertOptimized(final String query, final Map<String, Object> params, final String anchorOperator) {
    assertThat(explainOf(query, params)).as(query).contains("Cost-Based").contains(anchorOperator);
    assertSameRows(query, params);
  }

  /** A MATCH after a WITH keeps the traditional plan, and a leading WITH 1 AS one changes no row. */
  private void assertSameRows(final String query, final Map<String, Object> params) {
    final List<String> expected = rowsOf("WITH 1 AS one " + query, params);
    assertThat(explainOf("WITH 1 AS one " + query, params)).as(query).doesNotContain("Cost-Based");
    final List<String> actual = rowsOf(query, params);
    if (query.contains("ORDER BY"))
      assertThat(actual).as(query + " " + params).isEqualTo(expected);
    else
      assertThat(actual).as(query + " " + params).containsExactlyInAnyOrderElementsOf(expected);
  }

  private List<String> rowsOf(final String query, final Map<String, Object> params) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Object a = row.getProperty("a");
        final Object b = row.getProperty("b");
        final Object c = row.getProperty("c");
        rows.add(a + "|" + b + "|" + c);
      }
    }
    return rows;
  }

  private String explainOf(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, params)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
