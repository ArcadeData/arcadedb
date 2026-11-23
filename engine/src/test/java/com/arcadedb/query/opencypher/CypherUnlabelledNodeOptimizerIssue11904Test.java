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
import java.util.Collections;
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
    database.command("sql", "CREATE VERTEX TYPE Z");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE L");
    // a type whose edges only their source stores: the hop back to its source is read through the lookup of the incoming ones (#8625)
    database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
    database.command("sql", "CREATE PROPERTY K.w INTEGER");
    database.command("sql", "CREATE INDEX ON K (w) NOTUNIQUE");
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
      int weight = 0;
      for (final Vertex from : vertices) {
        final int degree = random.nextInt(4);
        for (int k = 0; k < degree; k++) {
          final Vertex to = vertices.get(random.nextInt(vertices.size()));
          // the K edges carry a distinct weight each, except every seventh: the key an edge index seeks
          if (random.nextBoolean()) {
            if (++weight % 7 == 0)
              from.asVertex().newEdge("K", to);
            else
              from.asVertex().newEdge("K", to, "w", weight);
          } else
            from.asVertex().newEdge("L", to);
        }
        if (random.nextInt(3) == 0)
          from.asVertex().newEdge("U", vertices.get(random.nextInt(vertices.size())));
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

  /** What else a statement around the unlabelled node does: a path, a predicate on it, a cycle, a second part, a WITH, an aggregate. */
  @Test
  void otherShapesAroundTheUnlabelledNodeAnswerAsTheTraditionalPlan() {
    final List<String> statements = List.of(
        "MATCH path = (p:P)-[:K]->(q) WHERE p.id < $v RETURN id(p) AS a, length(path) AS b, id(q) AS c",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v AND q:Q RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v AND q.g = 3 RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v AND NOT (q)-[:L]->() RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)-[:K]->(q)-[:K]->(p) WHERE p.id < $v RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v RETURN DISTINCT id(q) AS b",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v OPTIONAL MATCH (q)-[:L]->(r) RETURN id(p) AS a, id(q) AS b, id(r) AS c",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v WITH q, count(*) AS n RETURN id(q) AS a, n AS b",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v RETURN id(q) AS a, count(*) AS b",
        "MATCH (p:P)-[:K]->(q), (p)-[:L]->(r) WHERE p.id < $v RETURN id(p) AS a, id(q) AS b, id(r) AS c",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v AND q <> p RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)-[:K]->(q)<-[:L]-(r) WHERE p.id < $v RETURN id(p) AS a, id(q) AS b, id(r) AS c",
        "MATCH (p:P)-[:K]->(q)-[:K]->(r) WHERE p.id < $v AND r:P RETURN id(p) AS a, id(q) AS b, id(r) AS c",
        "MATCH (p:P)-[:K]-(q) WHERE p.id < $v RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v RETURN p.id AS a, id(q) AS b ORDER BY a DESC, b SKIP 1 LIMIT 9",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v AND q.x = 3 RETURN id(p) AS a, id(q) AS b",
        "UNWIND [1, 2] AS u MATCH (p:P)-[:K]->(q) WHERE p.id < $v RETURN id(p) AS a, id(q) AS b, u AS c");
    int optimized = 0;
    long answered = 0;
    for (final String statement : statements) {
      if (explainOf(statement, Map.of("v", 50)).contains("Cost-Based"))
        optimized++;
      for (final Object value : Arrays.asList(-1, 0, 5, 37, 150, 1000)) {
        final Map<String, Object> params = Map.of("v", value);
        assertSameRows(statement, params);
        answered += rowsOf(statement, params).size();
      }
    }
    assertThat(optimized).as("statements the optimizer plans").isGreaterThan(statements.size() / 2);
    assertThat(answered).as("rows answered: the comparison is not between empty answers").isPositive();
  }

  /**
   * A type whose edges only their source stores cannot be walked back from the target, so a hop from the labelled anchor to an
   * unlabelled source reads the incoming edges through a lookup of the whole type (#8625), and an anchor that cannot reach every node
   * along the directions is never moved to an unlabelled one. An untyped relationship may be one of them as well.
   */
  @Test
  void aUnidirectionalEdgeTypeAnswersAsTheTraditionalPlan() {
    final List<String> shapes = List.of("MATCH (p:P)-[:U]->(q) WHERE %s RETURN id(p) AS a, id(q) AS b",
        "MATCH (q)-[:U]->(p:P) WHERE %s RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)<-[:U]-(q) WHERE %s RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)-[:U]-(q) WHERE %s RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)-[]->(q) WHERE %s RETURN id(p) AS a, id(q) AS b",
        "MATCH (q)-[]->(p:P) WHERE %s RETURN id(p) AS a, id(q) AS b",
        "MATCH (p:P)-[:K]->(q)-[:U]->(r) WHERE %s RETURN id(p) AS a, id(q) AS b, id(r) AS c",
        "MATCH (r)-[:U]->(q)-[:K]->(p:P) WHERE %s RETURN id(p) AS a, id(q) AS b, id(r) AS c");
    final List<String> predicates = List.of("p.id < $v", "p.id = $v", "p.g IN [1, 2, $v]", "p.d >= $v");
    final List<Object> values = Arrays.asList(-1, 5, 37, 150, 10.5, "zzz", null);
    int optimized = 0;
    long answered = 0;
    for (final String shape : shapes)
      for (final String predicate : predicates) {
        final String query = String.format(shape, predicate);
        if (explainOf(query, Map.of("v", 5)).contains("Cost-Based"))
          optimized++;
        for (final Object value : values) {
          final Map<String, Object> params = new HashMap<>();
          params.put("v", value);
          assertSameRows(query, params);
          answered += rowsOf(query, params).size();
        }
      }
    assertThat(optimized).as("statements the optimizer plans").isGreaterThan(shapes.size());
    assertThat(answered).as("rows answered: the comparison is not between empty answers").isPositive();
  }

  /**
   * A relationship the index of its type pins is a start of its own (#11481), and either endpoint of it may now have no label: the
   * seek binds both and the labelled one is checked. The answer is the one of the traditional plan, and of a scan that reads every
   * edge of the type for the same key.
   */
  @Test
  void anEdgeIndexSeedWithAnUnlabelledEndpointAnswersAsTheTraditionalPlan() {
    final List<String> shapes = List.of("MATCH (p:P)-[r:K]->(q) WHERE r.w = $v RETURN id(p) AS a, id(q) AS b, id(r) AS c",
        "MATCH (q)-[r:K]->(p:P) WHERE r.w = $v RETURN id(p) AS a, id(q) AS b, id(r) AS c",
        "MATCH (p:P)-[r:K]->(q) WHERE r.w IN [$v, 12] RETURN id(p) AS a, id(q) AS b, id(r) AS c",
        "MATCH (p:P)-[r:K]->(q)-[:L]->(s) WHERE r.w = $v RETURN id(p) AS a, id(q) AS b, id(r) AS c, id(s) AS d",
        "MATCH (p:P)<-[r:K]-(q) WHERE r.w = $v AND p.id < 150 RETURN id(p) AS a, id(q) AS b, id(r) AS c");
    long answered = 0;
    for (final String shape : shapes) {
      assertThat(explainOf(shape, Map.of("v", 5))).as(shape).contains("Cost-Based").contains("EdgeIndexSeek");
      for (final Object value : Arrays.asList(-1, 5, 14, 37, 150, 1000, 10.5, "zzz", true, null)) {
        final Map<String, Object> params = new HashMap<>();
        params.put("v", value);
        assertSameRows(shape, params);
        answered += rowsOf(shape, params).size();
        // what the seek must find: every K edge of that weight, read without the index
        if (value instanceof Integer && !shape.contains("IN [") && !shape.contains("-[:L]->") && !shape.contains("p.id < 150"))
          assertThat(rowsOf(shape, params)).as(shape + " " + value)
              .containsExactlyInAnyOrderElementsOf(rowsOf(shape.replace("r.w = $v", "r.w + 0 = $v"), params));
      }
    }
    assertThat(answered).as("rows answered: the comparison is not between empty answers").isPositive();
  }

  /**
   * A statement that writes after such a MATCH reaches the optimizer too, with its eagerness analysis and its deferred loading of
   * vertices: it leaves the graph as the traditional plan leaves it. Each statement runs in a transaction that is rolled back, so the
   * next starts from the same graph.
   */
  @Test
  void aStatementThatWritesAfterTheMatchLeavesTheGraphAsTheTraditionalPlanDoes() {
    // a value that depends on the row would be written by whichever of the rows reaching a vertex comes last, and no plan owes an order
    final List<String> statements = List.of("MATCH (p:P)-[:K]->(q) WHERE p.id < $v SET q.touched = 1 RETURN count(*) AS a",
        "MATCH (q)-[:K]->(p:P) WHERE p.id < $v SET q.touched = $v RETURN count(*) AS a",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v DETACH DELETE q RETURN count(*) AS a",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v CREATE (q)-[:L]->(:Z {n: p.id}) RETURN count(*) AS a",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v MERGE (q)-[:L]->(p) RETURN count(*) AS a",
        "MATCH (p:P)-[:K]->(q) WHERE p.id < $v SET p.g = 100 + p.id RETURN count(*) AS a",
        // writes the key of the index it walks
        "MATCH (p:P)-[:K]->(q) WHERE p.g < $v SET p.g = p.g + 100 RETURN count(*) AS a");
    // the state of the graph a statement that writes nothing leaves, to tell the comparison is not between two untouched graphs
    final List<String> untouched = outcomeOfWriting("RETURN 1 AS a", Map.of()).subList(1, STATE.size() + 1);
    for (final String statement : statements) {
      assertThat(explainOf(statement, Map.of("v", 5))).as(statement).contains("Cost-Based");
      for (final int bound : new int[] { 0, 12, 100, 1000 })
        assertSameWrites(statement, Map.of("v", bound));
      assertThat(outcomeOfWriting(statement, Map.of("v", 1000)).subList(1, STATE.size() + 1)).as(statement + " writes")
          .isNotEqualTo(untouched);
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

  /** What the graph holds after the statement ran, and what it returned: the same on the optimizer's plan as on the traditional one. */
  private void assertSameWrites(final String query, final Map<String, Object> params) {
    final List<String> expected = outcomeOfWriting("WITH 1 AS one " + query, params);
    assertThat(explainOf("WITH 1 AS one " + query, params)).as(query).doesNotContain("Cost-Based");
    assertThat(outcomeOfWriting(query, params)).as(query + " " + params).isEqualTo(expected);
  }

  private static final List<String> STATE = List.of("SELECT @rid AS a FROM P ORDER BY @rid", "SELECT @rid AS a FROM Q ORDER BY @rid",
      "SELECT @rid AS a, touched AS b, g AS c FROM P WHERE touched IS NOT NULL OR g >= 100 ORDER BY @rid",
      "SELECT @rid AS a, touched AS b, g AS c FROM Q WHERE touched IS NOT NULL OR g >= 100 ORDER BY @rid",
      "SELECT count(@rid) AS a FROM Z", "SELECT count(@rid) AS a FROM K", "SELECT count(@rid) AS a FROM L",
      "SELECT count(@rid) AS a FROM U");

  /**
   * Runs the statement in a transaction, reads the state of the graph and the rows it returned, and rolls the transaction back. The
   * rows of each read come in the order of the index that serves it, which is not what is compared.
   */
  private List<String> outcomeOfWriting(final String query, final Map<String, Object> params) {
    database.begin();
    try {
      final List<String> outcome = new ArrayList<>();
      outcome.add(String.join("\n", sortedRows(database.command("opencypher", query, params))));
      for (final String state : STATE)
        outcome.add(String.join("\n", sortedRows(database.query("sql", state))));
      return outcome;
    } finally {
      database.rollback();
    }
  }

  private static List<String> sortedRows(final ResultSet rs) {
    final List<String> rows = new ArrayList<>();
    try (rs) {
      while (rs.hasNext())
        rows.add(rs.next().toJSON().toString());
    }
    Collections.sort(rows);
    return rows;
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
