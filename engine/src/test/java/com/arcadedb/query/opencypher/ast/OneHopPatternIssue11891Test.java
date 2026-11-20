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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11891: a one-hop {@code COUNT { }}, {@code EXISTS { }}, bare pattern predicate and {@code size()} of a pattern
 * comprehension evaluated per row are answered off the edge list of the bound vertex, rather than by planning and running
 * a subquery (or walking the edge records) for every row. The answers must be the ones the general path gives: the same
 * expressions with a relationship variable, which keeps them off the fast path without changing what they match, are
 * the oracle.
 */
class OneHopPatternIssue11891Test extends TestHelper {
  private static final String[] RELATIONSHIPS = { "-[%s]->", "<-[%s]-", "-[%s]-" };
  private static final String[] TYPES         = { ":K", ":K|L", "", ":K2", ":U", ":Missing", ":K*1..1" };
  private static final String[] FAR_ENDS      = { "()", "(:P)", "(:Q)", "(:P|Q)", "(x)" };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE Q");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE L");
    database.command("sql", "CREATE EDGE TYPE K2 EXTENDS K");
    database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
    // A vertex carrying both labels, so a far end of (:P) or (:Q) is checked on a composite type too
    database.command("opencypher", "UNWIND range(1, 4) AS i CREATE (:P:Q {i: i})");

    final Random random = new Random(11891);
    database.transaction(() -> {
      final List<Vertex> vertices = new ArrayList<>();
      for (int i = 0; i < 30; i++)
        vertices.add(database.newVertex(i % 3 == 0 ? "Q" : "P").set("i", i).save());
      try (final ResultSet both = database.query("opencypher", "MATCH (v:P:Q) RETURN v")) {
        while (both.hasNext())
          vertices.add(both.next().<Vertex>getProperty("v"));
      }
      final String[] edgeTypes = { "K", "L", "K2", "U" };
      for (int i = 0; i < 120; i++) {
        final Vertex from = vertices.get(random.nextInt(vertices.size()));
        // One edge in eight is a self loop, and a pair is often linked more than once
        final Vertex to = random.nextInt(8) == 0 ? from : vertices.get(random.nextInt(vertices.size()));
        from.modify().newEdge(edgeTypes[random.nextInt(edgeTypes.length)], to);
      }
      // Vertices with no edge at all
      for (int i = 0; i < 3; i++)
        database.newVertex("P").set("i", 100 + i).save();
    });
  }

  /** The issue's shapes are answered off the edge list: one answer per outer row. */
  @Test
  void theOneHopShapesAreAnsweredOffTheEdgeList() {
    final long rows = count("MATCH (p:P) RETURN count(p) AS c");
    for (final String query : List.of(//
        "MATCH (p:P) WHERE COUNT { (p)-[:K]->() } > 1 RETURN count(p) AS c",
        "MATCH (p:P) WHERE COUNT { (p)-[:K]->(:P) } > 1 RETURN count(p) AS c",
        "MATCH (p:P) WHERE COUNT { MATCH (p)-[:K]->() } > 1 RETURN count(p) AS c",
        "MATCH (p:P) WHERE COUNT { (:P)-[:K]->(p) } > 1 RETURN count(p) AS c",
        "MATCH (p:P) WHERE EXISTS { (p)-[:K]->() } RETURN count(p) AS c",
        "MATCH (p:P) WHERE EXISTS { MATCH (p)-[:K|L]-() } RETURN count(p) AS c",
        "MATCH (p:P) WHERE (p)-[:K]->() RETURN count(p) AS c",
        "MATCH (p:P) WHERE NOT (p)-[:K]->() RETURN count(p) AS c",
        "MATCH (p:P) WHERE size([(p)-[:K]->(q) | q]) > 1 RETURN count(p) AS c",
        "MATCH (p:P) RETURN sum(size([(p)<-[:K]-(:Q) | 1])) AS c")) {
      final long before = OneHopPattern.rowsAnswered();
      count(query);
      assertThat(OneHopPattern.rowsAnswered() - before).as(query).isEqualTo(rows);
    }
  }

  /** Anything the edge list cannot decide on its own stays on the general path. */
  @Test
  void aBodyThatConstrainsMoreThanTheHopIsNotAnsweredOffTheEdgeList() {
    for (final String query : List.of(//
        "MATCH (p:P) RETURN COUNT { (p)-[:K {w: 1}]->() } AS c",
        "MATCH (p:P) RETURN COUNT { (p)-[r:K]->() } AS c",
        "MATCH (p:P) RETURN COUNT { (p)-[:K WHERE true]->() } AS c",
        "MATCH (p:P) RETURN COUNT { (p)-[:K]->(x) WHERE x.i > 3 } AS c",
        "MATCH (p:P) RETURN COUNT { (p)-[:K]->({i: 3}) } AS c",
        "MATCH (p:P) RETURN COUNT { (p)-[:K]->(p) } AS c",
        "MATCH (p:P) RETURN COUNT { (p:Q)-[:K]->() } AS c",
        "MATCH (p:P) RETURN COUNT { (p)-[:K*1..2]->() } AS c",
        "MATCH (p:P) RETURN COUNT { (p)-[:K*1..1]->() } AS c",
        "MATCH (p:P) RETURN COUNT { (p)-[:K]->()-[:K]->() } AS c",
        "MATCH (p:P) RETURN COUNT { MATCH (p)-[:K]->() RETURN 1 LIMIT 1 } AS c",
        "MATCH (p:P), (x:Q) RETURN COUNT { (p)-[:K]->(x) } AS c",
        "MATCH (p:P) RETURN EXISTS { MATCH (p)-[:K]->() WHERE p.i > 3 } AS c",
        "MATCH (p:P) WHERE (p)-[:K {w: 1}]->() RETURN count(p) AS c",
        "MATCH (p:P) WHERE (p)-[:K]->(:P {i: 1}) RETURN count(p) AS c",
        "MATCH (p:P) RETURN size([(p)-[:K]->(q) WHERE q.i > 3 | q]) AS c",
        "MATCH (p:P) RETURN size([(p)-[:K]->(q) | q.i]) AS c",
        "OPTIONAL MATCH (p:Missing) RETURN COUNT { (p)-[:K]->() } AS c",
        // Shapes whose replaced path asks other permissions than the edge list would (OneHopPatternSecurityIssue11891Test):
        // a predicate or a comprehension bound by their last node only scan the first node's types
        "MATCH (p:P) WHERE (:P)-[:K]->(p) RETURN count(p) AS c",
        "MATCH (p:P) WHERE ()<-[:K]-(p) RETURN count(p) AS c",
        "MATCH (p:P) WHERE NOT ()-[:K]-(p) RETURN count(p) AS c",
        "MATCH (p:P) RETURN size([(:P)-[:K]->(p) | 1]) AS c")) {
      final long before = OneHopPattern.rowsAnswered();
      drain(query);
      assertThat(OneHopPattern.rowsAnswered() - before).as(query).isZero();
    }
  }

  @Test
  void theEdgeListAnswersWhatTheGeneralPathAnswers() {
    assertSameAnswers();
  }

  /** The vertex copy the transaction holds carries the edges created and deleted in it, self loops included. */
  @Test
  void theEdgeListAnswersWhatTheGeneralPathAnswersInsideATransaction() {
    database.begin();
    try {
      final Random random = new Random(7);
      final List<Vertex> vertices = new ArrayList<>();
      try (final ResultSet all = database.query("opencypher", "MATCH (v) RETURN v")) {
        while (all.hasNext())
          vertices.add(all.next().<Vertex>getProperty("v"));
      }
      for (int i = 0; i < 30; i++) {
        final Vertex from = vertices.get(random.nextInt(vertices.size()));
        final Vertex to = random.nextInt(4) == 0 ? from : vertices.get(random.nextInt(vertices.size()));
        from.modify().newEdge(i % 2 == 0 ? "K" : "U", to);
      }
      final MutableVertex fresh = database.newVertex("Q").set("i", 500).save();
      fresh.newEdge("K", fresh);
      fresh.newEdge("K", vertices.getFirst());
      final List<Edge> deleted = new ArrayList<>();
      try (final ResultSet edges = database.query("opencypher", "MATCH ()-[e:K]->() RETURN e LIMIT 10")) {
        while (edges.hasNext())
          deleted.add(edges.next().<Edge>getProperty("e"));
      }
      for (final Edge edge : deleted)
        edge.delete();

      assertSameAnswers();

      // The row holds the vertex as it was read, before the statement itself added an edge to it
      final String query = "MATCH (p:P) CREATE (p)-[:K]->(p) RETURN COUNT { (p)-[:K]->() } AS f, COUNT { (p)-[r:K]->() } AS g, "
          + "EXISTS { (p)-[:K]-(:Q) } AS fe, EXISTS { (p)-[r:K]-(:Q) } AS ge, size([(p)-[:K]-() | 1]) AS fs, "
          + "size([(p)-[r:K]-() | 1]) AS gs";
      try (final ResultSet rs = database.command("opencypher", query)) {
        int rows = 0;
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat(row.<Object>getProperty("f")).isEqualTo(row.getProperty("g"));
          assertThat(row.<Object>getProperty("fe")).isEqualTo(row.getProperty("ge"));
          assertThat(row.<Object>getProperty("fs")).isEqualTo(row.getProperty("gs"));
          ++rows;
        }
        assertThat(rows).isGreaterThan(0);
      }
    } finally {
      database.rollback();
    }
  }

  /** With an analytical view over the graph, the count push-down of the general path reads the CSR arrays. */
  @Test
  void theEdgeListAnswersWhatTheGeneralPathAnswersWithAGraphView() {
    final List<String> vertexTypes = new ArrayList<>();
    for (final DocumentType type : database.getSchema().getTypes())
      if (type instanceof VertexType)
        vertexTypes.add(type.getName());
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("issue11891-view")
        .withVertexTypes(vertexTypes.toArray(new String[0]))
        .withEdgeTypes("K", "L", "K2", "U")
        .build();
    try {
      assertSameAnswers();
    } finally {
      view.drop();
    }
  }

  /** A null outer variable keeps the answer it had: the edge list is not asked. */
  @Test
  void aNullOuterVariableKeepsItsAnswer() {
    final String query = "OPTIONAL MATCH (p:Missing) RETURN COUNT { (p)-[:K]->() } AS f, COUNT { (p)-[r:K]->() } AS g, "
        + "EXISTS { (p)-[:K]->() } AS fe, EXISTS { (p)-[r:K]->() } AS ge";
    try (final ResultSet rs = database.query("opencypher", query)) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("f")).isEqualTo(row.getProperty("g"));
      assertThat(row.<Object>getProperty("fe")).isEqualTo(row.getProperty("ge"));
    }
  }

  /**
   * Edge-list entries whose edge record is gone (ghosts), answered as the general path answers them. A variable-length hop,
   * {@code [:K*1..1]} included, is a walk that skips a ghost where the fixed hop's count follows it, so it stays on the general
   * path. An undirected self loop sits in both lists of its vertex: two self loops are read as K, L, K, L, so it is told apart
   * by its identity before its presence is asked, or a ghost K hid the live L (or a ghost L doubled the live K).
   */
  @Test
  void ghostEntriesAreAnsweredAsTheGeneralPathAnswersThem() {
    database.command("sql", "CREATE VERTEX TYPE G");
    final List<RID> ghosts = new ArrayList<>();
    database.transaction(() -> {
      // 1: a ghost K to one vertex, a live K to another
      final MutableVertex a = database.newVertex("G").set("g", 1).save();
      ghosts.add(a.newEdge("K", database.newVertex("G").set("g", 90).save()).getIdentity());
      a.newEdge("K", database.newVertex("G").set("g", 91).save());
      // 2: two self loops, the first one a ghost
      final MutableVertex b = database.newVertex("G").set("g", 2).save();
      ghosts.add(b.newEdge("K", b).getIdentity());
      b.newEdge("K", b);
      // 3: two self loops, the second one a ghost
      final MutableVertex c = database.newVertex("G").set("g", 3).save();
      c.newEdge("K", c);
      ghosts.add(c.newEdge("K", c).getIdentity());
      // 4: two live self loops
      final MutableVertex d = database.newVertex("G").set("g", 4).save();
      d.newEdge("K", d);
      d.newEdge("K", d);
    });
    database.transaction(() -> {
      for (final RID ghost : ghosts)
        TestHelper.deleteRecordAtLowLevel(database, ghost);
    });

    try {
      final String rows = "MATCH (p:G) WHERE p.g < 10 RETURN p.g AS id, %s AS v ORDER BY id";
      assertThat(perVertex(rows.formatted("COUNT { (p)-[:K*1..1]->() }"))).containsExactly("1=1", "2=1", "3=1", "4=2");
      assertThat(perVertex(rows.formatted("COUNT { (p)-[:K*1..1]-() }"))).containsExactly("1=1", "2=1", "3=1", "4=2");

      final long before = OneHopPattern.rowsAnswered();
      assertThat(perVertex(rows.formatted("size([(p)-[:K]-() | 1])"))).containsExactly("1=1", "2=1", "3=1", "4=2");
      assertThat(perVertex("MATCH (p:G) WHERE p.g IN [2, 3] AND (p)-[:K]-() RETURN p.g AS id, 1 AS v ORDER BY id"))
          .containsExactly("2=1", "3=1");
      assertThat(OneHopPattern.rowsAnswered() - before).as("the self loops are read off the edge list").isPositive();
    } finally {
      // the ghosts are this test's fixture: repaired, so the integrity check after it judges the rest of the database
      database.command("sql", "CHECK DATABASE FIX").close();
    }
  }

  /**
   * A supernode is one row that walks every entry of its edge list: the statement's deadline is asked now and then, as the
   * count push-down and the row pipeline this replaces ask it. The context's deadline is already past, so the walk stops at
   * the first ask whatever the clock reads; with none the same call counts every entry.
   */
  @Test
  void aWalkOverASupernodeHonoursTheStatementDeadline() {
    final int degree = 2100;
    final RID[] hubRid = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("P").set("i", 900).save();
      for (int i = 0; i < degree; i++)
        hub.newEdge("K", database.newVertex("Q").set("i", 1000 + i).save());
      hubRid[0] = hub.getIdentity();
    });
    final Vertex hub = database.lookupByRID(hubRid[0], true).asVertex();
    final ResultInternal row = new ResultInternal();
    row.setProperty("p", hub);

    final NodePattern bound = new NodePattern("p", null, null);
    final RelationshipPattern hop = new RelationshipPattern(null, List.of("K"), Direction.OUT, null, null, null);
    // A far-end label is read off the neighbour's type, the walk of a size() loads each relationship: both are O(degree)
    final OneHopPattern labelled = OneHopPattern.of(new PathPattern(bound, hop, new NodePattern(null, List.of("Q"), null)));
    final OneHopPattern plain = OneHopPattern.of(new PathPattern(bound, hop, new NodePattern(null, null, null)));

    final BasicCommandContext open = new BasicCommandContext();
    open.setDatabase(database);
    assertThat(labelled.count(row, open, OneHopPattern.Question.COUNT)).isEqualTo(degree);
    assertThat(plain.count(row, open, OneHopPattern.Question.SIZE)).isEqualTo(degree);

    final BasicCommandContext expired = new BasicCommandContext();
    expired.setDatabase(database);
    expired.setCommandDeadline(0L, "the test deadline", false);
    assertThatThrownBy(() -> labelled.count(row, expired, OneHopPattern.Question.COUNT)).isInstanceOf(TimeoutException.class);
    assertThatThrownBy(() -> plain.count(row, expired, OneHopPattern.Question.SIZE)).isInstanceOf(TimeoutException.class);
  }

  /**
   * Every combination of direction, type list, far end and bound end, through each of the expressions, against the same
   * expression with a relationship variable added: that keeps it on the general path and matches the same relationships.
   */
  private void assertSameAnswers() {
    long answered = 0;
    for (final String relationship : RELATIONSHIPS)
      for (final String type : TYPES)
        for (final String farEnd : FAR_ENDS)
          for (final boolean boundFirst : new boolean[] { true, false }) {
            final String fast = pattern(relationship, type, farEnd, boundFirst, "");
            final String general = pattern(relationship, type, farEnd, boundFirst, "r");
            for (final String expression : List.of("COUNT { %s }", "EXISTS { %s }", "COUNT { MATCH %s }",
                "size([%s | 1])")) {
              final long before = OneHopPattern.rowsAnswered();
              final List<String> expected = perVertex(
                  "MATCH (p) RETURN id(p) AS id, " + expression.formatted(general) + " AS v ORDER BY id");
              assertThat(OneHopPattern.rowsAnswered()).as(general).isEqualTo(before);
              final List<String> actual = perVertex(
                  "MATCH (p) RETURN id(p) AS id, " + expression.formatted(fast) + " AS v ORDER BY id");
              answered += OneHopPattern.rowsAnswered() - before;
              assertThat(actual).as(fast + " in " + expression).isEqualTo(expected);
            }
            // A pattern predicate cannot introduce a variable, so its oracle is the EXISTS { } spelling of the general form
            if (farEnd.equals("(x)"))
              continue;
            for (final String negation : List.of("", "NOT ")) {
              final List<String> expected = perVertex(
                  "MATCH (p) WHERE " + negation + "EXISTS { " + general + " } RETURN id(p) AS id, 1 AS v ORDER BY id");
              final long before = OneHopPattern.rowsAnswered();
              final List<String> actual = perVertex(
                  "MATCH (p) WHERE " + negation + fast + " RETURN id(p) AS id, 1 AS v ORDER BY id");
              answered += OneHopPattern.rowsAnswered() - before;
              assertThat(actual).as("WHERE " + negation + fast).isEqualTo(expected);
            }
          }
    assertThat(answered).as("the comparison has to run the fast path").isGreaterThan(0);
  }

  private static String pattern(final String relationship, final String type, final String farEnd, final boolean boundFirst,
      final String variable) {
    final String hop = relationship.formatted(variable + type);
    return boundFirst ? "(p)" + hop + farEnd : farEnd + hop + "(p)";
  }

  private List<String> perVertex(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.getProperty("id") + "=" + row.getProperty("v"));
      }
    }
    return rows;
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private void drain(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
