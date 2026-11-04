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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10548: a variable-length hop that reports distinct end vertices walks a ready Graph Analytical View's adjacency
 * ({@code GAVVarLengthExpand}) instead of the edge lists of the records. The view is a performance feature, so the oracle is the
 * answer without it: the path enumeration of the same statement made ineligible by a named path, which no view serves.
 */
class Issue10548GAVVarLengthExpandTest extends TestHelper {
  private static final String VIEW     = "gav10548";
  private static final String OPERATOR    = "GAVVarLengthExpand";
  private static final String FUSED_COUNT = "DISTINCT ENDPOINT COUNT";
  private static final int    VERTICES = 7;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V").createProperty("id", Type.INTEGER);
      database.getSchema().createEdgeType("E");
      database.getSchema().createEdgeType("F");
    });
  }

  /**
   * On small random multigraphs (self loops, parallel and antiparallel edges, cycles, two relationship types) every eligible shape
   * walked through the view answers what the path enumeration answers: OUT, IN and BOTH, every path mode, lower bounds 0 and 1,
   * with and without {@code WHERE b <> a}, from every start vertex and from one seeded by its id.
   */
  @Test
  void everyShapeThroughTheViewAnswersWhatPathEnumerationAnswers() {
    final String[][] directions = { { "-", "->" }, { "<-", "-" }, { "-", "-" } };
    final String[] modes = { "", "WALK ", "ACYCLIC ", "TRAIL " };
    final String[] tails = {
        "RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
        "WHERE b <> a RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
        "RETURN DISTINCT a.id AS s, b.id AS v ORDER BY s, v",
        // the issue's form: the end vertices are counted as the walk reaches them, no row is built per end vertex
        "RETURN count(DISTINCT b) AS c",
        "WHERE a <> b RETURN count(DISTINCT b) AS c",
        "WHERE b <> a RETURN count(DISTINCT a) AS c" };
    int compared = 0;
    int undirectedTrails = 0;
    for (int seed = 0; seed < 4; seed++) {
      buildGraph(seed);
      createView();
      for (final String[] dir : directions)
        for (final String mode : modes)
          for (final int min : new int[] { 0, 1 })
            for (final int max : new int[] { 1, 2, 3, 4 })
              for (final String types : new String[] { ":E", ":E|F", "" }) {
                final String hop = dir[0] + "[" + types + "*" + min + ".." + max + "]" + dir[1];
                for (final String start : new String[] { "(a:V)", "(a:V {id: " + (seed % VERTICES) + "})" })
                  // an unlabeled end node is planned by the step-based executor, whose ExpandPathStep walks the view too
                  for (final String end : new String[] { "(b:V) ", "(b) " })
                    for (final String tail : tails) {
                      final String eligible = "MATCH " + mode + start + hop + end + tail;
                      final String reference = "MATCH p = " + mode + start + hop + end + tail;
                      assertThat(profileOf(eligible)).as(eligible).contains("provider=" + VIEW);
                      if (end.equals("(b:V) ")) {
                        assertThat(planOf(eligible)).as(eligible).contains(OPERATOR);
                        // seeded on an unindexed id, the plan may filter the far end above the expansion: that filter is no b <> a
                        if (start.equals("(a:V)") && (tail.endsWith("count(DISTINCT b) AS c") || tail.endsWith("count(DISTINCT a) AS c")))
                          assertThat(profileOf(eligible)).as(eligible).contains(FUSED_COUNT);
                      }
                      assertThat(profileOf(reference)).as(reference).doesNotContain("provider=");
                      compared++;
                      if (dir[0].equals("-") && dir[1].equals("-") && min == 1 && (mode.isEmpty() || mode.equals("TRAIL ")))
                        undirectedTrails++;
                      assertThat(rows(eligible)).as("seed=%d %s", seed, eligible).isEqualTo(rows(reference));
                    }
              }
      dropView();
    }
    assertThat(compared).isGreaterThan(24_000);
    // the undirected trails with a lower bound of 1 decide the start from a cycle search of their own
    assertThat(undirectedTrails).isGreaterThan(1_500);
  }

  /**
   * The issue's statement on a graph wider than one checkpoint interval, from every start vertex in turn: the visited set reused
   * from one start to the next must forget the previous walk, and the answers must be the record walk's.
   */
  @Test
  void manyStartsOnALargerGraphAnswerWhatTheRecordWalkAnswers() {
    final Random random = new Random(10548);
    database.transaction(() -> {
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i < 1500; i++)
        vertices.add(database.newVertex("V").set("id", i).save());
      for (final MutableVertex from : vertices)
        for (int e = 0; e < 3; e++)
          from.newEdge("E", vertices.get(random.nextInt(vertices.size()))).save();
    });

    final String[] queries = {
        "MATCH (a:V)-[:E*1..3]->(b:V) WHERE b <> a RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
        "MATCH (a:V {id: 1})-[:E*1..6]->(b:V) WHERE b <> a RETURN count(DISTINCT b) AS c",
        "MATCH (a:V {id: 1})-[:E*0..]-(b:V) RETURN count(DISTINCT b) AS c",
        "MATCH (a:V {id: 1})<-[:E*1..4]-(b:V) RETURN count(DISTINCT b) AS c" };
    final List<List<String>> expected = new ArrayList<>();
    for (final String query : queries) {
      assertThat(planOf(query)).as(query).contains("DISTINCT ENDPOINTS").doesNotContain(OPERATOR);
      expected.add(rows(query));
    }
    // the record walk itself must reach something, or the comparison below proves nothing
    assertThat(expected.get(1).get(0)).isNotEqualTo("c=0");

    createView();
    for (int i = 0; i < queries.length; i++) {
      assertThat(planOf(queries[i])).as(queries[i]).contains(OPERATOR);
      assertThat(rows(queries[i])).as(queries[i]).isEqualTo(expected.get(i));
    }
  }

  /** The view is not used where it cannot answer alone, and the statement falls back to walking the records. */
  @Test
  void fallsBackWhereTheViewCannotAnswer() {
    buildGraph(3);

    // no view at all
    final String eligible = "MATCH (a:V)-[:E*1..3]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s";
    assertThat(planOf(eligible)).contains("DISTINCT ENDPOINTS").doesNotContain(OPERATOR);

    database.transaction(() -> database.command("sql", "UPDATE E SET w = 1"));
    createView();
    assertThat(planOf(eligible)).contains(OPERATOR);

    // the path, the relationship variable read downstream, an inline property map or WHERE: none of them is the view's to answer
    for (final String query : new String[] {
        "MATCH p = (a:V)-[:E*1..3]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
        "MATCH (a:V)-[r:E*1..3]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c, collect(DISTINCT size(r)) AS l ORDER BY s",
        "MATCH (a:V)-[:E*1..3 {w: 1}]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
        "MATCH (a:V)-[r:E*1..3 WHERE r.w = 1]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
        // a lower bound of 2 enumerates paths
        "MATCH (a:V)-[:E*2..3]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s" })
      assertThat(planOf(query)).as(query).doesNotContain(OPERATOR);

    // a view over the types the pattern does not cover is not one that can serve it
    assertThat(planOf("MATCH (a:V)-[:F*1..3]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s")).contains(OPERATOR);
    dropView();
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + VIEW + " VERTEX TYPES (V) EDGE TYPES (F) UPDATE MODE SYNCHRONOUS");
    awaitReady();
    assertThat(planOf(eligible)).doesNotContain(OPERATOR);
  }

  /**
   * The count is taken from the walks only where nothing else reads the rows: another RETURN item, a predicate besides
   * {@code b <> a}, or a count that is not DISTINCT keeps the rows and the aggregation. Every form answers what the rows answer.
   */
  @Test
  void theCountIsFusedOnlyWhereNothingElseReadsTheRows() {
    buildGraph(7);
    database.transaction(() -> database.command("sql", "CREATE VERTEX TYPE W EXTENDS V"));
    database.transaction(() -> {
      final MutableVertex w = database.newVertex("W").set("id", 50).save();
      w.newEdge("E", database.select().fromType("V").where().property("id").eq().value(1).vertices().next()).save();
      database.select().fromType("V").where().property("id").eq().value(2).vertices().next().newEdge("E", w).save();
    });
    createView();

    final String[] fused = {
        "MATCH (a:V {id: 2})-[:E*1..4]-(b:V) WHERE b <> a RETURN count(DISTINCT b) AS c",
        "MATCH (a:V)-[:E*0..3]->(b:V) RETURN count(DISTINCT b) AS c",
        "MATCH (a:V)-[:E*1..3]->(b:V) WHERE a <> b RETURN count(DISTINCT b) AS c",
        // the end vertex label is tested on the node id, sub-types included
        "MATCH (a:V)-[:E|F*1..3]->(b:W) RETURN count(DISTINCT b) AS c",
        "MATCH (a:W)-[:E*1..3]-(b:V) WHERE b <> a RETURN count(DISTINCT b) AS c",
        // anchored on the other end: the expansion counts the vertices it reaches, whichever end of the pattern they are
        "MATCH (b:V)-[:E*1..2]->(a:V {id: 3}) WHERE a <> b RETURN count(DISTINCT b) AS c",
        // the end the walk starts from: counted when its walk reaches an end vertex
        "MATCH (a:V {id: 2})-[:E*1..3]->(b:V) WHERE b <> a RETURN count(DISTINCT a) AS c",
        "MATCH (a:V)-[:E*1..3]->(b:W) RETURN count(DISTINCT a) AS c",
        "MATCH (a:V)-[:E*1..1]->(b:V) WHERE b <> a RETURN count(DISTINCT a) AS c" };
    final String[] notFused = {
        "MATCH (a:V)-[:E*1..3]->(b:V) WHERE b <> a AND b.id > 1 RETURN count(DISTINCT b) AS c",
        "MATCH (a:V)-[:E*1..3]->(b:V) WHERE b.id <> a.id RETURN count(DISTINCT b) AS c",
        "MATCH (a:V)-[:E*1..3]->(b:V) RETURN count(DISTINCT b) AS c, count(DISTINCT a) AS d",
        "MATCH (a:V)-[:E*1..3]->(b:V) RETURN count(DISTINCT b.id) AS c" };
    for (final String query : fused) {
      assertThat(profileOf(query)).as(query).contains(FUSED_COUNT);
      assertThat(rows(query)).as(query).isEqualTo(rows(query.replace("MATCH ", "MATCH p = ")));
    }
    for (final String query : notFused) {
      assertThat(profileOf(query)).as(query).doesNotContain(FUSED_COUNT);
      assertThat(rows(query)).as(query).isEqualTo(rows(query.replace("MATCH ", "MATCH p = ")));
    }
    // the fused count is not a constant: it is what the rows count
    assertThat(rows(fused[3])).doesNotContain("c=0");
  }

  /**
   * A cached plan that walks a view is planned again once the view stops serving: a view that does not follow commits goes stale
   * on the next one, and the cached plan must not keep reading the graph as it stood before it.
   */
  @Test
  void aCachedPlanStopsReadingAViewThatWentStale() {
    buildGraph(2);
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName(VIEW).withVertexTypes("V").withEdgeTypes("E", "F")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF).withUseWhenStale(false).build();
    assertThat(view.isReady()).isTrue();

    final String query = "MATCH (a:V)-[:E*1..2]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s";
    final String count = "MATCH (a:V {id: 0})-[:E*1..1]->(b:V) RETURN count(DISTINCT b) AS c";
    assertThat(planOf(query)).contains(OPERATOR);
    // executed twice, so the second run is served by the cached plan
    rows(query);
    rows(count);
    final List<String> before = rows(count);

    database.transaction(() -> {
      final MutableVertex late = database.newVertex("V").set("id", 200).save();
      database.select().fromType("V").where().property("id").eq().value(0).vertices().next().newEdge("E", late).save();
    });
    assertThat(view.isStale()).isTrue();
    assertThat(view.isReady()).isFalse();

    assertThat(rows(query)).isEqualTo(rows(query.replace("MATCH ", "MATCH p = ")));
    assertThat(rows(count)).isNotEqualTo(before).isEqualTo(rows(count.replace("MATCH ", "MATCH p = ")));
  }

  /** Writes committed after the view was built reach the walk: a new vertex, new edges, a deleted edge. */
  @Test
  void writesAfterTheBuildAreSeen() {
    buildGraph(5);
    createView();
    final String query = "MATCH (a:V)-[:E*1..4]-(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s";
    final String reference = "MATCH p = (a:V)-[:E*1..4]-(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s";
    assertThat(rows(query)).isEqualTo(rows(reference));

    database.transaction(() -> {
      final MutableVertex late = database.newVertex("V").set("id", 100).save();
      late.newEdge("E", database.select().fromType("V").where().property("id").eq().value(0).vertices().next()).save();
      database.command("sql", "DELETE FROM E LIMIT 2");
    });
    assertThat(rows(query)).isEqualTo(rows(reference));
    assertThat(rows("MATCH (a:V {id: 100})-[:E*1..1]-(b:V) RETURN count(DISTINCT b) AS c")).containsExactly("c=1");
  }

  private void buildGraph(final int seed) {
    final Random random = new Random(seed);
    database.transaction(() -> {
      database.command("sql", "DELETE FROM V");
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i < VERTICES; i++)
        vertices.add(database.newVertex("V").set("id", i).save());
      final int edges = VERTICES + random.nextInt(VERTICES * 2);
      for (int e = 0; e < edges; e++) {
        final MutableVertex from = vertices.get(random.nextInt(VERTICES));
        // a fifth of the edges are self loops, the rest may repeat or run both ways between a pair
        final MutableVertex to = random.nextInt(5) == 0 ? from : vertices.get(random.nextInt(VERTICES));
        from.newEdge(random.nextInt(3) == 0 ? "F" : "E", to).save();
      }
    });
  }

  private void createView() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + VIEW + " VERTEX TYPES (V) EDGE TYPES (E, F) UPDATE MODE SYNCHRONOUS");
    awaitReady();
  }

  private void awaitReady() {
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW);
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.onSpinWait();
    assertThat(view.isReady()).isTrue();
  }

  private void dropView() {
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW " + VIEW);
  }

  private String planOf(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", "EXPLAIN " + query)) {
      return String.valueOf(resultSet.next().<Object>getProperty("executionPlan"));
    }
  }

  private String profileOf(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", "PROFILE " + query)) {
      while (resultSet.hasNext())
        resultSet.next();
      return resultSet.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  /** The rows as text, with the elements of every list value sorted: a collected list has no order the statement promised. */
  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final List<String> cells = new ArrayList<>();
        for (final String name : row.getPropertyNames()) {
          final Object value = row.getProperty(name);
          if (value instanceof List<?> list) {
            final List<String> elements = new ArrayList<>(list.size());
            for (final Object element : list)
              elements.add(String.valueOf(element));
            Collections.sort(elements);
            cells.add(name + "=" + elements);
          } else
            cells.add(name + "=" + value);
        }
        rows.add(String.join(",", cells));
      }
    }
    return rows;
  }
}
