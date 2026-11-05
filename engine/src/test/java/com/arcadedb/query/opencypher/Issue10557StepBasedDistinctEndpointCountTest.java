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
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10557: {@code count(DISTINCT b)} over a variable-length hop was fused into the walk on the cost-based path only (#10548). The
 * step-based path, taken for instance when the end node has no label, built one row per end vertex, filtered it for
 * {@code WHERE b <> a} and aggregated it. Its {@code ExpandPathStep} now counts while it walks, through a view or the records, and
 * answers what the rows answer. The oracle is the same statement with a named path, which enumerates paths and fuses nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10557StepBasedDistinctEndpointCountTest extends TestHelper {
  private static final String VIEW        = "gav10557";
  private static final String STEP_WALK   = "EXPAND PATH";
  private static final String FUSED_COUNT = "DISTINCT ENDPOINT COUNT";
  private static final int    VERTICES    = 7;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V").createProperty("id", Type.INTEGER);
      database.getSchema().createVertexType("W").addSuperType("V");
      database.getSchema().createEdgeType("E");
      database.getSchema().createEdgeType("F");
    });
  }

  /** The issue's statement: PROFILE shows the fused count, and no aggregation, through the records and through a view. */
  @Test
  void theIssueStatementIsFusedOnTheStepBasedPath() {
    // 1 -> 2 -> 3 -> 4 -> 1 over E, and 3 -> 5 over F, which the walk must not follow
    database.transaction(() -> {
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i <= 5; i++)
        vertices.add(database.newVertex(i == 4 ? "W" : "V").set("id", i).save());
      for (int i = 1; i <= 4; i++)
        vertices.get(i).newEdge("E", vertices.get(i == 4 ? 1 : i + 1)).save();
      vertices.get(3).newEdge("F", vertices.get(5)).save();
    });
    final String query = "MATCH (a:V {id: 1})-[:E*1..5]->(b) WHERE b <> a RETURN count(DISTINCT b) AS c";
    final String reference = query.replace("MATCH ", "MATCH p = ");

    assertThat(profileOf(query)).contains(FUSED_COUNT).contains(STEP_WALK).doesNotContainIgnoringCase("aggregat");
    assertThat(rows(query)).isEqualTo(rows(reference)).containsExactly("c=3");

    createView();
    assertThat(profileOf(query)).contains(FUSED_COUNT);
    assertThat(rows(query)).isEqualTo(rows(reference));
  }

  /**
   * Every shape the step-based executor plans with a distinct-endpoint {@code ExpandPathStep} and a lone {@code count(DISTINCT)}
   * is fused, and answers what the path enumeration answers: OUT, IN and BOTH, every path mode, lower bounds 0 and 1, the count of
   * either end, with and without {@code WHERE b <> a}, end node patterns the view cannot test (inline properties, a label
   * disjunction), with and without a view.
   */
  @Test
  void everyFusedShapeAnswersWhatPathEnumerationAnswers() {
    final String[][] directions = { { "-", "->" }, { "<-", "-" }, { "-", "-" } };
    final String[] modes = { "", "ACYCLIC ", "TRAIL " };
    final String[] tails = {
        "RETURN count(DISTINCT b) AS c",
        "WHERE b <> a RETURN count(DISTINCT b) AS c",
        "WHERE a <> b RETURN count(DISTINCT b) AS c",
        "RETURN count(DISTINCT a) AS c",
        "WHERE b <> a RETURN count(DISTINCT a) AS c" };
    final String[] ends = { "(b)", "(b {id: 3})", "(b:W|V)" };
    int fused = 0;
    int compared = 0;
    for (int seed = 0; seed < 3; seed++) {
      buildGraph(seed);
      for (final boolean withView : new boolean[] { false, true }) {
        if (withView)
          createView();
        for (final String[] dir : directions)
          for (final String mode : modes)
            for (final int min : new int[] { 0, 1 })
              for (final int max : new int[] { 1, 3 })
                for (final String types : new String[] { ":E", "" })
                  for (final String start : new String[] { "(a:V)", "(a:V {id: " + (seed % VERTICES) + "})", "(a)" })
                    for (final String end : ends)
                      for (final String tail : tails) {
                        final String query = "MATCH " + mode + start + dir[0] + "[" + types + "*" + min + ".." + max + "]" + dir[1] + end
                            + " " + tail;
                        final String profile = profileOf(query);
                        if (profile.contains(FUSED_COUNT))
                          fused++;
                        else
                          assertThat(profile.contains(STEP_WALK) && profile.contains("DISTINCT ENDPOINTS"))
                              .as("a distinct-endpoint ExpandPathStep counted by an aggregation: %s%n%s", query, profile).isFalse();
                        assertThat(rows(query)).as("seed=%d view=%s %s", seed, withView, query)
                            .isEqualTo(rows(query.replace("MATCH ", "MATCH p = ")));
                        compared++;
                      }
        if (withView)
          dropView();
      }
    }
    assertThat(compared).isGreaterThan(3_000);
    // the step-based fusion is reached at all, or the comparison above proves nothing about it
    assertThat(fused).as("statements answered by the fused count of an ExpandPathStep").isGreaterThan(compared / 4);
  }

  /**
   * A target an earlier pattern of the clause bound, a second filter, another RETURN item, a count that is not DISTINCT and an
   * OPTIONAL MATCH: either fused and still the rows' answer, or not fused. Every one answers what the rows answer.
   */
  @Test
  void whatTheRowsDecideIsHonouredOrNotFused() {
    buildGraph(4);
    createView();
    final String[] fused = {
        "MATCH (a:V)-[r:E*1..3]->(b) RETURN count(DISTINCT b) AS c",
        "MATCH (a:V)-[:E*0..2]-(b {id: 2}) WHERE a <> b RETURN count(DISTINCT a) AS c" };
    // planned either way, depending on which end the planner anchors: b is bound by the first pattern before the walk reaches it
    final String[] either = {
        "MATCH (b:V)-[:F]->(c), (a:V)-[:E*1..3]->(b) RETURN count(DISTINCT b) AS c",
        "MATCH (b:V)-[:F]->(c), (a:V)-[:E*1..3]->(b) RETURN count(DISTINCT a) AS c" };
    for (final String query : either)
      assertThat(rows(query)).as(query).isEqualTo(rows(query.replace("(a:V)-", "p = (a:V)-")));
    final String[] notFused = {
        "MATCH (a:V)-[:E*1..3]->(b) WHERE b <> a AND b.id > 1 RETURN count(DISTINCT b) AS c",
        "MATCH (a:V)-[:E*1..3]->(b) WHERE b.id <> a.id RETURN count(DISTINCT b) AS c",
        "MATCH (a:V)-[:E*1..3]->(b) RETURN count(DISTINCT b) AS c, count(DISTINCT a) AS d",
        "MATCH (a:V)-[:E*1..3]->(b) RETURN count(b) AS c",
        "MATCH (a:V)-[:E*1..3]->(b) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
        "OPTIONAL MATCH (a:V)-[:E*1..3]->(b) RETURN count(DISTINCT b) AS c" };
    for (final String query : fused) {
      assertThat(profileOf(query)).as(query).contains(FUSED_COUNT);
      assertThat(rows(query)).as(query).isEqualTo(rows(query.replace("(a:V)-", "p = (a:V)-")));
    }
    for (final String query : notFused) {
      assertThat(profileOf(query)).as(query).doesNotContain(FUSED_COUNT);
      assertThat(rows(query)).as(query).isEqualTo(rows(query.replace("(a:V)-", "p = (a:V)-")));
    }
  }

  private void buildGraph(final int seed) {
    final Random random = new Random(seed);
    database.transaction(() -> {
      database.command("sql", "DELETE FROM V");
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i < VERTICES; i++)
        vertices.add(database.newVertex(i % 3 == 0 ? "W" : "V").set("id", i).save());
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
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + VIEW + " VERTEX TYPES (V, W) EDGE TYPES (E, F) UPDATE MODE SYNCHRONOUS");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW);
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.onSpinWait();
    assertThat(view.isReady()).isTrue();
  }

  private void dropView() {
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW " + VIEW);
  }

  private String profileOf(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", "PROFILE " + query)) {
      while (resultSet.hasNext())
        resultSet.next();
      return resultSet.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final List<String> cells = new ArrayList<>();
        for (final String name : row.getPropertyNames())
          cells.add(name + "=" + row.getProperty(name));
        rows.add(String.join(",", cells));
      }
    }
    return rows;
  }
}
