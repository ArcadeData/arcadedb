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
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10213: a MATCH clause whose hops may bind the same relationship is walked through a Graph Analytical View also
 * when an edge type it walks has sub-types. The view answers {@code E} from the {@code E} and {@code F EXTENDS E} slices,
 * and the label each hop binds used to name the hop's REQUESTED type, so one {@code F} edge read by a hop on {@code E}
 * and by a hop on {@code F} carried two names and the collision went unseen: such a clause had to walk the edge records
 * (#8394). The walk now reads one concrete slice at a time and names a relationship after its slice.
 * <p>
 * The view is a performance feature, so the oracle is the answer without it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10213GAVEdgeSubTypeUniquenessTest extends TestHelper {
  private static final int VERTICES = 25;

  private static final String[] QUERIES = {
      // A chain over the parent, the sub-type, and the two mixed
      "MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:E]-(b:P)-[:E]-(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:E]->(b:P)-[:F]->(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:F]-(b:P)-[:E]-(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:F]-(b:P)-[:F]-(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P)-[:E]->(d:P) RETURN count(*) AS n",
      // A grandchild type, and a type listed together with its sub-type
      "MATCH (a:P)-[:G]-(b:P)-[:E]-(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:E|F]->(b:P)-[:F]->(c:P) RETURN count(*) AS n",
      // Rows and groups, not only counts
      "MATCH (a:P)-[:E]->(b:P)-[:F]->(c:P) RETURN a.id AS a, b.id AS b, c.id AS c ORDER BY a, b, c",
      "MATCH (a:P)-[:E]-(b:P)-[:E]-(c:P) WITH a, count(*) AS n RETURN a.id AS a, n ORDER BY a",
      // Branching, and closing a cycle
      "MATCH (a:P)-[:E]->(b:P), (a)-[:F]->(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:E]->(b:P)-[:F]->(a) RETURN count(*) AS n",
      "MATCH (a:P)-[:E]-(b:P)-[:E]-(a) RETURN count(*) AS n",
      // Untyped hops walk every type, sub-types included, each once
      "MATCH (a:P)-[]->(b:P)-[]->(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[]-(b:P)-[:F]-(c:P) RETURN count(*) AS n" };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE PROPERTY P.id INTEGER");
    database.command("sql", "CREATE EDGE TYPE E");
    database.command("sql", "CREATE EDGE TYPE F EXTENDS E");
    database.command("sql", "CREATE EDGE TYPE G EXTENDS F");
    database.command("sql", "CREATE EDGE TYPE L");

    final Random random = new Random(10213);
    final String[] types = { "E", "F", "G", "L" };
    database.transaction(() -> {
      final List<Vertex> vertices = new ArrayList<>();
      for (int i = 0; i < VERTICES; i++)
        vertices.add(database.newVertex("P").set("id", i).save());
      for (final Vertex v : vertices) {
        for (int k = 0; k < 4; k++)
          v.asVertex().modify().newEdge(types[random.nextInt(types.length)], vertices.get(random.nextInt(VERTICES)));
        // Self-loops and parallel relationships, of a type and of its sub-type, between the same pair
        if (random.nextInt(3) == 0)
          v.asVertex().modify().newEdge(random.nextBoolean() ? "E" : "F", v);
        if (random.nextInt(3) == 0) {
          final Vertex other = vertices.get(random.nextInt(VERTICES));
          v.asVertex().modify().newEdge("F", other);
          v.asVertex().modify().newEdge("F", other);
          v.asVertex().modify().newEdge("E", other);
        }
      }
    });
  }

  @Test
  void aClauseOverEdgeSubTypesIsServedByTheViewWithTheSameAnswers() {
    final Map<String, List<String>> expected = expectedWithoutView();

    createView("VERTEX TYPES (P) EDGE TYPES (E, L) PROPERTIES (id)");

    final List<String> failures = new ArrayList<>();
    for (final String query : QUERIES) {
      final String plan = plan(query);
      if (!plan.contains("unique relationships"))
        failures.add(query + "\n  not walked through the view with relationship labels\n" + plan);
      final List<String> actual = answer(query);
      if (!actual.equals(expected.get(query)))
        failures.add(query + "\n  expected " + expected.get(query) + "\n  actual   " + actual + "\n" + plan);
    }
    assertThat(failures).isEmpty();
  }

  /**
   * A view over part of the hierarchy only - F and the G under it, not E. A hop on E is not covered and the clause walks
   * the records; hops on F and G are walked through the view, F answering for G, with the same answers.
   */
  @Test
  void aViewOverPartOfTheHierarchyKeepsTheAnswers() {
    final String[] queries = {
        "MATCH (a:P)-[:F]-(b:P)-[:F]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:F]->(b:P)-[:G]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:G]-(b:P)-[:F]-(a) RETURN count(*) AS n",
        "MATCH (a:P)-[:E]->(b:P)-[:F]->(c:P) RETURN count(*) AS n" };
    final Map<String, List<String>> expected = new LinkedHashMap<>();
    for (final String query : queries)
      expected.put(query, answer(query));

    createView("VERTEX TYPES (P) EDGE TYPES (F) PROPERTIES (id)");
    for (int i = 0; i < 3; i++)
      assertThat(plan(queries[i])).as(queries[i]).contains("unique relationships");
    for (final String query : queries)
      assertThat(answer(query)).as(query + "\n" + plan(query)).isEqualTo(expected.get(query));
  }

  @Test
  void theIssueRepro() {
    final String query = "MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P) RETURN count(*) AS n";
    final List<String> expected = answer(query);
    createView("VERTEX TYPES (P) EDGE TYPES (E, L) PROPERTIES (id)");
    assertThat(plan(query)).contains("GAVFusedChain").contains("unique relationships");
    assertThat(answer(query)).isEqualTo(expected);
  }

  @Test
  void subTypeEdgesServedFromTheOverlayStayUnique() {
    createView("VERTEX TYPES (P) EDGE TYPES (E, L) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
    database.transaction(() -> {
      final Vertex x = database.newVertex("P").set("id", 100).save();
      final Vertex y = database.query("sql", "SELECT FROM P WHERE id = 1").next().getVertex().get();
      x.modify().newEdge("F", x);
      x.modify().newEdge("E", x);
      x.modify().newEdge("F", y);
      x.modify().newEdge("F", y);
      y.modify().newEdge("G", x);
    });

    final Map<String, List<String>> withView = new LinkedHashMap<>();
    for (final String query : QUERIES)
      withView.put(query, answer(query));
    assertThat(plan(QUERIES[0])).contains("provider=gav10213");

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav10213");
    for (final String query : QUERIES)
      assertThat(withView.get(query)).as(query).isEqualTo(answer(query));
  }

  @Test
  void aVertexTheViewDoesNotMapLabelsItsSubTypeEdgesLikeTheView() {
    // The record walk of an unmapped source names an edge after its own type, which is the slice's name in the view
    createView("VERTEX TYPES (P) EDGE TYPES (E, L) PROPERTIES (id) UPDATE MODE OFF");
    GraphAnalyticalViewRegistry.get(database, "gav10213").setUseWhenStale(true);
    database.transaction(() -> {
      final Vertex x = database.newVertex("P").set("id", 100).save();
      x.modify().newEdge("F", x);
      x.modify().newEdge("E", x);
    });

    // Two self-loops, one F and one E: each branch takes one of them, never the same one twice
    final String branching = "MATCH (a:P {id:100})-[:E]->(b:P), (a)-[:E]->(c:P) RETURN count(*) AS n";
    assertThat(plan(branching)).contains("unique relationships");
    assertThat(answer(branching)).containsExactly("n=2;");
    final String mixed = "MATCH (a:P {id:100})-[:E]->(b:P), (a)-[:F]->(c:P) RETURN count(*) AS n";
    assertThat(answer(mixed)).containsExactly("n=1;");
    final String cycle = "MATCH (a:P {id:100})-[:E]->(b:P)-[:F]->(a) RETURN count(*) AS n";
    assertThat(answer(cycle)).containsExactly("n=1;");
  }

  private Map<String, List<String>> expectedWithoutView() {
    final Map<String, List<String>> expected = new LinkedHashMap<>();
    for (final String query : QUERIES) {
      assertThat(plan(query)).as(query).doesNotContain("GAV");
      expected.put(query, answer(query));
    }
    return expected;
  }

  private void createView(final String definition) {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav10213 " + definition);
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav10213");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private List<String> answer(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder line = new StringBuilder();
        for (final String name : row.getPropertyNames())
          line.append(name).append('=').append((Object) row.getProperty(name)).append(';');
        rows.add(line.toString());
      }
    }
    return rows;
  }
}
