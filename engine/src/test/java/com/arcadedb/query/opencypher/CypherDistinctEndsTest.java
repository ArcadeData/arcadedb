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
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A statement that reads only the distinct far ends of a chain of anonymous hops is answered by ExpandToDistinctEnds, which
 * produces each far end once per input row instead of once per path. Every answer is checked against the same pattern with
 * its intermediate nodes named, which the rewrite never takes and so walks every path, on a graph built to hit the cases
 * relationship uniqueness decides: parallel relationships, self-loops, relationships of two types, lightweight
 * relationships, both directions, labels. And the shapes that can tell how many times a row comes keep the walk.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDistinctEndsTest extends TestHelper {
  private static final int NODES = 30;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("P");
    database.getSchema().createVertexType("Q");
    // The seek on p is what the optimizer plans a pattern with unlabelled nodes from (a label scan sends it to the step plan)
    database.command("sql", "CREATE PROPERTY P.id INTEGER");
    database.command("sql", "CREATE INDEX ON P (id) UNIQUE");
    database.getSchema().createEdgeType("K");
    database.getSchema().createEdgeType("W");
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
    database.transaction(() -> {
      final Random random = new Random(7);
      final MutableVertex[] nodes = new MutableVertex[NODES];
      for (int i = 0; i < NODES; i++)
        nodes[i] = database.newVertex(i % 7 == 0 ? "Q" : "P").set("id", i, "age", 20 + random.nextInt(40), "team", "t" + i % 3).save();
      for (int i = 0; i < NODES * 3; i++) {
        final int a = random.nextInt(NODES);
        final int b = random.nextInt(NODES);
        nodes[a].newEdge(random.nextInt(4) == 0 ? "W" : "K", nodes[b]);
        if (random.nextInt(5) == 0)
          nodes[a].newEdge("L", nodes[b]);
      }
      // Parallel relationships, a parallel pair of lightweight ones, self-loops, a dense pair
      nodes[0].newEdge("K", nodes[1]);
      nodes[0].newEdge("K", nodes[1]);
      nodes[1].newEdge("K", nodes[0]);
      nodes[2].newEdge("L", nodes[3]);
      nodes[2].newEdge("L", nodes[3]);
      nodes[4].newEdge("K", nodes[4]);
      nodes[5].newEdge("K", nodes[5]);
      nodes[5].newEdge("K", nodes[5]);
      nodes[6].newEdge("L", nodes[6]);
    });
  }

  @Test
  void theChainOfAFriendsOfFriendsCountIsAnsweredByDistinctEnds() {
    assertThat(plan("MATCH (p:P)-[:K]-(:P)-[:K]-(:P)-[:K]-(x:P) WHERE p.id = 1 RETURN count(DISTINCT x) AS n")).contains(
        "ExpandToDistinctEnds");
    assertThat(plan("MATCH (p:P)-[:K]-()-[:K]-(x) WHERE p.id = 1 AND x.age > 30 RETURN count(DISTINCT x) AS n")).contains(
        "ExpandToDistinctEnds");
    assertThat(plan("MATCH (p:P)-[:K]-()-[:K]-(x) WHERE p.id = 1 RETURN DISTINCT x.team AS team")).contains("ExpandToDistinctEnds");
  }

  @Test
  void aStatementThatCanCountItsRowsKeepsTheWalk() {
    final String chain = "MATCH (p:P)-[:K]-()-[:K]-(x) WHERE p.id = 1 ";
    for (final String tail : List.of(//
        "RETURN count(x) AS n", //
        "RETURN count(*) AS n", //
        "RETURN collect(DISTINCT x) AS n", //
        "RETURN sum(x.age) AS n", //
        "RETURN x", //
        "RETURN DISTINCT x LIMIT 3", //
        "RETURN count(DISTINCT x) AS n, rand() AS r", //
        "WITH DISTINCT x RETURN count(x) AS n"))
      assertThat(plan(chain + tail)).as(tail).doesNotContain("ExpandToDistinctEnds");

    assertThat(plan("MATCH path = (p:P)-[:K]-()-[:K]-(x) WHERE p.id = 1 RETURN count(DISTINCT x) AS n")).doesNotContain(
        "ExpandToDistinctEnds");
    assertThat(plan("MATCH (p:P)-[r:K]-()-[:K]-(x) WHERE p.id = 1 RETURN count(DISTINCT r) AS n")).doesNotContain(
        "ExpandToDistinctEnds");
    assertThat(plan("MATCH (p:P) OPTIONAL MATCH (p)-[:K]-()-[:K]-(x) RETURN count(DISTINCT x) AS n")).doesNotContain(
        "ExpandToDistinctEnds");
  }

  /** Every start, every shape: the distinct ends answer what the walk over the named pattern answers. */
  @Test
  void theDistinctEndsAnswerWhatTheWalkAnswers() {
    final String[][] shapes = {
        // anonymous (rewritten), named (walked)
        { "(p:P)-[:K]-()-[:K]-(x)", "(p:P)-[:K]-(a)-[:K]-(x)" },
        { "(p:P)-[:K]-(:P)-[:K]-(:P)-[:K]-(x:P)", "(p:P)-[:K]-(a:P)-[:K]-(b:P)-[:K]-(x:P)" },
        { "(p:P)-[:K]->()-[:K]->()<-[:K]-(x)", "(p:P)-[:K]->(a)-[:K]->(b)<-[:K]-(x)" },
        { "(p:P)-[:K|W]-()-[:K]-()-[:W|K]-(x)", "(p:P)-[:K|W]-(a)-[:K]-(b)-[:W|K]-(x)" },
        { "(p:P)-[:K]-()-[:W]-()-[:K]-(x)", "(p:P)-[:K]-(a)-[:W]-(b)-[:K]-(x)" },
        { "(p:P)-[:L]-()-[:L]-()-[:L]-(x)", "(p:P)-[:L]-(a)-[:L]-(b)-[:L]-(x)" },
        { "(p:P)-[:L|K]-()-[:L|K]-(x)", "(p:P)-[:L|K]-(a)-[:L|K]-(x)" },
        { "(p:P)-[:K]-()-[:K]-()-[:K]-()-[:K]-(x)", "(p:P)-[:K]-(a)-[:K]-(b)-[:K]-(c)-[:K]-(x)" },
        { "(p:P)-[:K]-(:Q)-[:K]-(x)", "(p:P)-[:K]-(a:Q)-[:K]-(x)" } };

    for (final String[] shape : shapes)
      for (int start = 0; start < NODES; start++) {
        final Map<String, Object> id = Map.of("id", start);
        final String where = " WHERE p.id = $id ";
        assertThat(single("MATCH " + shape[0] + where + "RETURN count(DISTINCT x) AS n", id)).as(shape[0] + " from " + start)
            .isEqualTo(single("MATCH " + shape[1] + where + "RETURN count(DISTINCT x) AS n", id));
        assertThat(single("MATCH " + shape[0] + where + "AND x.age > 35 RETURN count(DISTINCT x) AS n", id)).as(shape[0])
            .isEqualTo(single("MATCH " + shape[1] + where + "AND x.age > 35 RETURN count(DISTINCT x) AS n", id));
        assertThat(single("MATCH " + shape[0] + where + "RETURN max(x.age) AS n", id)).as(shape[0])
            .isEqualTo(single("MATCH " + shape[1] + where + "RETURN max(x.age) AS n", id));
      }
  }

  /** Many input rows, grouping keys, a DISTINCT projection, an ORDER BY: still the walk's answer. */
  @Test
  void groupsAndDistinctRowsAnswerWhatTheWalkAnswers() {
    // Labelled intermediates: from a label scan the optimizer plans only a pattern whose nodes all have labels
    final String anonymous = "MATCH (p:P)-[:K]-(:P)-[:K]-(:P)-[:K]-(x:P) ";
    final String named = "MATCH (p:P)-[:K]-(a:P)-[:K]-(b:P)-[:K]-(x:P) ";
    for (final String tail : List.of(//
        "RETURN p.team AS team, count(DISTINCT x) AS n ORDER BY team", //
        "RETURN p.id AS id, count(DISTINCT x) AS n, min(x.age) AS youngest ORDER BY id", //
        "WHERE p.age > 30 RETURN p.team AS team, avg(DISTINCT x.age) AS n ORDER BY team", //
        "RETURN DISTINCT p.id AS id, x.team AS team ORDER BY id, team", //
        "RETURN count(DISTINCT x) AS n")) {
      assertThat(plan(anonymous + tail)).as(tail).contains("ExpandToDistinctEnds");
      assertThat(rows(anonymous + tail)).as(tail).isEqualTo(rows(named + tail)).isNotEmpty();
    }
  }

  /** Inside a transaction the operator reads what the transaction wrote, as the walk does. */
  @Test
  void aTransactionsOwnWritesAreSeen() {
    final String query = "MATCH (p:P)-[:K]-()-[:K]-(x) WHERE p.id = 1 RETURN count(DISTINCT x) AS n";
    assertThat(plan(query)).contains("ExpandToDistinctEnds");
    final long before = single(query, Map.of());
    database.begin();
    try {
      final MutableVertex fresh = database.newVertex("P").set("id", 1_000).save();
      // A new neighbour of a neighbour of 1
      final Result neighbour = database.query("opencypher", "MATCH (p:P {id: 1})-[:K]-(a) RETURN a LIMIT 1").next();
      neighbour.<Vertex>getProperty("a").modify().newEdge("K", fresh);
      assertThat(single(query, Map.of())).isEqualTo(before + 1);
    } finally {
      database.rollback();
    }
    assertThat(single(query, Map.of())).isEqualTo(before);
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, Map.of("id", 0))) {
      return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
    }
  }

  private long single(final String query, final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      final Object n = rs.next().getProperty("n");
      return n == null ? -1 : ((Number) n).longValue();
    }
  }

  private List<Map<String, Object>> rows(final String query) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.toMap());
      }
    }
    return rows;
  }
}
