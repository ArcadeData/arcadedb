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
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #9282: LSQB Q9 (two undirected hops, a {@code NOT (p1)-[:KNOWS]-(p3)} anti-pattern and a
 * trailing expansion) ran slower through the Graph Analytical View than without it, because the CSR path of the
 * anti-join count operator materialized the whole two-hop frontier of every anchor.
 * <p>
 * The count is cross-checked against the row count of the same pattern projected without an aggregate, which is the
 * ordinary materialization pipeline answering the same question, on a random multigraph with reciprocal and
 * parallel edges, with and without the view.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherAntiJoinChainGAVIssue9282Test extends TestHelper {
  private static final String PERSON_CHAIN =
      "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag)";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("Tag");
      database.getSchema().createVertexType("Other");
      database.getSchema().createEdgeType("KNOWS");
      database.getSchema().createEdgeType("HAS_INTEREST");

      final Random random = new Random(9282);
      final int persons = 60;
      final List<MutableVertex> people = new ArrayList<>();
      for (int i = 0; i < persons; i++)
        people.add(database.newVertex(i % 7 == 0 ? "Other" : "Person").set("id", i).save());
      final List<MutableVertex> tags = new ArrayList<>();
      for (int i = 0; i < 15; i++)
        tags.add(database.newVertex("Tag").set("id", i).save());

      for (int i = 0; i < 260; i++) {
        final MutableVertex a = people.get(random.nextInt(persons));
        // parallel and reciprocal edges come out of the random draw; self-loops are left out because the count
        // push-downs count an undirected loop twice (#9283), which is not what this test is about
        final MutableVertex b = people.get(random.nextInt(persons));
        if (a.getIdentity().equals(b.getIdentity()))
          continue;
        a.newEdge("KNOWS", b).save();
      }
      // a hub knowing everyone, over parallel edges: its adjacency is more than 16 times a leaf's, which is what sends the
      // anti-join correction through the binary-search side of the intersection
      final MutableVertex hub = people.get(1);
      for (int copy = 0; copy < 5; copy++)
        for (final MutableVertex other : people)
          if (!other.getIdentity().equals(hub.getIdentity()))
            hub.newEdge("KNOWS", other).save();
      for (int i = 0; i < 150; i++)
        people.get(random.nextInt(persons)).newEdge("HAS_INTEREST", tags.get(random.nextInt(tags.size()))).save();
    });
  }

  @Test
  void q9MatchesThePipelineWithoutTheView() {
    assertQ9MatchesThePipeline();
  }

  @Test
  void q9MatchesThePipelineThroughTheView() {
    final GraphAnalyticalView view = buildView();
    try {
      assertThat(GraphTraversalProviderRegistry.awaitAll(database, 30, TimeUnit.SECONDS)).isTrue();
      assertThat(GraphTraversalProviderRegistry.findProvider(database, "KNOWS", "HAS_INTEREST")).isNotNull();
      assertQ9MatchesThePipeline();
    } finally {
      view.drop();
    }
  }

  /**
   * An undirected hop has no stored adjacency: the view merges and sorts the whole edge set to answer for it. A snapshot
   * never changes, so that is done once and shared by every later request for the same direction and edge type.
   */
  @Test
  void theMergedUndirectedViewIsBuiltOncePerSnapshot() {
    final GraphAnalyticalView view = buildView();
    try {
      assertThat(GraphTraversalProviderRegistry.awaitAll(database, 30, TimeUnit.SECONDS)).isTrue();
      final NeighborView first = view.getNeighborView(Vertex.DIRECTION.BOTH, "KNOWS");
      assertThat(first).isNotNull();
      assertThat(view.getNeighborView(Vertex.DIRECTION.BOTH, "KNOWS")).isSameAs(first);
      // one edge type or two, in any order, are different questions with their own answers
      final NeighborView both = view.getNeighborView(Vertex.DIRECTION.BOTH, "KNOWS", "HAS_INTEREST");
      assertThat(both).isNotSameAs(first);
      assertThat(view.getNeighborView(Vertex.DIRECTION.BOTH, "HAS_INTEREST", "KNOWS")).isSameAs(both);
      assertThat(both.edgeCount()).isGreaterThan(first.edgeCount());
    } finally {
      view.drop();
    }
  }

  private void assertQ9MatchesThePipeline() {
    final String[] wheres = {
        " WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3",
        " WHERE p1 <> p3 AND NOT (p1)-[:KNOWS]-(p3)",
        " WHERE NOT (p1)-[:KNOWS]->(p3) AND p1 <> p3",
        " WHERE NOT (p1)<-[:KNOWS]-(p3) AND p1 <> p3",
        " WHERE NOT (p3)-[:KNOWS]->(p1) AND p1 <> p3" };
    for (final String where : wheres) {
      final String query = PERSON_CHAIN + where;
      assertThat(explainOf(query + " RETURN count(*) AS c")).as(query).contains("ANTI-JOIN");
      assertThat(scalarOf(query + " RETURN count(*) AS c")).as(query).isEqualTo(rowCountOf(query + " RETURN p1"));
    }
    // without the inequality, and with unlabelled middle and check positions
    for (final String query : new String[] {
        "MATCH (p1:Person)-[:KNOWS]-(p2)-[:KNOWS]-(p3)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3)-[:HAS_INTEREST]->(t) WHERE NOT (p1)-[:KNOWS]->(p3) AND p1 <> p3" }) {
      assertThat(explainOf(query + " RETURN count(*) AS c")).as(query).contains("ANTI-JOIN");
      assertThat(scalarOf(query + " RETURN count(*) AS c")).as(query).isEqualTo(rowCountOf(query + " RETURN p1"));
    }
    // the same chain with the tail hop one position earlier: still a push-down, still the pipeline's answer
    final String shorter = "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3";
    assertThat(scalarOf(shorter + " RETURN count(*) AS c")).as(shorter).isEqualTo(rowCountOf(shorter + " RETURN p1"));
  }

  /**
   * A bare {@code WHERE NOT (a)-[:KNOWS]-(c)} is pushed into the fused chain, which materializes only the variables
   * the filter reads. The variables of a pattern predicate were collected from its text as {@code var.property}, so
   * a predicate that names none read a missing {@code c} and rejected every row (0 rows instead of thousands). Adding
   * {@code a <> c} hid it, because the comparison names both.
   */
  @Test
  void aBarePatternPredicateIsEvaluatedAgainstItsVariablesThroughTheView() {
    final String[] queries = {
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) WHERE NOT (p1)-[:KNOWS]-(p3) RETURN p1",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) WHERE NOT (p1)-[:KNOWS]->(p3) RETURN p1",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) RETURN p1" };
    final List<Integer> withoutView = new ArrayList<>();
    for (final String query : queries)
      withoutView.add(rowCountOf(query));
    assertThat(withoutView).allMatch(rows -> rows > 0);

    final GraphAnalyticalView view = buildView();
    try {
      assertThat(GraphTraversalProviderRegistry.awaitAll(database, 30, TimeUnit.SECONDS)).isTrue();
      for (int i = 0; i < queries.length; i++)
        assertThat(rowCountOf(queries[i])).as(queries[i]).isEqualTo(withoutView.get(i));
    } finally {
      view.drop();
    }
  }

  private GraphAnalyticalView buildView() {
    return GraphAnalyticalView.builder(database)
        .withName("q9")
        .withVertexTypes("Person", "Tag", "Other")
        .withEdgeTypes("KNOWS", "HAS_INTEREST")
        .build();
  }

  private long scalarOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private int rowCountOf(final String query) {
    int count = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count;
  }

  private String explainOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
