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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #9301: a view over some of the vertex types was used for every traversal over its edge types,
 * also when the traversal reached a vertex type the view does not hold, so SQL out(), SQL MATCH and openCypher MATCH
 * answered 0 where the edges exist. A view only accelerates: a statement answers the same with and without it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9301PartialVertexCoverageViewTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("A");
    database.getSchema().createVertexType("B");
    database.getSchema().createVertexType("C");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      final MutableVertex a1 = database.newVertex("A").set("id", 1).save();
      final MutableVertex b1 = database.newVertex("B").set("id", 11).save();
      final MutableVertex b2 = database.newVertex("B").set("id", 12).save();
      final MutableVertex c1 = database.newVertex("C").set("id", 21).save();
      final MutableVertex c2 = database.newVertex("C").set("id", 22).save();
      a1.newEdge("E", b1);
      a1.newEdge("E", b2);
      b1.newEdge("E", c1);
      b2.newEdge("E", c2);
    });
  }

  private long count(final String language, final String query) {
    return database.query(language, query).next().<Number>getProperty("n").longValue();
  }

  private void assertSameAnswers(final String state) {
    assertThat(count("sql", "SELECT count(*) AS n FROM (SELECT expand(out('E')) FROM B)")).as(state + " out()").isEqualTo(2);
    assertThat(count("sql", "SELECT sum(out('E').size()) AS n FROM B")).as(state + " sum(out().size())").isEqualTo(2);
    assertThat(count("sql", "MATCH {type: A, as: a}.out('E'){type: B, as: b}.out('E'){type: C, as: c} RETURN count(*) AS n"))
        .as(state + " SQL MATCH").isEqualTo(2);
    assertThat(count("opencypher", "MATCH (a:A)-[:E]->(b:B)-[:E]->(c:C) RETURN count(*) AS n")).as(state + " Cypher chain").isEqualTo(2);
    assertThat(count("opencypher", "MATCH (b:B)-[:E]->(c:C) RETURN count(*) AS n")).as(state + " Cypher hop").isEqualTo(2);
    assertThat(count("opencypher", "MATCH (c:C)<-[:E]-(b:B)<-[:E]-(a:A) RETURN count(*) AS n")).as(state + " Cypher reverse").isEqualTo(2);
    assertThat(database.query("opencypher", "MATCH (b:B)-[:E]->(c:C) RETURN c.id AS id ORDER BY id").stream()
        .map(r -> r.<Number>getProperty("id").intValue()).toList()).as(state + " Cypher ids").containsExactly(21, 22);
  }

  private void withView(final String vertexTypes, final boolean coversAll, final String state) throws Exception {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g9301 VERTEX TYPES (" + vertexTypes + ") EDGE TYPES (E) UPDATE MODE OFF");
    try {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g9301");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).as(state + " ready").isTrue();
      // the registry hands a view to a walk only when it can answer for every vertex the walk reaches
      assertThat(GraphTraversalProviderRegistry.findProvider(database, "E")).as(state + " findProvider")
          .isEqualTo(coversAll ? view : null);
      assertThat(GraphTraversalProviderRegistry.findProviderAllowingPartialVertexCoverage(database, "E")).as(state + " partial")
          .isSameAs(view);
      assertSameAnswers(state);
      // the one-hop scan, the one path that accepts a partial view, answers the same
      assertThat(count("opencypher", "MATCH (a:A)-[:E]->(b:B) RETURN count(*) AS n")).as(state + " Cypher one hop").isEqualTo(2);
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9301");
    }
  }

  @Test
  void viewOverSomeVertexTypesAnswersLikeNoView() throws Exception {
    assertSameAnswers("no view");
    withView("A, B", false, "view (A, B)");
    withView("A, C", false, "view (A, C)");
    withView("A, B, C", true, "view (A, B, C)");
    assertSameAnswers("view dropped");
  }

  @Test
  void newVertexTypeMakesTheViewPartial() throws Exception {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g9301 VERTEX TYPES (A, B, C) EDGE TYPES (E) UPDATE MODE OFF");
    try {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g9301");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertThat(view.coversVertexType(null)).isTrue();
      database.getSchema().createVertexType("D");
      assertThat(view.coversVertexType(null)).as("a vertex type the view does not hold appeared").isFalse();
      assertThat(GraphTraversalProviderRegistry.findProvider(database, "E")).isNull();
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9301");
    }
  }
}
