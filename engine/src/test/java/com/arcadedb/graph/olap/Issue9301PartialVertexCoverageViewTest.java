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
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.GraphBatch;
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
      assertThat(count("opencypher", "MATCH (a:A)-[:E]->(b:B) RETURN max(b.id) AS n")).as(state + " Cypher one hop max").isEqualTo(12);
      if ("A, B".equals(vertexTypes))
        assertThat(database.query("opencypher", "PROFILE MATCH (a:A)-[:E]->(b:B) RETURN max(b.id) AS n").getExecutionPlan().get()
            .prettyPrint(0, 2)).as(state + " one hop plan").contains("GAV ONE-HOP SCAN");
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
  void aVertexOfANewTypeMakesTheViewPartial() throws Exception {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g9301 VERTEX TYPES (A, B, C) EDGE TYPES (E) UPDATE MODE OFF");
    try {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g9301");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertThat(view.coversVertexType(null)).isTrue();
      database.getSchema().createVertexType("D");
      assertThat(view.coversVertexType(null)).as("a vertex type the view does not hold appeared, with no vertex yet").isTrue();
      database.transaction(() -> database.newVertex("D").set("id", 41).save());
      assertThat(view.coversVertexType(null)).as("a vertex the view does not hold appeared").isFalse();
      assertThat(GraphTraversalProviderRegistry.findProvider(database, "E")).isNull();
      database.getSchema().dropType("D");
      assertThat(view.coversVertexType(null)).as("the uncovered vertex type is gone").isTrue();
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9301");
    }
  }

  @Test
  void anEmptyVertexTypeTheViewDoesNotListKeepsItFull() throws Exception {
    // the LSQB shape: Post and Comment extend Message, which holds no vertex of its own, and the view lists the concrete
    // types only. No walk can reach a vertex of a type that has none, so the view still answers for every vertex
    database.getSchema().createVertexType("M");
    database.getSchema().getType("C").addSuperType("M");
    database.getSchema().createVertexType("Unused");
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g9301 VERTEX TYPES (A, B, C) EDGE TYPES (E) UPDATE MODE OFF");
    try {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g9301");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertThat(view.coversVertexType(null)).as("the unlisted types hold no vertex").isTrue();
      assertThat(GraphTraversalProviderRegistry.findProvider(database, "E")).isSameAs(view);
      assertSameAnswers("view (A, B, C) with M and Unused empty");
      assertThat(count("opencypher", "MATCH (b:B)-[:E]->(m:M) RETURN count(*) AS n")).isEqualTo(2);

      // a counter not known yet (fresh open with no statistics, unclean shutdown) still reads a bucket never written as empty
      for (final Bucket bucket : database.getSchema().getType("Unused").getBuckets(false))
        ((LocalBucket) bucket).setCachedRecordCount(-1);
      assertThat(view.coversVertexType(null)).as("unknown counter, no page written").isTrue();

      // an uncommitted vertex of an unlisted type, linked to a listed one: the view cannot see it, so no walk gets the view
      database.begin();
      try {
        final MutableVertex pending = database.newVertex("M").set("id", 32).save();
        database.query("sql", "SELECT FROM B WHERE id = 11").next().getVertex().get().newEdge("E", pending);
        assertThat(GraphTraversalProviderRegistry.findProvider(database, "E")).as("transaction holds changes").isNull();
        assertThat(count("opencypher", "MATCH (b:B)-[:E]->(m:M) RETURN count(*) AS n")).as("own write seen").isEqualTo(3);
        assertThat(count("sql", "SELECT count(*) AS n FROM (SELECT expand(out('E')) FROM B)")).as("own write seen").isEqualTo(3);
      } finally {
        database.rollback();
      }
      assertThat(GraphTraversalProviderRegistry.findProvider(database, "E")).as("rolled back").isSameAs(view);

      // a vertex of an unlisted type makes the view partial again, with no schema change to notice it
      final RID[] m1 = new RID[1];
      database.transaction(() -> m1[0] = database.newVertex("M").set("id", 31).save().getIdentity());
      assertThat(view.coversVertexType(null)).as("M holds a vertex").isFalse();
      assertThat(GraphTraversalProviderRegistry.findProvider(database, "E")).isNull();
      database.transaction(() -> m1[0].asVertex().delete());
      assertThat(view.coversVertexType(null)).as("M is empty again").isTrue();

      // emptied, then its counter lost: not provable without a scan, so partial until a count(*) recounts the bucket
      for (final Bucket bucket : database.getSchema().getType("M").getBuckets(false))
        ((LocalBucket) bucket).setCachedRecordCount(-1);
      assertThat(view.coversVertexType(null)).as("unknown counter, pages written").isFalse();
      assertThat(count("sql", "SELECT count(*) AS n FROM M")).as("C's vertices, polymorphic").isEqualTo(2);
      assertThat(view.coversVertexType(null)).as("recounted").isTrue();
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9301");
    }
  }

  /**
   * The coverage check trusts the committed record counter of the unlisted buckets: every way of creating a vertex has to
   * move it, or the view would read as full while a vertex it does not hold exists, and the wrong answers of #9301 would
   * be back. One unlisted type per path, so no path can hide behind another's vertex.
   */
  @Test
  void everyVertexCreatePathMakesTheViewPartial() throws Exception {
    final String[] paths = { "SqlInsert", "SqlCreateVertex", "CypherCreate", "ApiAsync", "GraphBatch", "GraphBatchProps" };
    for (final String type : paths)
      database.getSchema().createVertexType(type);
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g9301 VERTEX TYPES (A, B, C) EDGE TYPES (E) UPDATE MODE OFF");
    try {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g9301");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();

      final Runnable[] creates = {
          () -> database.command("sql", "INSERT INTO SqlInsert SET id = 51"),
          () -> database.command("sql", "CREATE VERTEX SqlCreateVertex SET id = 52"),
          () -> database.command("opencypher", "CREATE (:CypherCreate {id: 53})"),
          () -> {
            database.async().createRecord(database.newVertex("ApiAsync").set("id", 54), null);
            database.async().waitCompletion();
          },
          () -> {
            try (final GraphBatch batch = database.batch().build()) {
              batch.createVertices("GraphBatch", 2);
            }
          },
          () -> {
            try (final GraphBatch batch = database.batch().build()) {
              batch.createVertices("GraphBatchProps", new Object[][] { { "id", 56 } });
            }
          } };

      for (int i = 0; i < paths.length; i++) {
        assertThat(view.coversVertexType(null)).as("before " + paths[i]).isTrue();
        if (paths[i].startsWith("Sql") || paths[i].startsWith("Cypher"))
          database.transaction(creates[i]::run);
        else
          creates[i].run();
        assertThat(count("sql", "SELECT count(@rid) AS n FROM " + paths[i])).as(paths[i] + " created").isGreaterThan(0);
        assertThat(view.coversVertexType(null)).as("after " + paths[i]).isFalse();
        final String emptied = paths[i];
        database.transaction(() -> database.command("sql", "DELETE FROM " + emptied));
      }
      assertThat(view.coversVertexType(null)).as("all emptied again").isTrue();
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9301");
    }
  }

  @Test
  void oneHopScanSkipsAViewThatLacksAnEndpointForALaterOne() throws Exception {
    // the first view holds A and C, the second A and B: only the second can answer A -> B
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g9301ac VERTEX TYPES (A, C) EDGE TYPES (E) UPDATE MODE OFF");
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g9301ab VERTEX TYPES (A, B) EDGE TYPES (E) UPDATE MODE OFF");
    try {
      assertThat(GraphAnalyticalViewRegistry.get(database, "g9301ac").awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertThat(GraphAnalyticalViewRegistry.get(database, "g9301ab").awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertThat(count("opencypher", "MATCH (a:A)-[:E]->(b:B) RETURN max(b.id) AS n")).isEqualTo(12);
      assertThat(database.query("opencypher", "PROFILE MATCH (a:A)-[:E]->(b:B) RETURN max(b.id) AS n").getExecutionPlan().get()
          .prettyPrint(0, 2)).contains("GAV ONE-HOP SCAN");
      assertSameAnswers("two partial views");
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9301ac");
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9301ab");
    }
  }
}
