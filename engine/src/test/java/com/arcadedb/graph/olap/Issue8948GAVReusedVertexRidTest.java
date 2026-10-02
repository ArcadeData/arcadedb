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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8948: in a SYNCHRONOUS view, a new vertex taking the RID of a deleted base vertex
 * was skipped as "already in base" and so stayed marked deleted, hiding its edges from the Cypher count paths.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8948GAVReusedVertexRidTest extends TestHelper {

  @Test
  void reusedVertexRidKeepsItsEdgesInCounts() throws Exception {
    createSchemaAndView();

    final RID c = vertex("c").getIdentity();
    database.transaction(() -> vertex("c").delete());
    database.transaction(() -> {
      final MutableVertex nv = database.newVertex("V").set("name", "d").set("age", 20).save();
      assertThat(nv.getIdentity()).as("precondition: the new vertex reuses the deleted RID").isEqualTo(c);
      vertex("a").newEdge("K", nv);
      nv.newEdge("K", vertex("a"));
    });

    assertCounts();
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
    assertCounts();
  }

  /** Guard test: no RID reuse here, it pins that cascaded edge deletions of a plain base vertex are still recorded. */
  @Test
  void deletingVertexWithEdgesKeepsEdgeCountInStep() throws Exception {
    createSchemaAndView();
    database.transaction(() -> {
      vertex("a").newEdge("K", vertex("c"));
      vertex("c").newEdge("K", vertex("a"));
    });
    final GraphAnalyticalView gav = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(gav.getEdgeCount()).isEqualTo(2);
    database.transaction(() -> vertex("c").delete());
    assertThat(gav.getEdgeCount()).as("cascaded edge deletions must still be recorded").isEqualTo(0);
    assertThat(count("MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n")).isEqualTo(0L);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
  }

  @Test
  void reusedVertexPropertyUpdateAndDelete() throws Exception {
    createSchemaAndView();
    final RID c = vertex("c").getIdentity();
    database.transaction(() -> vertex("c").delete());
    database.transaction(() -> {
      final MutableVertex nv = database.newVertex("V").set("name", "d").set("age", 20).save();
      assertThat(nv.getIdentity()).as("precondition: the new vertex reuses the deleted RID").isEqualTo(c);
      vertex("a").newEdge("K", nv);
      nv.newEdge("K", vertex("a"));
    });
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(view.getNodeId(c)).as("the reused vertex is an overflow node").isGreaterThanOrEqualTo(2);
    assertThat(view.getEdgeCount()).isEqualTo(2);
    database.transaction(() -> vertex("d").set("age", 50).save());
    assertThat(count("MATCH (x:V)-[:K]->(y:V) WHERE y.age = 50 RETURN count(*) AS n")).isEqualTo(1L);
    database.transaction(() -> vertex("d").delete());
    assertThat(count("MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n")).isEqualTo(0L);
    assertThat(count("MATCH (y:V) RETURN count(*) AS n")).isEqualTo(1L);
    assertThat(GraphAnalyticalViewRegistry.get(database, "g").getEdgeCount()).isEqualTo(0);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
  }

  @Test
  void slotReusedTwice() throws Exception {
    createSchemaAndView();
    final RID c = vertex("c").getIdentity();
    database.transaction(() -> vertex("c").delete());
    database.transaction(() -> {
      final MutableVertex d = database.newVertex("V").set("name", "d").set("age", 20).save();
      assertThat(d.getIdentity()).as("precondition: d reuses the deleted RID").isEqualTo(c);
      vertex("a").newEdge("K", d);
    });
    database.transaction(() -> vertex("d").delete());
    database.transaction(() -> {
      final MutableVertex e = database.newVertex("V").set("name", "e").set("age", 40).save();
      assertThat(e.getIdentity()).as("precondition: e reuses the same RID again").isEqualTo(c);
      vertex("a").newEdge("K", e);
    });
    assertThat(count("MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n")).isEqualTo(1L);
    assertThat(count("MATCH (x:V)-[:K]->(y:V) WHERE y.age = 40 RETURN count(*) AS n")).isEqualTo(1L);
    assertThat(GraphAnalyticalViewRegistry.get(database, "g").getEdgeCount()).isEqualTo(1);
    database.transaction(() -> vertex("e").delete());
    assertThat(count("MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n")).isEqualTo(0L);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
  }

  /** Pins the invariant DeltaOverlay.merge() relies on: a TxDelta never carries both the delete and the reuse of a slot. */
  @Test
  void deleteAndCreateInOneTransaction() throws Exception {
    createSchemaAndView();
    final RID c = vertex("c").getIdentity();
    final RID[] reused = new RID[1];
    database.transaction(() -> {
      vertex("c").delete();
      final MutableVertex d = database.newVertex("V").set("name", "d").set("age", 20).save();
      reused[0] = d.getIdentity();
      vertex("a").newEdge("K", d);
    });
    assertThat(reused[0]).as("a slot freed by a transaction is not reused by that same transaction").isNotEqualTo(c);
    assertThat(count("MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n")).isEqualTo(1L);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
  }

  /** A plain new vertex (no slot reuse) must take later property updates too. */
  @Test
  void propertyUpdateOfPlainNewVertex() throws Exception {
    createSchemaAndView();
    database.transaction(() -> vertex("a").newEdge("K", database.newVertex("V").set("name", "n").set("age", 20).save()));
    database.transaction(() -> vertex("n").set("age", 60).save());
    assertThat(count("MATCH (x:V)-[:K]->(y:V) WHERE y.age = 60 RETURN count(*) AS n")).isEqualTo(1L);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
  }

  private void createSchemaAndView() throws Exception {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.name STRING");
    database.command("sql", "CREATE PROPERTY V.age INTEGER");
    database.command("sql", "CREATE EDGE TYPE K");
    database.transaction(() -> {
      database.newVertex("V").set("name", "a").set("age", 30).save();
      database.newVertex("V").set("name", "c").set("age", 10).save();
    });
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (V) EDGE TYPES (K) PROPERTIES (age) UPDATE MODE SYNCHRONOUS");
    GraphAnalyticalViewRegistry.get(database, "g").awaitReady(60, TimeUnit.SECONDS);
  }

  private void assertCounts() {
    assertThat(count("MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n")).isEqualTo(2L);
    assertThat(count("MATCH (x:V)-[:K]->(y:V) WHERE y.age > 15 RETURN count(*) AS n")).isEqualTo(2L);
    final StringBuilder groups = new StringBuilder();
    database.query("opencypher", "MATCH (x:V)-[:K]->(y:V) RETURN y.age AS age, count(*) AS n ORDER BY age").stream()
        .forEach(r -> groups.append("(").append(r.<Object>getProperty("age")).append(",").append(r.<Object>getProperty("n")).append(")"));
    assertThat(groups.toString()).isEqualTo("(20,1)(30,1)");
  }

  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private MutableVertex vertex(final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM V WHERE name = ?", name)) {
      return rs.next().getVertex().get().modify();
    }
  }
}
