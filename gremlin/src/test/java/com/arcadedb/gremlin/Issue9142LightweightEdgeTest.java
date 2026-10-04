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
package com.arcadedb.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/9142
 * <p>
 * Gremlin could not see the edges of a LIGHTWEIGHT edge type: outE(), inE(), bothE(), g.E() and drop() skipped them and
 * {@code out('X').count()} answered 0 while {@code out('X')} returned the neighbours. A lightweight edge has no record, so every
 * path that looked the edge up as a record, or scanned the bucket of its type, missed it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9142LightweightEdgeTest {
  private ArcadeGraph graph;
  private Database    db;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-issue9142");
    db = (Database) graph.getDatabase();
    db.command("sql", "CREATE VERTEX TYPE P").close();
    db.command("sql", "CREATE EDGE TYPE Light LIGHTWEIGHT").close();
    db.command("sql", "CREATE EDGE TYPE Reg").close();
    for (int n = 1; n <= 3; n++)
      graph.addVertex(T.label, "P", "n", n);
    graph.tx().commit();
    db.transaction(() -> {
      for (final String type : new String[] { "Light", "Reg" })
        for (final int[] e : new int[][] { { 1, 2 }, { 1, 3 }, { 2, 3 } })
          db.command("sql", "CREATE EDGE " + type + " FROM (SELECT FROM P WHERE n = " + e[0] + ") TO (SELECT FROM P WHERE n = " + e[1] + ")")
              .close();
    });
  }

  @AfterEach
  void teardown() {
    if (graph.tx().isOpen())
      graph.tx().rollback();
    graph.drop();
  }

  private long sqlCount(final String type) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM " + type)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  @Test
  void edgeCountsMatchTheRegularType() {
    final GraphTraversalSource g = graph.traversal();
    for (final String type : new String[] { "Light", "Reg" }) {
      assertThat(g.E().hasLabel(type).count().next()).as("E.hasLabel(%s).count", type).isEqualTo(3L);
      assertThat(g.E().hasLabel(type).toList()).as("E.hasLabel(%s).toList", type).hasSize(3);
      assertThat(g.V().outE(type).count().next()).as("outE(%s)", type).isEqualTo(3L);
      assertThat(g.V().inE(type).count().next()).as("inE(%s)", type).isEqualTo(3L);
      assertThat(g.V().bothE(type).count().next()).as("bothE(%s)", type).isEqualTo(6L);
      assertThat(g.V().has("n", 1).outE(type).count().next()).as("v1.outE(%s)", type).isEqualTo(2L);
      assertThat(g.V().has("n", 1).out(type).count().next()).as("v1.out(%s).count", type).isEqualTo(2L);
      assertThat(g.V().out(type).count().next()).as("out(%s).count", type).isEqualTo(3L);
    }
  }

  @Test
  void globalEdgeScanSeesBothTypes() {
    final GraphTraversalSource g = graph.traversal();
    assertThat(g.E().count().next()).isEqualTo(6L);
    assertThat(g.E().toList()).hasSize(6);
    assertThat(g.V().has("n", 1).outE().count().next()).isEqualTo(4L);
  }

  @Test
  void edgeElementsAreNavigable() {
    final GraphTraversalSource g = graph.traversal();
    assertThat(g.V().has("n", 1).outE("Light").inV().values("n").toList()).containsExactlyInAnyOrder(2, 3);
    assertThat(g.V().has("n", 1).outE("Light").as("e").inV().select("e").count().next()).isEqualTo(2L);
    assertThat(g.E().hasLabel("Light").label().toList()).containsOnly("Light");
  }

  @Test
  void dropRemovesLightweightEdges() {
    final GraphTraversalSource g = graph.traversal();
    g.E().hasLabel("Light").drop().iterate();
    graph.tx().commit();
    assertThat(sqlCount("Light")).isZero();
    assertThat(sqlCount("Reg")).isEqualTo(3L);
    assertThat(g.V().out("Light").count().next()).isZero();
  }

  @Test
  void dropThroughTheVertexRemovesThem() {
    final GraphTraversalSource g = graph.traversal();
    g.V().outE("Light").drop().iterate();
    graph.tx().commit();
    assertThat(sqlCount("Light")).isZero();
    assertThat(g.V().bothE("Light").count().next()).isZero();
  }

  @Test
  void addEOfALightweightTypeIsVisible() {
    final GraphTraversalSource g = graph.traversal();
    g.V().has("n", 3).addE("Light").to(__.V().has("n", 1)).iterate();
    graph.tx().commit();
    assertThat(sqlCount("Light")).isEqualTo(4L);
    assertThat(g.V().has("n", 3).outE("Light").count().next()).isEqualTo(1L);
    assertThat(g.E().hasLabel("Light").count().next()).isEqualTo(4L);
  }

  @Test
  void anAbandonedScanLeavesNothingBehind() {
    final GraphTraversalSource g = graph.traversal();
    assertThat(g.E().limit(1).toList()).hasSize(1);
    assertThat(g.E().hasLabel("Light").limit(1).toList()).hasSize(1);
    assertThat(g.E().hasLabel("Light").count().next()).isEqualTo(3L);
  }

  @Test
  void aRegularSuperTypeOfALightweightTypeIsNotReadTwice() {
    db.command("sql", "CREATE EDGE TYPE Base").close();
    db.command("sql", "CREATE EDGE TYPE SubLight EXTENDS Base LIGHTWEIGHT").close();
    db.transaction(() -> db.command("sql", "CREATE EDGE SubLight FROM (SELECT FROM P WHERE n = 1) TO (SELECT FROM P WHERE n = 2)").close());
    final GraphTraversalSource g = graph.traversal();
    assertThat(g.E().count().next()).isEqualTo(7L);
    assertThat(g.E().hasLabel("Base").count().next()).isEqualTo(1L);
    assertThat(g.E().hasLabel("SubLight").count().next()).isEqualTo(1L);
  }
}
