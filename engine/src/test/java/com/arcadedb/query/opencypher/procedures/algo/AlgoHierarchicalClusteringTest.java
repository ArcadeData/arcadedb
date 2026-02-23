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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.hierarchicalClustering Cypher procedure.
 */
class AlgoHierarchicalClusteringTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-hierarchical");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Two cliques {A,B,C} and {D,E,F} with one bridge A-D
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      final MutableVertex f = database.newVertex("Node").set("name", "F").save();
      // Clique 1
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", a, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", b, true, (Object[]) null).save();
      a.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", a, true, (Object[]) null).save();
      // Clique 2
      d.newEdge("LINK", e, true, (Object[]) null).save();
      e.newEdge("LINK", d, true, (Object[]) null).save();
      e.newEdge("LINK", f, true, (Object[]) null).save();
      f.newEdge("LINK", e, true, (Object[]) null).save();
      d.newEdge("LINK", f, true, (Object[]) null).save();
      f.newEdge("LINK", d, true, (Object[]) null).save();
      // Bridge
      a.newEdge("LINK", d, true, (Object[]) null).save();
      d.newEdge("LINK", a, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void hierarchicalClusteringReturnsOneRowPerVertex() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.hierarchicalClustering() YIELD nodeId, cluster RETURN nodeId, cluster");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(6);
  }

  @Test
  void hierarchicalClusteringWithTwoClusters() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.hierarchicalClustering('LINK', 2) YIELD nodeId, cluster RETURN nodeId, cluster");

    final Set<Integer> clusters = new HashSet<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("cluster");
      assertThat(val).isNotNull();
      clusters.add(((Number) val).intValue());
    }

    // Should produce at most 2 distinct clusters
    assertThat(clusters.size()).isLessThanOrEqualTo(2);
    assertThat(clusters.size()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void hierarchicalClusteringClusterIdsNonNegative() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.hierarchicalClustering('LINK', 2) YIELD nodeId, cluster RETURN nodeId, cluster");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("cluster");
      assertThat(((Number) val).intValue()).isGreaterThanOrEqualTo(0);
    }
  }

  @Test
  void hierarchicalClusteringWithOnlyOneCluster() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.hierarchicalClustering('LINK', 1) YIELD nodeId, cluster RETURN nodeId, cluster");

    final Set<Integer> clusters = new HashSet<>();
    int count = 0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("cluster");
      clusters.add(((Number) val).intValue());
      count++;
    }

    assertThat(count).isEqualTo(6);
    // With numClusters=1, all nodes should be in the same cluster
    assertThat(clusters.size()).isEqualTo(1);
  }

  @Test
  void hierarchicalClusteringEmptyGraph() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-hierarchical-empty");
    if (factory.exists())
      factory.open().drop();
    final Database emptyDb = factory.create();
    emptyDb.getSchema().createVertexType("Node");
    emptyDb.getSchema().createEdgeType("LINK");

    final ResultSet rs = emptyDb.query("opencypher",
        "CALL algo.hierarchicalClustering() YIELD nodeId, cluster RETURN nodeId, cluster");

    assertThat(rs.hasNext()).isFalse();
    emptyDb.drop();
  }
}
