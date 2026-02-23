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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.triangleCount Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoTriangleCountTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-triangle");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Triangle A-B-C plus isolated node D
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      database.newVertex("Node").set("name", "D").save(); // isolated
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      a.newEdge("LINK", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void triangleNodesHaveOneTriangle() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.triangleCount() YIELD node, triangles RETURN node, triangles");

    final Map<String, Long> triangles = new HashMap<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      if (nodeObj instanceof Vertex v)
        triangles.put(v.getString("name"), ((Number) r.getProperty("triangles")).longValue());
    }

    assertThat(triangles.get("A")).isEqualTo(1L);
    assertThat(triangles.get("B")).isEqualTo(1L);
    assertThat(triangles.get("C")).isEqualTo(1L);
    assertThat(triangles.get("D")).isEqualTo(0L);
  }

  @Test
  void isolatedNodeHasZeroTriangles() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.triangleCount() YIELD node, triangles RETURN node, triangles");
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      if (nodeObj instanceof Vertex v && "D".equals(v.getString("name"))) {
        final long count = ((Number) r.getProperty("triangles")).longValue();
        assertThat(count).isEqualTo(0L);
      }
    }
  }

  @Test
  void clusteringCoefficientOneForTriangleNodes() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.triangleCount() YIELD node, clusteringCoefficient RETURN node, clusteringCoefficient");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      if (nodeObj instanceof Vertex v && !"D".equals(v.getString("name"))) {
        final double coeff = ((Number) r.getProperty("clusteringCoefficient")).doubleValue();
        assertThat(coeff).isEqualTo(1.0);
      }
    }
  }

  @Test
  void triangleCountReturnsFourNodes() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.triangleCount() YIELD node, triangles RETURN node, triangles");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(4);
  }
}
