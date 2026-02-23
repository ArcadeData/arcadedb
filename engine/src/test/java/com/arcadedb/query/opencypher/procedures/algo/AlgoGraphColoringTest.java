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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.graphColoring Cypher procedure.
 */
class AlgoGraphColoringTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-graph-coloring");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Triangle A-B-C requires 3 colors (chromatic number = 3)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", a, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void graphColoringReturnsThreeNodes() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphColoring() YIELD node, color, chromaticNumber RETURN node, color, chromaticNumber");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);
  }

  @Test
  void graphColoringFieldsNotNull() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphColoring() YIELD node, color, chromaticNumber RETURN node, color, chromaticNumber");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object node = r.getProperty("node");
      final Object color = r.getProperty("color");
      final Object chromaticNumber = r.getProperty("chromaticNumber");
      assertThat(node).isNotNull();
      assertThat(color).isNotNull();
      assertThat(chromaticNumber).isNotNull();
    }
  }

  @Test
  void graphColoringColorsAreNonNegative() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphColoring() YIELD node, color RETURN color");

    while (rs.hasNext()) {
      final Object colorObj = rs.next().getProperty("color");
      assertThat(((Number) colorObj).intValue()).isGreaterThanOrEqualTo(0);
    }
  }

  @Test
  void graphColoringTriangleNeedsThreeColors() {
    // A triangle requires at least 3 colors
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphColoring() YIELD node, color, chromaticNumber RETURN node, color, chromaticNumber");

    int chromaticNumber = 0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      chromaticNumber = ((Number) r.getProperty("chromaticNumber")).intValue();
    }

    assertThat(chromaticNumber).isGreaterThanOrEqualTo(3);
  }

  @Test
  void graphColoringAdjacentNodesHaveDifferentColors() {
    // Build a simple edge A-B: adjacent nodes must have different colors
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-graph-coloring-edge");
    if (factory.exists())
      factory.open().drop();
    final Database db2 = factory.create();
    db2.getSchema().createVertexType("N");
    db2.getSchema().createEdgeType("E");
    final int[] colorA = { -1 };
    final int[] colorB = { -1 };

    db2.transaction(() -> {
      final MutableVertex a = db2.newVertex("N").set("name", "A").save();
      final MutableVertex b = db2.newVertex("N").set("name", "B").save();
      a.newEdge("E", b, true, (Object[]) null).save();
    });

    final ResultSet rs = db2.query("opencypher",
        "CALL algo.graphColoring() YIELD node, color RETURN node, color");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      if (nodeObj instanceof Vertex v) {
        final String name = v.getString("name");
        final int col = ((Number) r.getProperty("color")).intValue();
        if ("A".equals(name))
          colorA[0] = col;
        else if ("B".equals(name))
          colorB[0] = col;
      }
    }

    db2.drop();

    assertThat(colorA[0]).isNotEqualTo(-1);
    assertThat(colorB[0]).isNotEqualTo(-1);
    assertThat(colorA[0]).isNotEqualTo(colorB[0]);
  }
}
