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
 * Tests for the algo.biconnectedComponents Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoBiconnectedComponentsTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-biconnected");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Graph:
    // Triangle A-B-C (one biconnected component)
    // Bridge: C-D (biconnected component = {C,D})
    // Triangle D-E-F (one biconnected component)
    // Nodes C and D are each in 2 components (articulation points)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      final MutableVertex f = database.newVertex("Node").set("name", "F").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      c.newEdge("EDGE", d, true, (Object[]) null).save();
      d.newEdge("EDGE", e, true, (Object[]) null).save();
      e.newEdge("EDGE", f, true, (Object[]) null).save();
      d.newEdge("EDGE", f, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void biconnectedComponentsReturnsResults() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.biconnectedComponents() YIELD node, componentId RETURN node.name AS name, componentId");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isNotEmpty();
  }

  @Test
  void biconnectedComponentsMultipleComponents() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.biconnectedComponents() YIELD node, componentId RETURN DISTINCT componentId ORDER BY componentId");

    final Set<Integer> components = new HashSet<>();
    while (rs.hasNext())
      components.add(((Number) rs.next().getProperty("componentId")).intValue());

    // Should have at least 3 components: triangle A-B-C, bridge C-D, triangle D-E-F
    assertThat(components.size()).isGreaterThanOrEqualTo(3);
  }

  @Test
  void biconnectedComponentIdsAreNonNegative() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.biconnectedComponents() YIELD node, componentId RETURN componentId");

    while (rs.hasNext())
      assertThat(((Number) rs.next().getProperty("componentId")).intValue()).isGreaterThanOrEqualTo(0);
  }
}
