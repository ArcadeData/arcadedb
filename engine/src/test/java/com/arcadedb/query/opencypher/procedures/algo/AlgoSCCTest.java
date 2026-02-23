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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.scc Cypher procedure (Strongly Connected Components).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoSCCTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-scc");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // SCC1: A↔B (cycle)
    // SCC2: C↔D (cycle)
    // Bridge A→C (one direction only — does not merge the two SCCs)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", a, true, (Object[]) null).save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
      d.newEdge("LINK", c, true, (Object[]) null).save();
      // One-way bridge; does not create a cycle between {A,B} and {C,D}
      a.newEdge("LINK", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void sccTwoComponents() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.scc() YIELD node, componentId RETURN node, componentId");

    final Set<Integer> components = new HashSet<>();
    while (rs.hasNext())
      components.add(((Number) rs.next().getProperty("componentId")).intValue());

    assertThat(components).hasSize(2);
  }

  @Test
  void sccCyclicNodesSameComponent() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.scc() YIELD node, componentId RETURN node, componentId");

    final Map<String, Integer> compMap = new HashMap<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      if (nodeObj instanceof Vertex v)
        compMap.put(v.getString("name"), ((Number) r.getProperty("componentId")).intValue());
    }

    // A and B form one SCC; C and D form another
    assertThat(compMap.get("A")).isEqualTo(compMap.get("B"));
    assertThat(compMap.get("C")).isEqualTo(compMap.get("D"));
    assertThat(compMap.get("A")).isNotEqualTo(compMap.get("C"));
  }

  @Test
  void sccReturnsFourNodes() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.scc() YIELD node, componentId RETURN node, componentId");
    int count = 0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object node = r.getProperty("node");
      final Object componentId = r.getProperty("componentId");
      assertThat(node).isNotNull();
      assertThat(componentId).isNotNull();
      count++;
    }
    assertThat(count).isEqualTo(4);
  }
}
