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
 * Tests for the algo.kcore Cypher procedure (K-Core Decomposition).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoKCoreTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-kcore");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Clique {A,B,C}: all connected to each other (each has degree 2 within clique)
    // D connected only to A (degree 1) → D is in 1-core, A/B/C in 2-core
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      // Triangle clique A-B-C
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      a.newEdge("LINK", c, true, (Object[]) null).save();
      // D connects only to A
      d.newEdge("LINK", a, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void kcoreCliqueMembersHaveCoreTwo() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.kcore() YIELD node, coreNumber RETURN node, coreNumber");

    final Map<String, Integer> cores = new HashMap<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      if (nodeObj instanceof Vertex v)
        cores.put(v.getString("name"), ((Number) r.getProperty("coreNumber")).intValue());
    }

    assertThat(cores.get("A")).isEqualTo(2);
    assertThat(cores.get("B")).isEqualTo(2);
    assertThat(cores.get("C")).isEqualTo(2);
    assertThat(cores.get("D")).isEqualTo(1);
  }

  @Test
  void kcorePeripheralNodeHasLowerCore() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.kcore() YIELD node, coreNumber RETURN node, coreNumber");
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      if (nodeObj instanceof Vertex v && "D".equals(v.getString("name"))) {
        final int core = ((Number) r.getProperty("coreNumber")).intValue();
        assertThat(core).isEqualTo(1);
      }
    }
  }

  @Test
  void kcoreFourNodes() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.kcore() YIELD node, coreNumber RETURN node, coreNumber");
    int count = 0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object node = r.getProperty("node");
      final Object coreNumber = r.getProperty("coreNumber");
      assertThat(node).isNotNull();
      assertThat(coreNumber).isNotNull();
      count++;
    }
    assertThat(count).isEqualTo(4);
  }
}
