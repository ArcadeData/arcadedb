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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.randomWalk Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoRandomWalkTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-randomwalk");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Directed cycle A→B→C→D→A — walk can always continue
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
      d.newEdge("LINK", a, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void randomWalkFiveSteps() {
    // Walk 5 steps from A in a cycle; the walk never hits a dead-end so steps=5
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.randomWalk(a, 5) " +
            "YIELD path, steps " +
            "RETURN path, steps");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();

    final long steps = ((Number) result.getProperty("steps")).longValue();
    assertThat(steps).isEqualTo(5L);

    @SuppressWarnings("unchecked")
    final Map<String, Object> path = (Map<String, Object>) result.getProperty("path");
    assertThat(path).isNotNull();

    @SuppressWarnings("unchecked")
    final List<Object> nodes = (List<Object>) path.get("nodes");
    // start node + 5 steps = 6 nodes in the path
    assertThat(nodes).hasSize(6);
  }

  @Test
  void randomWalkPathNotNull() {
    // Walk 3 steps with a fixed seed for reproducibility
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.randomWalk(a, 3, null, 'OUT', 42) " +
            "YIELD path, steps " +
            "RETURN path, steps");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object path = result.getProperty("path");
    assertThat(path).isNotNull();
  }
}
