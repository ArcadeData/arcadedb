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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.longestPath Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoLongestPathDAGTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-longestpath");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Task");
    database.getSchema().createEdgeType("DEPENDS_ON");

    // DAG: A -> B -> D
    //       \-> C -> D
    // Longest path: A -> B -> D (length 2) or A -> C -> D (length 2)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Task").set("name", "A").save();
      final MutableVertex b = database.newVertex("Task").set("name", "B").save();
      final MutableVertex c = database.newVertex("Task").set("name", "C").save();
      final MutableVertex d = database.newVertex("Task").set("name", "D").save();
      a.newEdge("DEPENDS_ON", b, true, (Object[]) null).save();
      a.newEdge("DEPENDS_ON", c, true, (Object[]) null).save();
      b.newEdge("DEPENDS_ON", d, true, (Object[]) null).save();
      c.newEdge("DEPENDS_ON", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void longestPathReturnsEntryForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.longestPath() YIELD node, distance, source RETURN node.name AS name, distance ORDER BY name");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
  }

  @Test
  void longestPathDistanceIsCorrect() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.longestPath('DEPENDS_ON') YIELD node, distance, source " +
            "RETURN node.name AS name, distance ORDER BY distance DESC");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // D has the longest path from A (distance = 2)
    final Result maxNode = results.getFirst();
    assertThat((Object) maxNode.getProperty("name")).isEqualTo("D");
    assertThat(((Number) maxNode.getProperty("distance")).doubleValue()).isEqualTo(2.0);
  }

  @Test
  void longestPathOnCycleReturnsEmpty() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-longestpath-cycle");
    if (factory.exists())
      factory.open().drop();
    final Database cycleDb = factory.create();
    try {
      cycleDb.getSchema().createVertexType("Node");
      cycleDb.getSchema().createEdgeType("EDGE");
      cycleDb.transaction(() -> {
        final MutableVertex a = cycleDb.newVertex("Node").set("name", "A").save();
        final MutableVertex b = cycleDb.newVertex("Node").set("name", "B").save();
        a.newEdge("EDGE", b, true, (Object[]) null).save();
        b.newEdge("EDGE", a, true, (Object[]) null).save();
      });
      final ResultSet rs = cycleDb.query("opencypher",
          "CALL algo.longestPath() YIELD node RETURN node");
      assertThat(rs.hasNext()).as("cycle graph should yield no results").isFalse();
    } finally {
      cycleDb.drop();
    }
  }
}
