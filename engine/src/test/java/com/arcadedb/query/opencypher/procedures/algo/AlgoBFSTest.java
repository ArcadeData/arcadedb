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
 * Tests for the algo.bfs Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoBFSTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-bfs");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("CONNECTS");

    // Graph: A -CONNECTS-> B -CONNECTS-> C -CONNECTS-> D
    //        A -CONNECTS-> E
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      a.newEdge("CONNECTS", b, true, (Object[]) null).save();
      b.newEdge("CONNECTS", c, true, (Object[]) null).save();
      c.newEdge("CONNECTS", d, true, (Object[]) null).save();
      a.newEdge("CONNECTS", e, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void bfsReturnsAllReachableNodes() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (start:Node {name: 'A'}) CALL algo.bfs(start) YIELD node, depth RETURN node.name AS name, depth");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // Start node itself is not in results; 4 other nodes reachable
    assertThat(results).hasSize(4);
  }

  @Test
  void bfsDepthIsCorrect() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (start:Node {name: 'A'}) CALL algo.bfs(start, 'CONNECTS', 'OUT') YIELD node, depth RETURN node.name AS name, depth ORDER BY depth, name");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // B and E are at depth 1, C at depth 2, D at depth 3
    assertThat(results).hasSize(4);
    assertThat(((Number) results.get(0).getProperty("depth")).intValue()).isEqualTo(1);
    assertThat(((Number) results.get(1).getProperty("depth")).intValue()).isEqualTo(1);
    assertThat(((Number) results.get(2).getProperty("depth")).intValue()).isEqualTo(2);
    assertThat(((Number) results.get(3).getProperty("depth")).intValue()).isEqualTo(3);
  }

  @Test
  void bfsRespectMaxDepth() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (start:Node {name: 'A'}) CALL algo.bfs(start, null, 'OUT', 1) YIELD node, depth RETURN node.name AS name, depth");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // Only depth-1 neighbours: B and E
    assertThat(results).hasSize(2);
    for (final Result r : results)
      assertThat(((Number) r.getProperty("depth")).intValue()).isEqualTo(1);
  }

  @Test
  void bfsDisconnectedStartNodeReturnsEmpty() {
    database.transaction(() -> database.newVertex("Node").set("name", "Z").save());
    final ResultSet rs = database.query("opencypher",
        "MATCH (start:Node {name: 'Z'}) CALL algo.bfs(start, 'CONNECTS', 'BOTH') YIELD node RETURN node");
    assertThat(rs.hasNext()).isFalse();
  }
}
