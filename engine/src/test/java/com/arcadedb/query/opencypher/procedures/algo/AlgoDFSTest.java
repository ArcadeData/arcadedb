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
 * Tests for the algo.dfs Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoDFSTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-dfs");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Linear chain: A -> B -> C -> D
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
      c.newEdge("EDGE", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void dfsReturnsAllReachableNodes() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (start:Node {name: 'A'}) CALL algo.dfs(start) YIELD node, depth RETURN node.name AS name, depth");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3); // B, C, D
  }

  @Test
  void dfsRespectMaxDepth() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (start:Node {name: 'A'}) CALL algo.dfs(start, null, 'OUT', 2) YIELD node, depth RETURN node.name AS name, depth");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // Only B (depth 1) and C (depth 2) — D is at depth 3 and excluded
    assertThat(results).hasSize(2);
    for (final Result r : results)
      assertThat(((Number) r.getProperty("depth")).intValue()).isLessThanOrEqualTo(2);
  }

  @Test
  void dfsEachNodeHasPositiveDepth() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (start:Node {name: 'A'}) CALL algo.dfs(start, 'EDGE', 'OUT') YIELD node, depth RETURN depth");

    while (rs.hasNext()) {
      final Result r = rs.next();
      assertThat(((Number) r.getProperty("depth")).intValue()).isGreaterThan(0);
    }
  }
}
