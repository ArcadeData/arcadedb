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
 * Tests for the algo.bridges Cypher procedure.
 */
class AlgoBridgesTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-bridges");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Directed cycle A->B->C->A (no bridges within the cycle)
    // Plus C->D (bridge: D is a leaf with no path back into the cycle)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
      c.newEdge("EDGE", a, true, (Object[]) null).save();
      c.newEdge("EDGE", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void bridgeEdgeFound() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.bridges() YIELD source, target RETURN source, target");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSizeGreaterThanOrEqualTo(1);
  }

  @Test
  void bridgeSourceAndTargetSet() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.bridges() YIELD source, target RETURN source, target");

    while (rs.hasNext()) {
      final Result result = rs.next();
      final Object source = result.getProperty("source");
      final Object target = result.getProperty("target");
      assertThat(source).isNotNull();
      assertThat(target).isNotNull();
    }
  }

  @Test
  void noNonBridgeEdgesReturned() {
    // The directed cycle edges A->B, B->C, C->A should NOT be bridges
    // Only C->D is a bridge (D is a leaf with no path back)
    final ResultSet rs = database.query("opencypher",
        "CALL algo.bridges() YIELD source, target RETURN source.name AS src, target.name AS tgt");

    final List<String> bridgePairs = new ArrayList<>();
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String src = (String) result.getProperty("src");
      final String tgt = (String) result.getProperty("tgt");
      bridgePairs.add(src + "->" + tgt);
    }

    // The directed cycle edges are not bridges
    assertThat(bridgePairs).doesNotContain("A->B");
    assertThat(bridgePairs).doesNotContain("B->C");
    assertThat(bridgePairs).doesNotContain("C->A");
    // C->D is the bridge
    assertThat(bridgePairs).contains("C->D");
  }
}
