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
package com.arcadedb.function.sql.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the bellmanFord() SQL function.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionBellmanFordTest {

  private MutableVertex         v1, v2, v3, v4;
  private SQLFunctionBellmanFord functionBellmanFord;

  void setUp(final Database graph) {
    graph.transaction(() -> {
      graph.getSchema().createVertexType("node");
      graph.getSchema().createEdgeType("road");

      v1 = graph.newVertex("node").set("name", "A").save();
      v2 = graph.newVertex("node").set("name", "B").save();
      v3 = graph.newVertex("node").set("name", "C").save();
      v4 = graph.newVertex("node").set("name", "D").save();

      final MutableEdge e1 = v1.newEdge("road", v2, true, (Object[]) null);
      e1.set("weight", 1.0);
      e1.save();

      final MutableEdge e2 = v2.newEdge("road", v3, true, (Object[]) null);
      e2.set("weight", 1.0);
      e2.save();

      final MutableEdge e3 = v1.newEdge("road", v3, true, (Object[]) null);
      e3.set("weight", 100.0);
      e3.save();

      final MutableEdge e4 = v3.newEdge("road", v4, true, (Object[]) null);
      e4.set("weight", 1.0);
      e4.save();

      functionBellmanFord = new SQLFunctionBellmanFord();
    });
  }

  @Test
  void execute() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionBellmanFordTest", (graph) -> {
      setUp(graph);
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(graph);

      @SuppressWarnings("unchecked")
      final List<RID> result = (List<RID>) functionBellmanFord.execute(null, null, null,
          new Object[] { v1, v4, "'weight'" }, ctx);

      assertThat(result).hasSize(4);
      assertThat(result.get(0)).isEqualTo(v1.getIdentity());
      assertThat(result.get(1)).isEqualTo(v2.getIdentity());
      assertThat(result.get(2)).isEqualTo(v3.getIdentity());
      assertThat(result.get(3)).isEqualTo(v4.getIdentity());
    });
  }

  @Test
  void executeWithNegativeWeight() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionBellmanFordNegTest", (graph) -> {
      graph.transaction(() -> {
        graph.getSchema().createVertexType("node");
        graph.getSchema().createEdgeType("road");

        final MutableVertex a = graph.newVertex("node").set("name", "A").save();
        final MutableVertex b = graph.newVertex("node").set("name", "B").save();
        final MutableVertex c = graph.newVertex("node").set("name", "C").save();

        final MutableEdge eAB = a.newEdge("road", b, true, (Object[]) null);
        eAB.set("weight", 5.0);
        eAB.save();

        final MutableEdge eBC = b.newEdge("road", c, true, (Object[]) null);
        eBC.set("weight", -3.0);
        eBC.save();

        final MutableEdge eAC = a.newEdge("road", c, true, (Object[]) null);
        eAC.set("weight", 10.0);
        eAC.save();

        final SQLFunctionBellmanFord fn = new SQLFunctionBellmanFord();
        final BasicCommandContext ctx = new BasicCommandContext();
        ctx.setDatabase(graph);

        // Use OUT direction to avoid negative cycle issues with bidirectional edges
        // (B->C=-3 and C->B=-3 would form a negative cycle with BOTH direction)
        @SuppressWarnings("unchecked")
        final List<RID> result = (List<RID>) fn.execute(null, null, null,
            new Object[] { a, c, "'weight'", "OUT" }, ctx);

        // Shortest path A->B->C has weight 5+(-3)=2, shorter than direct A->C=10
        assertThat(result).hasSize(3);
        assertThat(result.get(0)).isEqualTo(a.getIdentity());
        assertThat(result.get(1)).isEqualTo(b.getIdentity());
        assertThat(result.get(2)).isEqualTo(c.getIdentity());
      });
    });
  }

  @Test
  void executeNoPath() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionBellmanFordNoPathTest", (graph) -> {
      graph.transaction(() -> {
        graph.getSchema().createVertexType("node");
        graph.getSchema().createEdgeType("road");

        final MutableVertex a = graph.newVertex("node").set("name", "A").save();
        final MutableVertex b = graph.newVertex("node").set("name", "B").save();
        // No edge from B to A

        final SQLFunctionBellmanFord fn = new SQLFunctionBellmanFord();
        final BasicCommandContext ctx = new BasicCommandContext();
        ctx.setDatabase(graph);

        @SuppressWarnings("unchecked")
        final List<RID> result = (List<RID>) fn.execute(null, null, null,
            new Object[] { b, a, "'weight'" }, ctx);

        assertThat(result).isEmpty();
      });
    });
  }

  @Test
  void executeWithDirection() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionBellmanFordDirTest", (graph) -> {
      setUp(graph);
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(graph);

      @SuppressWarnings("unchecked")
      final List<RID> result = (List<RID>) functionBellmanFord.execute(null, null, null,
          new Object[] { v1, v4, "'weight'", "OUT" }, ctx);

      assertThat(result).hasSize(4);
    });
  }
}
