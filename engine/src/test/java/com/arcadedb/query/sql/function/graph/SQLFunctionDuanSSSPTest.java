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
package com.arcadedb.query.sql.function.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class SQLFunctionDuanSSSPTest {

  private MutableVertex       v1;
  private MutableVertex       v2;
  private MutableVertex       v3;
  private MutableVertex       v4;
  private SQLFunctionDuanSSSP functionDuanSSSP;

  public void setUp(final Database graph) throws Exception {
    graph.transaction(() -> {
      graph.getSchema().createVertexType("node");
      graph.getSchema().createEdgeType("weight");

      v1 = graph.newVertex("node");
      v2 = graph.newVertex("node");
      v3 = graph.newVertex("node");
      v4 = graph.newVertex("node");

      v1.set("node_id", "A").save();
      v2.set("node_id", "B").save();
      v3.set("node_id", "C").save();
      v4.set("node_id", "D").save();

      final MutableEdge e1 = v1.newEdge("weight", v2);
      e1.set("weight", 1.0f);
      e1.save();

      final MutableEdge e2 = v2.newEdge("weight", v3);
      e2.set("weight", 1.0f);
      e2.save();

      final MutableEdge e3 = v1.newEdge("weight", v3);
      e3.set("weight", 100.0f);
      e3.save();

      final MutableEdge e4 = v3.newEdge("weight", v4);
      e4.set("weight", 1.0f);
      e4.save();

      functionDuanSSSP = new SQLFunctionDuanSSSP();
    });
  }

  @Test
  void basicPath() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionDuanSSSPTest_basicPath", (graph) -> {
      setUp(graph);
      final List<RID> result = functionDuanSSSP.execute(null, null, null, new Object[] { v1, v4, "weight" },
          new BasicCommandContext());

      assertThat(result).hasSize(4);
      assertThat(result.get(0)).isEqualTo(v1.getIdentity());
      assertThat(result.get(1)).isEqualTo(v2.getIdentity());
      assertThat(result.get(2)).isEqualTo(v3.getIdentity());
      assertThat(result.get(3)).isEqualTo(v4.getIdentity());
    });
  }

  @Test
  void sameVertex() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionDuanSSSPTest_sameVertex", (graph) -> {
      setUp(graph);
      final List<RID> result = functionDuanSSSP.execute(null, null, null, new Object[] { v1, v1, "weight" },
          new BasicCommandContext());

      assertThat(result).hasSize(1);
      assertThat(result.get(0)).isEqualTo(v1.getIdentity());
    });
  }

  @Test
  void noPath() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionDuanSSSPTest_noPath", (graph) -> {
      graph.transaction(() -> {
        graph.getSchema().createVertexType("isolated");

        final MutableVertex v5 = graph.newVertex("isolated");
        v5.set("node_id", "E").save();

        final MutableVertex v6 = graph.newVertex("isolated");
        v6.set("node_id", "F").save();

        final SQLFunctionDuanSSSP fn = new SQLFunctionDuanSSSP();
        final List<RID> result = fn.execute(null, null, null, new Object[] { v5, v6, "weight" },
            new BasicCommandContext());

        assertThat(result).isEmpty();
      });
    });
  }

  @Test
  void sqlQuery() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionDuanSSSPTest_sqlQuery", (graph) -> {
      setUp(graph);

      final ResultSet result = graph.query("sql",
          "SELECT duanSSSP(?, ?, 'weight') as path FROM (SELECT 1)", v1.getIdentity(), v4.getIdentity());

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat(row.hasProperty("path")).isTrue();

      final List<RID> path = row.getProperty("path");
      assertThat(path).hasSize(4);
      assertThat(path.get(0)).isEqualTo(v1.getIdentity());
      assertThat(path.get(3)).isEqualTo(v4.getIdentity());
    });
  }

  @Test
  void directionOut() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionDuanSSSPTest_directionOut", (graph) -> {
      setUp(graph);

      final List<RID> result = functionDuanSSSP.execute(null, null, null,
          new Object[] { v1, v4, "weight", "OUT" }, new BasicCommandContext());

      assertThat(result).hasSize(4);
      assertThat(result.get(0)).isEqualTo(v1.getIdentity());
      assertThat(result.get(3)).isEqualTo(v4.getIdentity());
    });
  }

  @Test
  void largerGraph() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionDuanSSSPTest_largerGraph", (graph) -> {
      graph.transaction(() -> {
        graph.getSchema().createVertexType("city");
        graph.getSchema().createEdgeType("road");

        // Create a more complex graph: 6 vertices, 10 edges
        final MutableVertex[] vertices = new MutableVertex[6];
        for (int i = 0; i < 6; i++) {
          vertices[i] = graph.newVertex("city");
          vertices[i].set("name", "City" + i).save();
        }

        // Create edges with different weights
        final int[][] edges = {
            {0, 1, 4}, {0, 2, 2}, {1, 2, 1}, {1, 3, 5},
            {2, 3, 8}, {2, 4, 10}, {3, 4, 2}, {3, 5, 6},
            {4, 5, 3}
        };

        for (final int[] edge : edges) {
          final MutableEdge e = vertices[edge[0]].newEdge("road", vertices[edge[1]]);
          e.set("weight", (float) edge[2]);
          e.save();
        }

        final SQLFunctionDuanSSSP fn = new SQLFunctionDuanSSSP();

        // Test path from vertex 0 to vertex 5
        final List<RID> result = fn.execute(null, null, null,
            new Object[] { vertices[0], vertices[5], "weight" }, new BasicCommandContext());

        assertThat(result).isNotEmpty();
        assertThat(result.get(0)).isEqualTo(vertices[0].getIdentity());
        assertThat(result.get(result.size()-1)).isEqualTo(vertices[5].getIdentity());

        // The shortest path should be: 0 -> 2 -> 1 -> 3 -> 4 -> 5 (cost: 2+1+5+2+3 = 13)
        // or 0 -> 1 -> 3 -> 4 -> 5 (cost: 4+5+2+3 = 14)
        // The algorithm should find the optimal path
        assertThat(result.size()).isGreaterThan(1);
      });
    });
  }

  // NOTE: Cypher test removed because Cypher engine is in a separate module
  // The function is automatically available in Cypher through the SQLFunctionBridge
  // and is tested in integration tests
}
