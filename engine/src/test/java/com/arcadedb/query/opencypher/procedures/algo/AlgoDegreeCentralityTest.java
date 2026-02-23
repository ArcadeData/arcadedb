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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.degree Cypher procedure (Degree Centrality).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoDegreeCentralityTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-degree");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Hub-and-spoke: A is hub with outDegree=3; B, C, D are spokes with inDegree=1
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      a.newEdge("LINK", c, true, (Object[]) null).save();
      a.newEdge("LINK", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void degreeHubHasHighestOutDegree() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.degree() YIELD node, outDegree RETURN node, outDegree");

    long maxOut = 0;
    String maxName = null;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final long out = ((Number) r.getProperty("outDegree")).longValue();
      if (out > maxOut) {
        maxOut = out;
        final Object n = r.getProperty("node");
        if (n instanceof Vertex v)
          maxName = v.getString("name");
      }
    }
    assertThat(maxOut).isEqualTo(3L);
    assertThat(maxName).isEqualTo("A");
  }

  @Test
  void degreeSpokeHasInDegreeOne() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.degree() YIELD node, inDegree RETURN node, inDegree");
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      if (nodeObj instanceof Vertex v && !"A".equals(v.getString("name"))) {
        final long in = ((Number) r.getProperty("inDegree")).longValue();
        assertThat(in).isEqualTo(1L);
      }
    }
  }

  @Test
  void degreeHubHasInDegreeZero() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.degree() YIELD node, inDegree RETURN node, inDegree");
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      if (nodeObj instanceof Vertex v && "A".equals(v.getString("name"))) {
        final long in = ((Number) r.getProperty("inDegree")).longValue();
        assertThat(in).isEqualTo(0L);
      }
    }
  }

  @Test
  void degreeReturnsFourResults() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.degree() YIELD node, degree RETURN node, degree");
    int count = 0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object node = r.getProperty("node");
      final Object degree = r.getProperty("degree");
      assertThat(node).isNotNull();
      assertThat(degree).isNotNull();
      count++;
    }
    assertThat(count).isEqualTo(4);
  }
}
