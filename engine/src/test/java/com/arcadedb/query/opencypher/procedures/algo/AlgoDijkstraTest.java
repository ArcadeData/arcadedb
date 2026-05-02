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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.dijkstra Cypher procedure.
 *
 * Regression for issue #4042: yielded weight was always 0 because the path returned by the
 * underlying A* implementation only contains vertex RIDs, not edges. Weight must be reconstructed
 * by traversing the edges between consecutive vertices on the path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoDijkstraTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-dijkstra");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("Road");

    // Start --2--> Middle --3--> Finish
    database.transaction(() -> {
      final MutableVertex start  = database.newVertex("Node").set("name", "Start").save();
      final MutableVertex middle = database.newVertex("Node").set("name", "Middle").save();
      final MutableVertex finish = database.newVertex("Node").set("name", "Finish").save();
      start.newEdge("Road", middle, true, new Object[] { "distance", 2 }).save();
      middle.newEdge("Road", finish, true, new Object[] { "distance", 3 }).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void dijkstraComputesCorrectWeight() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (src:Node {name:'Start'}),(dst:Node {name:'Finish'}) \
        CALL algo.dijkstra(src, dst, 'Road', 'distance') \
        YIELD path, weight RETURN weight""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object weight = result.getProperty("weight");
    assertThat(weight).isInstanceOf(Number.class);
    assertThat(((Number) weight).doubleValue()).isEqualTo(5.0);
  }

  @Test
  void dijkstraPathIncludesEdges() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (src:Node {name:'Start'}),(dst:Node {name:'Finish'}) \
        CALL algo.dijkstra(src, dst, 'Road', 'distance') \
        YIELD path, weight RETURN path, weight""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    @SuppressWarnings("unchecked")
    final Map<String, Object> path = (Map<String, Object>) result.getProperty("path");
    assertThat(path).isNotNull();

    final List<?> nodes = (List<?>) path.get("nodes");
    final List<?> relationships = (List<?>) path.get("relationships");
    assertThat(nodes).hasSize(3);
    assertThat(relationships).hasSize(2);
  }

  @Test
  void dijkstraStartEqualsEnd() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (src:Node {name:'Start'}) \
        CALL algo.dijkstra(src, src, 'Road', 'distance') \
        YIELD path, weight RETURN weight""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(((Number) result.getProperty("weight")).doubleValue()).isEqualTo(0.0);
  }

  @Test
  void dijkstraMultiHopAccumulatesWeight() {
    // Add a fourth hop: Finish --4--> End
    database.transaction(() -> {
      final Vertex finish = database.query("sql",
          "SELECT FROM Node WHERE name='Finish'").next().getElement().get().asVertex();
      final MutableVertex end = database.newVertex("Node").set("name", "End").save();
      finish.modify().newEdge("Road", end, true, new Object[] { "distance", 4 }).save();
    });

    final ResultSet rs = database.query("opencypher",
        """
        MATCH (src:Node {name:'Start'}),(dst:Node {name:'End'}) \
        CALL algo.dijkstra(src, dst, 'Road', 'distance') \
        YIELD path, weight RETURN weight""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(((Number) result.getProperty("weight")).doubleValue()).isEqualTo(9.0);
  }
}
