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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.leiden Cypher procedure.
 */
class AlgoLeidenTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-leiden");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("KNOWS");

    // Two clear communities: {A,B,C} and {D,E,F}
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      final MutableVertex f = database.newVertex("Node").set("name", "F").save();
      // Dense within community 1
      a.newEdge("KNOWS", b, true, (Object[]) null).save();
      b.newEdge("KNOWS", c, true, (Object[]) null).save();
      a.newEdge("KNOWS", c, true, (Object[]) null).save();
      // Dense within community 2
      d.newEdge("KNOWS", e, true, (Object[]) null).save();
      e.newEdge("KNOWS", f, true, (Object[]) null).save();
      d.newEdge("KNOWS", f, true, (Object[]) null).save();
      // Single bridge edge
      c.newEdge("KNOWS", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void leidenReturnsOneRowPerVertex() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.leiden() YIELD nodeId, community RETURN nodeId, community");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(6);
  }

  @Test
  void leidenAssignsCommunities() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.leiden() YIELD nodeId, community RETURN nodeId, community");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeId = r.getProperty("nodeId");
      final Object community = r.getProperty("community");
      assertThat(nodeId).isNotNull();
      assertThat(community).isNotNull();
      assertThat(((Number) community).intValue()).isGreaterThanOrEqualTo(0);
    }
  }

  @Test
  void leidenFindsTwoCommunities() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.leiden() YIELD nodeId, community RETURN nodeId, community");

    final Set<Integer> communities = new HashSet<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("community");
      communities.add(((Number) val).intValue());
    }

    // Should find at least 1 community, and ideally 2 for our well-separated graph
    assertThat(communities).isNotEmpty();
    assertThat(communities.size()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void leidenWithRelType() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.leiden('KNOWS') YIELD nodeId, community RETURN nodeId, community");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(6);
  }

  @Test
  void leidenWithMaxIterations() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.leiden('KNOWS', 5, 1.0) YIELD nodeId, community RETURN nodeId, community");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(6);
  }

  @Test
  void leidenEmptyGraph() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-leiden-empty");
    if (factory.exists())
      factory.open().drop();
    final Database emptyDb = factory.create();
    emptyDb.getSchema().createVertexType("Node");
    emptyDb.getSchema().createEdgeType("KNOWS");

    final ResultSet rs = emptyDb.query("opencypher",
        "CALL algo.leiden() YIELD nodeId, community RETURN nodeId, community");

    assertThat(rs.hasNext()).isFalse();
    emptyDb.drop();
  }
}
