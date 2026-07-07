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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #5071: count(*) with inline-filtered start node and anonymous
 * endpoint returns the global relationship count instead of the count of matching rows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5071CountStarTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testissue5071").create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");

      final MutableVertex n1 = database.newVertex("Node").set("id", 1).save();
      final MutableVertex n2 = database.newVertex("Node").set("id", 2).save();
      final MutableVertex n3 = database.newVertex("Node").set("id", 3).save();

      // 3 LINK relationships: n1->n2, n1->n3, n2->n3
      n1.newEdge("LINK", n2);
      n1.newEdge("LINK", n3);
      n2.newEdge("LINK", n3);
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void countStarInlineFilterAnonymousEndpoint() {
    // Node 1 has 2 outgoing LINK edges -> expected 2, not the global count of 3.
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {id: 1})-[:LINK]->(:Node) RETURN count(*) AS cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(2L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void countStarNode2() {
    // Node 2 has 1 outgoing LINK edge.
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {id: 2})-[:LINK]->(:Node) RETURN count(*) AS cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(1L);
    rs.close();
  }

  @Test
  void countStarNoMatch() {
    // Node 3 has 0 outgoing LINK edges.
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {id: 3})-[:LINK]->(:Node) RETURN count(*) AS cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(0L);
    rs.close();
  }

  @Test
  void countBoundRelationshipStillWorks() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {id: 1})-[r:LINK]->(:Node) RETURN count(r) AS cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(2L);
    rs.close();
  }

  @Test
  void countNamedEndNodeStillWorks() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {id: 1})-[:LINK]->(b:Node) RETURN count(*) AS cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(2L);
    rs.close();
  }
}
