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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for zero-length path patterns in OpenCypher.
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/3337
 */
class OpenCypherZeroLengthPathTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-zerolen-path").create();
    database.getSchema().createVertexType("SoloNode");
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void zeroLengthPathOnIsolatedNode() {
    // Issue #3337: zero-length path on a node with no relationships
    database.transaction(() -> {
      database.newVertex("SoloNode").save();
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (a:SoloNode) " +
            "MATCH p=(a)-[*0..1]->(b) " +
            "RETURN count(p) AS cnt");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(1L);
    result.close();
  }

  @Test
  void zeroLengthPathWithCreateAndWith() {
    // Exact query from issue #3337
    final ResultSet result = database.command("opencypher",
        "CREATE (a:SoloNode) " +
            "WITH a " +
            "MATCH p=(a)-[*0..1]->(b) " +
            "RETURN count(p) AS cnt");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(1L);
    result.close();
  }

  @Test
  void zeroLengthPathNodeWithRelationships() {
    // Node with one outgoing relationship: *0..1 should return 2 paths (self + neighbor)
    database.transaction(() -> {
      final Vertex alice = database.newVertex("Person").set("name", "Alice").save();
      final Vertex bob = database.newVertex("Person").set("name", "Bob").save();
      ((MutableVertex) alice).newEdge("KNOWS", bob, true, (Object[]) null).save();
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}) " +
            "MATCH p=(a)-[*0..1]->(b) " +
            "RETURN b.name AS name ORDER BY name");

    assertThat(result.hasNext()).isTrue();
    final Result row1 = result.next();
    assertThat((String) row1.getProperty("name")).isEqualTo("Alice"); // zero-hop: a=b

    assertThat(result.hasNext()).isTrue();
    final Result row2 = result.next();
    assertThat((String) row2.getProperty("name")).isEqualTo("Bob"); // one-hop

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void zeroMinHopsWithLargerMax() {
    // *0..2 should include self + 1-hop + 2-hop
    database.transaction(() -> {
      final Vertex alice = database.newVertex("Person").set("name", "Alice").save();
      final Vertex bob = database.newVertex("Person").set("name", "Bob").save();
      final Vertex charlie = database.newVertex("Person").set("name", "Charlie").save();
      ((MutableVertex) alice).newEdge("KNOWS", bob, true, (Object[]) null).save();
      ((MutableVertex) bob).newEdge("KNOWS", charlie, true, (Object[]) null).save();
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}) " +
            "MATCH p=(a)-[*0..2]->(b) " +
            "RETURN count(p) AS cnt");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    // 3 paths: Alice (0 hops), Bob (1 hop), Charlie (2 hops)
    assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(3L);
    result.close();
  }

  @Test
  void zeroExactHops() {
    // *0 should return only the node itself
    database.transaction(() -> {
      final Vertex alice = database.newVertex("Person").set("name", "Alice").save();
      final Vertex bob = database.newVertex("Person").set("name", "Bob").save();
      ((MutableVertex) alice).newEdge("KNOWS", bob, true, (Object[]) null).save();
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}) " +
            "MATCH p=(a)-[*0..0]->(b) " +
            "RETURN b.name AS name");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat((String) row.getProperty("name")).isEqualTo("Alice");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }
}
