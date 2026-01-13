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
package com.arcadedb.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for MERGE clause in OpenCypher queries.
 */
public class OpenCypherMergeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-merge").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("WORKS_AT");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testMergeCreatesNodeWhenNotExists() {
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'Alice'})");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Alice'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    final Vertex v = (Vertex) verify.next().getProperty("n");
    assertThat((String) v.get("name")).isEqualTo("Alice");
  }

  @Test
  void testMergeFindsNodeWhenExists() {
    // Create node
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Bob'})");
    });

    // MERGE should find it, not create duplicate
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'Bob'})");
    });

    // Verify only one Bob exists
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Bob'}) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(1);
  }

  @Test
  void testMergeWithReturn() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher", "MERGE (n:Person {name: 'Charlie'}) RETURN n");
      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().getProperty("n");
      assertThat((String) v.get("name")).isEqualTo("Charlie");
    });
  }

  @Test
  void testMergeMultipleTimes() {
    // First MERGE creates
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'David', age: 30})");
    });

    // Second MERGE finds
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'David', age: 30})");
    });

    // Third MERGE finds
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'David', age: 30})");
    });

    // Verify only one David exists
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'David'}) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(1);
  }

  @Test
  void testMergeRelationship() {
    // Create nodes first
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Eve'})");
      database.command("opencypher", "CREATE (b:Person {name: 'Frank'})");
    });

    // MERGE relationship
    database.transaction(() -> {
      database.command("opencypher",
          "MERGE (a:Person {name: 'Eve'})-[r:KNOWS]->(b:Person {name: 'Frank'})");
    });

    // Verify relationship exists
    ResultSet verify = database.query("opencypher",
        "MATCH (a:Person {name: 'Eve'})-[r:KNOWS]->(b:Person {name: 'Frank'}) RETURN r");
    assertThat(verify.hasNext()).isTrue();

    // MERGE again - should find existing relationship
    database.transaction(() -> {
      database.command("opencypher",
          "MERGE (a:Person {name: 'Eve'})-[r:KNOWS]->(b:Person {name: 'Frank'})");
    });

    // Verify still only one relationship
    verify = database.query("opencypher",
        "MATCH (a:Person {name: 'Eve'})-[r:KNOWS]->(b:Person {name: 'Frank'}) RETURN r");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(1);
  }
}
