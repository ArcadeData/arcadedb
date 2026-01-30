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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

;

/**
 * Tests for DELETE clause in OpenCypher queries.
 */
public class OpenCypherDeleteTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-delete").create();

    // Create schema
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
  void deleteSingleVertex() {
    // Create a person
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Alice'})");
    });

    // Verify created
    ResultSet verify = database.query("opencypher", "MATCH (n:Person) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    verify.next();
    assertThat(verify.hasNext()).isFalse();

    // Delete the person
    database.transaction(() -> {
      database.command("opencypher", "MATCH (n:Person {name: 'Alice'}) DELETE n");
    });

    // Verify deleted
    verify = database.query("opencypher", "MATCH (n:Person) RETURN n");
    assertThat(verify.hasNext()).isFalse();
  }

  @Test
  void deleteMultipleVertices() {
    // Create multiple people
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob'})");
      database.command("opencypher", "CREATE (c:Person {name: 'Charlie'})");
    });

    // Verify 3 created
    ResultSet verify = database.query("opencypher", "MATCH (n:Person) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(3);

    // Delete all people
    database.transaction(() -> {
      database.command("opencypher", "MATCH (n:Person) DELETE n");
    });

    // Verify all deleted
    verify = database.query("opencypher", "MATCH (n:Person) RETURN n");
    assertThat(verify.hasNext()).isFalse();
  }

  @Test
  void deleteWithWhere() {
    // Create multiple people
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob', age: 25})");
      database.command("opencypher", "CREATE (c:Person {name: 'Charlie', age: 35})");
    });

    // Delete people over 30
    database.transaction(() -> {
      database.command("opencypher", "MATCH (n:Person) WHERE n.age > 30 DELETE n");
    });

    // Verify only Charlie deleted, Alice and Bob remain
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person) RETURN n ORDER BY n.name");
    assertThat(verify.hasNext()).isTrue();
    final Vertex v1 = (Vertex) verify.next().toElement();
    assertThat((String) v1.get("name")).isEqualTo("Alice");
    assertThat(verify.hasNext()).isTrue();
    final Vertex v2 = (Vertex) verify.next().toElement();
    assertThat((String) v2.get("name")).isEqualTo("Bob");
    assertThat(verify.hasNext()).isFalse();
  }

  @Test
  void deleteEdge() {
    // Create two people and a relationship
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name: 'Alice'})-[r:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})");
    });

    // Verify relationship exists
    ResultSet verify = database.query("opencypher", "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r");
    assertThat(verify.hasNext()).isTrue();

    // Delete the relationship
    database.transaction(() -> {
      database.command("opencypher", "MATCH (a:Person)-[r:KNOWS]->(b:Person) DELETE r");
    });

    // Verify relationship deleted
    verify = database.query("opencypher", "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r");
    assertThat(verify.hasNext()).isFalse();

    // Verify nodes still exist
    verify = database.query("opencypher", "MATCH (n:Person) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  @Test
  void detachDeleteVertexWithRelationships() {
    // Create a graph: (Alice)-[:KNOWS]->(Bob)-[:KNOWS]->(Charlie)
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Charlie'})");
    });

    // Verify 2 relationships exist
    ResultSet verify = database.query("opencypher", "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r");
    int relCount = 0;
    while (verify.hasNext()) {
      verify.next();
      relCount++;
    }
    assertThat(relCount).isEqualTo(2);

    // DETACH DELETE Bob (removes Bob and his relationships)
    database.transaction(() -> {
      database.command("opencypher", "MATCH (n:Person {name: 'Bob'}) DETACH DELETE n");
    });

    // Verify Bob is deleted
    verify = database.query("opencypher", "MATCH (n:Person {name: 'Bob'}) RETURN n");
    assertThat(verify.hasNext()).isFalse();

    // Verify Alice and Charlie still exist
    verify = database.query("opencypher", "MATCH (n:Person) RETURN n");
    int nodeCount = 0;
    while (verify.hasNext()) {
      verify.next();
      nodeCount++;
    }
    assertThat(nodeCount).isEqualTo(2);

    // Verify relationships are gone
    verify = database.query("opencypher", "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r");
    assertThat(verify.hasNext()).isFalse();
  }

  @Test
  void detachDeleteMultipleVertices() {
    // Create a more complex graph
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'})");
      database.command("opencypher", "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)");
      database.command("opencypher",
          "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)");
      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Charlie'}) CREATE (a)-[:KNOWS]->(c)");
    });

    // Verify 3 relationships
    ResultSet verify = database.query("opencypher", "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r");
    int relCount = 0;
    while (verify.hasNext()) {
      verify.next();
      relCount++;
    }
    assertThat(relCount).isEqualTo(3);

    // DETACH DELETE Alice and Bob (delete individually since IN not implemented yet)
    database.transaction(() -> {
      database.command("opencypher", "MATCH (n:Person {name: 'Alice'}) DETACH DELETE n");
      database.command("opencypher", "MATCH (n:Person {name: 'Bob'}) DETACH DELETE n");
    });

    // Verify only Charlie remains
    verify = database.query("opencypher", "MATCH (n:Person) RETURN n.name");
    assertThat(verify.hasNext()).isTrue();
    assertThat((String) verify.next().getProperty("n.name")).isEqualTo("Charlie");
    assertThat(verify.hasNext()).isFalse();

    // Verify no relationships remain
    verify = database.query("opencypher", "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r");
    assertThat(verify.hasNext()).isFalse();
  }

  @Test
  void deleteWithReturn() {
    // Create a person
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'David', age: 40})");
    });

    // Delete with RETURN (returns count of deleted elements)
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person {name: 'David'}) DELETE n RETURN n");

      // Result contains the deleted element
      assertThat(result.hasNext()).isTrue();
      result.next();
    });

    // Verify deleted
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'David'}) RETURN n");
    assertThat(verify.hasNext()).isFalse();
  }

  @Test
  void deleteWithoutReturn() {
    // Create a person
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Eve', age: 28})");
    });

    // Delete without RETURN
    database.transaction(() -> {
      database.command("opencypher", "MATCH (n:Person {name: 'Eve'}) DELETE n");
    });

    // Verify deleted
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Eve'}) RETURN n");
    assertThat(verify.hasNext()).isFalse();
  }

  @Test
  void deletePartialGraph() {
    // Create graph with two relationship types
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Company {name: 'ArcadeDB'})");
      database.command("opencypher", "MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'ArcadeDB'}) CREATE (a)-[:WORKS_AT]->(c)");
      database.command("opencypher", "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)");
    });

    // Verify both relationships exist
    ResultSet verify = database.query("opencypher", "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r");
    assertThat(verify.hasNext()).isTrue();
    verify = database.query("opencypher", "MATCH (a:Person)-[r:WORKS_AT]->(c:Company) RETURN r");
    assertThat(verify.hasNext()).isTrue();

    // Delete only KNOWS relationship
    database.transaction(() -> {
      database.command("opencypher", "MATCH (a:Person)-[r:KNOWS]->(b:Person) DELETE r");
    });

    // Verify KNOWS relationship is gone
    verify = database.query("opencypher", "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r");
    assertThat(verify.hasNext()).isFalse();

    // Verify WORKS_AT relationship still exists
    verify = database.query("opencypher", "MATCH (a:Person)-[r:WORKS_AT]->(c:Company) RETURN r");
    assertThat(verify.hasNext()).isTrue();

    // Verify Person nodes still exist
    verify = database.query("opencypher", "MATCH (n:Person) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(2); // Alice, Bob
  }
}
