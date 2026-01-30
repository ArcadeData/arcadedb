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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for multi-label support in Cypher queries.
 * Tests CREATE with multiple labels, MATCH with labels, and labels() function.
 */
class CypherMultiLabelTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-multilabel").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void createVertexWithTwoLabels() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (n:Person:Developer {name: 'Alice'}) RETURN n");

      assertThat((Object) result).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Result r = result.next();
      final Object vertex = r.toElement();
      assertThat(vertex).isInstanceOf(Vertex.class);

      final Vertex v = (Vertex) vertex;
      // Composite type name should be alphabetically sorted
      assertThat(v.getTypeName()).isEqualTo("Developer~Person");
      assertThat((String) v.get("name")).isEqualTo("Alice");
    });
  }

  @Test
  void createVertexWithTwoLabelsReversed() {
    database.transaction(() -> {
      // Labels in reverse order should produce the same composite type
      final ResultSet result = database.command("opencypher",
          "CREATE (n:Developer:Person {name: 'Bob'}) RETURN n");

      assertThat((Object) result).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Result r = result.next();
      final Vertex v = (Vertex) r.toElement();
      // Same composite type regardless of order
      assertThat(v.getTypeName()).isEqualTo("Developer~Person");
    });
  }

  @Test
  void createVertexWithThreeLabels() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (n:Manager:Developer:Person {name: 'Carol'}) RETURN n");

      assertThat((Object) result).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Result r = result.next();
      final Vertex v = (Vertex) r.toElement();
      // Alphabetically sorted
      assertThat(v.getTypeName()).isEqualTo("Developer~Manager~Person");
    });
  }

  @Test
  void matchByFirstLabel() {
    database.transaction(() -> {
      // Create multi-label vertex
      database.command("opencypher", "CREATE (n:Person:Developer {name: 'Alice'})");
    });

    // Match by Person should find the vertex (polymorphic query)
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Person) RETURN n");

    assertThat(result.hasNext()).isTrue();
    final Vertex v = (Vertex) result.next().toElement();
    assertThat((String) v.get("name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void matchBySecondLabel() {
    database.transaction(() -> {
      // Create multi-label vertex
      database.command("opencypher", "CREATE (n:Person:Developer {name: 'Alice'})");
    });

    // Match by Developer should also find the vertex
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Developer) RETURN n");

    assertThat(result.hasNext()).isTrue();
    final Vertex v = (Vertex) result.next().toElement();
    assertThat((String) v.get("name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void matchByBothLabels() {
    database.transaction(() -> {
      // Create multi-label vertex
      database.command("opencypher", "CREATE (n:Person:Developer {name: 'Alice'})");
    });

    // Match by both labels should find the vertex
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Person:Developer) RETURN n");

    assertThat(result.hasNext()).isTrue();
    final Vertex v = (Vertex) result.next().toElement();
    assertThat((String) v.get("name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void matchByBothLabelsReversed() {
    database.transaction(() -> {
      // Create multi-label vertex
      database.command("opencypher", "CREATE (n:Person:Developer {name: 'Alice'})");
    });

    // Match with labels in reverse order should also work
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Developer:Person) RETURN n");

    assertThat(result.hasNext()).isTrue();
    final Vertex v = (Vertex) result.next().toElement();
    assertThat((String) v.get("name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void labelsFunction() {
    database.transaction(() -> {
      // Create multi-label vertex
      database.command("opencypher", "CREATE (n:Person:Developer {name: 'Alice'})");
    });

    // labels() should return all labels
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Person) RETURN labels(n) as labels");

    assertThat(result.hasNext()).isTrue();
    final Result r = result.next();
    @SuppressWarnings("unchecked")
    final List<String> labels = (List<String>) r.getProperty("labels");
    assertThat(labels).containsExactlyInAnyOrder("Developer", "Person");
  }

  @Test
  void labelsFunctionSingleLabel() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      // Create single-label vertex
      database.command("opencypher", "CREATE (n:Person {name: 'Bob'})");
    });

    // labels() should return single label
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Person) RETURN labels(n) as labels");

    assertThat(result.hasNext()).isTrue();
    final Result r = result.next();
    @SuppressWarnings("unchecked")
    final List<String> labels = (List<String>) r.getProperty("labels");
    assertThat(labels).containsExactly("Person");
  }

  @Test
  void mixedSingleAndMultiLabelVertices() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      // Create single-label vertex
      database.command("opencypher", "CREATE (n:Person {name: 'Bob'})");
      // Create multi-label vertex
      database.command("opencypher", "CREATE (n:Person:Developer {name: 'Alice'})");
    });

    // Match by Person should find both
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Person) RETURN n.name ORDER BY n.name");

    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("n.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("n.name")).isEqualTo("Bob");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void matchByLabelNotFound() {
    database.transaction(() -> {
      // Create multi-label vertex without Manager
      database.command("opencypher", "CREATE (n:Person:Developer {name: 'Alice'})");
    });

    // Match by Manager should not find the vertex
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Manager) RETURN n");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void matchByNonExistentCompositeLabels() {
    database.transaction(() -> {
      // Create multi-label vertex
      database.command("opencypher", "CREATE (n:Person:Developer {name: 'Alice'})");
    });

    // Match by Person:Manager should not find anything (no such composite type)
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Person:Manager) RETURN n");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void typeHierarchyWithCompositeTypes() {
    database.transaction(() -> {
      // Create composite type
      database.command("opencypher", "CREATE (n:Person:Developer {name: 'Alice'})");
    });

    // Verify schema has correct type hierarchy
    assertThat(database.getSchema().existsType("Developer~Person")).isTrue();
    assertThat(database.getSchema().existsType("Person")).isTrue();
    assertThat(database.getSchema().existsType("Developer")).isTrue();

    // Verify inheritance
    assertThat(database.getSchema().getType("Developer~Person").instanceOf("Person")).isTrue();
    assertThat(database.getSchema().getType("Developer~Person").instanceOf("Developer")).isTrue();
  }

  @Test
  void vertexHasLabelMethod() {
    database.transaction(() -> {
      // Create multi-label vertex
      final ResultSet result = database.command("opencypher",
          "CREATE (n:Person:Developer {name: 'Alice'}) RETURN n");

      final Vertex v = (Vertex) result.next().toElement();

      // Test hasLabel via Labels helper
      assertThat(Labels.hasLabel(v, "Person")).isTrue();
      assertThat(Labels.hasLabel(v, "Developer")).isTrue();
      assertThat(Labels.hasLabel(v, "Manager")).isFalse();
    });
  }

  @Test
  void multipleVerticesWithDifferentLabelCombinations() {
    database.transaction(() -> {
      // Create vertices with different label combinations
      database.command("opencypher", "CREATE (n:Person:Developer {name: 'Alice'})");
      database.command("opencypher", "CREATE (n:Person:Manager {name: 'Bob'})");
      database.command("opencypher", "CREATE (n:Developer:Manager {name: 'Carol'})");
    });

    // Match by Person should find Alice and Bob
    final ResultSet personResult = database.query("opencypher",
        "MATCH (n:Person) RETURN n.name ORDER BY n.name");

    assertThat(personResult.hasNext()).isTrue();
    assertThat((String) personResult.next().getProperty("n.name")).isEqualTo("Alice");
    assertThat(personResult.hasNext()).isTrue();
    assertThat((String) personResult.next().getProperty("n.name")).isEqualTo("Bob");
    assertThat(personResult.hasNext()).isFalse();

    // Match by Developer should find Alice and Carol
    final ResultSet devResult = database.query("opencypher",
        "MATCH (n:Developer) RETURN n.name ORDER BY n.name");

    assertThat(devResult.hasNext()).isTrue();
    assertThat((String) devResult.next().getProperty("n.name")).isEqualTo("Alice");
    assertThat(devResult.hasNext()).isTrue();
    assertThat((String) devResult.next().getProperty("n.name")).isEqualTo("Carol");
    assertThat(devResult.hasNext()).isFalse();

    // Match by Manager should find Bob and Carol
    final ResultSet mgrResult = database.query("opencypher",
        "MATCH (n:Manager) RETURN n.name ORDER BY n.name");

    assertThat(mgrResult.hasNext()).isTrue();
    assertThat((String) mgrResult.next().getProperty("n.name")).isEqualTo("Bob");
    assertThat(mgrResult.hasNext()).isTrue();
    assertThat((String) mgrResult.next().getProperty("n.name")).isEqualTo("Carol");
    assertThat(mgrResult.hasNext()).isFalse();
  }

  // Tests for duplicate label deduplication (GitHub issue #3264)

  @Test
  void createWithDuplicateLabels() {
    // CREATE (n:Person:Kebab:Person) should create type Kebab~Person (not Kebab~Person~Person)
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (n:Person:Kebab:Person {name: 'Person:Kebab:Person'}) RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().toElement();

      // Type should be Kebab~Person (deduplicated and sorted)
      assertThat(v.getTypeName()).isEqualTo("Kebab~Person");
      assertThat((String) v.get("name")).isEqualTo("Person:Kebab:Person");
    });

    // Verify schema structure
    assertThat(database.getSchema().existsType("Kebab~Person")).isTrue();
    assertThat(database.getSchema().existsType("Person")).isTrue();
    assertThat(database.getSchema().existsType("Kebab")).isTrue();
    // Should NOT have the duplicated type
    assertThat(database.getSchema().existsType("Kebab~Person~Person")).isFalse();
  }

  @Test
  void createWithAllDuplicateLabels() {
    // CREATE (n:Kebab:Kebab) should create type Kebab (single label)
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (n:Kebab:Kebab {name: 'Kebab:Kebab'}) RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().toElement();

      // Type should be just Kebab (deduplicated to single label)
      assertThat(v.getTypeName()).isEqualTo("Kebab");
    });

    // Verify schema structure - no composite type should be created
    assertThat(database.getSchema().existsType("Kebab")).isTrue();
    assertThat(database.getSchema().existsType("Kebab~Kebab")).isFalse();
  }

  @Test
  void matchWithDuplicateLabels() {
    // Create a Kebab vertex
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Kebab {name: 'Kebab1'})");
    });

    // MATCH (n:Kebab:Kebab) should find all Kebab nodes (not look for Kebab~Kebab type)
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Kebab:Kebab) RETURN n.name");

    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("n.name")).isEqualTo("Kebab1");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void labelsWithDuplicateLabelsCreate() {
    // CREATE (n:Person:Kebab:Person) should have labels [Kebab, Person]
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (n:Person:Kebab:Person {name: 'Test'})");
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (n {name: 'Test'}) RETURN labels(n) as labels");

    assertThat(result.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<String> labels = (List<String>) result.next().getProperty("labels");
    // Should be deduplicated
    assertThat(labels).containsExactlyInAnyOrder("Kebab", "Person");
  }
}
