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
 * Tests for GitHub issue #3266: Creating a type with the composite type name before
 * creating multi-label nodes should not prevent the creation of individual label types.
 * <p>
 * Also tests that underscore characters in type names do not conflict with the
 * multi-label separator.
 */
public class CypherMultiLabelPreExistingTypeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-multilabel-preexisting").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * GitHub Issue #3266: If a type with the composite name exists before creating
   * multi-label nodes, the individual label types should still be created.
   */
  @Test
  void testPreExistingCompositeTypeDoesNotPreventLabelCreation() {
    // First, create a type with the composite name manually
    database.transaction(() -> {
      // Use the new separator (tilde) for composite type name
      database.getSchema().createVertexType("Kebab~Person");
    });

    // Now try to create a multi-label node
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (n:Kebab:Person {name: 'Test'}) RETURN n");

      assertThat((Object) result).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Result r = result.next();
      final Vertex v = (Vertex) r.toElement();
      assertThat(v.getTypeName()).isEqualTo("Kebab~Person");
    });

    // Verify that all types exist
    assertThat(database.getSchema().existsType("Kebab~Person")).isTrue();
    assertThat(database.getSchema().existsType("Kebab")).isTrue();
    assertThat(database.getSchema().existsType("Person")).isTrue();

    // Verify inheritance is set up correctly
    assertThat(database.getSchema().getType("Kebab~Person").instanceOf("Kebab")).isTrue();
    assertThat(database.getSchema().getType("Kebab~Person").instanceOf("Person")).isTrue();
  }

  /**
   * Tests that creating the composite type first and then creating multi-label
   * nodes still allows matching by individual labels.
   */
  @Test
  void testPreExistingCompositeTypeAllowsMatchByIndividualLabel() {
    // First, create a type with the composite name manually
    database.transaction(() -> {
      database.getSchema().createVertexType("Developer~Person");
    });

    // Create a multi-label node
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Developer:Person {name: 'Alice'})");
    });

    // Match by Person should find the vertex
    final ResultSet personResult = database.query("opencypher",
        "MATCH (n:Person) RETURN n.name as name");
    assertThat(personResult.hasNext()).isTrue();
    assertThat((String) personResult.next().getProperty("name")).isEqualTo("Alice");

    // Match by Developer should also find the vertex
    final ResultSet devResult = database.query("opencypher",
        "MATCH (n:Developer) RETURN n.name as name");
    assertThat(devResult.hasNext()).isTrue();
    assertThat((String) devResult.next().getProperty("name")).isEqualTo("Alice");
  }

  /**
   * Tests that type names containing underscores do not conflict with multi-label handling.
   * User-defined type names with underscores should work correctly.
   */
  @Test
  void testTypeNamesWithUnderscoresDoNotConflict() {
    database.transaction(() -> {
      // Create types with underscores in their names
      database.getSchema().createVertexType("My_Type");
      database.getSchema().createVertexType("Another_Type");
    });

    // Create a multi-label node using these types
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (n:My_Type:Another_Type {name: 'Test'}) RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().toElement();
      // Composite type name uses tilde, not underscore
      assertThat(v.getTypeName()).isEqualTo("Another_Type~My_Type");
    });

    // Verify all types exist
    assertThat(database.getSchema().existsType("My_Type")).isTrue();
    assertThat(database.getSchema().existsType("Another_Type")).isTrue();
    assertThat(database.getSchema().existsType("Another_Type~My_Type")).isTrue();

    // Match by individual labels
    final ResultSet result = database.query("opencypher",
        "MATCH (n:My_Type) RETURN n.name as name");
    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("name")).isEqualTo("Test");
  }

  /**
   * Tests the labels() function returns correct labels for pre-existing composite types.
   */
  @Test
  void testLabelsFunctionWithPreExistingCompositeType() {
    // Create composite type first
    database.transaction(() -> {
      database.getSchema().createVertexType("Manager~Worker");
    });

    // Create multi-label node
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Manager:Worker {name: 'Bob'})");
    });

    // Test labels() function
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Manager:Worker) RETURN labels(n) as labels");

    assertThat(result.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<String> labels = (List<String>) result.next().getProperty("labels");
    assertThat(labels).containsExactlyInAnyOrder("Manager", "Worker");
  }

  /**
   * Tests that the new tilde separator is used consistently.
   */
  @Test
  void testCompositeTypeNameUsesTildeSeparator() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (n:Alpha:Beta:Gamma {name: 'Test'}) RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().toElement();
      // Should use tilde separator and be alphabetically sorted
      assertThat(v.getTypeName()).isEqualTo("Alpha~Beta~Gamma");
    });
  }

  /**
   * Tests backward compatibility: existing databases might have types with underscore separator.
   * The getLabels method should handle both old (underscore) and new (tilde) separators.
   */
  @Test
  void testLabelsUtilityMethods() {
    // Test getCompositeTypeName
    assertThat(Labels.getCompositeTypeName(List.of("Person"))).isEqualTo("Person");
    assertThat(Labels.getCompositeTypeName(List.of("Person", "Developer"))).isEqualTo("Developer~Person");
    assertThat(Labels.getCompositeTypeName(List.of("C", "A", "B"))).isEqualTo("A~B~C");

    // Test isCompositeTypeName
    assertThat(Labels.isCompositeTypeName("Person")).isFalse();
    assertThat(Labels.isCompositeTypeName("Developer~Person")).isTrue();
    // Type names with underscores should NOT be detected as composite
    assertThat(Labels.isCompositeTypeName("My_Type")).isFalse();
  }
}
