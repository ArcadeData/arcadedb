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

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the Labels utility class.
 * Tests composite type naming, label retrieval, and composite type creation.
 */
class LabelsTest extends TestHelper {

  // Tests for getCompositeTypeName()

  @Test
  void getCompositeTypeNameWithNull() {
    assertThat(Labels.getCompositeTypeName(null)).isEqualTo("V");
  }

  @Test
  void getCompositeTypeNameWithEmptyList() {
    assertThat(Labels.getCompositeTypeName(Collections.emptyList())).isEqualTo("V");
  }

  @Test
  void getCompositeTypeNameWithSingleLabel() {
    assertThat(Labels.getCompositeTypeName(List.of("Person"))).isEqualTo("Person");
  }

  @Test
  void getCompositeTypeNameWithTwoLabels() {
    // Should be sorted alphabetically
    assertThat(Labels.getCompositeTypeName(List.of("Person", "Developer"))).isEqualTo("Developer~Person");
  }

  @Test
  void getCompositeTypeNameWithTwoLabelsReversed() {
    // Should produce same result regardless of order
    assertThat(Labels.getCompositeTypeName(List.of("Developer", "Person"))).isEqualTo("Developer~Person");
  }

  @Test
  void getCompositeTypeNameWithThreeLabels() {
    assertThat(Labels.getCompositeTypeName(List.of("C", "A", "B"))).isEqualTo("A~B~C");
  }

  @Test
  void getCompositeTypeNamePreservesCase() {
    // Case-sensitive sorting: uppercase letters come before lowercase
    assertThat(Labels.getCompositeTypeName(List.of("person", "Developer"))).isEqualTo("Developer~person");
  }

  // Tests for isCompositeTypeName()

  @Test
  void isCompositeTypeNameWithNull() {
    assertThat(Labels.isCompositeTypeName(null)).isFalse();
  }

  @Test
  void isCompositeTypeNameWithSingleLabel() {
    assertThat(Labels.isCompositeTypeName("Person")).isFalse();
  }

  @Test
  void isCompositeTypeNameWithCompositeLabel() {
    assertThat(Labels.isCompositeTypeName("Developer~Person")).isTrue();
  }

  // Tests for ensureCompositeType()

  @Test
  void ensureCompositeTypeWithEmptyList() {
    database.transaction(() -> {
      String typeName = Labels.ensureCompositeType(database.getSchema(), Collections.emptyList());
      assertThat(typeName).isEqualTo("V");
    });
  }

  @Test
  void ensureCompositeTypeWithSingleLabel() {
    database.transaction(() -> {
      String typeName = Labels.ensureCompositeType(database.getSchema(), List.of("Person"));
      assertThat(typeName).isEqualTo("Person");
      assertThat(database.getSchema().existsType("Person")).isTrue();
    });
  }

  @Test
  void ensureCompositeTypeWithMultipleLabels() {
    database.transaction(() -> {
      String typeName = Labels.ensureCompositeType(database.getSchema(), List.of("Person", "Developer"));
      assertThat(typeName).isEqualTo("Developer~Person");

      // Verify composite type exists
      assertThat(database.getSchema().existsType("Developer~Person")).isTrue();

      // Verify base types exist
      assertThat(database.getSchema().existsType("Person")).isTrue();
      assertThat(database.getSchema().existsType("Developer")).isTrue();

      // Verify inheritance
      assertThat(database.getSchema().getType("Developer~Person").instanceOf("Person")).isTrue();
      assertThat(database.getSchema().getType("Developer~Person").instanceOf("Developer")).isTrue();
    });
  }

  @Test
  void ensureCompositeTypeReturnsExistingType() {
    database.transaction(() -> {
      // Create composite type first
      Labels.ensureCompositeType(database.getSchema(), List.of("A", "B"));

      // Call again - should return the same type name
      String typeName = Labels.ensureCompositeType(database.getSchema(), List.of("B", "A"));
      assertThat(typeName).isEqualTo("A~B");
    });
  }

  // Tests for getLabels() and hasLabel()

  @Test
  void getLabelsForSingleLabelVertex() {
    database.transaction(() -> {
      database.getSchema().getOrCreateVertexType("Person");
      MutableVertex vertex = database.newVertex("Person");
      vertex.set("name", "Alice");
      vertex.save();

      List<String> labels = Labels.getLabels(vertex);
      assertThat(labels).containsExactly("Person");
    });
  }

  @Test
  void getLabelsForCompositeTypeVertex() {
    database.transaction(() -> {
      // Create composite type
      Labels.ensureCompositeType(database.getSchema(), List.of("Person", "Developer"));

      MutableVertex vertex = database.newVertex("Developer~Person");
      vertex.set("name", "Alice");
      vertex.save();

      List<String> labels = Labels.getLabels(vertex);
      assertThat(labels).containsExactlyInAnyOrder("Developer", "Person");
    });
  }

  @Test
  void hasLabelForSingleLabelVertex() {
    database.transaction(() -> {
      database.getSchema().getOrCreateVertexType("Person");
      MutableVertex vertex = database.newVertex("Person");
      vertex.set("name", "Alice");
      vertex.save();

      assertThat(Labels.hasLabel(vertex, "Person")).isTrue();
      assertThat(Labels.hasLabel(vertex, "Developer")).isFalse();
    });
  }

  @Test
  void hasLabelForCompositeTypeVertex() {
    database.transaction(() -> {
      // Create composite type
      Labels.ensureCompositeType(database.getSchema(), List.of("Person", "Developer"));

      MutableVertex vertex = database.newVertex("Developer~Person");
      vertex.set("name", "Alice");
      vertex.save();

      // Should have both labels
      assertThat(Labels.hasLabel(vertex, "Person")).isTrue();
      assertThat(Labels.hasLabel(vertex, "Developer")).isTrue();
      assertThat(Labels.hasLabel(vertex, "Manager")).isFalse();
    });
  }

  @Test
  void polymorphicIterationWithCompositeType() {
    database.transaction(() -> {
      // Create composite type
      Labels.ensureCompositeType(database.getSchema(), List.of("Person", "Developer"));

      // Create a vertex with composite type
      MutableVertex vertex = database.newVertex("Developer~Person");
      vertex.set("name", "Alice");
      vertex.save();

      // Iterate by Person - should find the composite type vertex
      long countByPerson = 0;
      for (Vertex v : (Iterable<Vertex>) () -> database.iterateType("Person", true)) {
        countByPerson++;
      }
      assertThat(countByPerson).isEqualTo(1);

      // Iterate by Developer - should also find the composite type vertex
      long countByDeveloper = 0;
      for (Vertex v : (Iterable<Vertex>) () -> database.iterateType("Developer", true)) {
        countByDeveloper++;
      }
      assertThat(countByDeveloper).isEqualTo(1);
    });
  }

  @Test
  void threeLabelCompositeType() {
    database.transaction(() -> {
      // Create composite type with three labels
      String typeName = Labels.ensureCompositeType(database.getSchema(), List.of("Manager", "Developer", "Person"));
      assertThat(typeName).isEqualTo("Developer~Manager~Person");

      // Verify type exists and has correct inheritance
      assertThat(database.getSchema().existsType("Developer~Manager~Person")).isTrue();
      assertThat(database.getSchema().getType("Developer~Manager~Person").instanceOf("Person")).isTrue();
      assertThat(database.getSchema().getType("Developer~Manager~Person").instanceOf("Developer")).isTrue();
      assertThat(database.getSchema().getType("Developer~Manager~Person").instanceOf("Manager")).isTrue();

      // Create vertex and verify labels
      MutableVertex vertex = database.newVertex("Developer~Manager~Person");
      vertex.set("name", "Alice");
      vertex.save();

      List<String> labels = Labels.getLabels(vertex);
      assertThat(labels).containsExactlyInAnyOrder("Developer", "Manager", "Person");
    });
  }

  // Tests for duplicate label deduplication (GitHub issue #3264)

  @Test
  void getCompositeTypeNameWithDuplicateLabels() {
    // Duplicate labels should be deduplicated
    // Person:Kebab:Person should become Kebab~Person (not Kebab~Person~Person)
    assertThat(Labels.getCompositeTypeName(List.of("Person", "Kebab", "Person"))).isEqualTo("Kebab~Person");
  }

  @Test
  void getCompositeTypeNameWithAllDuplicateLabels() {
    // Kebab:Kebab should become just Kebab
    assertThat(Labels.getCompositeTypeName(List.of("Kebab", "Kebab"))).isEqualTo("Kebab");
  }

  @Test
  void getCompositeTypeNameWithMultipleDuplicates() {
    // A:B:A:B:A should become A~B
    assertThat(Labels.getCompositeTypeName(List.of("A", "B", "A", "B", "A"))).isEqualTo("A~B");
  }

  @Test
  void ensureCompositeTypeWithDuplicateLabels() {
    database.transaction(() -> {
      // Person:Kebab:Person should create Kebab~Person (with duplicates removed)
      String typeName = Labels.ensureCompositeType(database.getSchema(), List.of("Person", "Kebab", "Person"));
      assertThat(typeName).isEqualTo("Kebab~Person");

      // Verify composite type exists (without duplicate)
      assertThat(database.getSchema().existsType("Kebab~Person")).isTrue();

      // Verify base types exist
      assertThat(database.getSchema().existsType("Person")).isTrue();
      assertThat(database.getSchema().existsType("Kebab")).isTrue();

      // Verify inheritance
      assertThat(database.getSchema().getType("Kebab~Person").instanceOf("Person")).isTrue();
      assertThat(database.getSchema().getType("Kebab~Person").instanceOf("Kebab")).isTrue();
    });
  }

  @Test
  void ensureCompositeTypeWithAllDuplicateLabels() {
    database.transaction(() -> {
      // Kebab:Kebab should just create/return Kebab (single label)
      String typeName = Labels.ensureCompositeType(database.getSchema(), List.of("Kebab", "Kebab"));
      assertThat(typeName).isEqualTo("Kebab");

      // Verify only single type exists
      assertThat(database.getSchema().existsType("Kebab")).isTrue();
      // There should be no "Kebab~Kebab" type
      assertThat(database.getSchema().existsType("Kebab~Kebab")).isFalse();
    });
  }
}
