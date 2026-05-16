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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * LSM vector index behavior with type inheritance: parent type, child type definition, and type-filtered search.
 */
class LSMVectorIndexInheritanceTest extends TestHelper {

  private static final int DIMENSIONS = 128;

  // Issue #3120: inserting into a child vertex type that extends a parent with an LSM_VECTOR index must work without dimension mismatch errors.
  @Test
  void vectorIndexInheritance() {
    // Step 1: Create parent vertex type with vector property and index
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE EMBEDDING");
      database.command("sql", "CREATE PROPERTY EMBEDDING.vector ARRAY_OF_FLOATS");

      database.command("sql", """
          CREATE INDEX ON EMBEDDING (vector) LSM_VECTOR
          METADATA {
            "dimensions" : 1024,
            "similarity" : "COSINE"
          }""");
    });

    // Step 2: Insert into parent class (this should work)
    database.transaction(() -> {
      final float[] testVector = new float[1024];
      for (int i = 0; i < 1024; i++) {
        testVector[i] = (float) Math.random();
      }

      database.command("sql", "INSERT INTO EMBEDDING SET vector = ?", (Object) testVector);
    });

    // Step 3: Create child vertex type that extends parent
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE CHUNK_EMBEDDING EXTENDS EMBEDDING");
    });

    // Step 4: Insert into child class (this is where the bug occurs)
    database.transaction(() -> {
      final float[] testVector = new float[1024];
      for (int i = 0; i < 1024; i++) {
        testVector[i] = (float) Math.random();
      }

      database.command("sql", "INSERT INTO CHUNK_EMBEDDING SET vector = ?", (Object) testVector);
    });

    // Step 5: Verify both records exist
    database.transaction(() -> {
      final ResultSet parentRecords = database.query("sql", "SELECT count(*) as cnt FROM EMBEDDING");
      final long parentCount = parentRecords.hasNext() ?
          parentRecords.next().<Long>getProperty("cnt") : 0L;
      assertThat(parentCount).isEqualTo(2L); // Should have both parent and child records

      final ResultSet childRecords = database.query("sql", "SELECT count(*) as cnt FROM CHUNK_EMBEDDING");
      final long childCount = childRecords.hasNext() ?
          childRecords.next().<Long>getProperty("cnt") : 0L;
      assertThat(childCount).isEqualTo(1L);
    });
  }

  // Issue #3120: multi-level inheritance (grandparent -> parent -> child) must propagate the vector index correctly at every level.
  @Test
  void vectorIndexInheritanceMultipleLevels() {
    // Create hierarchy: EMBEDDING -> CHUNK_EMBEDDING -> SPECIFIC_CHUNK
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE EMBEDDING");
      database.command("sql", "CREATE PROPERTY EMBEDDING.vector ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON EMBEDDING (vector) LSM_VECTOR
          METADATA {
            "dimensions" : 128,
            "similarity" : "COSINE"
          }""");

      database.command("sql", "CREATE VERTEX TYPE CHUNK_EMBEDDING EXTENDS EMBEDDING");
      database.command("sql", "CREATE VERTEX TYPE SPECIFIC_CHUNK EXTENDS CHUNK_EMBEDDING");
    });

    // Test insert at each level
    database.transaction(() -> {
      final float[] testVector = new float[128];
      for (int i = 0; i < 128; i++) {
        testVector[i] = (float) (i * 0.01);
      }

      // Insert into grandparent
      database.command("sql", "INSERT INTO EMBEDDING SET vector = ?", (Object) testVector);

      // Insert into parent
      database.command("sql", "INSERT INTO CHUNK_EMBEDDING SET vector = ?", (Object) testVector);

      // Insert into child
      database.command("sql", "INSERT INTO SPECIFIC_CHUNK SET vector = ?", (Object) testVector);
    });

    // Verify all records
    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM EMBEDDING");
      final long count = result.hasNext() ? result.next().<Long>getProperty("cnt") : 0L;
      assertThat(count).isEqualTo(3L);
    });
  }

  // Issue #3121: vector.neighbors() must scope results to a specific child type's buckets when invoked as '<ChildType>[vector]'.
  @Test
  void vectorSearchByChildType() {
    // Step 1: Create parent type with vector index
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE EMBEDDING");
      database.command("sql", "CREATE PROPERTY EMBEDDING.vector ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON EMBEDDING (vector) LSM_VECTOR
          METADATA {
            "dimensions" : 128,
            "similarity" : "COSINE"
          }""");
    });

    // Step 2: Create child types
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE EMBEDDING_IMAGE EXTENDS EMBEDDING");
      database.command("sql", "CREATE VERTEX TYPE EMBEDDING_DOCUMENT EXTENDS EMBEDDING");
      database.command("sql", "CREATE VERTEX TYPE EMBEDDING_CHUNK EXTENDS EMBEDDING");
    });

    // Step 3: Insert test data into different types with distinguishable vectors
    database.transaction(() -> {
      // Create clearly different vectors for each type:
      // Images: vectors where first component is dominant
      // Documents: vectors where second component is dominant
      // Chunks: vectors where third component is dominant

      for (int i = 0; i < 10; i++) {
        float[] imageVector = createTestVector(0, i);      // dominant in dimension 0-9
        float[] documentVector = createTestVector(1, i);   // dominant in dimension 10-19
        float[] chunkVector = createTestVector(2, i);      // dominant in dimension 20-29

        database.command("sql", "INSERT INTO EMBEDDING_IMAGE SET vector = ?", (Object) imageVector);
        database.command("sql", "INSERT INTO EMBEDDING_DOCUMENT SET vector = ?", (Object) documentVector);
        database.command("sql", "INSERT INTO EMBEDDING_CHUNK SET vector = ?", (Object) chunkVector);
      }
    });

    // Step 4: Test type-specific vector search using child type name
    database.transaction(() -> {
      // Query vector similar to image vectors (dominant in first dimensions)
      float[] queryVector = createTestVector(0, 5);

      // Search ONLY in EMBEDDING_IMAGE by specifying the child type name
      ResultSet result = database.query("sql",
          "SELECT `vector.neighbors`('EMBEDDING_IMAGE[vector]', ?, 5) as neighbors FROM EMBEDDING_IMAGE LIMIT 1",
          queryVector);

      assertThat(result.hasNext()).as("Query should return results").isTrue();
      Result row = result.next();
      List<Map<String, Object>> neighbors = row.getProperty("neighbors");

      assertThat(neighbors).as("Should find neighbors").isNotNull();
      assertThat(neighbors.size()).as("Should return requested number of neighbors").isEqualTo(5);

      // All results should be from EMBEDDING_IMAGE type
      for (Map<String, Object> neighbor : neighbors) {
        Vertex vertex = (Vertex) neighbor.get("record");
        String typeName = vertex.getTypeName();
        assertThat(typeName).as("All results should be EMBEDDING_IMAGE").isEqualTo("EMBEDDING_IMAGE");
      }
    });

    // Step 5: Test cross-type vector search using parent type name
    database.transaction(() -> {
      // Same query vector
      float[] queryVector = createTestVector(0, 5);

      // Search across ALL types by specifying the parent type name
      ResultSet result = database.query("sql",
          "SELECT `vector.neighbors`('EMBEDDING[vector]', ?, 15) as neighbors FROM EMBEDDING LIMIT 1",
          queryVector);

      assertThat(result.hasNext()).as("Query should return results").isTrue();
      Result row = result.next();
      List<Map<String, Object>> neighbors = row.getProperty("neighbors");

      assertThat(neighbors).as("Should find neighbors").isNotNull();

      // Count types in results
      Set<String> typesFound = new HashSet<>();
      for (Map<String, Object> neighbor : neighbors) {
        Vertex vertex = (Vertex) neighbor.get("record");
        typesFound.add(vertex.getTypeName());
      }
      // Cross-type search may return multiple types depending on similarity
      // With our test vectors, images should be closest to the image query vector
    });

    // Step 6: Test searching in EMBEDDING_DOCUMENT type
    database.transaction(() -> {
      // Query vector similar to document vectors (dominant in second dimensions)
      float[] queryVector = createTestVector(1, 5);

      ResultSet result = database.query("sql",
          "SELECT `vector.neighbors`('EMBEDDING_DOCUMENT[vector]', ?, 5) as neighbors FROM EMBEDDING_DOCUMENT LIMIT 1",
          queryVector);

      assertThat(result.hasNext()).isTrue();
      List<Map<String, Object>> neighbors = result.next().getProperty("neighbors");

      assertThat(neighbors).isNotNull();
      assertThat(neighbors.size()).isEqualTo(5);

      // All results should be from EMBEDDING_DOCUMENT type
      for (Map<String, Object> neighbor : neighbors) {
        Vertex vertex = (Vertex) neighbor.get("record");
        assertThat(vertex.getTypeName()).isEqualTo("EMBEDDING_DOCUMENT");
      }
    });
  }

  // Issue #3121: type-specific vector search must exclude records belonging to sibling types.
  @Test
  void typeFilteringExcludesOtherTypes() {
    // Create schema
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE EMBEDDING");
      database.command("sql", "CREATE PROPERTY EMBEDDING.vector ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON EMBEDDING (vector) LSM_VECTOR
          METADATA { "dimensions" : 128, "similarity" : "COSINE" }""");

      database.command("sql", "CREATE VERTEX TYPE EMBEDDING_A EXTENDS EMBEDDING");
      database.command("sql", "CREATE VERTEX TYPE EMBEDDING_B EXTENDS EMBEDDING");
    });

    // Insert vectors in both types - use slightly different vectors to avoid deduplication
    database.transaction(() -> {
      // Insert 5 records in type A with slightly different vectors
      for (int i = 0; i < 5; i++) {
        float[] vector = createTestVector(0, i);
        database.command("sql", "INSERT INTO EMBEDDING_A SET vector = ?", (Object) vector);
      }

      // Insert 5 records in type B with different vectors
      for (int i = 0; i < 5; i++) {
        float[] vector = createTestVector(1, i);  // Different dimension range
        database.command("sql", "INSERT INTO EMBEDDING_B SET vector = ?", (Object) vector);
      }
    });

    // Search only in type A - should return only A records
    database.transaction(() -> {
      float[] queryVector = createTestVector(0, 2);

      ResultSet result = database.query("sql",
          "SELECT `vector.neighbors`('EMBEDDING_A[vector]', ?, 10) as neighbors FROM EMBEDDING_A LIMIT 1",
          queryVector);

      List<Map<String, Object>> neighbors = result.next().getProperty("neighbors");

      // All results should be from type A (no records from type B)
      assertThat(neighbors).as("Should find some neighbors").isNotEmpty();

      for (Map<String, Object> neighbor : neighbors) {
        Vertex vertex = (Vertex) neighbor.get("record");
        assertThat(vertex.getTypeName()).as("All results should be EMBEDDING_A").isEqualTo("EMBEDDING_A");
      }
    });
  }

  /**
   * Helper to create test vectors that are distinguishable by type.
   * Each type has vectors with a strong signal in a different dimension range.
   */
  private float[] createTestVector(int typeIndex, int variation) {
    float[] vector = new float[DIMENSIONS];
    // Set a strong signal at the position corresponding to the type
    vector[typeIndex * 10] = 1.0f;
    // Add some variation
    if (variation < 10) {
      vector[typeIndex * 10 + variation] += 0.5f;
    }
    // Normalize
    float sum = 0;
    for (float v : vector) {
      sum += v * v;
    }
    float norm = (float) Math.sqrt(sum);
    for (int i = 0; i < vector.length; i++) {
      vector[i] /= norm;
    }
    return vector;
  }
}
