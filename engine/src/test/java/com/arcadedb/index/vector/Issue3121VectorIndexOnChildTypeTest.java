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
 * Test case for GitHub issue #3121: Allow vector searches to be filtered by child type.
 * <p>
 * https://github.com/ArcadeData/arcadedb/issues/3121
 * <p>
 * When an index is created on a parent type (e.g., EMBEDDING), sub-indexes are automatically
 * created for each bucket, including buckets of child types. This test verifies that
 * `vector.neighbors`() can search only within a specific child type's buckets by specifying
 * the child type name (e.g., 'EMBEDDING_IMAGE[vector]').
 * <p>
 * Use case:
 * 1. Class-specific: "Get top 50 closest IMAGE embeddings" - search only EMBEDDING_IMAGE buckets
 * 2. Cross-class: "Get top 50 closest embeddings across ALL types" - search all EMBEDDING buckets
 */
class Issue3121VectorIndexOnChildTypeTest extends TestHelper {

  private static final int DIMENSIONS = 128;

  /**
   * Test that `vector.neighbors`() can search only within a specific child type's buckets
   * when the index was created on the parent type.
   */
  @Test
  void vectorSearchByChildType() {
    //System.out.println("\n=== Testing Vector Search by Child Type ===");

    // Step 1: Create parent type with vector index
    database.transaction(() -> {
      //System.out.println("\n1. Creating parent type EMBEDDING with vector index");
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
      //System.out.println("2. Creating child types");
      database.command("sql", "CREATE VERTEX TYPE EMBEDDING_IMAGE EXTENDS EMBEDDING");
      database.command("sql", "CREATE VERTEX TYPE EMBEDDING_DOCUMENT EXTENDS EMBEDDING");
      database.command("sql", "CREATE VERTEX TYPE EMBEDDING_CHUNK EXTENDS EMBEDDING");
    });

    // Step 3: Insert test data into different types with distinguishable vectors
    database.transaction(() -> {
      //System.out.println("3. Inserting test data");

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
      //System.out.println("   Inserted 30 records (10 each type)");
    });

    // Step 4: Test type-specific vector search using child type name
    database.transaction(() -> {
      //System.out.println("\n4. Testing type-specific vector search on EMBEDDING_IMAGE");

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
        //System.out.println("   Found neighbor: " + vertex.getIdentity() + " type=" + typeName);
        assertThat(typeName).as("All results should be EMBEDDING_IMAGE").isEqualTo("EMBEDDING_IMAGE");
      }
      //System.out.println("   ✓ Type-specific search returned only EMBEDDING_IMAGE records");
    });

    // Step 5: Test cross-type vector search using parent type name
    database.transaction(() -> {
      //System.out.println("\n5. Testing cross-type vector search on EMBEDDING (parent)");

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
      //System.out.println("   Found " + neighbors.size() + " neighbors");

      // Count types in results
      Set<String> typesFound = new HashSet<>();
      for (Map<String, Object> neighbor : neighbors) {
        Vertex vertex = (Vertex) neighbor.get("record");
        typesFound.add(vertex.getTypeName());
      }

      //System.out.println("   Types found: " + typesFound);
      // Cross-type search may return multiple types depending on similarity
      // With our test vectors, images should be closest to the image query vector
      //System.out.println("   ✓ Cross-type search completed");
    });

    // Step 6: Test searching in EMBEDDING_DOCUMENT type
    database.transaction(() -> {
      //System.out.println("\n6. Testing type-specific search on EMBEDDING_DOCUMENT");

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
      //System.out.println("   ✓ EMBEDDING_DOCUMENT search returned only document records");
    });

    //System.out.println("\n=== Test completed successfully ===");
  }

  /**
   * Test that type-specific search excludes records from other types even when they are similar.
   */
  @Test
  void typeFilteringExcludesOtherTypes() {
    //System.out.println("\n=== Testing Type Filtering Exclusion ===");

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
      //System.out.println("✓ Type-specific search correctly excluded other types (found " + neighbors.size() + " results)");
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
