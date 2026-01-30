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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to reproduce issue #3117: LSM vector index returns empty results for some queries.
 * https://github.com/ArcadeData/arcadedb/issues/3117
 */
class Issue3117VectorIndexTest extends TestHelper {

  @Test
  void vectorNeighborsWithZeroVector() {
    // Step 1: Create schema and index
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE EmbeddingNode");
      database.command("sql", "CREATE PROPERTY EmbeddingNode.vector ARRAY_OF_FLOATS");

      // Create LSM vector index with 4 dimensions using cosine similarity
      database.command("sql", """
          CREATE INDEX ON EmbeddingNode (vector) LSM_VECTOR
          METADATA {
            "dimensions" : 4,
            "similarity" : "COSINE"
          }""");
    });

    // Step 2: Insert test data in separate transaction
    database.transaction(() -> {
      database.command("sql", "INSERT INTO EmbeddingNode SET vector = [0.1, 0.1, 0.1, 0.1]");
      database.command("sql", "INSERT INTO EmbeddingNode SET vector = [0.2, 0.2, 0.2, 0.2]");
      database.command("sql", "INSERT INTO EmbeddingNode SET vector = [0.3, 0.3, 0.3, 0.3]");
      database.command("sql", "INSERT INTO EmbeddingNode SET vector = [0.4, 0.4, 0.4, 0.4]");
    });

    // Test 1: Query with non-zero vector (this should work)
    database.transaction(() -> {
      final float[] queryVector1 = new float[] { 0.0f, 0.1f, 0.1f, 0.1f };
      final ResultSet result = database.query("sql",
          "SELECT `vector.neighbors`('EmbeddingNode[vector]', ?, 10) as neighbors FROM EmbeddingNode LIMIT 1",
          queryVector1);

      assertThat(result.hasNext()).as("Query should return results").isTrue();
      final Object neighbors = result.next().getProperty("neighbors");
      assertThat(neighbors).isNotNull();
      if (neighbors instanceof Object[]) {
        final Object[] neighborsArray = (Object[]) neighbors;
        assertThat(neighborsArray.length).isGreaterThan(0);
      }
    });

    // Test 2: Query with zero vector should give clear error message
    // (zero vectors cause undefined cosine similarity)
    database.transaction(() -> {
      final float[] queryVector2 = new float[] { 0.0f, 0.0f, 0.0f, 0.0f };

      // This should throw IllegalArgumentException with clear message
      boolean exceptionThrown = false;
      try {
        final ResultSet rs = database.query("sql",
            "SELECT `vector.neighbors`('EmbeddingNode[vector]', ?, 10) as neighbors FROM EmbeddingNode LIMIT 1",
            queryVector2);
        // Trigger lazy evaluation by calling next()
        if (rs.hasNext()) {
          rs.next();
        }
      } catch (final IllegalArgumentException e) {
        exceptionThrown = true;
        assertThat(e.getMessage())
            .contains("zero vector")
            .contains("COSINE")
            .withFailMessage("Exception should mention zero vector and COSINE similarity");
      }
      assertThat(exceptionThrown)
          .withFailMessage("Expected IllegalArgumentException for zero vector with COSINE similarity")
          .isTrue();
    });

    // Test 3: Query with another vector should work fine
    database.transaction(() -> {
      final float[] queryVector3 = new float[] { 0.5f, 0.5f, 0.5f, 0.5f };
      final ResultSet result = database.query("sql",
          "SELECT `vector.neighbors`('EmbeddingNode[vector]', ?, 10) as neighbors FROM EmbeddingNode LIMIT 1",
          queryVector3);

      assertThat(result.hasNext()).as("Query should return results").isTrue();
      final Object neighbors = result.next().getProperty("neighbors");
      assertThat(neighbors).isNotNull();
      if (neighbors instanceof Object[]) {
        final Object[] neighborsArray = (Object[]) neighbors;
        assertThat(neighborsArray.length).isGreaterThan(0);
      }
    });
  }
}
