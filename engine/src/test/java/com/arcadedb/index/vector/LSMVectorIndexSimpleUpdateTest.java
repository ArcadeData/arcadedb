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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simplified test for LSM Vector Index update issue.
 * Tests the basic scenario: insert with zero vector, update with real vector, close/reopen, search.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexSimpleUpdateTest extends TestHelper {

  private static final int EMBEDDING_DIM = 128;

  @Test
  void testSimpleUpdateAndReopen() {
    final Random random = new Random(42);

    // Create schema
    database.transaction(() -> {
      database.getSchema().createVertexType("ContentV");
      database.getSchema().getType("ContentV").createProperty("id", Type.STRING);
      database.getSchema().getType("ContentV").createProperty("embedding", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON ContentV (embedding) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "COSINE"
          }""".formatted(EMBEDDING_DIM));
    });

    // Insert record with zero embedding
    //System.out.println("Inserting record with zero embedding...");
    float[] zeroEmbedding = new float[EMBEDDING_DIM];
    database.transaction(() -> {
      database.command("sql", "INSERT INTO ContentV SET id=?, embedding=?", "content-1", zeroEmbedding);
    });

    // Update with real embedding
    //System.out.println("Updating with real embedding...");
    float[] realEmbedding = generateRandomEmbedding(random, EMBEDDING_DIM);
    database.transaction(() -> {
      database.command("sql", "UPDATE ContentV SET embedding=? WHERE id=?", realEmbedding, "content-1");
    });

    // Verify before close
    //System.out.println("Verifying before close...");
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT embedding FROM ContentV WHERE id=?", "content-1")) {
        assertThat(rs.hasNext()).isTrue();
        float[] retrieved = rs.next().getProperty("embedding");
        assertThat(retrieved).isNotNull();
        assertThat(retrieved.length).isEqualTo(EMBEDDING_DIM);
      }
    }, true);

    // Close and reopen
    //System.out.println("Closing and reopening database...");
    String dbPath = database.getDatabasePath();
    database.close();

    DatabaseFactory factory = new DatabaseFactory(dbPath);
    database = factory.open();
    //System.out.println("Database reopened");

    // Check index state after reopen
    TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ContentV[embedding]");
    assertThat(typeIndex).as("Index should exist after reopen").isNotNull();

    long indexEntries = typeIndex.countEntries();
    //System.out.println("Index entry count after reopen: " + indexEntries);

    // Verify record can still be read
    //System.out.println("Verifying record after reopen...");
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT embedding FROM ContentV WHERE id=?", "content-1")) {
        assertThat(rs.hasNext()).as("Record should exist after reopen").isTrue();
        float[] retrieved = rs.next().getProperty("embedding");
        assertThat(retrieved).as("Embedding should not be null").isNotNull();
        assertThat(retrieved.length).as("Embedding dimension should match").isEqualTo(EMBEDDING_DIM);
      }
    }, true);

    // Attempt vector search after reopen
    //System.out.println("Attempting vector search after reopen...");
    float[] queryVector = generateRandomEmbedding(new Random(99), EMBEDDING_DIM);

    LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    List<Pair<RID, Float>> neighbors = lsmIndex.findNeighborsFromVector(queryVector, 10);
    //System.out.println("Found " + neighbors.size() + " neighbors after reopen");
    assertThat(neighbors).as("Should find at least 1 neighbor after reopen").isNotEmpty();

    //System.out.println("TEST PASSED");
  }

  private float[] generateRandomEmbedding(Random random, int dimensions) {
    float[] embedding = new float[dimensions];
    float norm = 0;
    for (int i = 0; i < dimensions; i++) {
      embedding[i] = random.nextFloat() * 2 - 1;
      norm += embedding[i] * embedding[i];
    }
    norm = (float) Math.sqrt(norm);
    if (norm > 0) {
      for (int i = 0; i < dimensions; i++) {
        embedding[i] /= norm;
      }
    }
    return embedding;
  }
}
