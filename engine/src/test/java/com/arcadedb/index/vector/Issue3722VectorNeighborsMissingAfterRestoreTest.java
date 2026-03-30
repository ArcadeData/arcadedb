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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #3722 (reopened): vectorNeighbors returns only 66 results instead of 500
 * when the user has 546 image embeddings. Simulates the user's exact scenario:
 * - 546 records with 768-dimensional vectors (matching the user's IMAGE_EMBEDDING type)
 * - Vectors inserted in many small transactions (simulating HTTP API inserts)
 * - Graph auto-built at mutation threshold, then more vectors added
 * - Database closed and reopened (simulating backup/restore)
 * - vectorNeighbors query should return the requested number of results
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3722VectorNeighborsMissingAfterRestoreTest extends TestHelper {

  private static final int DIMENSIONS = 128; // Use 128 for faster testing (768 in production)
  private static final int TOTAL_VECTORS = 546;
  private static final int QUERY_LIMIT = 500;

  /**
   * Test 1: Exact reproduction of user's scenario.
   * Insert 546 vectors with auto-rebuild at default threshold, reopen, query for 500.
   */
  @Test
  void vectorNeighborsShouldReturnAllVectorsAfterReopenWithAutoRebuild() {
    // Use a low mutation threshold to trigger auto-rebuilds during insertion (simulates production)
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 50);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Create schema
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("ImageEmbedding");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("ImageEmbedding", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    // Insert vectors one by one in separate transactions (simulating HTTP API calls)
    for (int i = 0; i < TOTAL_VECTORS; i++) {
      final int idx = i;
      database.transaction(() -> {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + idx);
        vertex.set("vector", createDeterministicVector(idx));
        vertex.save();
      });
    }

    // Verify pre-reopen state
    database.transaction(() -> {
      final long count = database.countType("ImageEmbedding", true);
      assertThat(count).as("Should have %d ImageEmbedding records", TOTAL_VECTORS).isEqualTo(TOTAL_VECTORS);
    });

    // Reopen database (simulates backup/restore or server restart)
    reopenDatabase();

    // Query and verify
    database.transaction(() -> {
      final TypeIndex ti = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
      final LSMVectorIndex idx = (LSMVectorIndex) ti.getSubIndexes().iterator().next();
      final Map<String, Long> stats = idx.getStats();

      // Verify all vectors were loaded from pages
      assertThat(stats.get("totalVectors"))
          .as("vectorIndex should have all %d vectors loaded from pages", TOTAL_VECTORS)
          .isEqualTo(TOTAL_VECTORS);

      // Search for neighbors
      final float[] queryVector = new float[DIMENSIONS];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('ImageEmbedding[vector]', ?, ?) AS neighbors",
          queryVector, QUERY_LIMIT);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();

      // Must return close to QUERY_LIMIT results (HNSW is approximate but with 546 vectors should find ~500)
      assertThat(neighbors.size())
          .as("vectorNeighbors should return close to %d results out of %d total vectors", QUERY_LIMIT, TOTAL_VECTORS)
          .isGreaterThan(QUERY_LIMIT * 9 / 10); // At least 90% of requested

      rs.close();
    });
  }

  /**
   * Test 2: Graph built and persisted early, then many more vectors added, then reopen.
   * This tests the stale graph detection and rebuild path.
   */
  @Test
  void vectorNeighborsShouldReturnAllVectorsAfterStaleGraphReopen() {
    // High threshold so graph is NOT auto-rebuilt after initial build
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Create schema
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("ImageEmbedding");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("ImageEmbedding", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    // Insert first batch (66 vectors - matching the user's reported "working" count)
    database.transaction(() -> {
      for (int i = 0; i < 66; i++) {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + i);
        vertex.set("vector", createDeterministicVector(i));
        vertex.save();
      }
    });

    // Force graph build and persist with only the first 66 vectors
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getSubIndexes().iterator().next();
    index.buildVectorGraphNow();

    // Verify graph was built with 66 vectors
    assertThat(index.getStats().get("graphNodeCount")).isGreaterThanOrEqualTo(66L);

    // Insert remaining 480 vectors (one at a time to simulate API calls)
    for (int i = 66; i < TOTAL_VECTORS; i++) {
      final int idx = i;
      database.transaction(() -> {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + idx);
        vertex.set("vector", createDeterministicVector(idx));
        vertex.save();
      });
    }

    // Reopen database (graph file has 66 nodes, pages have 546 entries)
    reopenDatabase();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Query and verify
    database.transaction(() -> {
      final TypeIndex ti = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
      final LSMVectorIndex idx = (LSMVectorIndex) ti.getSubIndexes().iterator().next();

      // Verify all vectors were loaded
      assertThat(idx.getStats().get("totalVectors"))
          .as("All %d vectors should be loaded from pages", TOTAL_VECTORS)
          .isEqualTo(TOTAL_VECTORS);

      // Search for neighbors
      final float[] queryVector = new float[DIMENSIONS];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('ImageEmbedding[vector]', ?, ?) AS neighbors",
          queryVector, QUERY_LIMIT);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();

      // Key assertion: must return significantly more than 66 (the stale graph size)
      assertThat(neighbors.size())
          .as("vectorNeighbors must return more than the 66 vectors in the stale persisted graph")
          .isGreaterThan(QUERY_LIMIT * 9 / 10);

      rs.close();
    });
  }

  /**
   * Test 3: Multiple reopens to verify graph persistence after auto-rebuild.
   */
  @Test
  void vectorNeighborsShouldSurviveMultipleReopens() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Create schema and insert all vectors at once
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("ImageEmbedding");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("ImageEmbedding", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    database.transaction(() -> {
      for (int i = 0; i < TOTAL_VECTORS; i++) {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + i);
        vertex.set("vector", createDeterministicVector(i));
        vertex.save();
      }
    });

    // Trigger first search (builds graph) then close
    database.transaction(() -> {
      final float[] queryVector = new float[DIMENSIONS];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('ImageEmbedding[vector]', ?, ?) AS neighbors",
          queryVector, QUERY_LIMIT);
      assertThat(rs.hasNext()).isTrue();
      final List<?> neighbors = rs.next().getProperty("neighbors");
      assertThat(neighbors.size()).isGreaterThan(QUERY_LIMIT * 9 / 10);
      rs.close();
    });

    // Reopen multiple times and verify each time
    for (int reopen = 0; reopen < 3; reopen++) {
      reopenDatabase();
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

      final int reopenNum = reopen;
      database.transaction(() -> {
        final float[] queryVector = new float[DIMENSIONS];
        Arrays.fill(queryVector, 0.5f);
        final ResultSet rs = database.query("sql",
            "SELECT vectorNeighbors('ImageEmbedding[vector]', ?, ?) AS neighbors",
            queryVector, QUERY_LIMIT);

        assertThat(rs.hasNext()).isTrue();
        final List<?> neighbors = rs.next().getProperty("neighbors");
        assertThat(neighbors.size())
            .as("Reopen #%d: should still return close to %d results", reopenNum + 1, QUERY_LIMIT)
            .isGreaterThan(QUERY_LIMIT * 9 / 10);
        rs.close();
      });
    }
  }

  private float[] createDeterministicVector(final int index) {
    final float[] vector = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      vector[j] = (float) Math.sin(index * 0.1 + j * 0.3) * 0.5f + 0.5f;
    return vector;
  }
}
