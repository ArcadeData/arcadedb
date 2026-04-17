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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3864: Cypher batch creation is slow.
 * <p>
 * Tests that OpenCypher batch vertex creation with vector embeddings
 * performs acceptably, including on a database that already has data
 * and a persisted HNSW graph index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue3864CypherBatchCreationPerformanceTest extends TestHelper {

  private static final int VECTOR_DIMENSIONS = 1024;
  private static final int BATCH_SIZE = 3000;
  private static final Random RANDOM = new Random(42);

  @Override
  public void beginTest() {
    // Use a short inactivity rebuild timeout so the graph gets built quickly between batches
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 500);

    database.transaction(() -> {
      final VertexType type = database.getSchema().getOrCreateVertexType("VectorDoc");
      type.getOrCreateProperty("name", Type.STRING);
      type.getOrCreateProperty("embedding", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("VectorDoc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(VECTOR_DIMENSIONS)
          .withSimilarity("COSINE")
          .create();
    });
  }

  @Test
  void batchCreateVectorsOnFreshDatabase() {
    final List<Map<String, Object>> batch = generateBatch(BATCH_SIZE, "fresh_");

    final Map<String, Object> params = new HashMap<>();
    params.put("batch", batch);

    final long start = System.currentTimeMillis();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "UNWIND $batch AS item CREATE (n:VectorDoc {name: item.name, embedding: item.embedding}) RETURN count(n) AS cnt",
          params)) {
        assertThat(rs.hasNext()).isTrue();
        final long count = ((Number) rs.next().getProperty("cnt")).longValue();
        assertThat(count).isEqualTo(BATCH_SIZE);
      }
    });
    final long freshTime = System.currentTimeMillis() - start;

    // Verify the vertices were created
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM VectorDoc")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(BATCH_SIZE);
      }
    });

    assertThat(freshTime).as("Fresh database batch insert took %d ms", freshTime).isLessThan(10_000);
  }

  /**
   * Main regression test: inserts an initial batch, forces a graph build (simulating what
   * happens when the inactivity timer fires between user requests), then measures whether
   * the second batch is dramatically slower due to per-vector HNSW graph inserts.
   */
  @Test
  void batchCreateVectorsAfterGraphBuild() {
    // Step 1: Insert initial batch
    final List<Map<String, Object>> initialBatch = generateBatch(BATCH_SIZE, "initial_");
    final Map<String, Object> initialParams = new HashMap<>();
    initialParams.put("batch", initialBatch);

    final long startFresh = System.currentTimeMillis();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "UNWIND $batch AS item CREATE (n:VectorDoc {name: item.name, embedding: item.embedding}) RETURN count(n) AS cnt",
          initialParams)) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(BATCH_SIZE);
      }
    });
    final long freshTime = System.currentTimeMillis() - startFresh;

    // Step 2: Force graph build - simulates what happens between API calls when
    // the inactivity timer fires and builds the HNSW graph
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("VectorDoc[embedding]");
    final LSMVectorIndex vectorIndex = (LSMVectorIndex) typeIndex.getSubIndexes().iterator().next();
    vectorIndex.buildVectorGraphNow();

    // Verify graph was built
    assertThat(vectorIndex.getStats().get("graphNodeCount"))
        .as("Graph should contain the initial vectors")
        .isGreaterThanOrEqualTo((long) BATCH_SIZE);

    // Step 3: Insert second batch - now the HNSW graph is active, and the old code
    // would do per-vector O(log n) HNSW inserts during commit replay
    final List<Map<String, Object>> secondBatch = generateBatch(BATCH_SIZE, "second_");
    final Map<String, Object> secondParams = new HashMap<>();
    secondParams.put("batch", secondBatch);

    final long startPopulated = System.currentTimeMillis();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "UNWIND $batch AS item CREATE (n:VectorDoc {name: item.name, embedding: item.embedding}) RETURN count(n) AS cnt",
          secondParams)) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(BATCH_SIZE);
      }
    });
    final long populatedTime = System.currentTimeMillis() - startPopulated;

    // Verify total vertex count
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM VectorDoc")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(2 * BATCH_SIZE);
      }
    });

    // The second batch should not be dramatically slower than the first.
    // Before the fix, the second batch triggered per-vector HNSW inserts (O(n*log(n)*M))
    // causing significant degradation. After the fix, both use O(n) delta-scan path.
    assertThat(populatedTime)
        .as("Populated batch (%dms) should not be >3x slower than fresh batch (%dms)",
            populatedTime, freshTime)
        .isLessThan(freshTime * 3 + 500);
  }

  @Test
  void batchCreateVectorsWithoutIndex() {
    // Create a type without vector index to compare performance
    database.transaction(() -> {
      database.getSchema().getOrCreateVertexType("PlainDoc")
          .getOrCreateProperty("name", Type.STRING);
      database.getSchema().getType("PlainDoc")
          .getOrCreateProperty("embedding", Type.ARRAY_OF_FLOATS);
    });

    final List<Map<String, Object>> batch = generateBatch(BATCH_SIZE, "plain_");
    final Map<String, Object> params = new HashMap<>();
    params.put("batch", batch);

    final long start = System.currentTimeMillis();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "UNWIND $batch AS item CREATE (n:PlainDoc {name: item.name, embedding: item.embedding}) RETURN count(n) AS cnt",
          params)) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(BATCH_SIZE);
      }
    });
    final long noIndexTime = System.currentTimeMillis() - start;

    assertThat(noIndexTime).as("No-index batch insert took %d ms", noIndexTime).isLessThan(10_000);
  }

  private static List<Map<String, Object>> generateBatch(final int size, final String namePrefix) {
    final List<Map<String, Object>> batch = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      final Map<String, Object> item = new HashMap<>();
      item.put("name", namePrefix + i);
      // Generate random embedding as List<Double> (mimicking JSON parsing)
      final List<Double> embedding = new ArrayList<>(VECTOR_DIMENSIONS);
      for (int d = 0; d < VECTOR_DIMENSIONS; d++)
        embedding.add(RANDOM.nextDouble());
      item.put("embedding", embedding);
      batch.add(item);
    }
    return batch;
  }
}
