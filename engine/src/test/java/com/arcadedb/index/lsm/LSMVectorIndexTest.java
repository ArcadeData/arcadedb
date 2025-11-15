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
package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.Schema;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for LSMVectorIndex Phase 3 implementation.
 *
 * <p>Tests the complete lifecycle of vector indexes including:
 * - Factory creation via IndexFactoryHandler
 * - Vector insertion (put operations)
 * - Vector search (get operations)
 * - Compaction scheduling and execution
 * - Cross-component search (mutable + compacted)
 *
 * @author Arcade Data
 * @since 24.12.0
 */
public class LSMVectorIndexTest extends TestHelper {

  @Test
  public void testFactoryCreatesLSMVectorIndex() {
    database.transaction(() -> {
      // Create a simple LSM vector index via SQL
      database.command("sqlscript",
          """
              CREATE VERTEX TYPE TestVector IF NOT EXISTS;
              CREATE PROPERTY TestVector.embedding IF NOT EXISTS ARRAY_OF_FLOATS;

              CREATE INDEX ON TestVector (embedding) LSM_VECTOR METADATA {
                                  "dimensions" : 10,
                                  "similarity" : COSINE,
                                  "maxConnections" : 16,
                                  "beamWidth" : 100
                                  };
              """);

      // Verify index was created
      // Note: The auto-generated index name is "TestVector[embedding]" (TypeName + properties)
      final Schema schema = database.getSchema();
      final Index index = schema.getIndexByName("TestVector[embedding]");

      assertThat(index).isNotNull();
      assertThat(index).isInstanceOf(LSMVectorIndex.class);

      final LSMVectorIndex vectorIndex = (LSMVectorIndex) index;
      assertThat(vectorIndex.getName()).isEqualTo("TestVector[embedding]");
      assertThat(vectorIndex.getMutableIndex()).isNotNull();
    });
  }

  @Test
  public void testVectorInsertion() {
    database.transaction(() -> {
      // Create index using Java API
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", "embedding")
          .withIndexName("test_insert_idx")
          .withDimensions(5)
          .withSimilarity(VectorSimilarityFunction.EUCLIDEAN)
          .create();

      // Create test RIDs
      final RID rid1 = new RID(database, 1, 0);
      final RID rid2 = new RID(database, 1, 1);

      // Insert vectors
      final float[] vector1 = { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f };
      final float[] vector2 = { 1.1f, 2.1f, 3.1f, 4.1f, 5.1f };

      index.put(new Object[] { vector1 }, new RID[] { rid1 });
      index.put(new Object[] { vector2 }, new RID[] { rid2 });

      // Verify count
      assertThat(index.countEntries()).isEqualTo(2);
    });
  }

  @Test
  public void testVectorSearch() {
    database.transaction(() -> {
      // Create index using Java API
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", "embedding")
          .withIndexName("test_search_idx")
          .withDimensions(3)
          .withSimilarity(VectorSimilarityFunction.COSINE)
          .create();

      // Create test vectors
      final RID rid1 = new RID(database, 1, 0);
      final float[] vector1 = { 1.0f, 0.0f, 0.0f };

      // Insert vector
      index.put(new Object[] { vector1 }, new RID[] { rid1 });

      // Search for exact match
      final IndexCursor cursor = index.get(new Object[] { vector1 });
      assertThat(cursor.hasNext()).isTrue();
      final RID found = (RID) cursor.next();
      assertThat(found).isEqualTo(rid1);
    });
  }

  @Test
  public void testCompactionScheduling() {
    database.transaction(() -> {
      // Create index using Java API
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", "embedding")
          .withIndexName("test_compact_idx")
          .withDimensions(2)
          .withSimilarity(VectorSimilarityFunction.DOT_PRODUCT)
          .create();

      // Verify initial status
      assertThat(index.getStatus()).isEqualTo(LSMVectorIndex.STATUS.AVAILABLE);

      // Schedule compaction
      final boolean scheduled = index.scheduleCompaction();
      assertThat(scheduled).isTrue();

      // Verify status changed
      assertThat(index.getStatus()).isEqualTo(LSMVectorIndex.STATUS.COMPACTION_SCHEDULED);
    });
  }

  @Test
  public void testCompactionExecution() {
    database.transaction(() -> {
      // Create index using Java API
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", "embedding")
          .withIndexName("test_exec_compact_idx")
          .withDimensions(4)
          .withSimilarity(VectorSimilarityFunction.COSINE)
          .create();

      // Insert some vectors
      final RID rid1 = new RID(database, 1, 0);
      final RID rid2 = new RID(database, 1, 1);
      final RID rid3 = new RID(database, 1, 2);

      final float[] v1 = { 1.0f, 2.0f, 3.0f, 4.0f };
      final float[] v2 = { 1.1f, 2.1f, 3.1f, 4.1f };
      final float[] v3 = { 1.2f, 2.2f, 3.2f, 4.2f };

      index.put(new Object[] { v1 }, new RID[] { rid1 });
      index.put(new Object[] { v2 }, new RID[] { rid2 });
      index.put(new Object[] { v3 }, new RID[] { rid3 });

      assertThat(index.countEntries()).isEqualTo(3);

      // Schedule and execute compaction
      index.scheduleCompaction();
      final boolean compacted;
      try {
        compacted = index.compact();
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
      assertThat(compacted).isTrue();

      // Verify status back to AVAILABLE
      assertThat(index.getStatus()).isEqualTo(LSMVectorIndex.STATUS.AVAILABLE);

      // Verify data is still accessible
      assertThat(index.countEntries()).isEqualTo(3);
    });
  }

  @Test
  public void testVectorSearchAfterCompaction()  {
    database.transaction(() -> {
      // Create index using Java API
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", "embedding")
          .withIndexName("test_search_after_compact_idx")
          .withDimensions(2)
          .withSimilarity(VectorSimilarityFunction.EUCLIDEAN)
          .create();

      // Insert vectors
      final RID rid1 = new RID(database, 1, 0);
      final RID rid2 = new RID(database, 1, 1);

      final float[] v1 = { 1.0f, 2.0f };
      final float[] v2 = { 3.0f, 4.0f };

      index.put(new Object[] { v1 }, new RID[] { rid1 });
      index.put(new Object[] { v2 }, new RID[] { rid2 });

      // Execute compaction
      index.scheduleCompaction();
      try {
        index.compact();
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }

      // Search after compaction
      final IndexCursor cursor1 = index.get(new Object[] { v1 });
      assertThat(cursor1.hasNext()).isTrue();
      assertThat(cursor1.next()).isEqualTo(rid1);

      final IndexCursor cursor2 = index.get(new Object[] { v2 });
      assertThat(cursor2.hasNext()).isTrue();
      assertThat(cursor2.next()).isEqualTo(rid2);
    });
  }

  @Test
  public void testEmptyIndexCursor() {
    database.transaction(() -> {
      // Create index using Java API
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", "embedding")
          .withIndexName("test_empty_cursor_idx")
          .withDimensions(1)
          .withSimilarity(VectorSimilarityFunction.COSINE)
          .create();

      // Search for non-existent vector
      final float[] vector = { 1.0f };
      final IndexCursor cursor = index.get(new Object[] { vector });

      assertThat(cursor.hasNext()).isFalse();
      assertThat(cursor.estimateSize()).isEqualTo(0);
    });
  }

  @Test
  public void testJSONSerialization() {
    database.transaction(() -> {
      // Create index using Java API
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", "embedding")
          .withIndexName("test_json_idx")
          .withDimensions(8)
          .withSimilarity(VectorSimilarityFunction.COSINE)
          .create();

      // Serialize to JSON
      final var json = index.toJSON();

      assertThat(json.get("name")).isEqualTo("test_json_idx");
      assertThat(json.get("type")).isEqualTo("LSM_VECTOR");
      assertThat(json.get("dimensions")).isEqualTo(8);
      assertThat(json.get("similarity")).isEqualTo(VectorSimilarityFunction.COSINE.name());
    });
  }

  @Test
  public void testKNNSearchOnMutableIndex() {
    database.transaction(() -> {
      // Create index using Java API
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", "embedding")
          .withIndexName("test_knn_mutable_idx")
          .withDimensions(3)
          .withSimilarity(VectorSimilarityFunction.COSINE)
          .create();

      // Insert test vectors
      final RID rid1 = new RID(database, 1, 0);
      final RID rid2 = new RID(database, 1, 1);
      final RID rid3 = new RID(database, 1, 2);

      final float[] v1 = { 1.0f, 0.0f, 0.0f };
      final float[] v2 = { 0.9f, 0.1f, 0.0f };
      final float[] v3 = { 0.0f, 1.0f, 0.0f };

      index.put(new Object[] { v1 }, new RID[] { rid1 });
      index.put(new Object[] { v2 }, new RID[] { rid2 });
      index.put(new Object[] { v3 }, new RID[] { rid3 });

      // KNN search
      final List<LSMVectorIndexMutable.VectorSearchResult> results =
          index.knnSearch(v1, 2);

      assertThat(results).hasSize(2);
      assertThat(results.get(0).rids).contains(rid1); // v1 is closest to v1
      assertThat(results.get(1).rids).contains(rid2); // v2 is 2nd closest
    });
  }

  @Test
  public void testKNNSearchWithHNSWAfterCompaction() {
    database.transaction(() -> {
      // Create index using Java API
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", "embedding")
          .withIndexName("test_knn_hnsw_idx")
          .withDimensions(4)
          .withSimilarity(VectorSimilarityFunction.EUCLIDEAN)
          .create();

      // Insert vectors
      final RID rid1 = new RID(database, 1, 0);
      final RID rid2 = new RID(database, 1, 1);
      final RID rid3 = new RID(database, 1, 2);
      final RID rid4 = new RID(database, 1, 3);

      final float[] v1 = { 1.0f, 0.0f, 0.0f, 0.0f };
      final float[] v2 = { 1.1f, 0.1f, 0.0f, 0.0f };
      final float[] v3 = { 0.0f, 1.0f, 0.0f, 0.0f };
      final float[] v4 = { 0.0f, 0.0f, 1.0f, 0.0f };

      index.put(new Object[] { v1 }, new RID[] { rid1 });
      index.put(new Object[] { v2 }, new RID[] { rid2 });
      index.put(new Object[] { v3 }, new RID[] { rid3 });
      index.put(new Object[] { v4 }, new RID[] { rid4 });

      // Trigger compaction to build HNSW
      index.scheduleCompaction();
      try {
        index.compact();
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }

      // KNN search using HNSW (from compacted)
      final List<LSMVectorIndexMutable.VectorSearchResult> results =
          index.knnSearch(v1, 2);

      assertThat(results).hasSize(2);
      // v1 itself should be closest (exact match, distance 0)
      assertThat(results.get(0).rids).contains(rid1);
      // v2 should be 2nd closest (Euclidean distance ~0.141)
      assertThat(results.get(1).rids).contains(rid2);
    });
  }

  @Test
  public void testKNNSearchAllDistanceMetrics() {
    database.transaction(() -> {
      // Test COSINE similarity
      testKNNWithMetric(VectorSimilarityFunction.COSINE, 3, 0);

      // Test EUCLIDEAN distance
      testKNNWithMetric(VectorSimilarityFunction.EUCLIDEAN, 3, 10);

      // Test DOT_PRODUCT
      testKNNWithMetric(VectorSimilarityFunction.DOT_PRODUCT, 3, 20);
    });
  }

  private void testKNNWithMetric(final VectorSimilarityFunction metric, final int dimensions, final int ridOffset) {
    final Schema schema = database.getSchema();
    // Use unique property name for each metric to avoid schema conflicts
    final String propertyName = "embedding_" + metric.name().toLowerCase();
    final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", propertyName)
        .withIndexName("test_knn_" + metric + "_idx")
        .withDimensions(dimensions)
        .withSimilarity(metric)
        .create();

    // Create test vectors with better distinction for all metrics
    // v1: [1.0, 0.0, 0.0, ...] - first component only
    // v2: [0.0, 1.0, 0.0, ...] - second component only (orthogonal to v1)
    final float[] v1 = new float[dimensions];
    final float[] v2 = new float[dimensions];
    v1[0] = 1.0f;
    if (dimensions > 1) {
      v2[1] = 1.0f;
    } else {
      v2[0] = 0.5f; // For 1D, use different value
    }

    // Use unique RIDs for each metric test to avoid confusion
    final RID rid1 = new RID(database, 1, ridOffset);
    final RID rid2 = new RID(database, 1, ridOffset + 1);

    index.put(new Object[] { v1 }, new RID[] { rid1 });
    index.put(new Object[] { v2 }, new RID[] { rid2 });

    // KNN search should work with all metrics
    final List<LSMVectorIndexMutable.VectorSearchResult> results =
        index.knnSearch(v1, 1);

    assertThat(results).isNotEmpty();
    assertThat(results.getFirst().rids).contains(rid1);
  }

  @Test
  public void testHybridKNNSearchMutableAndCompacted()  {
    database.transaction(() -> {
      // Create index using Java API
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", "embedding")
          .withIndexName("test_hybrid_knn_idx")
          .withDimensions(2)
          .withSimilarity(VectorSimilarityFunction.COSINE)
          .create();

      // Insert vectors in mutable
      final RID rid1 = new RID(database, 1, 0);
      final RID rid2 = new RID(database, 1, 1);
      final float[] v1 = { 1.0f, 0.0f };
      final float[] v2 = { 0.9f, 0.1f };

      index.put(new Object[] { v1 }, new RID[] { rid1 });
      index.put(new Object[] { v2 }, new RID[] { rid2 });

      // Trigger compaction (moves to HNSW)
      index.scheduleCompaction();
      try {
        index.compact();
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }

      // Insert new vectors in fresh mutable
      final RID rid3 = new RID(database, 1, 2);
      final float[] v3 = { 0.95f, 0.05f };
      index.put(new Object[] { v3 }, new RID[] { rid3 });

      // KNN search should find results from both mutable and compacted
      final List<LSMVectorIndexMutable.VectorSearchResult> results =
          index.knnSearch(v1, 3);

      assertThat(results.size()).isGreaterThanOrEqualTo(2);
      // Should include vectors from both components
      final var allRids = results.stream()
          .flatMap(r -> r.rids.stream())
          .collect(Collectors.toSet());

      assertThat(allRids).contains(rid1, rid2, rid3);
    });
  }
}
