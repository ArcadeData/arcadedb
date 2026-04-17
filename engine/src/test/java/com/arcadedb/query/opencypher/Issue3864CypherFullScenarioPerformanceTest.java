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
import com.arcadedb.graph.MutableVertex;
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
 * End-to-end regression test for GitHub issue #3864 reproducing the exact scenario
 * reported by vitoprr in comment 4258049953: a Cypher batch ingest of vector embeddings
 * tied to existing CHUNK vertices via an edge.
 * <p>
 * Pattern under test:
 * <pre>
 * UNWIND $batch AS BatchEntry
 * MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID
 * CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector})
 * CREATE (p)-[:embb]-&gt;(b)
 * </pre>
 * Scale: 3,423 entries with 1,024-dim vectors (matches the user's payload).
 * <p>
 * Two variants are exercised: with and without a vector index on
 * {@code CHUNK_EMBEDDING.vector}, since the user's database schema is unknown.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue3864CypherFullScenarioPerformanceTest extends TestHelper {

  private static final int    CHUNK_COUNT      = 3_423;
  private static final int    BATCH_SIZE       = 3_423;
  private static final int    VECTOR_DIM       = 1_024;
  private static final long   MAX_ELAPSED_MS   = 6_000L;
  private static final Random RANDOM           = new Random(42);

  @Test
  void fullScenarioWithoutVectorIndex() {
    setupSchema(false);
    final List<String> chunkRids = createChunks();
    final List<Map<String, Object>> batch = buildBatch(chunkRids);

    final long elapsed = runIngestBatch(batch);
    assertCounts();

    assertThat(elapsed)
        .as("Full scenario (no vector index) on %d entries x dim %d took %d ms",
            BATCH_SIZE, VECTOR_DIM, elapsed)
        .isLessThan(MAX_ELAPSED_MS);
  }

  @Test
  void fullScenarioWithVectorIndex() {
    setupSchema(true);
    final List<String> chunkRids = createChunks();
    final List<Map<String, Object>> batch = buildBatch(chunkRids);

    final long elapsed = runIngestBatch(batch);
    assertCounts();

    assertThat(elapsed)
        .as("Full scenario (with vector index) on %d entries x dim %d took %d ms",
            BATCH_SIZE, VECTOR_DIM, elapsed)
        .isLessThan(MAX_ELAPSED_MS);
  }

  private void setupSchema(final boolean withVectorIndex) {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 500);
    database.transaction(() -> {
      final VertexType chunkType = database.getSchema().getOrCreateVertexType("CHUNK");
      chunkType.getOrCreateProperty("name", Type.STRING);

      final VertexType embType = database.getSchema().getOrCreateVertexType("CHUNK_EMBEDDING");
      embType.getOrCreateProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().getOrCreateEdgeType("embb");

      if (withVectorIndex) {
        database.getSchema().buildTypeIndex("CHUNK_EMBEDDING", new String[] { "vector" })
            .withLSMVectorType()
            .withDimensions(VECTOR_DIM)
            .withSimilarity("COSINE")
            .create();
      }
    });
  }

  private List<String> createChunks() {
    final List<String> chunkRids = new ArrayList<>(CHUNK_COUNT);
    database.transaction(() -> {
      for (int i = 0; i < CHUNK_COUNT; i++) {
        final MutableVertex v = database.newVertex("CHUNK");
        v.set("name", "chunk_" + i);
        v.save();
        chunkRids.add(v.getIdentity().toString());
      }
    });
    assertThat(chunkRids).hasSize(CHUNK_COUNT);
    return chunkRids;
  }

  private List<Map<String, Object>> buildBatch(final List<String> chunkRids) {
    final List<Map<String, Object>> batch = new ArrayList<>(BATCH_SIZE);
    for (int i = 0; i < BATCH_SIZE; i++) {
      final Map<String, Object> entry = new HashMap<>(2);
      entry.put("destRID", chunkRids.get(i));
      final List<Double> vector = new ArrayList<>(VECTOR_DIM);
      for (int d = 0; d < VECTOR_DIM; d++)
        vector.add(RANDOM.nextDouble());
      entry.put("vector", vector);
      batch.add(entry);
    }
    return batch;
  }

  private long runIngestBatch(final List<Map<String, Object>> batch) {
    final Map<String, Object> params = new HashMap<>();
    params.put("batch", batch);

    final long start = System.currentTimeMillis();
    database.transaction(() -> database.command("opencypher",
        "UNWIND $batch AS BatchEntry "
            + "MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID "
            + "CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) "
            + "CREATE (p)-[:embb]->(b)",
        params));
    return System.currentTimeMillis() - start;
  }

  private void assertCounts() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM CHUNK_EMBEDDING")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(BATCH_SIZE);
      }
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM embb")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(BATCH_SIZE);
      }
    });
  }
}
