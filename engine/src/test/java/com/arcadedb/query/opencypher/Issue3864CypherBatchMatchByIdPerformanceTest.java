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
 * Regression test for GitHub issue #3864: Cypher UNWIND+MATCH by ID is slow.
 * <p>
 * When executing:
 * <pre>
 * UNWIND $batch AS BatchEntry
 * MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID
 * CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector})
 * CREATE (p)-[:embb]->(b)
 * </pre>
 * The MATCH should use a direct RID lookup (O(1)) per UNWIND row,
 * not a full type scan (O(N)) per row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue3864CypherBatchMatchByIdPerformanceTest extends TestHelper {

  private static final int CHUNK_COUNT = 10_000;
  private static final int BATCH_SIZE = 1000;
  private static final int VECTOR_DIM = 128;
  private static final Random RANDOM = new Random(42);

  @Override
  public void beginTest() {
    database.transaction(() -> {
      final VertexType chunkType = database.getSchema().getOrCreateVertexType("CHUNK");
      chunkType.getOrCreateProperty("name", Type.STRING);

      final VertexType embType = database.getSchema().getOrCreateVertexType("CHUNK_EMBEDDING");
      embType.getOrCreateProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().getOrCreateEdgeType("embb");
    });

    // Create CHUNK vertices
    database.transaction(() -> {
      for (int i = 0; i < CHUNK_COUNT; i++) {
        final MutableVertex v = database.newVertex("CHUNK");
        v.set("name", "chunk_" + i);
        v.save();
      }
    });
  }

  /**
   * Core test: UNWIND + MATCH by ID(b) = dynamic expression + CREATE must use direct RID lookup.
   * With a full type scan this would be O(CHUNK_COUNT * BATCH_SIZE), with RID lookup it's O(BATCH_SIZE).
   */
  @Test
  void unwindMatchByDynamicIdShouldUseLookup() {
    // Collect RIDs of existing CHUNK vertices to use in the batch
    final List<String> chunkRids = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM CHUNK LIMIT " + BATCH_SIZE)) {
        while (rs.hasNext())
          chunkRids.add(rs.next().getProperty("rid").toString());
      }
    });
    assertThat(chunkRids).hasSize(BATCH_SIZE);

    // Build batch entries
    final List<Map<String, Object>> batch = new ArrayList<>(BATCH_SIZE);
    for (final String rid : chunkRids) {
      final Map<String, Object> entry = new HashMap<>();
      entry.put("destRID", rid);
      final List<Double> vector = new ArrayList<>(VECTOR_DIM);
      for (int d = 0; d < VECTOR_DIM; d++)
        vector.add(RANDOM.nextDouble());
      entry.put("vector", vector);
      batch.add(entry);
    }

    final Map<String, Object> params = new HashMap<>();
    params.put("batch", batch);

    // Execute the UNWIND + MATCH + CREATE query
    final long start = System.currentTimeMillis();
    database.transaction(() -> {
      database.command("opencypher",
          "UNWIND $batch AS BatchEntry "
              + "MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID "
              + "CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) "
              + "CREATE (p)-[:embb]->(b)",
          params);
    });
    final long elapsed = System.currentTimeMillis() - start;

    // Verify correctness: BATCH_SIZE embeddings and BATCH_SIZE edges created
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

    // Performance: with direct RID lookup this should complete well under 4 seconds.
    // With a full type scan over 5000 vertices * 500 batch entries, it would be much slower.
    assertThat(elapsed)
        .as("UNWIND+MATCH(ID)+CREATE took %d ms - should be under 4s with RID lookup optimization", elapsed)
        .isLessThan(4_000);
  }

  /**
   * Verify correctness: each CHUNK_EMBEDDING should be connected to the correct CHUNK vertex.
   */
  @Test
  void unwindMatchByDynamicIdProducesCorrectEdges() {
    // Create a small batch with known RIDs
    final List<String> chunkRids = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM CHUNK LIMIT 10")) {
        while (rs.hasNext())
          chunkRids.add(rs.next().getProperty("rid").toString());
      }
    });

    final List<Map<String, Object>> batch = new ArrayList<>();
    for (final String rid : chunkRids) {
      final Map<String, Object> entry = new HashMap<>();
      entry.put("destRID", rid);
      final List<Double> vector = new ArrayList<>(3);
      vector.add(1.0);
      vector.add(2.0);
      vector.add(3.0);
      entry.put("vector", vector);
      batch.add(entry);
    }

    final Map<String, Object> params = new HashMap<>();
    params.put("batch", batch);

    database.transaction(() -> {
      database.command("opencypher",
          "UNWIND $batch AS BatchEntry "
              + "MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID "
              + "CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) "
              + "CREATE (p)-[:embb]->(b)",
          params);
    });

    // Verify each edge connects CHUNK_EMBEDDING to the correct CHUNK
    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (p:CHUNK_EMBEDDING)-[:embb]->(b:CHUNK) RETURN ID(p) AS pid, ID(b) AS bid")) {
        int count = 0;
        while (rs.hasNext()) {
          final var result = rs.next();
          assertThat((Object) result.getProperty("pid")).isNotNull();
          assertThat((Object) result.getProperty("bid")).isNotNull();
          // bid should be one of the original CHUNK RIDs
          assertThat(chunkRids).contains(result.getProperty("bid").toString());
          count++;
        }
        assertThat(count).isEqualTo(chunkRids.size());
      }
    });
  }

  /**
   * Verify that elementId() works the same as ID() for dynamic UNWIND+MATCH lookups.
   */
  @Test
  void unwindMatchByElementIdShouldUseLookup() {
    final List<String> chunkRids = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM CHUNK LIMIT 10")) {
        while (rs.hasNext())
          chunkRids.add(rs.next().getProperty("rid").toString());
      }
    });

    final List<Map<String, Object>> batch = new ArrayList<>();
    for (final String rid : chunkRids) {
      final Map<String, Object> entry = new HashMap<>();
      entry.put("destRID", rid);
      entry.put("vector", List.of(1.0, 2.0, 3.0));
      batch.add(entry);
    }

    database.transaction(() -> {
      database.command("opencypher",
          "UNWIND $batch AS BatchEntry "
              + "MATCH (b:CHUNK) WHERE elementId(b) = BatchEntry.destRID "
              + "CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) "
              + "CREATE (p)-[:embb]->(b)",
          Map.of("batch", batch));
    });

    // Verify correctness: same number of embeddings and edges as batch entries
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM CHUNK_EMBEDDING")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(chunkRids.size());
      }
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM embb")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(chunkRids.size());
      }
    });
  }
}
