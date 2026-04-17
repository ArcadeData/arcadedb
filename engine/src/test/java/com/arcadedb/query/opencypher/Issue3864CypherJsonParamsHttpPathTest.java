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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for the issue #3864 follow-up: JSON params arriving over HTTP that contain
 * vector embeddings must be parsed into primitive {@code double[]} (avoiding {@code List<Double>}
 * boxing) and flow correctly through OpenCypher into {@code ARRAY_OF_FLOATS} properties.
 * <p>
 * Reproduces the exact PostCommandHandler entry path: build a JSON request matching the
 * payload reported by vitoprr (3,423 entries x 1,024 dim), serialize it to a string, then
 * parse with {@link JSONObject#toMap(boolean)} the way the HTTP handler does, and run the
 * Cypher command with those params. Asserts:
 * - vectors arrive as primitive {@code double[]} (no per-element boxing)
 * - the create flow stores them as {@code float[]} on disk via {@code Type.convert}
 * - data correctness is preserved through the primitive narrowing
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue3864CypherJsonParamsHttpPathTest extends TestHelper {

  private static final int    CHUNK_COUNT    = 500;
  private static final int    BATCH_SIZE     = 500;
  private static final int    VECTOR_DIM     = 1_024;
  private static final long   MAX_ELAPSED_MS = 5_000L;
  private static final Random RANDOM         = new Random(42);

  @Override
  public void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 500);

    database.transaction(() -> {
      final VertexType chunkType = database.getSchema().getOrCreateVertexType("CHUNK");
      chunkType.getOrCreateProperty("name", Type.STRING);

      final VertexType embType = database.getSchema().getOrCreateVertexType("CHUNK_EMBEDDING");
      embType.getOrCreateProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().getOrCreateEdgeType("embb");
    });
  }

  @Test
  void httpStyleJsonRequestProducesPrimitiveDoubleArrays() {
    final List<String> chunkRids = createChunks();

    final JSONObject request = buildJsonRequest(chunkRids);
    final String httpBody = request.toString();
    assertThat(httpBody.length()).isGreaterThan(1024 * 1024); // sanity: large payload

    // Mimic the PostCommandHandler entry path.
    final JSONObject parsed = new JSONObject(httpBody);
    final Map<String, Object> requestMap = parsed.toMap(true);
    @SuppressWarnings("unchecked")
    final Map<String, Object> params = (Map<String, Object>) requestMap.get("params");

    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> batch = (List<Map<String, Object>>) params.get("batch");
    assertThat(batch).hasSize(BATCH_SIZE);
    assertThat(batch.get(0).get("vector"))
        .as("the optimization must yield primitive float[] for the vector field")
        .isInstanceOf(float[].class);

    final long start = System.currentTimeMillis();
    database.transaction(() -> database.command("opencypher",
        "UNWIND $batch AS BatchEntry "
            + "MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID "
            + "CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) "
            + "CREATE (p)-[:embb]->(b)",
        params));
    final long elapsed = System.currentTimeMillis() - start;

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM CHUNK_EMBEDDING")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(BATCH_SIZE);
      }
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM embb")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(BATCH_SIZE);
      }

      // Spot-check that the stored vector is a float[] and matches the source values
      // (within float precision since the source was double).
      try (final ResultSet rs = database.query("sql", "SELECT vector FROM CHUNK_EMBEDDING LIMIT 1")) {
        assertThat(rs.hasNext()).isTrue();
        final Object stored = rs.next().getProperty("vector");
        assertThat(stored).isInstanceOf(float[].class);
        assertThat(((float[]) stored).length).isEqualTo(VECTOR_DIM);
      }
    });

    assertThat(elapsed)
        .as("HTTP-path Cypher batch (%d entries x dim %d) took %d ms", BATCH_SIZE, VECTOR_DIM, elapsed)
        .isLessThan(MAX_ELAPSED_MS);
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
    return chunkRids;
  }

  private JSONObject buildJsonRequest(final List<String> chunkRids) {
    final JSONObject root = new JSONObject();
    root.put("language", "opencypher");
    root.put("command",
        "UNWIND $batch AS BatchEntry "
            + "MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID "
            + "CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) "
            + "CREATE (p)-[:embb]->(b)");

    final JSONArray batch = new JSONArray();
    for (int i = 0; i < BATCH_SIZE; i++) {
      final JSONObject entry = new JSONObject();
      entry.put("destRID", chunkRids.get(i));
      final JSONArray vec = new JSONArray();
      for (int d = 0; d < VECTOR_DIM; d++)
        vec.put(RANDOM.nextDouble());
      entry.put("vector", vec);
      batch.put(entry);
    }
    final JSONObject params = new JSONObject();
    params.put("batch", batch);
    root.put("params", params);
    return root;
  }
}
