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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * OpenCypher batch creation, match-by-id, full-scenario, and bound-anchor MERGE performance benchmarks.
 */
@Tag("benchmark")
class OpenCypherBatchBenchmark {

  @Nested
  class BatchCreation extends TestHelper {

    private static final int    VECTOR_DIMENSIONS = 1024;
    private static final int    BATCH_SIZE        = 3000;
    private static final Random RANDOM            = new Random(42);

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

    // Issue #3864: fresh-database batch insert of vector vertices must complete within budget.
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

    // Issue #3864: batch insert after HNSW graph build must not degrade dramatically (no per-vector O(log n) inserts).
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

      assertThat(populatedTime)
          .as("Populated batch (%dms) should not be >3x slower than fresh batch (%dms)",
              populatedTime, freshTime)
          .isLessThan(freshTime * 3 + 500);
    }

    // Issue #3864: control benchmark - batch insert without vector index must stay within budget.
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

  @Nested
  class BatchMatchById extends TestHelper {

    private static final int    CHUNK_COUNT = 10_000;
    private static final int    BATCH_SIZE  = 1000;
    private static final int    VECTOR_DIM  = 128;
    private static final Random RANDOM      = new Random(42);

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

    // Issue #3864: UNWIND + MATCH by dynamic ID(b) + CREATE must use RID lookup (O(batch)), not O(N*batch) full scan.
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
            """
            UNWIND $batch AS BatchEntry \
            MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID \
            CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) \
            CREATE (p)-[:embb]->(b)""",
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

      assertThat(elapsed)
          .as("UNWIND+MATCH(ID)+CREATE took %d ms - should be under 4s with RID lookup optimization", elapsed)
          .isLessThan(4_000);
    }

    // Issue #3864: each CHUNK_EMBEDDING produced by UNWIND+MATCH(ID)+CREATE must connect to the correct CHUNK.
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
            """
            UNWIND $batch AS BatchEntry \
            MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID \
            CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) \
            CREATE (p)-[:embb]->(b)""",
            params);
      });

      // Verify each edge connects CHUNK_EMBEDDING to the correct CHUNK.
      // Use elementId() (Neo4j-compatible string identifier) to compare with the RID strings captured
      // above; id() now returns the Neo4j-compatible Long encoding (issue #4183) which would not
      // match the RID-formatted strings.
      database.transaction(() -> {
        try (final ResultSet rs = database.query("opencypher",
            "MATCH (p:CHUNK_EMBEDDING)-[:embb]->(b:CHUNK) RETURN elementId(p) AS pid, elementId(b) AS bid")) {
          int count = 0;
          while (rs.hasNext()) {
            final var result = rs.next();
            assertThat((Object) result.getProperty("pid")).isNotNull();
            assertThat((Object) result.getProperty("bid")).isNotNull();
            // bid should be one of the original CHUNK RIDs
            assertThat(chunkRids).contains(result.<String>getProperty("bid"));
            count++;
          }
          assertThat(count).isEqualTo(chunkRids.size());
        }
      });
    }

    // Issue #3864: elementId() must drive the same RID lookup as ID() for UNWIND+MATCH dynamic lookups.
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
            """
            UNWIND $batch AS BatchEntry \
            MATCH (b:CHUNK) WHERE elementId(b) = BatchEntry.destRID \
            CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) \
            CREATE (p)-[:embb]->(b)""",
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

  @Nested
  class FullScenario extends TestHelper {

    private static final int    CHUNK_COUNT    = 3_423;
    private static final int    BATCH_SIZE     = 3_423;
    private static final int    VECTOR_DIM     = 1_024;
    private static final long   MAX_ELAPSED_MS = 6_000L;
    private static final Random RANDOM         = new Random(42);

    // Issue #3864: full end-to-end ingest scenario without a vector index must complete within budget.
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

    // Issue #3864: full end-to-end ingest scenario with a vector index must complete within budget.
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
          """
          UNWIND $batch AS BatchEntry \
          MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID \
          CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) \
          CREATE (p)-[:embb]->(b)""",
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

  @Nested
  class MergeBoundAnchor {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-4226-merge-bound-anchor-perf").create();
      database.getSchema().createVertexType("DOCUMENT");
      database.getSchema().createVertexType("CHUNK");
      database.getSchema().createEdgeType("in");
      database.getSchema().getType("CHUNK").createProperty("name", String.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "CHUNK", "name");
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    // Issue #4226: consecutive MERGE batches against a single parent must not scale with the parent's degree.
    @Test
    void mergePerBatchTimeStaysFlatAsParentDegreeGrows() {
      database.transaction(() -> database.command("opencypher", "CREATE (:DOCUMENT {name:'parent'})"));

      final int batches = 10;
      final int perBatch = 200;
      final long[] elapsedMs = new long[batches];

      for (int b = 0; b < batches; b++) {
        final int offset = b * perBatch;
        final long start = System.nanoTime();
        database.transaction(() -> {
          for (int i = 0; i < perBatch; i++) {
            final String name = "chunk_" + (offset + i);
            database.command("opencypher",
                "MATCH (p:DOCUMENT {name:'parent'}) "
                    + "MERGE (n:CHUNK {name:'" + name + "'})-[:in]->(p)");
          }
        });
        elapsedMs[b] = (System.nanoTime() - start) / 1_000_000L;
      }

      final ResultSet rs = database.query("opencypher",
          "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'parent'}) RETURN count(n) AS cnt");
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo((long) batches * perBatch);

      final long warm = Math.max(elapsedMs[1], 30L); // ignore first batch (JIT warmup)
      final long last = elapsedMs[batches - 1];
      assertThat(last)
          .as("last-batch / warm-batch ratio must not grow linearly with parent degree (perBatchMs: " + java.util.Arrays.toString(elapsedMs) + ")")
          .isLessThan(warm * 2);
    }

    // Issue #4226: same flatness invariant when the bound anchor sits at the start of the MERGE pattern.
    @Test
    void mergePerBatchTimeStaysFlatWhenAnchorAtStart() {
      database.transaction(() -> database.command("opencypher", "CREATE (:DOCUMENT {name:'parent'})"));

      final int batches = 10;
      final int perBatch = 200;
      final long[] elapsedMs = new long[batches];

      for (int b = 0; b < batches; b++) {
        final int offset = b * perBatch;
        final long start = System.nanoTime();
        database.transaction(() -> {
          for (int i = 0; i < perBatch; i++) {
            final String name = "chunk_out_" + (offset + i);
            database.command("opencypher",
                "MATCH (p:DOCUMENT {name:'parent'}) "
                    + "MERGE (p)-[:in]->(n:CHUNK {name:'" + name + "'})");
          }
        });
        elapsedMs[b] = (System.nanoTime() - start) / 1_000_000L;
      }

      final ResultSet rs = database.query("opencypher",
          "MATCH (:DOCUMENT {name:'parent'})-[:in]->(n:CHUNK) RETURN count(n) AS cnt");
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo((long) batches * perBatch);

      final long warm = Math.max(elapsedMs[1], 30L);
      final long last = elapsedMs[batches - 1];
      assertThat(last)
          .as("last-batch / warm-batch ratio must not grow linearly with parent's outgoing degree (perBatchMs: " + java.util.Arrays.toString(elapsedMs) + ")")
          .isLessThan(warm * 2);
    }

    // Issue #4226: UNWIND + MATCH parent BY ID + MERGE must scale with batch rows, not with parent degree.
    @Test
    void unwindMatchMergeBatchScalesLinearlyWithRowsNotParentDegree() {
      final String[] parentRid = new String[1];
      database.transaction(() ->
          parentRid[0] = database.command("opencypher", "CREATE (p:DOCUMENT {name:'p'}) RETURN p AS p")
              .next().<com.arcadedb.graph.Vertex>getProperty("p").getIdentity().toString());

      final int batches = 8;
      final int perBatch = 250;
      final long[] elapsedMs = new long[batches];

      for (int b = 0; b < batches; b++) {
        final List<Map<String, Object>> batch = new ArrayList<>(perBatch);
        for (int i = 0; i < perBatch; i++) {
          final Map<String, Object> entry = new HashMap<>();
          entry.put("_parent_rid", parentRid[0]);
          entry.put("subtype", "CHUNK");
          entry.put("name", "chunk_" + (b * perBatch + i));
          entry.put("text", "body for chunk " + (b * perBatch + i));
          entry.put("index", b * perBatch + i);
          batch.add(entry);
        }

        final Map<String, Object> params = new HashMap<>();
        params.put("batch", batch);

        final long start = System.nanoTime();
        database.transaction(() -> {
          try (final ResultSet ignored = database.command("opencypher",
              "UNWIND $batch AS BatchEntry "
                  + "MATCH (parent) WHERE ID(parent) = BatchEntry._parent_rid "
                  + "MERGE (n:CHUNK {subtype: BatchEntry.subtype, name: BatchEntry.name, text: BatchEntry.text, index: BatchEntry.index})-[:`in`]->(parent) "
                  + "RETURN ID(n) AS id", params)) {
            while (ignored.hasNext()) ignored.next();
          }
        });
        elapsedMs[b] = (System.nanoTime() - start) / 1_000_000L;
      }

      final ResultSet rs = database.query("opencypher",
          "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'p'}) RETURN count(n) AS cnt");
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo((long) batches * perBatch);

      final long warm = Math.max(elapsedMs[1], 30L);
      final long last = elapsedMs[batches - 1];
      assertThat(last)
          .as("UNWIND+MATCH+MERGE batch time must not grow linearly with parent degree (perBatchMs: " + java.util.Arrays.toString(elapsedMs) + ")")
          .isLessThan(warm * 2);
    }
  }
}
