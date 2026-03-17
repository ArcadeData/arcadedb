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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Random;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests and benchmarks for {@link GraphBatch}.
 * Compares batch import performance against the standard edge creation API.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphBatchTest extends TestHelper {

  private static final int VERTEX_COUNT = 5_000;
  private static final int EDGE_COUNT   = 50_000;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("BatchPerson");
      database.getSchema().createEdgeType("BATCH_KNOWS");
    });
  }

  /**
   * Correctness test: verifies that edges are properly created and traversable (both OUT and IN).
   */
  @Test
  void correctnessSmall() {
    final int vertices = 100;
    final int edges = 500;

    // Create vertices
    final RID[] vertexRIDs = new RID[vertices];
    database.transaction(() -> {
      for (int i = 0; i < vertices; i++) {
        final MutableVertex v = database.newVertex("BatchPerson");
        v.set("id", i);
        v.save();
        vertexRIDs[i] = v.getIdentity();
      }
    });

    // Create edges using batch importer (with deferred incoming edges)
    final Random rng = new Random(42);
    try (final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(200)
        .withEdgeListInitialSize(256)
        .withLightEdges(true)
        .build()) {

      for (int i = 0; i < edges; i++) {
        final int src = rng.nextInt(vertices);
        int dst = rng.nextInt(vertices);
        while (dst == src)
          dst = rng.nextInt(vertices);
        importer.newEdge(vertexRIDs[src], "BATCH_KNOWS", vertexRIDs[dst]);
      }
    }

    // Verify both OUT and IN edges
    database.transaction(() -> {
      long totalOutEdges = 0;
      long totalInEdges = 0;
      for (int i = 0; i < vertices; i++) {
        final Vertex v = vertexRIDs[i].asVertex();
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.OUT, "BATCH_KNOWS"))
          totalOutEdges++;
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.IN, "BATCH_KNOWS"))
          totalInEdges++;
      }
      assertThat(totalOutEdges).isEqualTo(edges);
      assertThat(totalInEdges).isEqualTo(edges);
    });
  }

  /**
   * Correctness test for regular (non-light) edges with properties.
   */
  @Test
  void correctnessWithProperties() {
    final int vertices = 50;
    final int edges = 200;

    final RID[] vertexRIDs = new RID[vertices];
    database.transaction(() -> {
      for (int i = 0; i < vertices; i++) {
        final MutableVertex v = database.newVertex("BatchPerson");
        v.set("id", i);
        v.save();
        vertexRIDs[i] = v.getIdentity();
      }
    });

    try (final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(100)
        .withLightEdges(true) // still true, but edges with properties will be regular
        .build()) {

      for (int i = 0; i < edges; i++)
        importer.newEdge(vertexRIDs[i % vertices], "BATCH_KNOWS", vertexRIDs[(i + 1) % vertices],
            "weight", 0.5 + i, "label", "edge_" + i);
    }

    // Verify properties are stored
    database.transaction(() -> {
      long count = 0;
      for (int i = 0; i < vertices; i++) {
        final Vertex v = vertexRIDs[i].asVertex();
        for (final Edge e : v.getEdges(Vertex.DIRECTION.OUT, "BATCH_KNOWS")) {
          assertThat(e.get("weight")).isNotNull();
          assertThat(e.get("label")).isNotNull();
          count++;
        }
      }
      assertThat(count).isEqualTo(edges);
    });
  }

  /**
   * Correctness test for createVertex with pre-allocated edge segments.
   */
  @Test
  void createVertexPreAllocate() {
    final int vertices = 50;
    final int edges = 200;

    final RID[] vertexRIDs = new RID[vertices];

    try (final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(100)
        .withPreAllocateEdgeChunks(true)
        .build()) {

      // Use createVertex which pre-allocates edge chunks
      database.begin();
      for (int i = 0; i < vertices; i++) {
        final MutableVertex v = importer.createVertex("BatchPerson", "id", i);
        vertexRIDs[i] = v.getIdentity();
      }
      database.commit();

      // Verify edge chunks were pre-created
      database.transaction(() -> {
        for (int i = 0; i < vertices; i++) {
          final VertexInternal v = (VertexInternal) vertexRIDs[i].asVertex();
          assertThat(v.getOutEdgesHeadChunk()).isNotNull();
          assertThat(v.getInEdgesHeadChunk()).isNotNull();
        }
      });

      // Add edges
      for (int i = 0; i < edges; i++)
        importer.newEdge(vertexRIDs[i % vertices], "BATCH_KNOWS", vertexRIDs[(i + 1) % vertices]);
    }

    // Verify edges
    database.transaction(() -> {
      long totalOut = 0;
      long totalIn = 0;
      for (int i = 0; i < vertices; i++) {
        final Vertex v = vertexRIDs[i].asVertex();
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.OUT))
          totalOut++;
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.IN))
          totalIn++;
      }
      assertThat(totalOut).isEqualTo(edges);
      assertThat(totalIn).isEqualTo(edges);
    });
  }

  /**
   * Correctness test for deferred incoming edges across multiple flushes.
   */
  @Test
  void deferredIncomingAcrossFlushes() {
    final int vertices = 100;
    final int edges = 500;

    final RID[] vertexRIDs = new RID[vertices];
    database.transaction(() -> {
      for (int i = 0; i < vertices; i++) {
        final MutableVertex v = database.newVertex("BatchPerson");
        v.set("id", i);
        v.save();
        vertexRIDs[i] = v.getIdentity();
      }
    });

    // Use tiny batch size to force multiple flushes
    try (final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(50) // will flush 10 times for 500 edges
        .withEdgeListInitialSize(256)
        .build()) {

      for (int i = 0; i < edges; i++)
        importer.newEdge(vertexRIDs[i % vertices], "BATCH_KNOWS", vertexRIDs[(i + 1) % vertices]);

      // In sequential mode, incoming edges are connected inline (no deferral).
      // In parallel mode, they would be deferred.
      // Either way, after close() the edges must be correct.
    }

    // After close(), all incoming edges should be connected
    database.transaction(() -> {
      long totalOut = 0;
      long totalIn = 0;
      for (int i = 0; i < vertices; i++) {
        final Vertex v = vertexRIDs[i].asVertex();
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.OUT))
          totalOut++;
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.IN))
          totalIn++;
      }
      assertThat(totalOut).isEqualTo(edges);
      assertThat(totalIn).isEqualTo(edges);
    });
  }

  /**
   * Correctness test for batch vertex creation using createVertices().
   */
  @Test
  void createVerticesBatch() {
    final int vertices = 200;
    final int edges = 1000;

    final RID[] vertexRIDs;
    try (final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(500)
        .withPreAllocateEdgeChunks(true)
        .build()) {

      // Batch create vertices
      vertexRIDs = importer.createVertices("BatchPerson", vertices);
      assertThat(vertexRIDs).hasSize(vertices);

      // Verify all RIDs are valid
      for (int i = 0; i < vertices; i++)
        assertThat(vertexRIDs[i]).isNotNull();

      // Add edges
      for (int i = 0; i < edges; i++)
        importer.newEdge(vertexRIDs[i % vertices], "BATCH_KNOWS", vertexRIDs[(i + 1) % vertices]);
    }

    // Verify
    database.transaction(() -> {
      long totalOut = 0;
      long totalIn = 0;
      for (int i = 0; i < vertices; i++) {
        for (final Edge ignored : vertexRIDs[i].asVertex().getEdges(Vertex.DIRECTION.OUT))
          totalOut++;
        for (final Edge ignored : vertexRIDs[i].asVertex().getEdges(Vertex.DIRECTION.IN))
          totalIn++;
      }
      assertThat(totalOut).isEqualTo(edges);
      assertThat(totalIn).isEqualTo(edges);
    });
  }

  /**
   * Correctness test for createVertices with properties.
   */
  @Test
  void createVerticesBatchWithProperties() {
    final int vertices = 50;

    final Object[][] props = new Object[vertices][];
    for (int i = 0; i < vertices; i++)
      props[i] = new Object[] { "id", i, "name", "person_" + i };

    final RID[] vertexRIDs;
    try (final GraphBatch importer = GraphBatch.builder(database)
        .withPreAllocateEdgeChunks(true)
        .build()) {
      vertexRIDs = importer.createVertices("BatchPerson", props);
    }

    database.transaction(() -> {
      for (int i = 0; i < vertices; i++) {
        final Vertex v = vertexRIDs[i].asVertex();
        assertThat(v.getInteger("id")).isEqualTo(i);
        assertThat(v.getString("name")).isEqualTo("person_" + i);
      }
    });
  }

  /**
   * Correctness test for parallel flush mode.
   */
  @Test
  void parallelFlushCorrectness() {
    final int vertices = 200;
    final int edges = 2000;

    final RID[] vertexRIDs;
    try (final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(200) // forces multiple flushes
        .withEdgeListInitialSize(256)
        .withPreAllocateEdgeChunks(true)
        .withParallelFlush(true)
        .build()) {

      vertexRIDs = importer.createVertices("BatchPerson", vertices);

      final Random rng = new Random(99);
      for (int i = 0; i < edges; i++) {
        final int src = rng.nextInt(vertices);
        int dst = rng.nextInt(vertices);
        while (dst == src)
          dst = rng.nextInt(vertices);
        importer.newEdge(vertexRIDs[src], "BATCH_KNOWS", vertexRIDs[dst]);
      }
    }

    // Verify both directions
    database.transaction(() -> {
      long totalOut = 0;
      long totalIn = 0;
      for (int i = 0; i < vertices; i++) {
        final Vertex v = vertexRIDs[i].asVertex();
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.OUT, "BATCH_KNOWS"))
          totalOut++;
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.IN, "BATCH_KNOWS"))
          totalIn++;
      }
      assertThat(totalOut).isEqualTo(edges);
      assertThat(totalIn).isEqualTo(edges);
    });
  }

  /**
   * Benchmark: GraphBatch vs standard API.
   * Compares throughput of batch import vs one-edge-at-a-time insertion.
   */
  @Test
  void benchmarkBatchVsStandard() {
    final StringBuilder report = new StringBuilder();
    report.append(String.format("%n=== GraphBatch Benchmark ===%nVertices: %d, Edges: %d%n",
        VERTEX_COUNT, EDGE_COUNT));

    // --- Generate random edge list ---
    final Random rng = new Random(12345);
    final int[] edgeSrc = new int[EDGE_COUNT];
    final int[] edgeDst = new int[EDGE_COUNT];
    for (int i = 0; i < EDGE_COUNT; i++) {
      edgeSrc[i] = rng.nextInt(VERTEX_COUNT);
      edgeDst[i] = rng.nextInt(VERTEX_COUNT);
      while (edgeDst[i] == edgeSrc[i])
        edgeDst[i] = rng.nextInt(VERTEX_COUNT);
    }

    // ========================================================
    // Benchmark 1: Standard API (one edge per transaction batch of 1000)
    // ========================================================
    final String stdPath = "target/databases/GraphBatchBench_std";
    FileUtils.deleteRecursively(new File(stdPath));

    final long stdTimeNs;
    {
      final Database stdDb = new DatabaseFactory(stdPath).create();
      stdDb.transaction(() -> {
        stdDb.getSchema().createVertexType("BatchPerson");
        stdDb.getSchema().createEdgeType("BATCH_KNOWS");
      });

      final RID[] vRIDs = new RID[VERTEX_COUNT];
      stdDb.transaction(() -> {
        for (int i = 0; i < VERTEX_COUNT; i++) {
          final MutableVertex v = stdDb.newVertex("BatchPerson");
          v.set("id", i);
          v.save();
          vRIDs[i] = v.getIdentity();
        }
      });

      final long start = System.nanoTime();
      int txCount = 0;
      stdDb.begin();
      for (int i = 0; i < EDGE_COUNT; i++) {
        final Vertex src = vRIDs[edgeSrc[i]].asVertex();
        src.newLightEdge("BATCH_KNOWS", vRIDs[edgeDst[i]]);
        txCount++;
        if (txCount >= 1000) {
          stdDb.commit();
          stdDb.begin();
          txCount = 0;
        }
      }
      if (txCount > 0)
        stdDb.commit();
      stdTimeNs = System.nanoTime() - start;

      // Verify
      stdDb.begin();
      long stdTotal = 0;
      for (int i = 0; i < VERTEX_COUNT; i++)
        for (final Edge ignored : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.OUT, "BATCH_KNOWS"))
          stdTotal++;
      stdDb.rollback();
      assertThat(stdTotal).isEqualTo(EDGE_COUNT);

      stdDb.close();
    }
    FileUtils.deleteRecursively(new File(stdPath));

    // ========================================================
    // Benchmark 2: GraphBatch (with deferred incoming)
    // ========================================================
    final String batchPath = "target/databases/GraphBatchBench_batch";
    FileUtils.deleteRecursively(new File(batchPath));

    final long batchTimeNs;
    {
      final Database batchDb = new DatabaseFactory(batchPath).create();
      batchDb.transaction(() -> {
        batchDb.getSchema().createVertexType("BatchPerson");
        batchDb.getSchema().createEdgeType("BATCH_KNOWS");
      });

      final RID[] vRIDs = new RID[VERTEX_COUNT];
      batchDb.transaction(() -> {
        for (int i = 0; i < VERTEX_COUNT; i++) {
          final MutableVertex v = batchDb.newVertex("BatchPerson");
          v.set("id", i);
          v.save();
          vRIDs[i] = v.getIdentity();
        }
      });

      final long start = System.nanoTime();
      try (final GraphBatch importer = GraphBatch.builder(batchDb)
          .withBatchSize(10_000)
          .withEdgeListInitialSize(2048)
          .withLightEdges(true)
          .withWAL(false)
          .withCommitEvery(0)
          .build()) {

        for (int i = 0; i < EDGE_COUNT; i++)
          importer.newEdge(vRIDs[edgeSrc[i]], "BATCH_KNOWS", vRIDs[edgeDst[i]]);
      }
      batchTimeNs = System.nanoTime() - start;

      // Verify both OUT and IN
      batchDb.begin();
      long batchOutTotal = 0;
      long batchInTotal = 0;
      for (int i = 0; i < VERTEX_COUNT; i++) {
        for (final Edge ignored : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.OUT, "BATCH_KNOWS"))
          batchOutTotal++;
        for (final Edge ignored : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.IN, "BATCH_KNOWS"))
          batchInTotal++;
      }
      batchDb.rollback();
      assertThat(batchOutTotal).isEqualTo(EDGE_COUNT);
      assertThat(batchInTotal).isEqualTo(EDGE_COUNT);

      batchDb.close();
    }
    FileUtils.deleteRecursively(new File(batchPath));

    // ========================================================
    // Benchmark 3: GraphBatch with pre-allocated vertices
    // ========================================================
    final String preAllocPath = "target/databases/GraphBatchBench_prealloc";
    FileUtils.deleteRecursively(new File(preAllocPath));

    final long preAllocTimeNs;
    {
      final Database paDb = new DatabaseFactory(preAllocPath).create();
      paDb.transaction(() -> {
        paDb.getSchema().createVertexType("BatchPerson");
        paDb.getSchema().createEdgeType("BATCH_KNOWS");
      });

      final long start = System.nanoTime();
      final RID[] vRIDs;
      try (final GraphBatch importer = GraphBatch.builder(paDb)
          .withBatchSize(10_000)
          .withEdgeListInitialSize(2048)
          .withLightEdges(true)
          .withWAL(false)
          .withPreAllocateEdgeChunks(true)
          .withCommitEvery(0)
          .build()) {

        // Batch vertex creation with pre-allocated edge chunks
        vRIDs = importer.createVertices("BatchPerson", VERTEX_COUNT);

        for (int i = 0; i < EDGE_COUNT; i++)
          importer.newEdge(vRIDs[edgeSrc[i]], "BATCH_KNOWS", vRIDs[edgeDst[i]]);
      }
      preAllocTimeNs = System.nanoTime() - start;

      // Verify
      paDb.begin();
      long paTotal = 0;
      for (int i = 0; i < VERTEX_COUNT; i++)
        for (final Edge ignored : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.OUT, "BATCH_KNOWS"))
          paTotal++;
      paDb.rollback();
      assertThat(paTotal).isEqualTo(EDGE_COUNT);

      paDb.close();
    }
    FileUtils.deleteRecursively(new File(preAllocPath));

    // ========================================================
    // Benchmark 4: GraphBatch with parallel flush
    // ========================================================
    final String parallelPath = "target/databases/GraphBatchBench_parallel";
    FileUtils.deleteRecursively(new File(parallelPath));

    final long parallelTimeNs;
    {
      final Database parDb = new DatabaseFactory(parallelPath).create();
      parDb.transaction(() -> {
        parDb.getSchema().createVertexType("BatchPerson");
        parDb.getSchema().createEdgeType("BATCH_KNOWS");
      });

      final long start = System.nanoTime();
      final RID[] vRIDs;
      try (final GraphBatch importer = GraphBatch.builder(parDb)
          .withBatchSize(10_000)
          .withEdgeListInitialSize(2048)
          .withLightEdges(true)
          .withWAL(false)
          .withPreAllocateEdgeChunks(true)
          .withParallelFlush(true)
          .withCommitEvery(0)
          .build()) {

        vRIDs = importer.createVertices("BatchPerson", VERTEX_COUNT);

        for (int i = 0; i < EDGE_COUNT; i++)
          importer.newEdge(vRIDs[edgeSrc[i]], "BATCH_KNOWS", vRIDs[edgeDst[i]]);
      }
      parallelTimeNs = System.nanoTime() - start;

      // Verify both OUT and IN
      parDb.begin();
      long parOutTotal = 0;
      long parInTotal = 0;
      for (int i = 0; i < VERTEX_COUNT; i++) {
        for (final Edge ignored : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.OUT, "BATCH_KNOWS"))
          parOutTotal++;
        for (final Edge ignored : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.IN, "BATCH_KNOWS"))
          parInTotal++;
      }
      parDb.rollback();
      assertThat(parOutTotal).isEqualTo(EDGE_COUNT);
      assertThat(parInTotal).isEqualTo(EDGE_COUNT);

      parDb.close();
    }
    FileUtils.deleteRecursively(new File(parallelPath));

    // ========================================================
    // Report
    // ========================================================
    final double stdMs = stdTimeNs / 1_000_000.0;
    final double batchMs = batchTimeNs / 1_000_000.0;
    final double preAllocMs = preAllocTimeNs / 1_000_000.0;
    final double parallelMs = parallelTimeNs / 1_000_000.0;

    report.append(String.format(
        "%-40s %12s %14s%n%-40s %12.1f %14.0f%n%-40s %12.1f %14.0f%n%-40s %12.1f %14.0f%n%-40s %12.1f %14.0f%n",
        "Method", "Time(ms)", "Edges/sec",
        "Standard API (tx/1000)", stdMs, EDGE_COUNT / (stdMs / 1000.0),
        "BatchImporter (deferred IN)", batchMs, EDGE_COUNT / (batchMs / 1000.0),
        "BatchImporter + preAlloc vertices", preAllocMs, EDGE_COUNT / (preAllocMs / 1000.0),
        "BatchImporter + preAlloc + parallel", parallelMs, EDGE_COUNT / (parallelMs / 1000.0)));

    report.append(String.format(
        "%nSpeedup vs standard: Batch=%.2fx, preAlloc=%.2fx, parallel=%.2fx%n",
        stdMs / batchMs, stdMs / preAllocMs, stdMs / parallelMs));

    System.out.println(report);
    LogManager.instance().log(this, Level.INFO, report.toString());
  }
}
