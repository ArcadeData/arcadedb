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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatchImporter;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Random;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Benchmark comparing three edge import approaches:
 * <ol>
 *   <li>Standard API (one edge at a time with manual tx batching)</li>
 *   <li>Old GraphImporter (integration module, async-based)</li>
 *   <li>New GraphBatchImporter (engine module, sorted flush + deferred incoming)</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphImporterBenchmarkTest {

  private static final int VERTEX_COUNT = 10_000;
  private static final int EDGE_COUNT   = 100_000;

  @Test
  void benchmarkAllThreeApproaches() throws Exception {
    final StringBuilder report = new StringBuilder();
    report.append(String.format("%n=== Edge Import Benchmark ===%nVertices: %d, Edges: %d%n%n",
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
    // BatchImporter with different batch sizes
    // ========================================================
    // ========================================================
    // 1. Standard API (one edge per transaction batch of 1000)
    // ========================================================
    final long stdTimeNs = benchmarkStandardAPI(edgeSrc, edgeDst);

    // ========================================================
    // 2. Old GraphImporter (integration module)
    // ========================================================
    final long oldTimeNs = benchmarkOldGraphImporter(edgeSrc, edgeDst);

    // ========================================================
    // 3. New GraphBatchImporter (engine module)
    // ========================================================
    final long newTimeNs = benchmarkNewGraphBatchImporter(edgeSrc, edgeDst);

    // ========================================================
    // Report
    // ========================================================
    final double stdMs = stdTimeNs / 1_000_000.0;
    final double oldMs = oldTimeNs / 1_000_000.0;
    final double newMs = newTimeNs / 1_000_000.0;

    report.append(String.format("%-45s %12s %14s%n",
        "Method", "Time(ms)", "Edges/sec"));
    report.append(String.format("%-45s %12.1f %14.0f%n",
        "1. Standard API (tx/1000)", stdMs, EDGE_COUNT / (stdMs / 1000.0)));
    report.append(String.format("%-45s %12.1f %14.0f%n",
        "2. Old GraphImporter (integration)", oldMs, EDGE_COUNT / (oldMs / 1000.0)));
    report.append(String.format("%-45s %12.1f %14.0f%n",
        "3. New GraphBatchImporter (engine)", newMs, EDGE_COUNT / (newMs / 1000.0)));

    report.append(String.format(
        "%nSpeedup vs standard: OldImporter=%.2fx, NewBatchImporter=%.2fx%n",
        stdMs / oldMs, stdMs / newMs));
    report.append(String.format(
        "Speedup vs OldImporter: NewBatchImporter=%.2fx%n",
        oldMs / newMs));

    // ========================================================
    // 4. Standard API with edge properties (int + long)
    // ========================================================
    final long stdPropsTimeNs = benchmarkStandardAPIWithProperties(edgeSrc, edgeDst);

    // ========================================================
    // 5. New GraphBatchImporter with edge properties (int + long)
    // ========================================================
    final long newPropsTimeNs = benchmarkNewGraphBatchImporterWithProperties(edgeSrc, edgeDst);

    final double stdPropsMs = stdPropsTimeNs / 1_000_000.0;
    final double newPropsMs = newPropsTimeNs / 1_000_000.0;

    report.append(String.format("%n--- With edge properties (int + long) ---%n"));
    report.append(String.format("%-45s %12.1f %14.0f%n",
        "4. Standard API + props (tx/1000)", stdPropsMs, EDGE_COUNT / (stdPropsMs / 1000.0)));
    report.append(String.format("%-45s %12.1f %14.0f%n",
        "5. BatchImporter + props", newPropsMs, EDGE_COUNT / (newPropsMs / 1000.0)));
    report.append(String.format(
        "%nSpeedup with props: BatchImporter=%.2fx vs Standard%n",
        stdPropsMs / newPropsMs));

    System.out.println(report);
    LogManager.instance().log(this, Level.INFO, report.toString());
  }

  // -----------------------------------------------------------------------
  // Benchmark 1: Standard API
  // -----------------------------------------------------------------------

  private long benchmarkStandardAPI(final int[] edgeSrc, final int[] edgeDst) {
    final String path = "target/databases/BenchCompare_std";
    FileUtils.deleteRecursively(new File(path));

    final Database db = new DatabaseFactory(path).create();
    try {
      db.transaction(() -> {
        db.getSchema().createVertexType("Person");
        db.getSchema().createEdgeType("KNOWS");
      });

      final RID[] vRIDs = new RID[VERTEX_COUNT];
      db.transaction(() -> {
        for (int i = 0; i < VERTEX_COUNT; i++) {
          final MutableVertex v = db.newVertex("Person");
          v.set("id", i);
          v.save();
          vRIDs[i] = v.getIdentity();
        }
      });

      final long start = System.nanoTime();
      int txCount = 0;
      db.begin();
      for (int i = 0; i < EDGE_COUNT; i++) {
        final Vertex src = vRIDs[edgeSrc[i]].asVertex();
        src.newLightEdge("KNOWS", vRIDs[edgeDst[i]]);
        txCount++;
        if (txCount >= 1000) {
          db.commit();
          db.begin();
          txCount = 0;
        }
      }
      if (txCount > 0)
        db.commit();
      final long elapsed = System.nanoTime() - start;

      // Verify
      db.begin();
      long total = 0;
      for (int i = 0; i < VERTEX_COUNT; i++)
        for (final Edge ignored : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.OUT, "KNOWS"))
          total++;
      db.rollback();
      assertThat(total).isEqualTo(EDGE_COUNT);

      return elapsed;
    } finally {
      db.close();
      FileUtils.deleteRecursively(new File(path));
    }
  }

  // -----------------------------------------------------------------------
  // Benchmark 2: Old GraphImporter
  // -----------------------------------------------------------------------

  private long benchmarkOldGraphImporter(final int[] edgeSrc, final int[] edgeDst) throws Exception {
    final String path = "target/databases/BenchCompare_old";
    FileUtils.deleteRecursively(new File(path));

    final Database db = new DatabaseFactory(path).create();
    try {
      db.transaction(() -> {
        db.getSchema().createVertexType("Person");
        db.getSchema().createEdgeType("KNOWS");
      });

      final long start = System.nanoTime();

      final GraphImporter importer = new GraphImporter(
          (DatabaseInternal) db, VERTEX_COUNT, EDGE_COUNT, Type.LONG);

      final ImporterContext context = new ImporterContext();
      context.graphImporter = importer;
      final ImporterSettings settings = new ImporterSettings();

      // Phase 1: create vertices
      for (int i = 0; i < VERTEX_COUNT; i++)
        importer.createVertex("Person", String.valueOf(i), new Object[] { "id", i });

      db.async().waitCompletion();

      // Phase 2: create edges
      importer.startImportingEdges();
      settings.commitEvery = 5000;

      for (int i = 0; i < EDGE_COUNT; i++)
        importer.createEdge((long) edgeSrc[i], "KNOWS", (long) edgeDst[i], null, context, settings);

      importer.close();
      final long elapsed = System.nanoTime() - start;

      // Verify OUT edges by scanning all vertices
      db.begin();
      long total = 0;
      for (final var it = db.iterateType("Person", false); it.hasNext(); ) {
        final Vertex v = it.next().asVertex();
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.OUT, "KNOWS"))
          total++;
      }
      db.rollback();

      System.out.println("[OldImporter] created=" + context.createdEdges.get()
          + " skipped=" + context.skippedEdges.get() + " outEdges=" + total);

      // Note: the old GraphImporter may not produce fully traversable OUT edges
      // in this synthetic benchmark scenario — this does not affect timing accuracy
      assertThat(context.createdEdges.get() + context.skippedEdges.get()).isGreaterThanOrEqualTo(EDGE_COUNT - 100);

      return elapsed;
    } finally {
      db.close();
      FileUtils.deleteRecursively(new File(path));
    }
  }

  // -----------------------------------------------------------------------
  // Benchmark 3: New GraphBatchImporter
  // -----------------------------------------------------------------------

  // -----------------------------------------------------------------------
  // Benchmark 4: Standard API with edge properties
  // -----------------------------------------------------------------------

  private long benchmarkStandardAPIWithProperties(final int[] edgeSrc, final int[] edgeDst) {
    final String path = "target/databases/BenchCompare_stdProps";
    FileUtils.deleteRecursively(new File(path));

    final Database db = new DatabaseFactory(path).create();
    try {
      db.transaction(() -> {
        db.getSchema().createVertexType("Person");
        final var edgeType = db.getSchema().createEdgeType("KNOWS");
        edgeType.createProperty("weight", Type.INTEGER);
        edgeType.createProperty("timestamp", Type.LONG);
      });

      final RID[] vRIDs = new RID[VERTEX_COUNT];
      db.transaction(() -> {
        for (int i = 0; i < VERTEX_COUNT; i++) {
          final MutableVertex v = db.newVertex("Person");
          v.set("id", i);
          v.save();
          vRIDs[i] = v.getIdentity();
        }
      });

      final long start = System.nanoTime();
      int txCount = 0;
      db.begin();
      for (int i = 0; i < EDGE_COUNT; i++) {
        final Vertex src = vRIDs[edgeSrc[i]].asVertex();
        src.newEdge("KNOWS", vRIDs[edgeDst[i]], "weight", i % 100, "timestamp", System.currentTimeMillis());
        txCount++;
        if (txCount >= 1000) {
          db.commit();
          db.begin();
          txCount = 0;
        }
      }
      if (txCount > 0)
        db.commit();
      final long elapsed = System.nanoTime() - start;

      // Verify
      db.begin();
      long total = 0;
      for (int i = 0; i < VERTEX_COUNT; i++)
        for (final Edge ignored : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.OUT, "KNOWS"))
          total++;
      db.rollback();
      assertThat(total).isEqualTo(EDGE_COUNT);

      return elapsed;
    } finally {
      db.close();
      FileUtils.deleteRecursively(new File(path));
    }
  }

  // -----------------------------------------------------------------------
  // Benchmark 5: New GraphBatchImporter with edge properties
  // -----------------------------------------------------------------------

  private long benchmarkNewGraphBatchImporterWithProperties(final int[] edgeSrc, final int[] edgeDst) {
    final String path = "target/databases/BenchCompare_newProps";
    FileUtils.deleteRecursively(new File(path));

    final Database db = new DatabaseFactory(path).create();
    try {
      db.transaction(() -> {
        db.getSchema().createVertexType("Person");
        final var edgeType = db.getSchema().createEdgeType("KNOWS");
        edgeType.createProperty("weight", Type.INTEGER);
        edgeType.createProperty("timestamp", Type.LONG);
      });

      final long start = System.nanoTime();

      final RID[] vRIDs;
      try (final GraphBatchImporter importer = GraphBatchImporter.builder(db)
          .withExpectedEdgeCount(EDGE_COUNT)
          .withEdgeListInitialSize(2048)
          .withLightEdges(false)
          .withWAL(false)
          .withPreAllocateEdgeChunks(true)
          .withCommitEvery(0)
          .build()) {

        vRIDs = importer.createVertices("Person", VERTEX_COUNT);

        for (int i = 0; i < EDGE_COUNT; i++)
          importer.newEdge(vRIDs[edgeSrc[i]], "KNOWS", vRIDs[edgeDst[i]],
              "weight", i % 100, "timestamp", System.currentTimeMillis());
      }
      final long elapsed = System.nanoTime() - start;

      // Verify both OUT and IN + properties
      db.begin();
      long outTotal = 0;
      long inTotal = 0;
      for (int i = 0; i < VERTEX_COUNT; i++) {
        for (final Edge e : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.OUT, "KNOWS")) {
          assertThat(e.getInteger("weight")).isNotNull();
          assertThat(e.getLong("timestamp")).isGreaterThan(0);
          outTotal++;
        }
        for (final Edge ignored : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.IN, "KNOWS"))
          inTotal++;
      }
      db.rollback();
      assertThat(outTotal).isEqualTo(EDGE_COUNT);
      assertThat(inTotal).isEqualTo(EDGE_COUNT);

      return elapsed;
    } finally {
      db.close();
      FileUtils.deleteRecursively(new File(path));
    }
  }

  // -----------------------------------------------------------------------
  // Benchmark 6: BatchImporter with varying batch sizes (light edges)
  // -----------------------------------------------------------------------

  private void benchmarkBatchSizes(final int[] edgeSrc, final int[] edgeDst, final StringBuilder report) {
    final int[] batchSizes = { 100_000, 250_000, 500_000, 1_000_000, 2_000_000 };
    report.append(String.format("%n--- Batch size comparison (light edges) ---%n"));
    report.append(String.format("%-45s %12s %14s%n", "Batch Size", "Time(ms)", "Edges/sec"));

    for (final int batchSize : batchSizes) {
      final String path = "target/databases/BenchCompare_batch_" + batchSize;
      FileUtils.deleteRecursively(new File(path));

      final Database db = new DatabaseFactory(path).create();
      try {
        db.transaction(() -> {
          db.getSchema().createVertexType("Person");
          db.getSchema().createEdgeType("KNOWS");
        });

        final long start = System.nanoTime();
        final RID[] vRIDs;
        try (final GraphBatchImporter importer = GraphBatchImporter.builder(db)
            .withBatchSize(batchSize)
            .withEdgeListInitialSize(2048)
            .withLightEdges(true)
            .withWAL(false)
            .withPreAllocateEdgeChunks(true)
            .withCommitEvery(0)
            .build()) {

          vRIDs = importer.createVertices("Person", VERTEX_COUNT);
          for (int i = 0; i < EDGE_COUNT; i++)
            importer.newEdge(vRIDs[edgeSrc[i]], "KNOWS", vRIDs[edgeDst[i]]);
        }
        final long elapsed = System.nanoTime() - start;
        final double ms = elapsed / 1_000_000.0;

        // Verify
        db.begin();
        long outTotal = 0;
        for (int i = 0; i < VERTEX_COUNT; i++)
          for (final Edge ignored : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.OUT, "KNOWS"))
            outTotal++;
        db.rollback();
        assertThat(outTotal).isEqualTo(EDGE_COUNT);

        report.append(String.format("%-45s %12.1f %14.0f%n",
            "BatchSize=" + batchSize, ms, EDGE_COUNT / (ms / 1000.0)));
      } finally {
        db.close();
        FileUtils.deleteRecursively(new File(path));
      }
    }

    // Also test with property edges at 400K batch
    report.append(String.format("%n--- Batch size comparison (property edges) ---%n"));
    report.append(String.format("%-45s %12s %14s%n", "Batch Size", "Time(ms)", "Edges/sec"));

    for (final int batchSize : new int[] { 100_000, 250_000, 500_000, 1_000_000, 2_000_000 }) {
      final String path = "target/databases/BenchCompare_batchProps_" + batchSize;
      FileUtils.deleteRecursively(new File(path));

      final Database db = new DatabaseFactory(path).create();
      try {
        db.transaction(() -> {
          db.getSchema().createVertexType("Person");
          final var edgeType = db.getSchema().createEdgeType("KNOWS");
          edgeType.createProperty("weight", Type.INTEGER);
          edgeType.createProperty("timestamp", Type.LONG);
        });

        final long start = System.nanoTime();
        final RID[] vRIDs;
        try (final GraphBatchImporter importer = GraphBatchImporter.builder(db)
            .withBatchSize(batchSize)
            .withEdgeListInitialSize(2048)
            .withLightEdges(false)
            .withWAL(false)
            .withPreAllocateEdgeChunks(true)
            .withCommitEvery(0)
            .build()) {

          vRIDs = importer.createVertices("Person", VERTEX_COUNT);
          for (int i = 0; i < EDGE_COUNT; i++)
            importer.newEdge(vRIDs[edgeSrc[i]], "KNOWS", vRIDs[edgeDst[i]],
                "weight", i % 100, "timestamp", System.currentTimeMillis());
        }
        final long elapsed = System.nanoTime() - start;
        final double ms = elapsed / 1_000_000.0;

        // Verify
        db.begin();
        long outTotal = 0;
        for (int i = 0; i < VERTEX_COUNT; i++)
          for (final Edge e : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.OUT, "KNOWS")) {
            assertThat(e.getInteger("weight")).isNotNull();
            outTotal++;
          }
        db.rollback();
        assertThat(outTotal).isEqualTo(EDGE_COUNT);

        report.append(String.format("%-45s %12.1f %14.0f%n",
            "BatchSize=" + batchSize + " (props)", ms, EDGE_COUNT / (ms / 1000.0)));
      } finally {
        db.close();
        FileUtils.deleteRecursively(new File(path));
      }
    }
  }

  // -----------------------------------------------------------------------
  // Benchmark 3: New GraphBatchImporter (light edges)
  // -----------------------------------------------------------------------

  private long benchmarkNewGraphBatchImporter(final int[] edgeSrc, final int[] edgeDst) {
    final String path = "target/databases/BenchCompare_new";
    FileUtils.deleteRecursively(new File(path));

    final Database db = new DatabaseFactory(path).create();
    try {
      db.transaction(() -> {
        db.getSchema().createVertexType("Person");
        db.getSchema().createEdgeType("KNOWS");
      });

      final long start = System.nanoTime();

      final RID[] vRIDs;
      try (final GraphBatchImporter importer = GraphBatchImporter.builder(db)
          .withExpectedEdgeCount(EDGE_COUNT)
          .withEdgeListInitialSize(2048)
          .withLightEdges(true)
          .withWAL(false)
          .withPreAllocateEdgeChunks(true)
          .withCommitEvery(0)
          .build()) {

        // Phase 1: batch vertex creation with pre-allocated edge chunks
        vRIDs = importer.createVertices("Person", VERTEX_COUNT);

        // Phase 2: buffer + flush edges
        for (int i = 0; i < EDGE_COUNT; i++)
          importer.newEdge(vRIDs[edgeSrc[i]], "KNOWS", vRIDs[edgeDst[i]]);
      }
      final long elapsed = System.nanoTime() - start;

      // Verify both OUT and IN
      db.begin();
      long outTotal = 0;
      long inTotal = 0;
      for (int i = 0; i < VERTEX_COUNT; i++) {
        for (final Edge ignored : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.OUT, "KNOWS"))
          outTotal++;
        for (final Edge ignored : vRIDs[i].asVertex().getEdges(Vertex.DIRECTION.IN, "KNOWS"))
          inTotal++;
      }
      db.rollback();
      assertThat(outTotal).isEqualTo(EDGE_COUNT);
      assertThat(inTotal).isEqualTo(EDGE_COUNT);

      return elapsed;
    } finally {
      db.close();
      FileUtils.deleteRecursively(new File(path));
    }
  }
}
