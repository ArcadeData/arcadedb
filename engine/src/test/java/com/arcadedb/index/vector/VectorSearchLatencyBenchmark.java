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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Benchmark reproducing the tae898 benchmark scenario from GitHub discussion #3674.
 * Uses 384-dim vectors (matching all-MiniLM-L6-v2), k=50, ef_search=100.
 * Tests both the direct Java API and the SQL vectorNeighbors() path.
 * <p>
 * Configurations tested:
 * 1. Default (no quantization, no inline vectors) — matches what the benchmark uses
 * 2. storeVectorsInGraph=true — vectors in graph file for mmap'd access
 * 3. PRODUCT quantization with FusedPQ — approximate scoring during traversal
 * <p>
 * Data and queries come from one set of clusters ({@link ClusteredVectorDataset}), and every latency line is printed
 * next to the recall@K it was achieved at against the exact neighbours: a change that trades recall for latency must
 * not read as a pure win (#11711). {@code vector.bench.clusters} and {@code vector.bench.clusterSpread} (per-dimension
 * Gaussian noise, default {@code 1 / sqrt(dimensions)}) shape the clusters.
 * <p>
 * Usage ({@code -DexcludedGroups=} because the class is tagged {@code benchmark}, which the build excludes and
 * {@code -Dtest} does not bring back; {@code -am} so it runs against the working tree; {@code -DenableAssertions=false}
 * because Surefire's {@code -ea} turns on jvector's internal asserts on the distance hot path):
 *   mvn -pl engine -am test -Dtest=VectorSearchLatencyBenchmark -DexcludedGroups= \
 *     -Dsurefire.failIfNoSpecifiedTests=false -DenableAssertions=false -Dvector.bench.numVectors=500000
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class VectorSearchLatencyBenchmark {
  private static final String DB_PATH = "target/test-databases/VectorSearchLatencyBenchmark";

  // Match tae898 benchmark: 384-dim (all-MiniLM-L6-v2), k=50, ef_search=100
  private static final int NUM_VECTORS = Integer.getInteger("vector.bench.numVectors", 500_000);
  private static final int DIMENSIONS = Integer.getInteger("vector.bench.dimensions", 384);
  private static final int NUM_QUERIES = Integer.getInteger("vector.bench.numQueries", 100);
  private static final int K = Integer.getInteger("vector.bench.k", 50);
  private static final int EF_SEARCH = Integer.getInteger("vector.bench.efSearch", 100);
  private static final int BATCH_SIZE = Integer.getInteger("vector.bench.batchSize", 10_000);
  private static final int NUM_CLUSTERS = Integer.getInteger("vector.bench.clusters", Math.max(50, NUM_VECTORS / 500));
  private static final float CLUSTER_SPREAD = Float.parseFloat(
      System.getProperty("vector.bench.clusterSpread", Float.toString(ClusteredVectorDataset.defaultSpread(DIMENSIONS))));
  private static final int WARMUP_QUERIES = 20;
  private static final long SEED = 42L;

  @Test
  void runLatencyBenchmark() {
    System.out.println("=== VECTOR SEARCH LATENCY BENCHMARK (tae898 scenario) ===");
    System.out.printf("Vectors: %,d | Dimensions: %d | K: %d | efSearch: %d | Queries: %d%n",
        NUM_VECTORS, DIMENSIONS, K, EF_SEARCH, NUM_QUERIES);
    System.out.println();

    System.out.printf("Generating clustered vectors (%d clusters, spread %.4f per dimension)...", NUM_CLUSTERS, CLUSTER_SPREAD);
    final ClusteredVectorDataset dataset = ClusteredVectorDataset.generate(NUM_VECTORS, NUM_QUERIES, DIMENSIONS, NUM_CLUSTERS,
        CLUSTER_SPREAD, SEED);
    final float[][] dataVectors = dataset.data;
    final float[][] queryVectors = dataset.queries;
    System.out.println(" done.");

    System.out.print("Computing exact ground truth...");
    final int[][] groundTruth = dataset.exactNeighbors(K);
    System.out.println(" done.");

    // Config 1: Default (matches what the tae898 benchmark uses)
    System.out.println();
    System.out.println("=== Config 1: DEFAULT (no quantization, storeVectorsInGraph=false) ===");
    FileUtils.deleteRecursively(new File(DB_PATH));
    runConfig("DEFAULT", dataVectors, queryVectors, groundTruth, "NONE", false);

    // Config 2: INT8 quantization (reads from index pages instead of documents)
    System.out.println();
    System.out.println("=== Config 2: INT8 quantization ===");
    FileUtils.deleteRecursively(new File(DB_PATH));
    runConfig("INT8", dataVectors, queryVectors, groundTruth, "INT8", false);

    // Config 3: PRODUCT quantization (now with FusedPQ disabled, uses exact scoring)
    System.out.println();
    System.out.println("=== Config 3: PRODUCT quantization ===");
    FileUtils.deleteRecursively(new File(DB_PATH));
    runConfig("PQ", dataVectors, queryVectors, groundTruth, "PRODUCT", true);
  }

  private void runConfig(final String label, final float[][] dataVectors, final float[][] queryVectors,
      final int[][] groundTruth, final String quantization, final boolean storeVectorsInGraph) {

    GlobalConfiguration.PROFILE.setValue("high-performance");
    final Runtime rt = Runtime.getRuntime();
    rt.gc();
    final long memBefore = rt.totalMemory() - rt.freeMemory();

    // Phase 1: Ingest
    System.out.printf("[%s] Phase 1: Ingesting %,d vectors...%n", label, NUM_VECTORS);
    final long ingestStart = System.nanoTime();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("VectorData");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);

          final StringBuilder metadata = new StringBuilder();
          metadata.append(String.format("\"dimensions\": %d, \"similarity\": \"COSINE\"", DIMENSIONS));
          if (!"NONE".equals(quantization))
            metadata.append(String.format(", \"quantization\": \"%s\"", quantization));
          if (storeVectorsInGraph)
            metadata.append(", \"storeVectorsInGraph\": true");

          db.command("sql", String.format(
              "CREATE INDEX ON VectorData (vector) LSM_VECTOR METADATA { %s }", metadata));
        });

        int inserted = 0;
        db.begin();
        for (int i = 0; i < NUM_VECTORS; i++) {
          db.newDocument("VectorData").set("id", i).set("vector", dataVectors[i]).save();
          inserted++;
          if (inserted % BATCH_SIZE == 0) {
            db.commit();
            final double elapsed = (System.nanoTime() - ingestStart) / 1e9;
            System.out.printf("\r[%s]   Inserted %,d / %,d (%.0f v/s, %.1fs)  ",
                label, inserted, NUM_VECTORS, inserted / elapsed, elapsed);
            db.begin();
          }
        }
        if (db.isTransactionActive())
          db.commit();
      }

      final double ingestSec = (System.nanoTime() - ingestStart) / 1e9;
      System.out.printf("%n[%s] Ingestion done: %.1fs (%,.0f vectors/sec)%n",
          label, ingestSec, NUM_VECTORS / ingestSec);

      // Phase 2: Close + Reopen + Graph build (simulates database restart)
      System.out.printf("[%s] Phase 2: Reopen + graph build...%n", label);
      final long reopenStart = System.nanoTime();

      try (final Database db = factory.open()) {
        final double reopenSec = (System.nanoTime() - reopenStart) / 1e9;
        System.out.printf("[%s]   Reopen: %.2fs%n", label, reopenSec);

        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("VectorData")
            .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];

        // First query triggers graph build
        final long buildStart = System.nanoTime();
        index.findNeighborsFromVector(queryVectors[0], K, EF_SEARCH);
        final double buildSec = (System.nanoTime() - buildStart) / 1e9;

        rt.gc();
        final long memAfter = rt.totalMemory() - rt.freeMemory();
        System.out.printf("[%s]   Graph build (first query): %.1fs | Memory delta: %d MB%n",
            label, buildSec, (memAfter - memBefore) / (1024 * 1024));

        // Phase 3: Direct API search
        System.out.printf("[%s] Phase 3: Direct API search (k=%d, efSearch=%d)...%n", label, K, EF_SEARCH);
        benchmarkDirectSearch(label, db, index, queryVectors, groundTruth);

        // Phase 4: SQL vectorNeighbors() search (matches what the Python benchmark does)
        System.out.printf("[%s] Phase 4: SQL vectorNeighbors() search...%n", label);
        benchmarkSQLSearch(label, db, queryVectors, groundTruth);

        // Phase 5: Print stats
        final Map<String, Long> stats = index.getStats();
        System.out.printf("[%s] Vector fetch stats: graph=%d doc=%d quantized=%d%n",
            label,
            stats.getOrDefault("vectorFetchFromGraph", 0L),
            stats.getOrDefault("vectorFetchFromDocuments", 0L),
            stats.getOrDefault("vectorFetchFromQuantized", 0L));
        System.out.printf("[%s] Search ops: %d, total search time: %d ms%n",
            label,
            stats.getOrDefault("searchOperations", 0L),
            stats.getOrDefault("totalSearchLatencyMs", 0L));
      }
    }
  }

  private void benchmarkDirectSearch(final String label, final Database db, final LSMVectorIndex index,
      final float[][] queryVectors, final int[][] groundTruth) {
    // Warmup
    for (int i = 0; i < WARMUP_QUERIES; i++)
      index.findNeighborsFromVector(queryVectors[i % queryVectors.length], K, EF_SEARCH);

    // Reset metrics after warmup
    index.getStats(); // just read current values

    final long[] latenciesNs = new long[NUM_QUERIES];
    final RID[][] found = new RID[NUM_QUERIES][];
    int totalResults = 0;

    for (int q = 0; q < NUM_QUERIES; q++) {
      final long qStart = System.nanoTime();
      final List<Pair<RID, Float>> results = index.findNeighborsFromVector(queryVectors[q], K, EF_SEARCH);
      latenciesNs[q] = System.nanoTime() - qStart;
      totalResults += results.size();

      found[q] = new RID[results.size()];
      for (int i = 0; i < found[q].length; i++)
        found[q][i] = results.get(i).getFirst();
    }

    printLatencyStats(label + " API", latenciesNs, totalResults, meanRecall(db, found, groundTruth));
  }

  private void benchmarkSQLSearch(final String label, final Database db, final float[][] queryVectors,
      final int[][] groundTruth) {
    // Warmup
    for (int i = 0; i < WARMUP_QUERIES; i++) {
      final String vectorStr = vectorToString(queryVectors[i % queryVectors.length]);
      try (final ResultSet rs = db.query("sql",
          String.format("SELECT vectorNeighbors('VectorData[vector]', %s, %d, %d) as res", vectorStr, K, EF_SEARCH))) {
        while (rs.hasNext()) rs.next();
      }
    }

    final long[] latenciesNs = new long[NUM_QUERIES];
    final List<?>[] rows = new List<?>[NUM_QUERIES];
    int totalResults = 0;

    for (int q = 0; q < NUM_QUERIES; q++) {
      final String vectorStr = vectorToString(queryVectors[q]);
      final long qStart = System.nanoTime();
      try (final ResultSet rs = db.query("sql",
          String.format("SELECT vectorNeighbors('VectorData[vector]', %s, %d, %d) as res", vectorStr, K, EF_SEARCH))) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final Object res = row.getProperty("res");
          if (res instanceof List<?> list) {
            totalResults += list.size();
            rows[q] = list;
          }
        }
      }
      latenciesNs[q] = System.nanoTime() - qStart;
    }

    // Resolved outside the timed loop: each neighbour row carries the RID of the record it found
    final RID[][] found = new RID[NUM_QUERIES][];
    for (int q = 0; q < NUM_QUERIES; q++) {
      final List<?> list = rows[q] != null ? rows[q] : List.of();
      found[q] = new RID[list.size()];
      for (int i = 0; i < found[q].length; i++)
        if (list.get(i) instanceof Map<?, ?> neighbour && neighbour.get("@rid") instanceof Identifiable rid)
          found[q][i] = rid.getIdentity();
    }

    printLatencyStats(label + " SQL", latenciesNs, totalResults, meanRecall(db, found, groundTruth));
  }

  /**
   * Mean recall@K of the neighbours found against the exact ones. Resolves each RID to the data index it was inserted
   * with (the "id" property), after the timed loop.
   */
  private double meanRecall(final Database db, final RID[][] found, final int[][] groundTruth) {
    double total = 0;
    for (int q = 0; q < found.length; q++) {
      final int[] ids = new int[found[q].length];
      for (int i = 0; i < ids.length; i++)
        ids[i] = found[q][i] == null ? -1 : db.lookupByRID(found[q][i], true).asDocument().getInteger("id");
      total += ClusteredVectorDataset.recall(groundTruth[q], ids);
    }
    return total / found.length;
  }

  private void printLatencyStats(final String label, final long[] latenciesNs, final int totalResults,
      final double recall) {
    Arrays.sort(latenciesNs);
    final double meanMs = Arrays.stream(latenciesNs).average().orElse(0) / 1e6;
    final double p50Ms = latenciesNs[latenciesNs.length / 2] / 1e6;
    final double p95Ms = latenciesNs[(int) (latenciesNs.length * 0.95)] / 1e6;
    final double p99Ms = latenciesNs[(int) (latenciesNs.length * 0.99)] / 1e6;
    final double minMs = latenciesNs[0] / 1e6;
    final double maxMs = latenciesNs[latenciesNs.length - 1] / 1e6;
    final double avgResultsPerQuery = (double) totalResults / latenciesNs.length;

    System.out.printf(
        "[%s] Recall@%d: %.3f | Latency: mean=%.2fms  p50=%.2fms  p95=%.2fms  p99=%.2fms  min=%.2fms  max=%.2fms%n",
        label, K, recall, meanMs, p50Ms, p95Ms, p99Ms, minMs, maxMs);
    System.out.printf("[%s] Avg results/query: %.1f%n", label, avgResultsPerQuery);
  }

  private String vectorToString(final float[] vector) {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < vector.length; i++) {
      if (i > 0)
        sb.append(',');
      sb.append(vector[i]);
    }
    sb.append(']');
    return sb.toString();
  }
}
