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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;

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
 * Usage:
 *   mvn test -pl engine -Dtest=VectorSearchLatencyBenchmark -DfailIfNoTests=false \
 *     -Dvector.bench.numVectors=500000
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
  private static final int WARMUP_QUERIES = 20;
  private static final long SEED = 42L;

  @Test
  void runLatencyBenchmark() {
    System.out.println("=== VECTOR SEARCH LATENCY BENCHMARK (tae898 scenario) ===");
    System.out.printf("Vectors: %,d | Dimensions: %d | K: %d | efSearch: %d | Queries: %d%n",
        NUM_VECTORS, DIMENSIONS, K, EF_SEARCH, NUM_QUERIES);
    System.out.println();

    final Random rng = new Random(SEED);

    System.out.print("Generating clustered data vectors...");
    final float[][] dataVectors = generateClusteredVectors(NUM_VECTORS, DIMENSIONS, rng);
    final float[][] queryVectors = generateClusteredVectors(NUM_QUERIES, DIMENSIONS, rng);
    System.out.println(" done.");

    // Config 1: Default (matches what the tae898 benchmark uses)
    System.out.println();
    System.out.println("=== Config 1: DEFAULT (no quantization, storeVectorsInGraph=false) ===");
    FileUtils.deleteRecursively(new File(DB_PATH));
    runConfig("DEFAULT", dataVectors, queryVectors, "NONE", false);

    // Config 2: INT8 quantization (reads from index pages instead of documents)
    System.out.println();
    System.out.println("=== Config 2: INT8 quantization ===");
    FileUtils.deleteRecursively(new File(DB_PATH));
    runConfig("INT8", dataVectors, queryVectors, "INT8", false);
  }

  private void runConfig(final String label, final float[][] dataVectors, final float[][] queryVectors,
      final String quantization, final boolean storeVectorsInGraph) {

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
        benchmarkDirectSearch(label, index, queryVectors);

        // Phase 4: SQL vectorNeighbors() search (matches what the Python benchmark does)
        System.out.printf("[%s] Phase 4: SQL vectorNeighbors() search...%n", label);
        benchmarkSQLSearch(label, db, queryVectors);

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

  private void benchmarkDirectSearch(final String label, final LSMVectorIndex index, final float[][] queryVectors) {
    // Warmup
    for (int i = 0; i < WARMUP_QUERIES; i++)
      index.findNeighborsFromVector(queryVectors[i % queryVectors.length], K, EF_SEARCH);

    // Reset metrics after warmup
    index.getStats(); // just read current values

    final long[] latenciesNs = new long[NUM_QUERIES];
    int totalResults = 0;

    for (int q = 0; q < NUM_QUERIES; q++) {
      final long qStart = System.nanoTime();
      final List<Pair<RID, Float>> results = index.findNeighborsFromVector(queryVectors[q], K, EF_SEARCH);
      latenciesNs[q] = System.nanoTime() - qStart;
      totalResults += results.size();
    }

    printLatencyStats(label + " API", latenciesNs, totalResults);
  }

  private void benchmarkSQLSearch(final String label, final Database db, final float[][] queryVectors) {
    // Warmup
    for (int i = 0; i < WARMUP_QUERIES; i++) {
      final String vectorStr = vectorToString(queryVectors[i % queryVectors.length]);
      try (final ResultSet rs = db.query("sql",
          String.format("SELECT vectorNeighbors('VectorData[vector]', %s, %d, %d) as res", vectorStr, K, EF_SEARCH))) {
        while (rs.hasNext()) rs.next();
      }
    }

    final long[] latenciesNs = new long[NUM_QUERIES];
    int totalResults = 0;

    for (int q = 0; q < NUM_QUERIES; q++) {
      final String vectorStr = vectorToString(queryVectors[q]);
      final long qStart = System.nanoTime();
      try (final ResultSet rs = db.query("sql",
          String.format("SELECT vectorNeighbors('VectorData[vector]', %s, %d, %d) as res", vectorStr, K, EF_SEARCH))) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final Object res = row.getProperty("res");
          if (res instanceof List<?> list)
            totalResults += list.size();
        }
      }
      latenciesNs[q] = System.nanoTime() - qStart;
    }

    printLatencyStats(label + " SQL", latenciesNs, totalResults);
  }

  private void printLatencyStats(final String label, final long[] latenciesNs, final int totalResults) {
    Arrays.sort(latenciesNs);
    final double meanMs = Arrays.stream(latenciesNs).average().orElse(0) / 1e6;
    final double p50Ms = latenciesNs[latenciesNs.length / 2] / 1e6;
    final double p95Ms = latenciesNs[(int) (latenciesNs.length * 0.95)] / 1e6;
    final double p99Ms = latenciesNs[(int) (latenciesNs.length * 0.99)] / 1e6;
    final double minMs = latenciesNs[0] / 1e6;
    final double maxMs = latenciesNs[latenciesNs.length - 1] / 1e6;
    final double avgResultsPerQuery = (double) totalResults / latenciesNs.length;

    System.out.printf("[%s] Latency: mean=%.2fms  p50=%.2fms  p95=%.2fms  p99=%.2fms  min=%.2fms  max=%.2fms%n",
        label, meanMs, p50Ms, p95Ms, p99Ms, minMs, maxMs);
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

  private float[][] generateClusteredVectors(final int count, final int dims, final Random rng) {
    // Generate clustered vectors that mimic real-world embeddings
    final int numClusters = Math.max(50, count / 500);
    final float clusterSpread = 0.15f;

    final float[][] centroids = new float[numClusters][dims];
    for (int c = 0; c < numClusters; c++) {
      float norm = 0;
      for (int d = 0; d < dims; d++) {
        centroids[c][d] = (float) rng.nextGaussian();
        norm += centroids[c][d] * centroids[c][d];
      }
      norm = (float) Math.sqrt(norm);
      for (int d = 0; d < dims; d++)
        centroids[c][d] /= norm;
    }

    final float[][] vectors = new float[count][dims];
    for (int i = 0; i < count; i++) {
      final float[] centroid = centroids[i % numClusters];
      float norm = 0;
      for (int d = 0; d < dims; d++) {
        vectors[i][d] = centroid[d] + (float) (rng.nextGaussian() * clusterSpread);
        norm += vectors[i][d] * vectors[i][d];
      }
      norm = (float) Math.sqrt(norm);
      for (int d = 0; d < dims; d++)
        vectors[i][d] /= norm;
    }
    return vectors;
  }
}
