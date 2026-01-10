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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;

/**
 * Microbenchmark comparing search performance with and without storeVectorsInGraph.
 * Tests measure:
 * - Search latency in microseconds (P50, P95, P99)
 * - Search quality (Recall@K) compared against ground truth
 * - Vector fetch sources (graph vs documents vs quantized)
 * - Memory usage patterns
 * - Graph loading time
 *
 * Quality metrics are computed by comparing approximate search results against ground truth
 * (exact nearest neighbors via brute-force linear scan).
 */
public class LSMVectorIndexStorageBenchmark {
  private static final String DB_PATH = "target/test-databases/LSMVectorIndexStorageBenchmark";

  // Benchmark parameters
  private static final int NUM_VECTORS = 10_000;   // Number of vectors to insert
  private static final int DIMENSIONS  = 100;         // Vector dimensions (moderate size)
  private static final int NUM_QUERIES = 100;         // Number of search queries to run
  private static final int K_NEIGHBORS = 10;          // Number of neighbors to retrieve

  @BeforeEach
  public void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    GlobalConfiguration.PROFILE.setValue("high-performance");
  }

  @AfterEach
  public void cleanup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /**
   * Benchmark 1: Compare search performance WITHOUT storeVectorsInGraph (baseline).
   * Vectors are fetched from documents via RID lookups.
   */
  @Test
  public void benchmarkWithoutGraphStorage() {
    System.out.println("\n========================================");
    System.out.println("Benchmark 1: WITHOUT storeVectorsInGraph");
    System.out.println("========================================");

    runBenchmark(false, VectorQuantizationType.NONE, "Without Graph Storage (Documents)");
  }

  /**
   * Benchmark 2: Compare search performance WITH storeVectorsInGraph.
   * Vectors are stored inline in graph file and fetched directly.
   */
  @Test
  public void benchmarkWithGraphStorage() {
    System.out.println("\n========================================");
    System.out.println("Benchmark 2: WITH storeVectorsInGraph");
    System.out.println("========================================");

    runBenchmark(true, VectorQuantizationType.NONE, "With Graph Storage (Inline Vectors)");
  }

  /**
   * Benchmark 3: Compare WITH storeVectorsInGraph + INT8 quantization.
   * Best of both worlds: inline storage + 4x compression.
   */
  @Test
  public void benchmarkWithGraphStorageAndInt8Quantization() {
    System.out.println("\n========================================");
    System.out.println("Benchmark 3: WITH storeVectorsInGraph + INT8");
    System.out.println("========================================");

    runBenchmark(true, VectorQuantizationType.INT8, "With Graph Storage + INT8 Quantization");
  }

  /**
   * Benchmark 4: Quantization only (without graph storage).
   * Vectors fetched from quantized pages.
   */
  @Test
  public void benchmarkQuantizationInt8Only() {
    System.out.println("\n========================================");
    System.out.println("Benchmark 4: INT8 Quantization");
    System.out.println("========================================");

    runBenchmark(false, VectorQuantizationType.INT8, "INT8 Quantization Only (Pages)");
  }

  /**
   * Benchmark 4: Quantization only (without graph storage).
   * Vectors fetched from quantized pages.
   */
  @Test
  public void benchmarkQuantizationBinaryOnly() {
    System.out.println("\n========================================");
    System.out.println("Benchmark 5: BINARY Quantization");
    System.out.println("========================================");

    runBenchmark(false, VectorQuantizationType.BINARY, "BINARY Quantization Only (Pages)");
  }

  private void runBenchmark(final boolean storeVectorsInGraph, final VectorQuantizationType quantization,
      final String label) {

    final long setupStart = System.nanoTime();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      // Phase 1: Create database and insert vectors
      try (final Database db = factory.create()) {
        db.transaction(() -> {
          final DocumentType type = db.getSchema().createDocumentType("VectorDoc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

          final String quantizationStr = quantization == VectorQuantizationType.NONE ? "" :
              String.format(",\n                \"quantization\": \"%s\"", quantization.name());

          final String sql = """
              CREATE INDEX ON VectorDoc (embedding) LSM_VECTOR
              METADATA {
                "dimensions": %d,
                "similarity": "COSINE",
                "storeVectorsInGraph": %s%s
              }
              """.formatted(DIMENSIONS, storeVectorsInGraph, quantizationStr);

          db.command("sql", sql);
        });

        // Insert vectors
        final long insertStart = System.nanoTime();
        db.begin();

        for (int i = 0; i < NUM_VECTORS; i++) {
          final float[] vector = generateRandomVector(DIMENSIONS, i);
          final var doc = db.newDocument("VectorDoc");
          doc.set("id", i);
          doc.set("embedding", vector);
          doc.save();

          if ((i + 1) % 20_000 == 0) {
            db.commit();
            System.out.println("Inserted " + (i + 1) + " vectors...");
            db.begin();
          }
        }

        db.commit();

        final long insertTime = (System.nanoTime() - insertStart) / 1_000;

        final long setupTime = (System.nanoTime() - setupStart) / 1_000;
        System.out.println(String.format("Setup: %d μs (insert: %d μs)", setupTime, insertTime));
      }

      System.out.println("Reopening database index...");

      // Phase 2: Reopen database and run benchmark
      final long reopenStart = System.nanoTime();
      try (final Database db = factory.open()) {
        final long reopenTime = (System.nanoTime() - reopenStart) / 1_000;

        System.out.println("Building index...");
        final LSMVectorIndex index = getVectorIndex(db, "VectorDoc", "embedding");

        // Warm-up queries (not measured)
        System.out.println("Warming up...");
        for (int i = 0; i < 10; i++) {
          final float[] queryVector = generateRandomVector(DIMENSIONS, 9999 + i);
          index.findNeighborsFromVector(queryVector, K_NEIGHBORS);
        }

        // Get stats before benchmark
        final Map<String, Long> statsBefore = index.getStats();

        // Run timed queries and collect results for quality measurement
        System.out.println(String.format("Running %d queries...", NUM_QUERIES));
        final long[] latencies = new long[NUM_QUERIES];
        final float[][] queryVectors = new float[NUM_QUERIES][];
        final List<List<Pair<RID, Float>>> approximateResults = new ArrayList<>(NUM_QUERIES);
        final long benchmarkStart = System.nanoTime();

        for (int i = 0; i < NUM_QUERIES; i++) {
          final float[] queryVector = generateRandomVector(DIMENSIONS, 10000 + i);
          queryVectors[i] = queryVector;

          final long queryStart = System.nanoTime();
          final List<Pair<RID, Float>> results = index.findNeighborsFromVector(queryVector, K_NEIGHBORS);
          final long queryEnd = System.nanoTime();

          latencies[i] = (queryEnd - queryStart) / 1_000; // Convert to microseconds
          approximateResults.add(results);

          // Verify we got results
          if (results.isEmpty()) {
            System.err.println("WARNING: Query " + i + " returned no results!");
          }
        }

        final long benchmarkTime = (System.nanoTime() - benchmarkStart) / 1_000;

        // Get stats after benchmark
        final Map<String, Long> statsAfter = index.getStats();

        // Phase 3: Compute ground truth and quality metrics
        System.out.println("Computing ground truth for quality measurement...");
        final QualityMetrics qualityMetrics = computeQualityMetrics(db, queryVectors, approximateResults);

        // Compute statistics
        Arrays.sort(latencies);
        final long p50 = latencies[NUM_QUERIES / 2];
        final long p95 = latencies[(int) (NUM_QUERIES * 0.95)];
        final long p99 = latencies[(int) (NUM_QUERIES * 0.99)];
        final long min = latencies[0];
        final long max = latencies[NUM_QUERIES - 1];
        final double avg = Arrays.stream(latencies).average().orElse(0);

        // Compute deltas
        final long fetchFromGraph = statsAfter.getOrDefault("vectorFetchFromGraph", 0L) -
            statsBefore.getOrDefault("vectorFetchFromGraph", 0L);
        final long fetchFromDocs = statsAfter.getOrDefault("vectorFetchFromDocuments", 0L) -
            statsBefore.getOrDefault("vectorFetchFromDocuments", 0L);
        final long fetchFromQuantized = statsAfter.getOrDefault("vectorFetchFromQuantized", 0L) -
            statsBefore.getOrDefault("vectorFetchFromQuantized", 0L);

        // Print results
        System.out.println("\n" + "=".repeat(60));
        System.out.println("Results: " + label);
        System.out.println("=".repeat(60));
        System.out.println(String.format("Configuration:"));
        System.out.println(String.format("  - Vectors: %d", NUM_VECTORS));
        System.out.println(String.format("  - Dimensions: %d", DIMENSIONS));
        System.out.println(String.format("  - Queries: %d (k=%d)", NUM_QUERIES, K_NEIGHBORS));
        System.out.println(String.format("  - storeVectorsInGraph: %s", storeVectorsInGraph));
        System.out.println(String.format("  - Quantization: %s", quantization));
        System.out.println();
        System.out.println(String.format("Timing:"));
        System.out.println(String.format("  - Database reopen: %d μs", reopenTime));
        System.out.println(String.format("  - Total benchmark: %d μs", benchmarkTime));
        System.out.println(String.format("  - Avg per query: %.2f μs", benchmarkTime / (double) NUM_QUERIES));
        System.out.println(String.format("  - Ground truth time: %d μs", qualityMetrics.groundTruthTime));
        System.out.println();
        System.out.println(String.format("Search Latency (microseconds):"));
        System.out.println(String.format("  - Min:  %d μs", min));
        System.out.println(String.format("  - P50:  %d μs", p50));
        System.out.println(String.format("  - Avg:  %.2f μs", avg));
        System.out.println(String.format("  - P95:  %d μs", p95));
        System.out.println(String.format("  - P99:  %d μs", p99));
        System.out.println(String.format("  - Max:  %d μs", max));
        System.out.println();
        System.out.println(String.format("Search Quality:"));
        System.out.println(String.format("  - Recall@%d (avg):     %.2f%%", K_NEIGHBORS,
            qualityMetrics.avgRecall * 100));
        System.out.println(String.format("  - Recall@%d (min):     %.2f%%", K_NEIGHBORS,
            qualityMetrics.minRecall * 100));
        System.out.println(String.format("  - Recall@%d (max):     %.2f%%", K_NEIGHBORS,
            qualityMetrics.maxRecall * 100));
        System.out.println(String.format("  - Perfect matches:     %d/%d (%.2f%%)",
            qualityMetrics.perfectMatches, NUM_QUERIES,
            (qualityMetrics.perfectMatches * 100.0 / NUM_QUERIES)));
        System.out.println();
        System.out.println(String.format("Vector Fetch Sources:"));
        System.out.println(String.format("  - From graph file:     %d", fetchFromGraph));
        System.out.println(String.format("  - From documents:      %d", fetchFromDocs));
        System.out.println(String.format("  - From quantized pages: %d", fetchFromQuantized));
        System.out.println();
        System.out.println(String.format("Index Stats:"));
        System.out.println(String.format("  - Total vectors:       %d", statsAfter.get("totalVectors")));
        System.out.println(String.format("  - Active vectors:      %d", statsAfter.get("activeVectors")));
        System.out.println(String.format("  - Graph state:         %d (0=LOADING, 1=IMMUTABLE, 2=MUTABLE)",
            statsAfter.get("graphState")));
        System.out.println(String.format("  - Graph nodes:         %d", statsAfter.get("graphNodeCount")));
        System.out.println(String.format("  - Search operations:   %d", statsAfter.get("searchOperations")));
        System.out.println("=".repeat(60));
        System.out.println();
      }
    }
  }

  // Helper methods

  private float[] generateRandomVector(final int dimensions, final int seed) {
    final Random rand = new Random(seed);
    final float[] vector = new float[dimensions];
    for (int i = 0; i < dimensions; i++) {
      vector[i] = rand.nextFloat() * 2.0f - 1.0f; // Range [-1, 1]
    }
    return vector;
  }

  private LSMVectorIndex getVectorIndex(final Database db, final String typeName, final String propertyName) {
    final Schema schema = db.getSchema();
    final DocumentType type = schema.getType(typeName);
    return (LSMVectorIndex) type.getPolymorphicIndexByProperties(propertyName).getIndexesOnBuckets()[0];
  }

  /**
   * Compute quality metrics by comparing approximate search results against ground truth.
   * Ground truth is computed via brute-force linear scan with exact distance calculations.
   */
  private QualityMetrics computeQualityMetrics(final Database db, final float[][] queryVectors,
      final List<List<Pair<RID, Float>>> approximateResults) {
    final long startTime = System.nanoTime();

    // Load all vectors from database for ground truth computation
    final Map<RID, float[]> allVectors = new HashMap<>();
    db.transaction(() -> {
      db.scanType("VectorDoc", true, record -> {
        final var doc = record.asDocument();
        final RID rid = doc.getIdentity();
        final Object vectorObj = doc.get("embedding");
        final float[] vector = VectorUtils.convertToFloatArray(vectorObj);
        if (vector != null) {
          allVectors.put(rid, vector);
        }
        return true;
      });
    });

    System.out.println(String.format("  Loaded %d vectors for ground truth computation", allVectors.size()));

    // Compute ground truth and recall for each query
    final double[] recalls = new double[queryVectors.length];
    int perfectMatches = 0;

    for (int q = 0; q < queryVectors.length; q++) {
      final float[] queryVector = queryVectors[q];
      final List<Pair<RID, Float>> approximateResult = approximateResults.get(q);

      // Compute ground truth: exact nearest neighbors via brute force
      final List<Pair<RID, Float>> groundTruth = new ArrayList<>();
      for (final Map.Entry<RID, float[]> entry : allVectors.entrySet()) {
        final float distance = computeCosineDistance(queryVector, entry.getValue());
        groundTruth.add(new Pair<>(entry.getKey(), distance));
      }

      // Sort by distance (ascending) and take top K
      groundTruth.sort(Comparator.comparing(Pair::getSecond));
      final List<RID> groundTruthRIDs = groundTruth.stream()
          .limit(K_NEIGHBORS)
          .map(Pair::getFirst)
          .toList();

      // Compute recall: how many of the approximate results are in ground truth
      final Set<RID> groundTruthSet = new HashSet<>(groundTruthRIDs);
      final Set<RID> approximateSet = approximateResult.stream()
          .map(Pair::getFirst)
          .collect(java.util.stream.Collectors.toSet());

      int matches = 0;
      for (final RID rid : approximateSet) {
        if (groundTruthSet.contains(rid)) {
          matches++;
        }
      }

      final double recall = (double) matches / K_NEIGHBORS;
      recalls[q] = recall;

      if (recall == 1.0) {
        perfectMatches++;
      }
    }

    // Compute aggregate metrics
    final double avgRecall = Arrays.stream(recalls).average().orElse(0.0);
    final double minRecall = Arrays.stream(recalls).min().orElse(0.0);
    final double maxRecall = Arrays.stream(recalls).max().orElse(0.0);

    final long groundTruthTime = (System.nanoTime() - startTime) / 1_000;

    return new QualityMetrics(avgRecall, minRecall, maxRecall, perfectMatches, groundTruthTime);
  }

  /**
   * Compute cosine distance between two vectors.
   * Distance = 1 - cosine_similarity
   *
   * IMPORTANT: JVector normalizes vectors internally when using COSINE similarity,
   * so we must also normalize for ground truth to match!
   */
  private float computeCosineDistance(final float[] v1, final float[] v2) {
    if (v1.length != v2.length) {
      throw new IllegalArgumentException("Vector dimensions must match");
    }

    // Normalize v1
    float norm1 = 0.0f;
    for (int i = 0; i < v1.length; i++) {
      norm1 += v1[i] * v1[i];
    }
    norm1 = (float) Math.sqrt(norm1);

    // Normalize v2
    float norm2 = 0.0f;
    for (int i = 0; i < v2.length; i++) {
      norm2 += v2[i] * v2[i];
    }
    norm2 = (float) Math.sqrt(norm2);

    // Compute dot product of normalized vectors
    float dotProduct = 0.0f;
    for (int i = 0; i < v1.length; i++) {
      dotProduct += (v1[i] / norm1) * (v2[i] / norm2);
    }

    // For normalized vectors, dot product = cosine similarity
    // Distance = 1 - similarity
    return 1.0f - dotProduct;
  }

  /**
   * Container for quality metrics computed against ground truth.
   */
  private static class QualityMetrics {
    final double avgRecall;
    final double minRecall;
    final double maxRecall;
    final int perfectMatches;
    final long groundTruthTime; // in microseconds

    QualityMetrics(final double avgRecall, final double minRecall, final double maxRecall,
        final int perfectMatches, final long groundTruthTime) {
      this.avgRecall = avgRecall;
      this.minRecall = minRecall;
      this.maxRecall = maxRecall;
      this.perfectMatches = perfectMatches;
      this.groundTruthTime = groundTruthTime;
    }
  }
}
