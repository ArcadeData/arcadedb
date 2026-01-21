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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

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
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LSMVectorIndexStorageBenchmark {
  private static final String DB_PATH = "target/test-databases/LSMVectorIndexStorageBenchmark";

  // Store benchmark results for final comparison table
  private final Map<String, BenchmarkResult> benchmarkResults = new LinkedHashMap<>();

  // Benchmark parameters
  private static final int NUM_VECTORS  = 10_000;    // Number of vectors to insert
  private static final int DIMENSIONS   = 100;       // Vector dimensions (moderate size)
  private static final int NUM_QUERIES  = 100;       // Number of search queries to run
  private static final int K_NEIGHBORS  = 10;        // Number of neighbors to retrieve
  private static final int NUM_CLUSTERS = 50;        // Number of clusters for synthetic data
  private static final float CLUSTER_SPREAD = 0.15f; // How spread out vectors are within a cluster
  private static final long RANDOM_SEED = 42L;       // Fixed seed for reproducibility

  // Pre-generated synthetic data (shared across all tests)
  private float[][] dataVectors;
  private float[][] queryVectors;
  private float[][] clusterCentroids;
  private int[] vectorClusterAssignments;

  @BeforeAll
  public void generateSyntheticData() {
    System.out.println("╔════════════════════════════════════════════════════════════╗");
    System.out.println("║         GENERATING SYNTHETIC CLUSTERED DATASET             ║");
    System.out.println("╚════════════════════════════════════════════════════════════╝");
    System.out.println();

    final Random random = new Random(RANDOM_SEED);

    // Step 1: Generate cluster centroids
    System.out.println("Generating " + NUM_CLUSTERS + " cluster centroids...");
    clusterCentroids = new float[NUM_CLUSTERS][DIMENSIONS];
    for (int c = 0; c < NUM_CLUSTERS; c++) {
      clusterCentroids[c] = generateNormalizedRandomVector(random);
    }

    // Step 2: Generate data vectors around cluster centroids
    System.out.println("Generating " + NUM_VECTORS + " clustered data vectors...");
    dataVectors = new float[NUM_VECTORS][DIMENSIONS];
    vectorClusterAssignments = new int[NUM_VECTORS];

    for (int i = 0; i < NUM_VECTORS; i++) {
      // Assign to a cluster (round-robin to ensure even distribution)
      final int clusterId = i % NUM_CLUSTERS;
      vectorClusterAssignments[i] = clusterId;

      // Generate vector as centroid + noise
      final float[] centroid = clusterCentroids[clusterId];
      dataVectors[i] = generateClusteredVector(centroid, CLUSTER_SPREAD, random);
    }

    // Step 3: Generate query vectors near cluster centroids
    // Each query is placed near a cluster centroid with slight offset
    System.out.println("Generating " + NUM_QUERIES + " query vectors near cluster centroids...");
    queryVectors = new float[NUM_QUERIES][DIMENSIONS];
    for (int q = 0; q < NUM_QUERIES; q++) {
      // Pick a random cluster for this query
      final int targetCluster = random.nextInt(NUM_CLUSTERS);
      final float[] centroid = clusterCentroids[targetCluster];

      // Query vector is near the centroid but with smaller spread than data vectors
      // This ensures queries have meaningful neighbors
      queryVectors[q] = generateClusteredVector(centroid, CLUSTER_SPREAD * 0.5f, random);
    }

    // Print dataset statistics
    System.out.println();
    System.out.println("Dataset Statistics:");
    System.out.println("  - Data vectors: " + NUM_VECTORS);
    System.out.println("  - Query vectors: " + NUM_QUERIES);
    System.out.println("  - Dimensions: " + DIMENSIONS);
    System.out.println("  - Clusters: " + NUM_CLUSTERS);
    System.out.println("  - Vectors per cluster: ~" + (NUM_VECTORS / NUM_CLUSTERS));
    System.out.println("  - Cluster spread: " + CLUSTER_SPREAD);
    System.out.println("  - Random seed: " + RANDOM_SEED);
    System.out.println();

    // Verify clustering quality by computing average intra-cluster distance
    double avgIntraClusterDist = 0;
    int count = 0;
    for (int c = 0; c < Math.min(5, NUM_CLUSTERS); c++) { // Sample first 5 clusters
      final float[] centroid = clusterCentroids[c];
      for (int i = 0; i < NUM_VECTORS; i++) {
        if (vectorClusterAssignments[i] == c) {
          avgIntraClusterDist += computeCosineDistance(centroid, dataVectors[i]);
          count++;
        }
      }
    }
    if (count > 0) {
      avgIntraClusterDist /= count;
      System.out.println("Quality Check:");
      System.out.println("  - Avg intra-cluster distance: " + String.format("%.4f", avgIntraClusterDist));
      System.out.println("  (lower = tighter clusters, expected ~" + String.format("%.4f", CLUSTER_SPREAD * 0.5) + ")");
    }
    System.out.println();
  }

  /**
   * Generates a normalized random vector (unit length).
   */
  private float[] generateNormalizedRandomVector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    float norm = 0;

    // Generate random components
    for (int i = 0; i < DIMENSIONS; i++) {
      vector[i] = (float) random.nextGaussian();
      norm += vector[i] * vector[i];
    }

    // Normalize to unit length
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] /= norm;

    return vector;
  }

  /**
   * Generates a vector clustered around a centroid with given spread.
   * Uses Gaussian noise and re-normalizes for cosine similarity.
   */
  private float[] generateClusteredVector(final float[] centroid, final float spread, final Random random) {
    final float[] vector = new float[DIMENSIONS];
    float norm = 0;

    // Add Gaussian noise to centroid
    for (int i = 0; i < DIMENSIONS; i++) {
      vector[i] = centroid[i] + (float) (random.nextGaussian() * spread);
      norm += vector[i] * vector[i];
    }

    // Normalize to unit length (important for cosine similarity)
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] /= norm;

    return vector;
  }

  @BeforeEach
  public void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    GlobalConfiguration.PROFILE.setValue("high-performance");
  }

  @AfterEach
  public void cleanup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterAll
  public void printComparisonTable() {
    if (benchmarkResults.isEmpty())
      return;

    System.out.println("\n");
    System.out.println("╔" + "═".repeat(158) + "╗");
    System.out.println("║" + centerText("BENCHMARK COMPARISON TABLE", 158) + "║");
    System.out.println("╠" + "═".repeat(158) + "╣");

    // Configuration section
    System.out.println("║" + centerText("CONFIGURATION", 158) + "║");
    System.out.println("╠" + "═".repeat(45) + "╤" + ("═".repeat(16) + "╤").repeat(benchmarkResults.size() - 1) + "═".repeat(16) + "╣");

    // Header row with test names
    final StringBuilder headerRow = new StringBuilder("║ " + padRight("Test", 43) + " │");
    for (final BenchmarkResult r : benchmarkResults.values())
      headerRow.append(centerText(truncate(r.label, 14), 16)).append("│");
    headerRow.setLength(headerRow.length() - 1);
    headerRow.append("║");
    System.out.println(headerRow);

    System.out.println("╟" + "─".repeat(45) + "┼" + ("─".repeat(16) + "┼").repeat(benchmarkResults.size() - 1) + "─".repeat(16) + "╢");

    // Configuration rows
    printTableRow("Store in Graph", benchmarkResults.values().stream().map(r -> String.valueOf(r.storeVectorsInGraph)).toArray(String[]::new));
    printTableRow("Quantization", benchmarkResults.values().stream().map(r -> r.quantization.name()).toArray(String[]::new));
    printTableRow("Approximate Search", benchmarkResults.values().stream().map(r -> String.valueOf(r.useApproximateSearch)).toArray(String[]::new));

    // Latency section
    System.out.println("╠" + "═".repeat(45) + "╪" + ("═".repeat(16) + "╪").repeat(benchmarkResults.size() - 1) + "═".repeat(16) + "╣");
    System.out.println("║" + centerText("LATENCY (μs)", 158) + "║");
    System.out.println("╟" + "─".repeat(45) + "┼" + ("─".repeat(16) + "┼").repeat(benchmarkResults.size() - 1) + "─".repeat(16) + "╢");

    printTableRow("Min", benchmarkResults.values().stream().map(r -> formatNumber(r.minLatency)).toArray(String[]::new));
    printTableRow("P50 (Median)", benchmarkResults.values().stream().map(r -> formatNumber(r.p50Latency)).toArray(String[]::new));
    printTableRow("Average", benchmarkResults.values().stream().map(r -> String.format("%.1f", r.avgLatency)).toArray(String[]::new));
    printTableRow("P95", benchmarkResults.values().stream().map(r -> formatNumber(r.p95Latency)).toArray(String[]::new));
    printTableRow("P99", benchmarkResults.values().stream().map(r -> formatNumber(r.p99Latency)).toArray(String[]::new));
    printTableRow("Max", benchmarkResults.values().stream().map(r -> formatNumber(r.maxLatency)).toArray(String[]::new));

    // Quality section
    System.out.println("╠" + "═".repeat(45) + "╪" + ("═".repeat(16) + "╪").repeat(benchmarkResults.size() - 1) + "═".repeat(16) + "╣");
    System.out.println("║" + centerText("SEARCH QUALITY", 158) + "║");
    System.out.println("╟" + "─".repeat(45) + "┼" + ("─".repeat(16) + "┼").repeat(benchmarkResults.size() - 1) + "─".repeat(16) + "╢");

    printTableRow("Recall@" + K_NEIGHBORS + " (Avg)", benchmarkResults.values().stream().map(r -> String.format("%.1f%%", r.avgRecall * 100)).toArray(String[]::new));
    printTableRow("Recall@" + K_NEIGHBORS + " (Min)", benchmarkResults.values().stream().map(r -> String.format("%.1f%%", r.minRecall * 100)).toArray(String[]::new));
    printTableRow("Recall@" + K_NEIGHBORS + " (Max)", benchmarkResults.values().stream().map(r -> String.format("%.1f%%", r.maxRecall * 100)).toArray(String[]::new));
    printTableRow("Perfect Matches", benchmarkResults.values().stream().map(r -> String.format("%d/%d", r.perfectMatches, NUM_QUERIES)).toArray(String[]::new));

    // Vector Fetch Sources section
    System.out.println("╠" + "═".repeat(45) + "╪" + ("═".repeat(16) + "╪").repeat(benchmarkResults.size() - 1) + "═".repeat(16) + "╣");
    System.out.println("║" + centerText("VECTOR FETCH SOURCES", 158) + "║");
    System.out.println("╟" + "─".repeat(45) + "┼" + ("─".repeat(16) + "┼").repeat(benchmarkResults.size() - 1) + "─".repeat(16) + "╢");

    printTableRow("From Graph", benchmarkResults.values().stream().map(r -> formatNumber(r.fetchFromGraph)).toArray(String[]::new));
    printTableRow("From Documents", benchmarkResults.values().stream().map(r -> formatNumber(r.fetchFromDocs)).toArray(String[]::new));
    printTableRow("From Quantized", benchmarkResults.values().stream().map(r -> formatNumber(r.fetchFromQuantized)).toArray(String[]::new));

    // Timing section
    System.out.println("╠" + "═".repeat(45) + "╪" + ("═".repeat(16) + "╪").repeat(benchmarkResults.size() - 1) + "═".repeat(16) + "╣");
    System.out.println("║" + centerText("TIMING (μs)", 158) + "║");
    System.out.println("╟" + "─".repeat(45) + "┼" + ("─".repeat(16) + "┼").repeat(benchmarkResults.size() - 1) + "─".repeat(16) + "╢");

    printTableRow("DB Reopen", benchmarkResults.values().stream().map(r -> formatNumber(r.reopenTime)).toArray(String[]::new));
    printTableRow("Total Benchmark", benchmarkResults.values().stream().map(r -> formatNumber(r.benchmarkTime)).toArray(String[]::new));
    printTableRow("Avg per Query", benchmarkResults.values().stream().map(r -> String.format("%.1f", r.avgPerQuery)).toArray(String[]::new));

    // PQ Stats section (only if any test uses PQ)
    final boolean hasPQ = benchmarkResults.values().stream().anyMatch(r -> r.quantization == VectorQuantizationType.PRODUCT);
    if (hasPQ) {
      System.out.println("╠" + "═".repeat(45) + "╪" + ("═".repeat(16) + "╪").repeat(benchmarkResults.size() - 1) + "═".repeat(16) + "╣");
      System.out.println("║" + centerText("PRODUCT QUANTIZATION", 158) + "║");
      System.out.println("╟" + "─".repeat(45) + "┼" + ("─".repeat(16) + "┼").repeat(benchmarkResults.size() - 1) + "─".repeat(16) + "╢");

      printTableRow("PQ Available", benchmarkResults.values().stream().map(r -> r.quantization == VectorQuantizationType.PRODUCT ? String.valueOf(r.pqAvailable) : "-").toArray(String[]::new));
      printTableRow("PQ Vector Count", benchmarkResults.values().stream().map(r -> r.quantization == VectorQuantizationType.PRODUCT ? formatNumber(r.pqVectorCount) : "-").toArray(String[]::new));
      printTableRow("Compression Ratio", benchmarkResults.values().stream().map(r -> r.quantization == VectorQuantizationType.PRODUCT ? String.format("%.1fx", r.compressionRatio) : "-").toArray(String[]::new));
    }

    // Footer
    System.out.println("╚" + "═".repeat(45) + "╧" + ("═".repeat(16) + "╧").repeat(benchmarkResults.size() - 1) + "═".repeat(16) + "╝");

    // Summary analysis
    System.out.println("\n" + "=".repeat(80));
    System.out.println("SUMMARY ANALYSIS");
    System.out.println("=".repeat(80));

    // Find best performers
    final BenchmarkResult fastestP50 = benchmarkResults.values().stream()
        .min(Comparator.comparingLong(r -> r.p50Latency)).orElse(null);
    final BenchmarkResult highestRecall = benchmarkResults.values().stream()
        .max(Comparator.comparingDouble(r -> r.avgRecall)).orElse(null);
    final BenchmarkResult lowestLatencyVariance = benchmarkResults.values().stream()
        .min(Comparator.comparingLong(r -> r.maxLatency - r.minLatency)).orElse(null);

    if (fastestP50 != null)
      System.out.println("  Fastest P50 Latency:     " + fastestP50.label + " (" + fastestP50.p50Latency + " μs)");
    if (highestRecall != null)
      System.out.println("  Highest Recall@" + K_NEIGHBORS + ":       " + highestRecall.label + " (" + String.format("%.1f%%", highestRecall.avgRecall * 100) + ")");
    if (lowestLatencyVariance != null)
      System.out.println("  Most Consistent:         " + lowestLatencyVariance.label + " (variance: " + (lowestLatencyVariance.maxLatency - lowestLatencyVariance.minLatency) + " μs)");

    System.out.println("=".repeat(80));
    System.out.println();
  }

  private void printTableRow(final String label, final String[] values) {
    final StringBuilder row = new StringBuilder("║ " + padRight(label, 43) + " │");
    for (final String value : values)
      row.append(centerText(value, 16)).append("│");
    row.setLength(row.length() - 1);
    row.append("║");
    System.out.println(row);
  }

  private static String padRight(final String s, final int n) {
    if (s.length() >= n)
      return s.substring(0, n);
    return s + " ".repeat(n - s.length());
  }

  private static String centerText(final String s, final int width) {
    if (s.length() >= width)
      return s.substring(0, width);
    final int padding = width - s.length();
    final int leftPad = padding / 2;
    final int rightPad = padding - leftPad;
    return " ".repeat(leftPad) + s + " ".repeat(rightPad);
  }

  private static String truncate(final String s, final int maxLen) {
    if (s.length() <= maxLen)
      return s;
    return s.substring(0, maxLen - 2) + "..";
  }

  private static String formatNumber(final long n) {
    if (n >= 1_000_000)
      return String.format("%.1fM", n / 1_000_000.0);
    if (n >= 1_000)
      return String.format("%.1fK", n / 1_000.0);
    return String.valueOf(n);
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
   * Benchmark 5: Quantization only (without graph storage).
   * Vectors fetched from quantized pages.
   */
  @Test
  public void benchmarkQuantizationBinaryOnly() {
    System.out.println("\n========================================");
    System.out.println("Benchmark 5: BINARY Quantization");
    System.out.println("========================================");

    runBenchmark(false, VectorQuantizationType.BINARY, false, "BINARY Quantization Only (Pages)");
  }

  /**
   * Benchmark 6: Product Quantization with exact search.
   * PQ is built but search uses exact vectors (fallback mode).
   */
  @Test
  public void benchmarkProductQuantizationExact() {
    System.out.println("\n========================================");
    System.out.println("Benchmark 6: PRODUCT Quantization (Exact Search)");
    System.out.println("========================================");

    runBenchmark(false, VectorQuantizationType.PRODUCT, false, "PRODUCT Quantization (Exact Search)");
  }

  /**
   * Benchmark 7: Product Quantization with approximate (zero-disk-I/O) search.
   * Uses in-memory PQ vectors for both navigation and scoring.
   * This is the fastest option with sub-millisecond latency.
   */
  @Test
  public void benchmarkProductQuantizationApproximate() {
    System.out.println("\n========================================");
    System.out.println("Benchmark 7: PRODUCT Quantization (Approximate/Zero-Disk-I/O)");
    System.out.println("========================================");

    runBenchmark(false, VectorQuantizationType.PRODUCT, true, "PRODUCT Quantization (Zero-Disk-I/O Approximate)");
  }

  private void runBenchmark(final boolean storeVectorsInGraph, final VectorQuantizationType quantization,
      final String label) {
    runBenchmark(storeVectorsInGraph, quantization, false, label);
  }

  private void runBenchmark(final boolean storeVectorsInGraph, final VectorQuantizationType quantization,
      final boolean useApproximateSearch, final String label) {

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

        // Insert vectors (using pre-generated synthetic data)
        final long insertStart = System.nanoTime();
        db.begin();

        for (int i = 0; i < NUM_VECTORS; i++) {
          final var doc = db.newDocument("VectorDoc");
          doc.set("id", i);
          doc.set("embedding", dataVectors[i]);
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

        // Warm-up queries (not measured) - use first few query vectors
        System.out.println("Warming up...");
        for (int i = 0; i < Math.min(10, NUM_QUERIES); i++)
          index.findNeighborsFromVector(queryVectors[i], K_NEIGHBORS);

        // Debug: verify PQ and exact search return similar results
        if (useApproximateSearch && index.isPQSearchAvailable()) {
          System.out.println("DEBUG: Verifying PQ search vs exact search...");
          final float[] testVector = queryVectors[0];
          final List<Pair<RID, Float>> exactResults = index.findNeighborsFromVector(testVector, 5);
          final List<Pair<RID, Float>> approxResults = index.findNeighborsFromVectorApproximate(testVector, 5);
          System.out.println("  Exact search returned: " + exactResults.size() + " results");
          System.out.println("  Approx search returned: " + approxResults.size() + " results");
          if (!exactResults.isEmpty())
            System.out.println("  Exact top result: RID=" + exactResults.get(0).getFirst() + ", dist=" + exactResults.get(0).getSecond());
          if (!approxResults.isEmpty())
            System.out.println("  Approx top result: RID=" + approxResults.get(0).getFirst() + ", dist=" + approxResults.get(0).getSecond());
          // Check overlap
          final var exactRIDs = exactResults.stream().map(p -> p.getFirst().toString()).collect(Collectors.toSet());
          final var approxRIDs = approxResults.stream().map(p -> p.getFirst().toString()).collect(Collectors.toSet());
          exactRIDs.retainAll(approxRIDs);
          System.out.println("  Overlap (exact vs approx): " + exactRIDs.size() + "/" + Math.min(exactResults.size(), approxResults.size()));
        }

        // Get stats before benchmark
        final Map<String, Long> statsBefore = index.getStats();

        // Run timed queries and collect results for quality measurement
        // Uses pre-generated query vectors for fair comparison across all tests
        System.out.println(String.format("Running %d queries (approximate=%s)...", NUM_QUERIES, useApproximateSearch));
        final long[] latencies = new long[NUM_QUERIES];
        final List<List<Pair<RID, Float>>> approximateResults = new ArrayList<>(NUM_QUERIES);
        final long benchmarkStart = System.nanoTime();

        for (int i = 0; i < NUM_QUERIES; i++) {
          final float[] queryVector = queryVectors[i];

          final long queryStart = System.nanoTime();
          final List<Pair<RID, Float>> results;
          if (useApproximateSearch)
            results = index.findNeighborsFromVectorApproximate(queryVector, K_NEIGHBORS);
          else
            results = index.findNeighborsFromVector(queryVector, K_NEIGHBORS);
          final long queryEnd = System.nanoTime();

          latencies[i] = (queryEnd - queryStart) / 1_000; // Convert to microseconds
          approximateResults.add(results);

          // Verify we got results
          if (results.isEmpty())
            System.err.println("WARNING: Query " + i + " returned no results!");
        }

        final long benchmarkTime = (System.nanoTime() - benchmarkStart) / 1_000;

        // Get stats after benchmark
        final Map<String, Long> statsAfter = index.getStats();

        // Phase 3: Compute ground truth and quality metrics
        System.out.println("Computing ground truth for quality measurement...");
        final QualityMetrics qualityMetrics = computeQualityMetrics(db, approximateResults);

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
        System.out.println(String.format("  - Approximate (Zero-Disk-I/O): %s", useApproximateSearch));
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

        // PQ-specific stats
        boolean pqAvailable = false;
        int pqVectorCount = 0;
        double compressionRatio = 1.0;

        if (quantization == VectorQuantizationType.PRODUCT) {
          System.out.println();
          System.out.println(String.format("Product Quantization Stats:"));
          pqAvailable = index.isPQSearchAvailable();
          pqVectorCount = index.getPQVectorCount();
          System.out.println(String.format("  - PQ available:        %s", pqAvailable));
          System.out.println(String.format("  - PQ vector count:     %d", pqVectorCount));

          // Estimate PQ RAM usage: each vector uses M bytes (1 byte per subspace)
          // Plus codebook: M subspaces * K clusters * subspace_dimension * 4 bytes
          final int estimatedSubspaces = DIMENSIONS / 4; // default: dimensions/4
          final long pqCodesBytes = (long) pqVectorCount * estimatedSubspaces;
          final long codebookBytes = (long) estimatedSubspaces * 256 * 4 * 4; // 256 clusters, float centroids
          final long totalPqRamBytes = pqCodesBytes + codebookBytes;
          System.out.println(String.format("  - Est. PQ codes RAM:   %.2f KB (%d vectors × %d subspaces)",
              pqCodesBytes / 1024.0, pqVectorCount, estimatedSubspaces));
          System.out.println(String.format("  - Est. codebook RAM:   %.2f KB", codebookBytes / 1024.0));
          System.out.println(String.format("  - Est. total PQ RAM:   %.2f KB", totalPqRamBytes / 1024.0));

          // Compare to full float vectors
          final long fullVectorBytes = (long) pqVectorCount * DIMENSIONS * 4;
          compressionRatio = (double) fullVectorBytes / totalPqRamBytes;
          System.out.println(String.format("  - Full vectors RAM:    %.2f KB", fullVectorBytes / 1024.0));
          System.out.println(String.format("  - Compression ratio:   %.1fx", compressionRatio));
        }
        System.out.println("=".repeat(60));
        System.out.println();

        // Store result in map for final comparison table
        final BenchmarkResult result = new BenchmarkResult(
            label, storeVectorsInGraph, quantization, useApproximateSearch,
            reopenTime, benchmarkTime, benchmarkTime / (double) NUM_QUERIES, qualityMetrics.groundTruthTime,
            min, p50, avg, p95, p99, max,
            qualityMetrics.avgRecall, qualityMetrics.minRecall, qualityMetrics.maxRecall, qualityMetrics.perfectMatches,
            fetchFromGraph, fetchFromDocs, fetchFromQuantized,
            statsAfter.get("totalVectors"), statsAfter.get("activeVectors"), statsAfter.get("graphState"),
            statsAfter.get("graphNodeCount"), statsAfter.get("searchOperations"),
            pqAvailable, pqVectorCount, compressionRatio);
        benchmarkResults.put(label, result);
      }
    }
  }

  // Helper methods

  private LSMVectorIndex getVectorIndex(final Database db, final String typeName, final String propertyName) {
    final Schema schema = db.getSchema();
    final DocumentType type = schema.getType(typeName);
    return (LSMVectorIndex) type.getPolymorphicIndexByProperties(propertyName).getIndexesOnBuckets()[0];
  }

  /**
   * Compute quality metrics by comparing approximate search results against ground truth.
   * Ground truth is computed via brute-force linear scan with exact distance calculations.
   * Uses the pre-generated queryVectors instance field for consistency.
   */
  private QualityMetrics computeQualityMetrics(final Database db,
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
    final double[] recalls = new double[NUM_QUERIES];
    int perfectMatches = 0;

    for (int q = 0; q < NUM_QUERIES; q++) {
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
          .collect(Collectors.toSet());

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

  /**
   * Container for all benchmark results to enable comparison across tests.
   */
  private static class BenchmarkResult {
    final String label;
    // Configuration
    final boolean storeVectorsInGraph;
    final VectorQuantizationType quantization;
    final boolean useApproximateSearch;
    // Timing (microseconds)
    final long reopenTime;
    final long benchmarkTime;
    final double avgPerQuery;
    final long groundTruthTime;
    // Latency (microseconds)
    final long minLatency;
    final long p50Latency;
    final double avgLatency;
    final long p95Latency;
    final long p99Latency;
    final long maxLatency;
    // Quality
    final double avgRecall;
    final double minRecall;
    final double maxRecall;
    final int perfectMatches;
    // Vector fetch sources
    final long fetchFromGraph;
    final long fetchFromDocs;
    final long fetchFromQuantized;
    // Index stats
    final long totalVectors;
    final long activeVectors;
    final long graphState;
    final long graphNodeCount;
    final long searchOperations;
    // PQ stats (optional)
    final boolean pqAvailable;
    final int pqVectorCount;
    final double compressionRatio;

    BenchmarkResult(final String label, final boolean storeVectorsInGraph,
        final VectorQuantizationType quantization, final boolean useApproximateSearch,
        final long reopenTime, final long benchmarkTime, final double avgPerQuery, final long groundTruthTime,
        final long minLatency, final long p50Latency, final double avgLatency,
        final long p95Latency, final long p99Latency, final long maxLatency,
        final double avgRecall, final double minRecall, final double maxRecall, final int perfectMatches,
        final long fetchFromGraph, final long fetchFromDocs, final long fetchFromQuantized,
        final long totalVectors, final long activeVectors, final long graphState,
        final long graphNodeCount, final long searchOperations,
        final boolean pqAvailable, final int pqVectorCount, final double compressionRatio) {
      this.label = label;
      this.storeVectorsInGraph = storeVectorsInGraph;
      this.quantization = quantization;
      this.useApproximateSearch = useApproximateSearch;
      this.reopenTime = reopenTime;
      this.benchmarkTime = benchmarkTime;
      this.avgPerQuery = avgPerQuery;
      this.groundTruthTime = groundTruthTime;
      this.minLatency = minLatency;
      this.p50Latency = p50Latency;
      this.avgLatency = avgLatency;
      this.p95Latency = p95Latency;
      this.p99Latency = p99Latency;
      this.maxLatency = maxLatency;
      this.avgRecall = avgRecall;
      this.minRecall = minRecall;
      this.maxRecall = maxRecall;
      this.perfectMatches = perfectMatches;
      this.fetchFromGraph = fetchFromGraph;
      this.fetchFromDocs = fetchFromDocs;
      this.fetchFromQuantized = fetchFromQuantized;
      this.totalVectors = totalVectors;
      this.activeVectors = activeVectors;
      this.graphState = graphState;
      this.graphNodeCount = graphNodeCount;
      this.searchOperations = searchOperations;
      this.pqAvailable = pqAvailable;
      this.pqVectorCount = pqVectorCount;
      this.compressionRatio = compressionRatio;
    }
  }
}
