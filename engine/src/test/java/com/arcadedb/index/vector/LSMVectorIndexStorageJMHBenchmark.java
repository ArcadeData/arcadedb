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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * JMH Microbenchmark comparing vector search performance across different quantization strategies.
 * <p>
 * This benchmark measures:
 * - Search latency (k=10, k=100)
 * - Search accuracy (recall@10, recall@100)
 * - Memory usage
 * - Vector fetch sources (graph vs documents vs quantized pages)
 * <p>
 * Requirements:
 * - Java 21+ (Java 25+ recommended for best Panama Vector API performance)
 * <p>
 * Or from IDE:
 * Just run the main() method (IntelliJ IDEA has good JMH support)
 * <p>
 * To use Java 25, set JAVA_HOME before running:
 * export JAVA_HOME=$(/usr/libexec/java_home -v 25)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = {
    "-Xms24G",
    "-Xmx24G",
    "--enable-native-access=ALL-UNNAMED",
    "--add-modules", "jdk.incubator.vector" // Split this into two strings
})
public class LSMVectorIndexStorageJMHBenchmark {

  @State(Scope.Benchmark)
  public static class DatabaseState {
    @Param({ "false" })
    public String storeVectorsInGraph;

    @Param({ "NONE", "INT8", "BINARY" })
    public String quantization;

    @Param({ "200000" })
    public int numVectors;

    private static final int    DIMENSIONS          = 100;
    private static final String DB_PATH_PREFIX      = "target/jmh-vector-bench-";
    private static final int    NUM_QUERY_VECTORS   = 100;
    private static final int    RECALL_TEST_QUERIES = 50; // Subset for recall calculation

    private Database        database;
    private DatabaseFactory factory;
    private LSMVectorIndex  index;
    private List<float[]>   queryVectors;
    private String          dbPath;

    // Shared ground truth across all configurations (computed once per numVectors)
    private static final Map<String, Map<Integer, List<RID>>> GROUND_TRUTH_K10  = new HashMap<>();
    private static final Map<String, Map<Integer, List<RID>>> GROUND_TRUTH_K100 = new HashMap<>();

    private long memoryUsedBytes;
    private long setupTimeMs;

    @Setup(Level.Trial)
    public void setupTrial() {
      // Generate unique DB path for this configuration
      dbPath = DB_PATH_PREFIX + storeVectorsInGraph + "-" + quantization + "-" + numVectors;
      FileUtils.deleteRecursively(new File(dbPath));

      System.out.println("\n========================================");
      System.out.println(
          "Setting up: storeVectorsInGraph=" + storeVectorsInGraph + ", quantization=" + quantization + ", vectors=" + numVectors);
      System.out.println("========================================");

      final long setupStart = System.currentTimeMillis();

      // Create database and insert vectors
      try (final DatabaseFactory setupFactory = new DatabaseFactory(dbPath)) {
        try (final Database db = setupFactory.create()) {
          db.transaction(() -> {
            final DocumentType type = db.getSchema().createDocumentType("VectorDoc");
            type.createProperty("id", Type.INTEGER);
            type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

            // Build metadata
            final StringBuilder metadata = new StringBuilder();
            metadata.append("{\n");
            metadata.append("  \"dimensions\": ").append(DIMENSIONS).append(",\n");
            metadata.append("  \"similarity\": \"COSINE\",\n");
            metadata.append("  \"storeVectorsInGraph\": ").append(storeVectorsInGraph);
            if (!quantization.equals("NONE")) {
              metadata.append(",\n  \"quantization\": \"").append(quantization).append("\"");
            }
            metadata.append("\n}");

            db.command("sql", "CREATE INDEX ON VectorDoc (embedding) LSM_VECTOR METADATA " + metadata.toString());
          });

          // Insert vectors
          final Random rand = new Random(12345);

          db.begin();

          for (int i = 0; i < numVectors; i++) {
            final float[] vector = new float[DIMENSIONS];
            for (int j = 0; j < DIMENSIONS; j++) {
              vector[j] = rand.nextFloat() * 2.0f - 1.0f;
            }

            final var doc = db.newDocument("VectorDoc");
            doc.set("id", i);
            doc.set("embedding", vector);
            doc.save();

            if ((i + 1) % 20000 == 0) {
              db.commit();
              System.out.println("Inserted vectors: " + (i + 1));
              db.begin();
            }
          }

          db.commit();
        }
      }

      System.out.println("Database setup completed in " + (System.currentTimeMillis() - setupStart) + "ms");

      // Reopen database for benchmarking
      factory = new DatabaseFactory(dbPath);
      database = factory.open();

      final Schema schema = database.getSchema();
      final DocumentType type = schema.getType("VectorDoc");
      index = (LSMVectorIndex) type.getPolymorphicIndexByProperties("embedding").getIndexesOnBuckets()[0];

      // Generate query vectors (same seed for all configurations)
      queryVectors = new ArrayList<>();
      final Random queryRand = new Random(99999);
      for (int i = 0; i < NUM_QUERY_VECTORS; i++) {
        final float[] query = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++) {
          query[j] = queryRand.nextFloat() * 2.0f - 1.0f;
        }
        queryVectors.add(query);
      }

      // Measure memory usage
      System.gc();
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
      final Runtime runtime = Runtime.getRuntime();
      memoryUsedBytes = runtime.totalMemory() - runtime.freeMemory();

      // Print initial stats
      final Map<String, Long> stats = index.getStats();
      System.out.println("Index Stats:");
      System.out.println("  Total vectors: " + stats.get("totalVectors"));
      System.out.println("  Graph state: " + stats.get("graphState") + " (0=LOADING, 1=IMMUTABLE, 2=MUTABLE)");
      System.out.println("  Graph nodes: " + stats.get("graphNodeCount"));
      System.out.println("  storeVectorsInGraph config: " + index.metadata.storeVectorsInGraph);
      System.out.println("  Quantization: " + index.metadata.quantizationType);
      System.out.println("  Memory used: " + (memoryUsedBytes / 1024 / 1024) + " MB");

      // Compute ground truth for NONE quantization (for recall calculation)
      final String gtKey = String.valueOf(numVectors);
      if (quantization.equals("NONE") && !GROUND_TRUTH_K10.containsKey(gtKey)) {
        System.out.println("Computing ground truth for recall measurement...");
        final Map<Integer, List<RID>> gtK10 = new HashMap<>();
        final Map<Integer, List<RID>> gtK100 = new HashMap<>();

        for (int i = 0; i < RECALL_TEST_QUERIES; i++) {
          final float[] query = queryVectors.get(i);

          final List<Pair<RID, Float>> resultsK10 = index.findNeighborsFromVector(query, 10);
          gtK10.put(i, resultsK10.stream().map(p -> p.getFirst()).toList());

          final List<Pair<RID, Float>> resultsK100 = index.findNeighborsFromVector(query, 100);
          gtK100.put(i, resultsK100.stream().map(p -> p.getFirst()).toList());
        }

        GROUND_TRUTH_K10.put(gtKey, gtK10);
        GROUND_TRUTH_K100.put(gtKey, gtK100);
        System.out.println("Ground truth computed and cached for " + RECALL_TEST_QUERIES + " queries");
      }

      setupTimeMs = System.currentTimeMillis() - setupStart;
      System.out.println("Setup completed in " + setupTimeMs + "ms");
    }

    @TearDown(Level.Trial)
    public void teardownTrial() {
      // Calculate recall metrics against ground truth
      final String gtKey = String.valueOf(numVectors);
      double recallK10 = 1.0;
      double recallK100 = 1.0;

      if (!quantization.equals("NONE") && GROUND_TRUTH_K10.containsKey(gtKey)) {
        final Map<Integer, List<RID>> gtK10 = GROUND_TRUTH_K10.get(gtKey);
        final Map<Integer, List<RID>> gtK100 = GROUND_TRUTH_K100.get(gtKey);

        double sumRecallK10 = 0.0;
        double sumRecallK100 = 0.0;

        for (int i = 0; i < RECALL_TEST_QUERIES; i++) {
          final float[] query = queryVectors.get(i);

          final List<Pair<RID, Float>> resultsK10 = index.findNeighborsFromVector(query, 10);
          sumRecallK10 += calculateRecall(resultsK10.stream().map(p -> p.getFirst()).toList(), gtK10.get(i));

          final List<Pair<RID, Float>> resultsK100 = index.findNeighborsFromVector(query, 100);
          sumRecallK100 += calculateRecall(resultsK100.stream().map(p -> p.getFirst()).toList(), gtK100.get(i));
        }

        recallK10 = sumRecallK10 / RECALL_TEST_QUERIES;
        recallK100 = sumRecallK100 / RECALL_TEST_QUERIES;
      }

      // Print final metrics
      System.out.println("\n" + "=".repeat(70));
      System.out.println("FINAL RESULTS - " + quantization + " (vectors=" + numVectors + ")");
      System.out.println("=".repeat(70));

      if (index != null) {
        final Map<String, Long> stats = index.getStats();
        System.out.println("Performance Metrics:");
        System.out.println("  Search operations: " + stats.get("searchOperations"));
        System.out.println("  Avg search latency: " + stats.get("avgSearchLatencyMs") + "ms");
        System.out.println();

        System.out.println("Vector Fetch Sources:");
        System.out.println("  From graph file:     " + stats.get("vectorFetchFromGraph"));
        System.out.println("  From documents:      " + stats.get("vectorFetchFromDocuments"));
        System.out.println("  From quantized pages: " + stats.get("vectorFetchFromQuantized"));
        System.out.println();

        System.out.println("Accuracy Metrics (vs NONE quantization baseline):");
        System.out.println(String.format("  Recall@10:  %.4f (%.2f%%)", recallK10, recallK10 * 100));
        System.out.println(String.format("  Recall@100: %.4f (%.2f%%)", recallK100, recallK100 * 100));
        System.out.println();

        System.out.println("Memory Usage:");
        System.out.println("  Estimated memory: " + (memoryUsedBytes / 1024 / 1024) + " MB");
      }
      System.out.println("=".repeat(70) + "\n");

      if (database != null) {
        database.close();
      }
      if (factory != null) {
        factory.close();
      }

      // Clean up
      FileUtils.deleteRecursively(new File(dbPath));
    }
  }

  /**
   * Benchmark K-NN search with k=10
   */
  @Benchmark
  public void searchK10(DatabaseState state, Blackhole blackhole) {
    final float[] query = state.queryVectors.get(ThreadLocalRandom.current().nextInt(state.queryVectors.size()));
    final List<Pair<RID, Float>> results = state.index.findNeighborsFromVector(query, 10);
    blackhole.consume(results);
  }

  /**
   * Benchmark K-NN search with k=100
   */
  @Benchmark
  public void searchK100(DatabaseState state, Blackhole blackhole) {
    final float[] query = state.queryVectors.get(ThreadLocalRandom.current().nextInt(state.queryVectors.size()));
    final List<Pair<RID, Float>> results = state.index.findNeighborsFromVector(query, 100);
    blackhole.consume(results);
  }

  /**
   * Helper method to calculate recall against ground truth
   */
  private static double calculateRecall(List<RID> results, List<RID> groundTruth) {
    if (groundTruth == null || groundTruth.isEmpty()) {
      return 1.0; // No ground truth available (for NONE quantization itself)
    }

    final Set<RID> groundTruthSet = new HashSet<>(groundTruth);
    long matches = results.stream().filter(groundTruthSet::contains).count();
    return (double) matches / groundTruth.size();
  }

  /**
   * Run the benchmark from main method
   */
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(LSMVectorIndexStorageJMHBenchmark.class.getSimpleName()).shouldFailOnError(true)
        .shouldDoGC(true).build();

    new Runner(opt).run();
  }
}
