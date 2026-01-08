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
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * JMH Microbenchmark comparing vector search performance with and without storeVectorsInGraph.
 *
 * Run with:
 * mvn clean test-compile exec:java -Dexec.mainClass="com.arcadedb.index.vector.LSMVectorIndexStorageJMHBenchmark" -Dexec.classpathScope=test
 *
 * Or from IDE:
 * Just run the main() method
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
public class LSMVectorIndexStorageJMHBenchmark {

  @State(Scope.Benchmark)
  public static class DatabaseState {
    @Param({"false", "true"})
    public String storeVectorsInGraph;

    @Param({"NONE", "INT8"})
    public String quantization;

    @Param({"1000", "10000", "100000"})
    public int numVectors;

    private static final int DIMENSIONS = 384;
    private static final String DB_PATH_PREFIX = "target/jmh-vector-bench-";

    private Database database;
    private DatabaseFactory factory;
    private LSMVectorIndex index;
    private List<float[]> queryVectors;
    private String dbPath;

    @Setup(Level.Trial)
    public void setupTrial() {
      // Generate unique DB path for this configuration
      dbPath = DB_PATH_PREFIX + storeVectorsInGraph + "-" + quantization + "-" + numVectors;
      FileUtils.deleteRecursively(new File(dbPath));

      System.out.println("\n========================================");
      System.out.println("Setting up: storeVectorsInGraph=" + storeVectorsInGraph +
                        ", quantization=" + quantization +
                        ", vectors=" + numVectors);
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
          db.transaction(() -> {
            for (int i = 0; i < numVectors; i++) {
              final float[] vector = new float[DIMENSIONS];
              for (int j = 0; j < DIMENSIONS; j++) {
                vector[j] = rand.nextFloat() * 2.0f - 1.0f;
              }

              final var doc = db.newDocument("VectorDoc");
              doc.set("id", i);
              doc.set("embedding", vector);
              doc.save();
            }
          });
        }
      }

      final long setupTime = System.currentTimeMillis() - setupStart;
      System.out.println("Database setup completed in " + setupTime + "ms");

      // Reopen database for benchmarking
      factory = new DatabaseFactory(dbPath);
      database = factory.open();

      final Schema schema = database.getSchema();
      final DocumentType type = schema.getType("VectorDoc");
      index = (LSMVectorIndex) type.getPolymorphicIndexByProperties("embedding").getIndexesOnBuckets()[0];

      // Generate query vectors
      queryVectors = new ArrayList<>();
      final Random queryRand = new Random(99999);
      for (int i = 0; i < 100; i++) {
        final float[] query = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++) {
          query[j] = queryRand.nextFloat() * 2.0f - 1.0f;
        }
        queryVectors.add(query);
      }

      // Print initial stats
      final Map<String, Long> stats = index.getStats();
      System.out.println("Index Stats:");
      System.out.println("  Total vectors: " + stats.get("totalVectors"));
      System.out.println("  Graph state: " + stats.get("graphState") + " (0=LOADING, 1=IMMUTABLE, 2=MUTABLE)");
      System.out.println("  Graph nodes: " + stats.get("graphNodeCount"));
      System.out.println("  storeVectorsInGraph config: " + index.metadata.storeVectorsInGraph);
      System.out.println("  Quantization: " + index.metadata.quantizationType);
    }

    @TearDown(Level.Trial)
    public void teardownTrial() {
      // Print final metrics
      if (index != null) {
        final Map<String, Long> stats = index.getStats();
        System.out.println("\nFinal Metrics:");
        System.out.println("  Search operations: " + stats.get("searchOperations"));
        System.out.println("  Vectors from graph: " + stats.get("vectorFetchFromGraph"));
        System.out.println("  Vectors from docs: " + stats.get("vectorFetchFromDocuments"));
        System.out.println("  Vectors from quantized: " + stats.get("vectorFetchFromQuantized"));
        System.out.println("  Avg search latency: " + stats.get("avgSearchLatencyMs") + "ms");
      }

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
   * Run the benchmark from main method
   */
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(LSMVectorIndexStorageJMHBenchmark.class.getSimpleName())
        .shouldFailOnError(true)
        .shouldDoGC(true)
        .build();

    new Runner(opt).run();
  }
}
