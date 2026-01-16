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
package performance;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.*;
import java.util.*;

/**
 * Comprehensive benchmark comparing query language performance in ArcadeDB.
 * Tests measure latency across different query patterns:
 * - Full scan with aggregation
 * - Index lookups
 * - Graph traversals (3 and 5 levels)
 * - Filtered queries
 *
 * Query languages tested:
 * - Java Native API (database.iterateType, database.lookupByKey, etc.)
 * - SQL (ArcadeDB SQL dialect)
 * - OpenCypher (Cypher graph query language)
 *
 * NOTE: Gremlin benchmarking requires the gremlin module dependency and should
 * be implemented in a separate benchmark in the gremlin module's test suite.
 *
 * Dataset: Synthetic social network with 1M vertices (Account) and 5M edges (Follows).
 * Each query is executed 100 times (after 10 warmup iterations) and statistics are
 * collected including min, max, avg, P50, P95, P99 latencies.
 *
 * Results are presented in a comparison table showing relative performance across
 * different query languages and patterns.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class QueryLanguageBenchmark {
  private static final String DB_PATH = "target/test-databases/QueryLanguageBenchmark";

  // Benchmark parameters
  private static final int NUM_VERTICES = 1_000_000;  // 1M vertices
  private static final int NUM_EDGES = 5_000_000;     // 5M edges (~5 per vertex avg)
  private static final int NUM_ITERATIONS = 100;       // Number of times to repeat each query
  private static final int WARMUP_ITERATIONS = 10;     // Warmup runs (not measured)

  // Sample IDs for lookup queries (randomly selected)
  private static final int NUM_SAMPLE_IDS = 100;
  private final long[] sampleIds = new long[NUM_SAMPLE_IDS];

  // Store benchmark results for final comparison table
  private final Map<String, Map<String, BenchmarkResult>> benchmarkResults = new LinkedHashMap<>();

  // Countries for data generation
  private static final String[] COUNTRIES = {"USA", "UK", "Germany", "France", "Italy", "Spain", "Canada", "Australia", "Japan", "China"};

  private Database database;

  @BeforeAll
  public void generateSyntheticData() {
    System.out.println("╔════════════════════════════════════════════════════════════╗");
    System.out.println("║         GENERATING SYNTHETIC SOCIAL NETWORK GRAPH          ║");
    System.out.println("╚════════════════════════════════════════════════════════════╝");
    System.out.println();

    FileUtils.deleteRecursively(new File(DB_PATH));
    GlobalConfiguration.PROFILE.setValue("high-performance");

    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    database = factory.create();

    System.out.println("Creating schema...");
    final Schema schema = database.getSchema();

    database.transaction(() -> {
      // Create vertex type
      final VertexType accountType = schema.createVertexType("Account");
      accountType.createProperty("id", Type.LONG);
      accountType.createProperty("name", Type.STRING);
      accountType.createProperty("age", Type.INTEGER);
      accountType.createProperty("email", Type.STRING);
      accountType.createProperty("country", Type.STRING);

      // Create unique index on id
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Account", "id");

      // Create edge type
      schema.createEdgeType("Follows");
    });

    System.out.println("Schema created successfully");
    System.out.println();

    // Generate vertices
    System.out.println("Generating " + NUM_VERTICES + " vertices...");
    final long startVertices = System.currentTimeMillis();
    final Random random = new Random(42L); // Fixed seed for reproducibility
    final List<Long> vertexIds = new ArrayList<>(NUM_VERTICES);

    database.begin();
    for (int i = 0; i < NUM_VERTICES; i++) {
      final MutableVertex v = database.newVertex("Account");
      final long id = i;
      v.set("id", id);
      v.set("name", "User" + i);
      v.set("age", 18 + random.nextInt(63)); // 18-80
      v.set("email", "user" + i + "@example.com");
      v.set("country", COUNTRIES[random.nextInt(COUNTRIES.length)]);
      v.save();

      vertexIds.add(id);

      if ((i + 1) % 100_000 == 0) {
        database.commit();
        System.out.println("  Created " + (i + 1) + " vertices...");
        database.begin();
      }
    }
    database.commit();

    final long verticesTime = System.currentTimeMillis() - startVertices;
    System.out.println("Vertices created in " + verticesTime + " ms");
    System.out.println();

    // Select sample IDs for lookup queries
    for (int i = 0; i < NUM_SAMPLE_IDS; i++)
      sampleIds[i] = random.nextInt(NUM_VERTICES);

    // Generate edges
    System.out.println("Generating " + NUM_EDGES + " edges...");
    final long startEdges = System.currentTimeMillis();

    database.begin();
    for (int i = 0; i < NUM_EDGES; i++) {
      final long fromId = vertexIds.get(random.nextInt(NUM_VERTICES));
      final long toId = vertexIds.get(random.nextInt(NUM_VERTICES));

      // Lookup vertices by ID using Java Native API (much faster than SQL)
      final com.arcadedb.index.IndexCursor fromResult = database.lookupByKey("Account", "id", fromId);
      final com.arcadedb.index.IndexCursor toResult = database.lookupByKey("Account", "id", toId);

      if (fromResult.hasNext() && toResult.hasNext()) {
        final Vertex from = fromResult.next().asVertex();
        final Vertex to = toResult.next().asVertex();
        from.modify().newEdge("Follows", to, true, new Object[0]);
      }

      if ((i + 1) % 100_000 == 0) {
        database.commit();
        System.out.println("  Created " + (i + 1) + " edges...");
        database.begin();
      }
    }
    database.commit();

    final long edgesTime = System.currentTimeMillis() - startEdges;
    System.out.println("Edges created in " + edgesTime + " ms");
    System.out.println();

    // Print statistics
    System.out.println("Dataset Statistics:");
    System.out.println("  - Vertices: " + NUM_VERTICES);
    System.out.println("  - Edges: " + NUM_EDGES);
    System.out.println("  - Avg edges per vertex: " + (NUM_EDGES / (double) NUM_VERTICES));
    System.out.println("  - Age range: 18-80");
    System.out.println("  - Countries: " + Arrays.toString(COUNTRIES));
    System.out.println("  - Random seed: 42");
    System.out.println("  - Total generation time: " + (verticesTime + edgesTime) + " ms");
    System.out.println();
  }

  @AfterAll
  public void cleanup() {
    if (database != null)
      database.close();

    printComparisonTable();

    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  public void benchmarkJavaNativeAPI() {
    System.out.println("\n========================================");
    System.out.println("Benchmark: Java Native API");
    System.out.println("========================================\n");

    final Map<String, BenchmarkResult> results = new LinkedHashMap<>();

    // 1. Full scan with aggregation (AVG age)
    results.put("Scan (AVG age)", benchmarkJavaAvgAge());

    // 2. Lookup by unique index
    results.put("Index Lookup", benchmarkJavaIndexLookup());

    // 3. Filter scan
    results.put("Filter Scan (age > 30)", benchmarkJavaFilterScan());

    // 4. Count
    results.put("Count", benchmarkJavaCount());

    // 5. 3-level traversal
    results.put("3-Level Traversal", benchmarkJavaTraversal(3));

    // 6. 5-level traversal
    results.put("5-Level Traversal", benchmarkJavaTraversal(5));

    benchmarkResults.put("Java Native", results);
  }

  @Test
  public void benchmarkSQL() {
    System.out.println("\n========================================");
    System.out.println("Benchmark: SQL");
    System.out.println("========================================\n");

    final Map<String, BenchmarkResult> results = new LinkedHashMap<>();

    results.put("Scan (AVG age)", benchmarkSQLAvgAge());
    results.put("Index Lookup", benchmarkSQLIndexLookup());
    results.put("Filter Scan (age > 30)", benchmarkSQLFilterScan());
    results.put("Count", benchmarkSQLCount());
    results.put("3-Level Traversal", benchmarkSQLTraversal(3));
    results.put("5-Level Traversal", benchmarkSQLTraversal(5));

    benchmarkResults.put("SQL", results);
  }

  @Test
  public void benchmarkOpenCypher() {
    System.out.println("\n========================================");
    System.out.println("Benchmark: OpenCypher");
    System.out.println("========================================\n");

    final Map<String, BenchmarkResult> results = new LinkedHashMap<>();

    results.put("Scan (AVG age)", benchmarkCypherAvgAge());
    results.put("Index Lookup", benchmarkCypherIndexLookup());
    results.put("Filter Scan (age > 30)", benchmarkCypherFilterScan());
    results.put("Count", benchmarkCypherCount());
    results.put("3-Level Traversal", benchmarkCypherTraversal(3));
    results.put("5-Level Traversal", benchmarkCypherTraversal(5));

    benchmarkResults.put("OpenCypher", results);
  }

  // ==================== Java Native Implementations ====================

  private BenchmarkResult benchmarkJavaAvgAge() {
    System.out.println("Running Java Native: Scan (AVG age)");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeJavaAvgAge();

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long start = System.nanoTime();
      executeJavaAvgAge();
      times[i] = (System.nanoTime() - start) / 1_000; // microseconds
    }

    return computeStatistics(times);
  }

  private double executeJavaAvgAge() {
    database.begin();
    try {
      long sum = 0;
      long count = 0;

      final Iterator<com.arcadedb.database.Record> iterator = database.iterateType("Account", true);
      while (iterator.hasNext()) {
        final com.arcadedb.database.Document doc = (com.arcadedb.database.Document) iterator.next();
        sum += doc.getInteger("age");
        count++;
      }

      return count > 0 ? sum / (double) count : 0;
    } finally {
      database.commit();
    }
  }

  private BenchmarkResult benchmarkJavaIndexLookup() {
    System.out.println("Running Java Native: Index Lookup");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeJavaIndexLookup(sampleIds[i % NUM_SAMPLE_IDS]);

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long id = sampleIds[i % NUM_SAMPLE_IDS];
      final long start = System.nanoTime();
      executeJavaIndexLookup(id);
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private Object executeJavaIndexLookup(final long id) {
    database.begin();
    try {
      final com.arcadedb.index.IndexCursor result = database.lookupByKey("Account", "id", id);
      if (result.hasNext())
        return result.next();
      return null;
    } finally {
      database.commit();
    }
  }

  private BenchmarkResult benchmarkJavaFilterScan() {
    System.out.println("Running Java Native: Filter Scan (age > 30)");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeJavaFilterScan();

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long start = System.nanoTime();
      executeJavaFilterScan();
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private int executeJavaFilterScan() {
    database.begin();
    try {
      int count = 0;
      final Iterator<com.arcadedb.database.Record> iterator = database.iterateType("Account", true);
      while (iterator.hasNext()) {
        final com.arcadedb.database.Document doc = (com.arcadedb.database.Document) iterator.next();
        if (doc.getInteger("age") > 30)
          count++;
      }
      return count;
    } finally {
      database.commit();
    }
  }

  private BenchmarkResult benchmarkJavaCount() {
    System.out.println("Running Java Native: Count");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeJavaCount();

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long start = System.nanoTime();
      executeJavaCount();
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private long executeJavaCount() {
    database.begin();
    try {
      return database.countType("Account", true);
    } finally {
      database.commit();
    }
  }

  private BenchmarkResult benchmarkJavaTraversal(final int depth) {
    System.out.println("Running Java Native: " + depth + "-Level Traversal");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeJavaTraversal(sampleIds[i % NUM_SAMPLE_IDS], depth);

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long id = sampleIds[i % NUM_SAMPLE_IDS];
      final long start = System.nanoTime();
      executeJavaTraversal(id, depth);
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private int executeJavaTraversal(final long startId, final int depth) {
    database.begin();
    try {
      final com.arcadedb.index.IndexCursor startResult = database.lookupByKey("Account", "id", startId);
      if (!startResult.hasNext())
        return 0;

      Set<Vertex> currentLevel = new HashSet<>();
      currentLevel.add(startResult.next().asVertex());

      for (int level = 0; level < depth; level++) {
        final Set<Vertex> nextLevel = new HashSet<>();
        for (final Vertex v : currentLevel) {
          final Iterator<Vertex> neighbors = v.getVertices(Vertex.DIRECTION.OUT, "Follows").iterator();
          while (neighbors.hasNext())
            nextLevel.add(neighbors.next());
        }
        currentLevel = nextLevel;
      }

      return currentLevel.size();
    } finally {
      database.commit();
    }
  }

  // ==================== SQL Implementations ====================

  private BenchmarkResult benchmarkSQLAvgAge() {
    System.out.println("Running SQL: Scan (AVG age)");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeSQLAvgAge();

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long start = System.nanoTime();
      executeSQLAvgAge();
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private double executeSQLAvgAge() {
    database.begin();
    try {
      final ResultSet result = database.query("sql", "SELECT AVG(age) as avgAge FROM Account");
      if (result.hasNext()) {
        final Object avgAge = result.next().getProperty("avgAge");
        if (avgAge instanceof Number)
          return ((Number) avgAge).doubleValue();
      }
      return 0;
    } finally {
      database.commit();
    }
  }

  private BenchmarkResult benchmarkSQLIndexLookup() {
    System.out.println("Running SQL: Index Lookup");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeSQLIndexLookup(sampleIds[i % NUM_SAMPLE_IDS]);

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long id = sampleIds[i % NUM_SAMPLE_IDS];
      final long start = System.nanoTime();
      executeSQLIndexLookup(id);
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private Object executeSQLIndexLookup(final long id) {
    database.begin();
    try {
      final ResultSet result = database.query("sql", "SELECT FROM Account WHERE id = ?", id);
      if (result.hasNext())
        return result.next();
      return null;
    } finally {
      database.commit();
    }
  }

  private BenchmarkResult benchmarkSQLFilterScan() {
    System.out.println("Running SQL: Filter Scan (age > 30)");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeSQLFilterScan();

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long start = System.nanoTime();
      executeSQLFilterScan();
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private int executeSQLFilterScan() {
    database.begin();
    try {
      int count = 0;
      final ResultSet result = database.query("sql", "SELECT FROM Account WHERE age > 30");
      while (result.hasNext()) {
        result.next();
        count++;
      }
      return count;
    } finally {
      database.commit();
    }
  }

  private BenchmarkResult benchmarkSQLCount() {
    System.out.println("Running SQL: Count");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeSQLCount();

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long start = System.nanoTime();
      executeSQLCount();
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private long executeSQLCount() {
    database.begin();
    try {
      final ResultSet result = database.query("sql", "SELECT COUNT(*) as count FROM Account");
      if (result.hasNext())
        return result.next().getProperty("count");
      return 0;
    } finally {
      database.commit();
    }
  }

  private BenchmarkResult benchmarkSQLTraversal(final int depth) {
    System.out.println("Running SQL: " + depth + "-Level Traversal");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeSQLTraversal(sampleIds[i % NUM_SAMPLE_IDS], depth);

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long id = sampleIds[i % NUM_SAMPLE_IDS];
      final long start = System.nanoTime();
      executeSQLTraversal(id, depth);
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private int executeSQLTraversal(final long startId, final int depth) {
    database.begin();
    try {
      final StringBuilder traversal = new StringBuilder("out('Follows')");
      for (int i = 1; i < depth; i++)
        traversal.append(".out('Follows')");

      final String query = "SELECT expand(" + traversal + ") FROM Account WHERE id = ?";
      final ResultSet result = database.query("sql", query, startId);

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      return count;
    } finally {
      database.commit();
    }
  }

  // ==================== OpenCypher Implementations ====================

  private BenchmarkResult benchmarkCypherAvgAge() {
    System.out.println("Running OpenCypher: Scan (AVG age)");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeCypherAvgAge();

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long start = System.nanoTime();
      executeCypherAvgAge();
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private double executeCypherAvgAge() {
    database.begin();
    try {
      final ResultSet result = database.query("opencypher", "MATCH (a:Account) RETURN AVG(a.age) as avgAge");
      if (result.hasNext()) {
        final Object avgAge = result.next().getProperty("avgAge");
        if (avgAge instanceof Number)
          return ((Number) avgAge).doubleValue();
      }
      return 0;
    } finally {
      database.commit();
    }
  }

  private BenchmarkResult benchmarkCypherIndexLookup() {
    System.out.println("Running OpenCypher: Index Lookup");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeCypherIndexLookup(sampleIds[i % NUM_SAMPLE_IDS]);

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long id = sampleIds[i % NUM_SAMPLE_IDS];
      final long start = System.nanoTime();
      executeCypherIndexLookup(id);
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private Object executeCypherIndexLookup(final long id) {
    database.begin();
    try {
      final ResultSet result = database.query("opencypher", "MATCH (a:Account {id: $id}) RETURN a", Map.of("id", id));
      if (result.hasNext())
        return result.next();
      return null;
    } finally {
      database.commit();
    }
  }

  private BenchmarkResult benchmarkCypherFilterScan() {
    System.out.println("Running OpenCypher: Filter Scan (age > 30)");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeCypherFilterScan();

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long start = System.nanoTime();
      executeCypherFilterScan();
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private int executeCypherFilterScan() {
    database.begin();
    try {
      int count = 0;
      final ResultSet result = database.query("opencypher", "MATCH (a:Account) WHERE a.age > 30 RETURN a");
      while (result.hasNext()) {
        result.next();
        count++;
      }
      return count;
    } finally {
      database.commit();
    }
  }

  private BenchmarkResult benchmarkCypherCount() {
    System.out.println("Running OpenCypher: Count");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeCypherCount();

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long start = System.nanoTime();
      executeCypherCount();
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private long executeCypherCount() {
    database.begin();
    try {
      final ResultSet result = database.query("opencypher", "MATCH (a:Account) RETURN COUNT(a) as count");
      if (result.hasNext())
        return result.next().getProperty("count");
      return 0;
    } finally {
      database.commit();
    }
  }

  private BenchmarkResult benchmarkCypherTraversal(final int depth) {
    System.out.println("Running OpenCypher: " + depth + "-Level Traversal");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      executeCypherTraversal(sampleIds[i % NUM_SAMPLE_IDS], depth);

    // Benchmark
    final long[] times = new long[NUM_ITERATIONS];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final long id = sampleIds[i % NUM_SAMPLE_IDS];
      final long start = System.nanoTime();
      executeCypherTraversal(id, depth);
      times[i] = (System.nanoTime() - start) / 1_000;
    }

    return computeStatistics(times);
  }

  private int executeCypherTraversal(final long startId, final int depth) {
    database.begin();
    try {
      final String query = "MATCH (a:Account {id: $id})-[:Follows*" + depth + "]->(b) RETURN b";
      final ResultSet result = database.query("opencypher", query, Map.of("id", startId));

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      return count;
    } finally {
      database.commit();
    }
  }

  // ==================== Statistics & Reporting ====================

  private BenchmarkResult computeStatistics(final long[] times) {
    Arrays.sort(times);

    final long min = times[0];
    final long max = times[times.length - 1];
    final long p50 = times[times.length / 2];
    final long p95 = times[(int) (times.length * 0.95)];
    final long p99 = times[(int) (times.length * 0.99)];

    long sum = 0;
    for (final long time : times)
      sum += time;
    final double avg = sum / (double) times.length;

    return new BenchmarkResult(min, max, avg, p50, p95, p99);
  }

  private void printComparisonTable() {
    if (benchmarkResults.isEmpty())
      return;

    System.out.println("\n");
    System.out.println("╔" + "═".repeat(138) + "╗");
    System.out.println("║" + centerText("QUERY LANGUAGE BENCHMARK COMPARISON", 138) + "║");
    System.out.println("╚" + "═".repeat(138) + "╝");
    System.out.println();

    // Get all query types (assumes all languages have same queries)
    final Set<String> queryTypes = benchmarkResults.values().iterator().next().keySet();

    for (final String queryType : queryTypes) {
      System.out.println("╔" + "═".repeat(138) + "╗");
      System.out.println("║" + centerText(queryType.toUpperCase(), 138) + "║");
      System.out.println("╠" + "═".repeat(30) + "╤" + ("═".repeat(26) + "╤").repeat(benchmarkResults.size() - 1) + "═".repeat(26) + "╣");

      // Header row
      final StringBuilder headerRow = new StringBuilder("║ " + padRight("Metric", 28) + " │");
      for (final String lang : benchmarkResults.keySet())
        headerRow.append(centerText(lang, 26)).append("│");
      headerRow.setLength(headerRow.length() - 1);
      headerRow.append("║");
      System.out.println(headerRow);

      System.out.println("╟" + "─".repeat(30) + "┼" + ("─".repeat(26) + "┼").repeat(benchmarkResults.size() - 1) + "─".repeat(26) + "╢");

      // Data rows
      printMetricRow("Min (μs)", queryType, r -> formatMicros(r.min));
      printMetricRow("P50 (μs)", queryType, r -> formatMicros(r.p50));
      printMetricRow("Avg (μs)", queryType, r -> formatMicros((long) r.avg));
      printMetricRow("P95 (μs)", queryType, r -> formatMicros(r.p95));
      printMetricRow("P99 (μs)", queryType, r -> formatMicros(r.p99));
      printMetricRow("Max (μs)", queryType, r -> formatMicros(r.max));

      System.out.println("╚" + "═".repeat(30) + "╧" + ("═".repeat(26) + "╧").repeat(benchmarkResults.size() - 1) + "═".repeat(26) + "╝");
      System.out.println();
    }

    // Summary
    System.out.println("=".repeat(80));
    System.out.println("SUMMARY");
    System.out.println("=".repeat(80));
    System.out.println("Configuration:");
    System.out.println("  - Vertices: " + NUM_VERTICES);
    System.out.println("  - Edges: " + NUM_EDGES);
    System.out.println("  - Iterations per query: " + NUM_ITERATIONS);
    System.out.println("  - Warmup iterations: " + WARMUP_ITERATIONS);
    System.out.println("=".repeat(80));
    System.out.println();
  }

  private void printMetricRow(final String metricName, final String queryType, final java.util.function.Function<BenchmarkResult, String> extractor) {
    final StringBuilder row = new StringBuilder("║ " + padRight(metricName, 28) + " │");
    for (final Map<String, BenchmarkResult> langResults : benchmarkResults.values()) {
      final BenchmarkResult result = langResults.get(queryType);
      if (result != null)
        row.append(centerText(extractor.apply(result), 26)).append("│");
      else
        row.append(centerText("-", 26)).append("│");
    }
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

  private static String formatMicros(final long micros) {
    if (micros >= 1_000_000)
      return String.format("%.2f s", micros / 1_000_000.0);
    if (micros >= 1_000)
      return String.format("%.2f ms", micros / 1_000.0);
    return String.format("%d μs", micros);
  }

  private static class BenchmarkResult {
    final long min;
    final long max;
    final double avg;
    final long p50;
    final long p95;
    final long p99;

    BenchmarkResult(final long min, final long max, final double avg, final long p50, final long p95, final long p99) {
      this.min = min;
      this.max = max;
      this.avg = avg;
      this.p50 = p50;
      this.p95 = p95;
      this.p99 = p99;
    }
  }
}
