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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Comparison benchmark between Legacy Gremlin-based Cypher and Native OpenCypher.
 * <p>
 * This benchmark compares performance of:
 * 1. Index seek performance
 * 2. Full scan performance
 * 3. Relationship traversal
 * 4. Multi-hop patterns
 * 5. Join operations
 * <p>
 * Expected improvements with Cost-Based Optimizer:
 * - Index selection: 10-100x speedup
 * - ExpandInto: 5-10x speedup
 * - Join ordering: 10-100x speedup
 * <p>
 * Note: You may see an ANTLR warning about version mismatch (4.9.1 vs 4.13.2).
 * This is harmless - the legacy Cypher translator library was compiled with ANTLR 4.9.1,
 * but we use ANTLR 4.13.2 for native OpenCypher. ANTLR 4.13.2 is backward compatible.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("benchmark")
class CypherEngineComparisonBenchmark {
  private static Database database;

  /**
   * Holds benchmark result data for summary table
   */
  private static class BenchmarkResult {
    final String name;
    final long   legacyMicros;
    final long   nativeMicros;
    final double speedup;

    BenchmarkResult(String name, long legacyMicros, long nativeMicros, double speedup) {
      this.name = name;
      this.legacyMicros = legacyMicros;
      this.nativeMicros = nativeMicros;
      this.speedup = speedup;
    }
  }

  private final List<BenchmarkResult> results = new ArrayList<>();

  private static final int PERSON_COUNT             = 1000;
  private static final int COMPANY_COUNT            = 100;
  private static final int RELATIONSHIPS_PER_PERSON = 50;

  private static final int WARMUP_ITERATIONS    = 20;
  private static final int BENCHMARK_ITERATIONS = 100;

  @BeforeAll
  static void setup() {
    // Use Java engine for Gremlin (secure, not Groovy)
    GlobalConfiguration.GREMLIN_ENGINE.setValue("java");

    FileUtils.deleteRecursively(new File("./databases/test-comparison-benchmark"));
    database = new DatabaseFactory("./databases/test-comparison-benchmark").create();

    // Create schema with properties
    database.transaction(() -> {
      final var personType = database.getSchema().createVertexType("Person");
      personType.createProperty("id", Integer.class);
      personType.createProperty("name", String.class);
      personType.createProperty("age", Integer.class);

      final var companyType = database.getSchema().createVertexType("Company");
      companyType.createProperty("id", Integer.class);
      companyType.createProperty("name", String.class);

      database.getSchema().createEdgeType("KNOWS");
      database.getSchema().createEdgeType("WORKS_AT");
    });

    // Populate test data
    System.out.println("Creating test data...");
    populateTestData();
    System.out.println("Test data created.");

    // Create indexes AFTER data exists
    System.out.println("Creating indexes...");
    database.transaction(() -> {
      // Create index on Person.id for selective queries
      database.getSchema().getOrCreateVertexType("Person")
          .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

      // Create index on Company.name
      database.getSchema().getOrCreateVertexType("Company")
          .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
    });
    System.out.println("Indexes created.");
  }

  @AfterAll
  static void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
    GlobalConfiguration.GREMLIN_ENGINE.reset();
  }

  /**
   * Populates the database with test data.
   */
  private static void populateTestData() {
    database.transaction(() -> {
      // Create companies
      System.out.println("Creating " + COMPANY_COUNT + " companies...");
      for (int i = 0; i < COMPANY_COUNT; i++) {
        database.command("opencypher",
            "CREATE (c:Company {name: 'Company" + i + "', id: " + i + "})");
      }
    });

    database.transaction(() -> {
      // Create persons
      System.out.println("Creating " + PERSON_COUNT + " people...");
      for (int i = 0; i < PERSON_COUNT; i++) {
        database.command("opencypher",
            "CREATE (p:Person {name: 'Person" + i + "', id: " + i + ", age: " + (20 + (i % 50)) + "})");
      }
    });

    database.transaction(() -> {
      // Create KNOWS relationships (social network)
      System.out.println("Creating " + RELATIONSHIPS_PER_PERSON + " edges per person...");
      for (int i = 0; i < PERSON_COUNT; i++) {

        if ((i + 1) % 100 == 0)
          System.out.println("- " + i + "/" + PERSON_COUNT + " persons processed");

        for (int j = 0; j < RELATIONSHIPS_PER_PERSON; j++) {
          final int targetId = (i + j + 1) % PERSON_COUNT;
          database.command("opencypher",
              "MATCH (a:Person {id: " + i + "}), (b:Person {id: " + targetId + "}) " +
                  "CREATE (a)-[:KNOWS]->(b)");
        }
      }
    });

    database.transaction(() -> {
      // Create WORKS_AT relationships (person to company)
      System.out.println("Creating " + PERSON_COUNT + " WORKS_AT edges person/company...");
      for (int i = 0; i < PERSON_COUNT; i++) {
        final int companyId = i % COMPANY_COUNT;
        database.command("opencypher",
            "MATCH (p:Person {id: " + i + "}), (c:Company {id: " + companyId + "}) " +
                "CREATE (p)-[:WORKS_AT]->(c)");
      }
    });
  }

  /**
   * Helper method to benchmark and compare both Cypher engines.
   */
  private void compareEngines(final String benchmarkName, final String query,
                              final int warmupIterations, final int benchmarkIterations,
                              final String expectedOptimization) {
    System.out.println("\n╔═══════════════════════════════════════════════════════════════════╗");
    System.out.println("║ " + String.format("%-65s", benchmarkName) + " ║");
    System.out.println("╚═══════════════════════════════════════════════════════════════════╝");

    // Warmup for legacy engine
    System.out.println("\nWarming up Legacy Cypher engine...");
    for (int i = 0; i < warmupIterations; i++) {
      database.query("cypher", query).close();
    }

    // Benchmark legacy (Gremlin-based) Cypher
    System.out.println("Benchmarking Legacy Cypher (Gremlin-based)...");
    long legacyStart = System.nanoTime();
    int legacyResultCount = 0;
    for (int i = 0; i < benchmarkIterations; i++) {
      final ResultSet rs = database.query("cypher", query);
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      if (i == 0) {
        legacyResultCount = count;
      }
      rs.close();
    }
    long legacyTime = System.nanoTime() - legacyStart;

    // Warmup for native engine
    System.out.println("Warming up Native OpenCypher engine...");
    for (int i = 0; i < warmupIterations; i++) {
      database.query("opencypher", query).close();
    }

    // Benchmark native OpenCypher
    System.out.println("Benchmarking Native OpenCypher (Optimized)...");
    long nativeStart = System.nanoTime();
    int nativeResultCount = 0;
    for (int i = 0; i < benchmarkIterations; i++) {
      final ResultSet rs = database.query("opencypher", query);
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      if (i == 0) {
        nativeResultCount = count;
      }
      rs.close();
    }
    long nativeTime = System.nanoTime() - nativeStart;

    // Calculate and display results
    final long legacyAvgMicros = legacyTime / benchmarkIterations / 1_000;
    final long nativeAvgMicros = nativeTime / benchmarkIterations / 1_000;
    final double speedup = (double) legacyTime / nativeTime;

    System.out.println("\n╔═══════════════════════════════════════════════════════════════════╗");
    System.out.println("║                      PERFORMANCE COMPARISON                       ║");
    System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
    System.out.println(String.format("║ Legacy Cypher (Gremlin):  %,10d μs  (%,6d results)    ║", legacyAvgMicros,
        legacyResultCount));
    System.out.println(String.format("║ Native OpenCypher:        %,10d μs  (%,6d results)    ║", nativeAvgMicros,
        nativeResultCount));
    System.out.println("╠═══════════════════════════════════════════════════════════════════╣");

    if (speedup > 1.0) {
      System.out.println(String.format("║ Speedup: %.2fx FASTER ⚡                                         ║", speedup));
    } else if (speedup < 1.0) {
      System.out.println(String.format("║ Speedup: %.2fx SLOWER                                           ║",
          1.0 / speedup));
    } else {
      System.out.println("║ Speedup: Same performance                                        ║");
    }
    System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
    System.out.println("║ Expected: " + String.format("%-56s", expectedOptimization) + "║");
    System.out.println("╚═══════════════════════════════════════════════════════════════════╝");

    // Store result for summary table
    results.add(new BenchmarkResult(benchmarkName, legacyAvgMicros, nativeAvgMicros, speedup));
  }

  @Test
  void compareIndexSeek() {
    // Use WHERE clause to allow optimizer
    final String query = "MATCH (p:Person) WHERE p.id = 500 RETURN p";

    compareEngines(
        "Benchmark 1: Index Seek (Selective Query)",
        query,
        WARMUP_ITERATIONS,
        BENCHMARK_ITERATIONS,
        "Should use index seek (cost ~5-10, rows ~1)"
    );
  }

  @Test
  void compareFullScan() {
    final String query = "MATCH (p:Person) WHERE p.age > 30 RETURN p";

    compareEngines(
        "Benchmark 2: Full Scan (Non-Selective Query)",
        query,
        WARMUP_ITERATIONS,
        10, // Fewer iterations for full scan
        "Should use NodeByLabelScan with filter"
    );
  }

  @Test
  void compareRelationshipTraversal() {
    // Use WHERE clause to allow optimizer
    final String query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.id = 100 RETURN b";

    compareEngines(
        "Benchmark 3: Relationship Traversal",
        query,
        WARMUP_ITERATIONS,
        BENCHMARK_ITERATIONS,
        "Index seek on Person.id + ExpandAll"
    );
  }

  @Test
  void compareMultiHopPattern() {
    // Use WHERE clause to allow optimizer
    final String query = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a.id = 100 RETURN c";

    compareEngines(
        "Benchmark 4: Multi-Hop Pattern (2-hop traversal)",
        query,
        5, // Fewer warmup iterations
        10, // Fewer benchmark iterations
        "Index seek + 2x ExpandAll"
    );
  }

  @Test
  void compareCrossTypeRelationship() {
    // Use WHERE clause to allow optimizer
    final String query = "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) WHERE p.id = 100 RETURN c";

    compareEngines(
        "Benchmark 5: Cross-Type Relationship",
        query,
        WARMUP_ITERATIONS,
        BENCHMARK_ITERATIONS,
        "Index seek + ExpandAll with label filter"
    );
  }

  @Test
  void compareJoinOrdering() {
    // Use WHERE clause to allow optimizer
    final String query = "MATCH (p:Person)-[:WORKS_AT]->(c:Company) WHERE c.name = 'Company5' RETURN p";

    compareEngines(
        "Benchmark 6: Join Ordering",
        query,
        WARMUP_ITERATIONS,
        10, // Fewer iterations
        "Should start from Company index seek (20 results) instead of Person scan (1000 results)"
    );
  }

  @AfterAll
  void printSummary() {
    System.out.println("\n\n");
    System.out.println(
        "╔═════════════════════════════════════════════════════════════════════════════════════════════════╗");
    System.out.println(
        "║                               BENCHMARK RESULTS SUMMARY                                         ║");
    System.out.println(
        "╠═════════════════════════════════════════════════════════════════════════════════════════════════╣");
    System.out.println(
        "║ Query Type                          │  Legacy (µs) │  Native (µs) │    Diff (µs) │    Speedup   ║");
    System.out.println(
        "╟─────────────────────────────────────┼──────────────┼──────────────┼──────────────┼──────────────╢");

    long totalLegacy = 0;
    long totalNative = 0;

    for (BenchmarkResult result : results) {
      totalLegacy += result.legacyMicros;
      totalNative += result.nativeMicros;
      final long diff = result.legacyMicros - result.nativeMicros;

      // Extract short name (remove "Benchmark N: " prefix)
      String shortName = result.name;
      if (shortName.contains(":")) {
        shortName = shortName.substring(shortName.indexOf(":") + 1).trim();
      }

      System.out.println(String.format("║ %-35s │ %,12d │ %,12d │ %,12d │ %,9.2fx ⚡ ║",
          shortName,
          result.legacyMicros,
          result.nativeMicros,
          diff,
          result.speedup));
    }

    final long totalDiff = totalLegacy - totalNative;
    final double totalSpeedup = (double) totalLegacy / totalNative;

    System.out.println(
        "╟─────────────────────────────────────┼──────────────┼──────────────┼──────────────┼──────────────╢");
    System.out.println(String.format("║ %-35s │ %,12d │ %,12d │ %,12d │ %,9.2fx ⚡ ║",
        "TOTAL",
        totalLegacy,
        totalNative,
        totalDiff,
        totalSpeedup));
    System.out.println(
        "╚═════════════════════════════════════════════════════════════════════════════════════════════════╝");
    System.out.println();
    System.out.println(String.format("Overall: Native OpenCypher is %.2fx faster than Legacy Cypher (Gremlin-based)",
        totalSpeedup));
    System.out.println();
  }
}

/**
 * ==========================================================================================
 * BENCHMARK RESULTS SUMMARY
 * ==========================================================================================
 * Query Type                     │  Legacy (µs) │  Native (µs) │    Diff (µs) │     Speedup
 * ------------------------------─┼─------------─┼─------------─┼─------------─┼─------------
 * Index Seek (Selective)         │        4,991 │           32 │       -4,959 │   153.17x ⚡
 * Full Scan (Non-Selective)      │        9,893 │        2,367 │       -7,526 │     4.18x ⚡
 * Relationship Traversal         │        5,087 │           47 │       -5,040 │   107.09x ⚡
 * Multi-Hop Pattern (2-hop)      │       15,388 │          715 │      -14,673 │    21.51x ⚡
 * Cross-Type Relationship        │        5,510 │           33 │       -5,477 │   163.44x ⚡
 * Join Ordering (Selective)      │       26,833 │        1,182 │      -25,651 │    22.68x ⚡
 * ------------------------------─┼─------------─┼─------------─┼─------------─┼─------------
 * TOTAL                          │       67,702 │        4,376 │      -63,326 │    15.47x ⚡
 * ==========================================================================================
 * Overall: Native OpenCypher is 15.47x faster than Legacy Cypher (Gremlin-based)
 * ==========================================================================================
 * <p>
 * Key Observations:
 * <p>
 * 1. Index Seek Performance: 153x speedup
 * - Native OpenCypher's cost-based optimizer correctly identifies and uses index seeks
 * - Legacy Gremlin-based approach has higher overhead for simple indexed lookups
 * <p>
 * 2. Cross-Type Relationship: 163x speedup
 * - Demonstrates the power of proper query planning with index-based starting points
 * - Legacy approach suffers from lack of optimization
 * <p>
 * 3. Relationship Traversal: 107x speedup
 * - Native implementation has optimized graph traversal operations
 * - Lower overhead per edge traversal
 * <p>
 * 4. Multi-Hop Pattern: 21.5x speedup
 * - Multi-step traversals benefit from optimized expand operations
 * - Cost-based optimizer makes better decisions about traversal order
 * <p>
 * 5. Join Ordering: 22.7x speedup
 * - Cost-based optimizer starts from the selective side (Company with 20 results)
 * - Legacy approach likely starts from Person scan (1000 results)
 * - This demonstrates the importance of proper join ordering
 * <p>
 * 6. Full Scan: 4.2x speedup (smallest improvement)
 * - Even simple full scans benefit from native implementation
 * - Lower per-row processing overhead
 * <p>
 * Conclusion:
 * The Native OpenCypher engine with its cost-based optimizer delivers massive performance
 * improvements across all query types, with particularly dramatic gains for queries that
 * benefit from index usage and optimal join ordering (15-163x faster). Even simple scans
 * show meaningful improvements (4x faster).
 */
