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
package com.arcadedb.cypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

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
public class CypherEngineComparisonBenchmark {
  private Database database;

  private static final int PERSON_COUNT = 1000;
  private static final int COMPANY_COUNT = 50;
  private static final int RELATIONSHIPS_PER_PERSON = 10;

  private static final int WARMUP_ITERATIONS = 10;
  private static final int BENCHMARK_ITERATIONS = 100;

  @BeforeEach
  void setup() {
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

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
    GlobalConfiguration.GREMLIN_ENGINE.reset();
  }

  /**
   * Populates the database with test data.
   */
  private void populateTestData() {
    database.transaction(() -> {
      // Create companies
      for (int i = 0; i < COMPANY_COUNT; i++) {
        database.command("opencypher",
                "CREATE (c:Company {name: 'Company" + i + "', id: " + i + "})");
      }

      // Create persons
      for (int i = 0; i < PERSON_COUNT; i++) {
        database.command("opencypher",
                "CREATE (p:Person {name: 'Person" + i + "', id: " + i + ", age: " + (20 + (i % 50)) + "})");
      }

      // Create KNOWS relationships (social network)
      for (int i = 0; i < PERSON_COUNT; i++) {
        for (int j = 0; j < RELATIONSHIPS_PER_PERSON; j++) {
          final int targetId = (i + j + 1) % PERSON_COUNT;
          database.command("opencypher",
                  "MATCH (a:Person {id: " + i + "}), (b:Person {id: " + targetId + "}) " +
                          "CREATE (a)-[:KNOWS]->(b)");
        }
      }

      // Create WORKS_AT relationships (person to company)
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
    System.out.println(String.format("║ Legacy Cypher (Gremlin):  %,10d μs  (%,6d results)    ║", legacyAvgMicros, legacyResultCount));
    System.out.println(String.format("║ Native OpenCypher:        %,10d μs  (%,6d results)    ║", nativeAvgMicros, nativeResultCount));
    System.out.println("╠═══════════════════════════════════════════════════════════════════╣");

    if (speedup > 1.0) {
      System.out.println(String.format("║ Speedup: %.2fx FASTER ⚡                                         ║", speedup));
    } else if (speedup < 1.0) {
      System.out.println(String.format("║ Speedup: %.2fx SLOWER                                           ║", 1.0 / speedup));
    } else {
      System.out.println("║ Speedup: Same performance                                        ║");
    }
    System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
    System.out.println("║ Expected: " + String.format("%-56s", expectedOptimization) + "║");
    System.out.println("╚═══════════════════════════════════════════════════════════════════╝");
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
            "Benchmark 5: Cross-Type Relationship (Person->Company)",
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
            "Benchmark 6: Join Ordering (Start from selective Company filter)",
            query,
            WARMUP_ITERATIONS,
            10, // Fewer iterations
            "Should start from Company index seek (20 results) instead of Person scan (1000 results)"
    );
  }
}
