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
package com.arcadedb.opencypher;

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
 * Performance benchmark comparing Native OpenCypher vs Legacy Gremlin-based Cypher.
 * <p>
 * This benchmark measures:
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
 */
public class CypherPerformanceBenchmark {
  private Database database;

  private static final int PERSON_COUNT = 1000;
  private static final int COMPANY_COUNT = 50;
  private static final int RELATIONSHIPS_PER_PERSON = 10;

  private static final int WARMUP_ITERATIONS = 10;
  private static final int BENCHMARK_ITERATIONS = 100;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File("./databases/test-benchmark"));
    database = new DatabaseFactory("./databases/test-benchmark").create();

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
   * Helper method to benchmark a query on both engines and compare performance.
   */
  private void compareEngines(final String benchmarkName, final String query,
                              final int warmupIterations, final int benchmarkIterations,
                              final String expectedOptimization) {
    System.out.println("\n=== " + benchmarkName + " ===");

    // Check if legacy engine is available (requires gremlin module)
    boolean legacyAvailable = false;
    try {
      database.query("cypher", "MATCH (n) RETURN n LIMIT 1").close();
      legacyAvailable = true;
    } catch (final IllegalArgumentException e) {
      System.out.println("Legacy Cypher engine not available (requires gremlin module)");
      System.out.println("Benchmarking Native OpenCypher only...");
    }

    Long legacyTime = null;
    if (legacyAvailable) {
      // Warmup for legacy engine
      for (int i = 0; i < warmupIterations; i++) {
        database.query("cypher", query).close();
      }

      // Benchmark legacy (Gremlin-based) Cypher
      long legacyStart = System.nanoTime();
      for (int i = 0; i < benchmarkIterations; i++) {
        final ResultSet rs = database.query("cypher", query);
        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }
        rs.close();
      }
      legacyTime = System.nanoTime() - legacyStart;
    }

    // Warmup for native engine
    for (int i = 0; i < warmupIterations; i++) {
      database.query("opencypher", query).close();
    }

    // Benchmark native OpenCypher
    long nativeStart = System.nanoTime();
    for (int i = 0; i < benchmarkIterations; i++) {
      final ResultSet rs = database.query("opencypher", query);
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      rs.close();
    }
    long nativeTime = System.nanoTime() - nativeStart;

    // Show EXPLAIN output for native engine
    System.out.println("\nNative OpenCypher EXPLAIN:");
    final ResultSet explainResult = database.query("opencypher", "EXPLAIN " + query);
    while (explainResult.hasNext()) {
      System.out.println((String) explainResult.next().getProperty("plan"));
    }
    explainResult.close();

    // Calculate and display results
    final long nativeAvgMicros = nativeTime / benchmarkIterations / 1_000;

    System.out.println("\n--- Performance Comparison ---");
    if (legacyTime != null) {
      final long legacyAvgMicros = legacyTime / benchmarkIterations / 1_000;
      final double speedup = (double) legacyTime / nativeTime;

      System.out.println("Legacy Cypher (Gremlin-based): " + legacyAvgMicros + " μs");
      System.out.println("Native OpenCypher (Optimized): " + nativeAvgMicros + " μs");

      if (speedup > 1.0) {
        System.out.println("Speedup: " + String.format("%.2fx", speedup) + " faster ⚡");
      } else if (speedup < 1.0) {
        System.out.println("Speedup: " + String.format("%.2fx", 1.0 / speedup) + " slower");
      } else {
        System.out.println("Speedup: Same performance");
      }
    } else {
      System.out.println("Native OpenCypher (Optimized): " + nativeAvgMicros + " μs");
    }

    System.out.println("Expected: " + expectedOptimization);
  }

  @Test
  void benchmarkIndexSeek() {
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
  void benchmarkFullScan() {
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
  void benchmarkRelationshipTraversal() {
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
  void benchmarkMultiHopPattern() {
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
  void benchmarkCrossTypeRelationship() {
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
  void benchmarkJoinOrdering() {
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
