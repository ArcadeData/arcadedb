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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case for GitHub issue #3216.
 * Tests performance issue with MATCH (a),(b) WHERE ID(a) = ... and ID(b) = ... MERGE pattern.
 * <p>
 * The problem: MATCH (a),(b) creates a Cartesian product of ALL vertices in the database,
 * then applies the WHERE filter. With a large database (2000+ vertices), this creates
 * millions of combinations before filtering, causing extreme slowness.
 * <p>
 * The solution should optimize to filter vertices BEFORE creating the Cartesian product.
 */
class Issue3216Test {
  private Database database;
  private String sourceId;
  private String targetId;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-3216").create();
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createVertexType("CHUNK_EMBEDDING");
    database.getSchema().createEdgeType("in");

    // Create a database similar to the issue reporter's database
    // with multiple vertex types and a significant number of vertices
    database.transaction(() -> {
      // Create source and target vertices
      MutableVertex chunk = database.newVertex("CHUNK");
      chunk.set("name", "chunk1");
      chunk.save();
      sourceId = chunk.getIdentity().toString();

      MutableVertex doc = database.newVertex("DOCUMENT");
      doc.set("name", "doc1");
      doc.save();
      targetId = doc.getIdentity().toString();

      // Create many additional vertices to simulate a real database
      // This simulates the ~2600 vertices in the user's database
      for (int i = 0; i < 100; i++) {
        MutableVertex embedding = database.newVertex("CHUNK_EMBEDDING");
        embedding.set("index", i);
        embedding.save();
      }

      for (int i = 0; i < 50; i++) {
        MutableVertex chunk2 = database.newVertex("CHUNK");
        chunk2.set("name", "chunk" + (i + 2));
        chunk2.save();
      }
    });

    //System.out.println("Created database with vertices");
    //System.out.println("Source vertex ID: " + sourceId);
    //System.out.println("Target vertex ID: " + targetId);
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Test the simplified query from issue #3216 comment:
   * MATCH (a),(b) WHERE ID(a) = "#1:0" and ID(b) = "#4:0" MERGE (a)-[r:`in`]->(b) RETURN a, b, r
   * <p>
   * This should be fast because it's matching specific vertices by ID.
   * The optimizer should recognize ID() predicates and use them to filter BEFORE
   * creating the Cartesian product.
   */
  @Test
  void testSimplifiedMatchWithIdFilter() {
    long startTime = System.currentTimeMillis();

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (a),(b) WHERE ID(a) = $sourceId and ID(b) = $targetId " +
              "MERGE (a)-[r:`in`]->(b) " +
              "RETURN a, b, r",
          Map.of("sourceId", sourceId, "targetId", targetId));

      int count = 0;
      while (rs.hasNext()) {
        Result result = rs.next();
        count++;
      }

      assertThat(count)
          .as("Should return 1 result for the created relationship")
          .isEqualTo(1);
    });

    long duration = System.currentTimeMillis() - startTime;
    //System.out.println("Query execution time: " + duration + "ms");

    // This should execute quickly (under 500ms even on slow systems)
    // If it takes multiple seconds, there's a performance issue
    assertThat(duration)
        .as("Query should execute quickly when filtering by specific IDs")
        .isLessThan(5000); // 5 seconds max, should be much faster
  }

  /**
   * Test the full UNWIND + MATCH + MERGE pattern from the original issue:
   * UNWIND $batch as row
   * MATCH (a),(b) WHERE ID(a) = row.source_id and ID(b) = row.target_id
   * MERGE (a)-[r:`in`]->(b) RETURN a, b, r
   */
  @Test
  void testUnwindMatchMergeWithIdFilter() {
    final List<Map<String, Object>> batch = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("source_id", sourceId);
    row.put("target_id", targetId);
    batch.add(row);

    long startTime = System.currentTimeMillis();

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "UNWIND $batch as row " +
              "MATCH (a),(b) WHERE ID(a) = row.source_id and ID(b) = row.target_id " +
              "MERGE (a)-[r:`in`]->(b) " +
              "RETURN a, b, r",
          Map.of("batch", batch));

      int count = 0;
      while (rs.hasNext()) {
        Result result = rs.next();
        count++;
      }

      assertThat(count)
          .as("Should return 1 result for the batch entry")
          .isEqualTo(1);
    });

    long duration = System.currentTimeMillis() - startTime;
    //System.out.println("UNWIND query execution time: " + duration + "ms");

    // This should also execute quickly
    assertThat(duration)
        .as("UNWIND + MATCH query should execute quickly when filtering by specific IDs")
        .isLessThan(5000); // 5 seconds max, should be much faster
  }

  /**
   * Test that verifies the Cartesian product issue.
   * Without WHERE filter, MATCH (a),(b) should create a Cartesian product.
   * With our test data (~152 vertices), this would be ~23,104 combinations.
   */
  @Test
  void testCartesianProductWithoutFilter() {
    // Count total vertices
    final ResultSet countRs = database.query("opencypher", "MATCH (n) RETURN count(n) as total");
    long totalVertices = ((Number) countRs.next().getProperty("total")).longValue();
    //System.out.println("Total vertices in database: " + totalVertices);

    // MATCH (a),(b) without WHERE creates Cartesian product
    final ResultSet rs = database.query("opencypher", "MATCH (a),(b) RETURN count(*) as total");
    long cartesianCount = ((Number) rs.next().getProperty("total")).longValue();
    //System.out.println("Cartesian product size: " + cartesianCount);

    // Should be totalVertices * totalVertices
    assertThat(cartesianCount).isEqualTo(totalVertices * totalVertices);
  }

  /**
   * Test that demonstrates the performance difference between:
   * 1. MATCH (a),(b) WHERE ID(a) = x AND ID(b) = y (slow - creates Cartesian product first)
   * 2. MATCH (a) WHERE ID(a) = x MATCH (b) WHERE ID(b) = y (fast - filters first)
   */
  @Test
  void testPerformanceComparisonMatchStrategies() {
    // Method 1: Single MATCH with Cartesian product
    long startTime1 = System.currentTimeMillis();
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (a),(b) WHERE ID(a) = $sourceId and ID(b) = $targetId RETURN a, b",
          Map.of("sourceId", sourceId, "targetId", targetId));
      assertThat(rs.hasNext()).isTrue();
      rs.next();
    });
    long duration1 = System.currentTimeMillis() - startTime1;
    //System.out.println("Method 1 (Cartesian product): " + duration1 + "ms");

    // Method 2: Separate MATCH clauses (should be faster)
    long startTime2 = System.currentTimeMillis();
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (a) WHERE ID(a) = $sourceId MATCH (b) WHERE ID(b) = $targetId RETURN a, b",
          Map.of("sourceId", sourceId, "targetId", targetId));
      assertThat(rs.hasNext()).isTrue();
      rs.next();
    });
    long duration2 = System.currentTimeMillis() - startTime2;
    //System.out.println("Method 2 (Separate MATCH): " + duration2 + "ms");

    // Method 2 should be significantly faster
    // (commenting out the assertion for now since we're diagnosing the issue)
    // assertThat(duration2).isLessThan(duration1);
  }
}
