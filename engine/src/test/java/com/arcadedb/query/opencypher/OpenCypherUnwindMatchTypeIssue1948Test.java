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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for GitHub Issue #1948: UNWIND + MATCH with type check returns incomplete results.
 * <p>
 * When executing a Cypher query that combines UNWIND, a type constraint on a node match,
 * and batch processing where multiple batch entries share the same source_id,
 * ArcadeDB should return results for all batch entries, not just the first one.
 * <p>
 * Bug: With type constraint (a:CHUNK), only 1 result returned instead of expected 2.
 * Workaround: Removing type constraint makes the query work correctly.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/1948">Issue #1948</a>
 */
public class OpenCypherUnwindMatchTypeIssue1948Test {
  private static Database database;
  private static final String DB_PATH = "./target/test-databases/issue-1948-test";

  @BeforeAll
  public static void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterAll
  public static void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @BeforeEach
  void setupTypes() {
    database.transaction(() -> {
      // Create types if they don't exist
      if (!database.getSchema().existsType("CHUNK")) {
        database.getSchema().createVertexType("CHUNK");
      }
      if (!database.getSchema().existsType("TARGET")) {
        database.getSchema().createVertexType("TARGET");
      }
      if (!database.getSchema().existsType("TEST_EDGE")) {
        database.getSchema().createEdgeType("TEST_EDGE");
      }
    });
  }

  @AfterEach
  void cleanupData() {
    // Clean up any edges and vertices created during tests
    database.transaction(() -> {
      try {
        database.command("sql", "DELETE FROM TEST_EDGE");
      } catch (Exception e) {
        // Ignore if type doesn't exist
      }
    });
  }

  /**
   * This test reproduces the exact bug from Issue #1948.
   *
   * The problematic query:
   * UNWIND $batch as row
   * MATCH (a:CHUNK) WHERE ID(a) = row.source_id
   * MATCH (b) WHERE ID(b) = row.target_id
   * MERGE (a)-[r:TEST_EDGE]->(b)
   * RETURN a, b, r
   *
   * With batch containing 2 entries with the same source_id but different target_ids,
   * only 1 result is returned instead of 2.
   */
  @Test
  void testUnwindMatchWithTypeConstraintSameSource() {
    // Create test vertices
    RID chunkRid = null;
    RID target1Rid = null;
    RID target2Rid = null;

    database.transaction(() -> {
      database.command("opencypher", "CREATE (c:CHUNK {name: 'chunk1'})");
      database.command("opencypher", "CREATE (t1:TARGET {name: 'target1'})");
      database.command("opencypher", "CREATE (t2:TARGET {name: 'target2'})");
    });

    // Get the RIDs
    try (ResultSet rs = database.query("opencypher", "MATCH (c:CHUNK {name: 'chunk1'}) RETURN c")) {
      assertTrue(rs.hasNext(), "CHUNK vertex should exist");
      Vertex chunk = (Vertex) rs.next().toElement();
      chunkRid = chunk.getIdentity();
    }

    try (ResultSet rs = database.query("opencypher", "MATCH (t:TARGET {name: 'target1'}) RETURN t")) {
      assertTrue(rs.hasNext(), "TARGET1 vertex should exist");
      Vertex target1 = (Vertex) rs.next().toElement();
      target1Rid = target1.getIdentity();
    }

    try (ResultSet rs = database.query("opencypher", "MATCH (t:TARGET {name: 'target2'}) RETURN t")) {
      assertTrue(rs.hasNext(), "TARGET2 vertex should exist");
      Vertex target2 = (Vertex) rs.next().toElement();
      target2Rid = target2.getIdentity();
    }

    // Create batch with 2 entries having the same source_id but different target_ids
    final List<Map<String, Object>> batch = new ArrayList<>();
    final RID finalChunkRid = chunkRid;
    final RID finalTarget1Rid = target1Rid;
    final RID finalTarget2Rid = target2Rid;
    batch.add(Map.of("source_id", finalChunkRid.toString(), "target_id", finalTarget1Rid.toString()));
    batch.add(Map.of("source_id", finalChunkRid.toString(), "target_id", finalTarget2Rid.toString()));

    // Execute the query with type constraint - THIS IS THE BUG
    final List<Map<String, Object>> resultsWithType = new ArrayList<>();
    database.transaction(() -> {
      final String queryWithType =
          "UNWIND $batch as row " +
          "MATCH (a:CHUNK) WHERE ID(a) = row.source_id " +
          "MATCH (b) WHERE ID(b) = row.target_id " +
          "MERGE (a)-[r:TEST_EDGE]->(b) " +
          "RETURN a, b, r";

      try (ResultSet rs = database.command("opencypher", queryWithType, Map.of("batch", batch))) {
        while (rs.hasNext()) {
          var row = rs.next();
          resultsWithType.add(Map.of(
              "a", row.getProperty("a"),
              "b", row.getProperty("b"),
              "r", row.getProperty("r")));
        }
      }
    });

    // Bug: With type constraint, only 1 result is returned
    // Expected: 2 results (one for each batch entry)
    assertEquals(2, resultsWithType.size(),
        "With type constraint (a:CHUNK), should return 2 results for 2 batch entries with same source_id");
  }

  /**
   * This test verifies that removing the type constraint works correctly (workaround).
   */
  @Test
  void testUnwindMatchWithoutTypeConstraintSameSource() {
    // Create test vertices
    RID chunkRid = null;
    RID target1Rid = null;
    RID target2Rid = null;

    database.transaction(() -> {
      database.command("opencypher", "CREATE (c:CHUNK {name: 'chunk2'})");
      database.command("opencypher", "CREATE (t1:TARGET {name: 'target3'})");
      database.command("opencypher", "CREATE (t2:TARGET {name: 'target4'})");
    });

    // Get the RIDs
    try (ResultSet rs = database.query("opencypher", "MATCH (c:CHUNK {name: 'chunk2'}) RETURN c")) {
      assertTrue(rs.hasNext(), "CHUNK vertex should exist");
      Vertex chunk = (Vertex) rs.next().toElement();
      chunkRid = chunk.getIdentity();
    }

    try (ResultSet rs = database.query("opencypher", "MATCH (t:TARGET {name: 'target3'}) RETURN t")) {
      assertTrue(rs.hasNext(), "TARGET vertex should exist");
      Vertex target1 = (Vertex) rs.next().toElement();
      target1Rid = target1.getIdentity();
    }

    try (ResultSet rs = database.query("opencypher", "MATCH (t:TARGET {name: 'target4'}) RETURN t")) {
      assertTrue(rs.hasNext(), "TARGET vertex should exist");
      Vertex target2 = (Vertex) rs.next().toElement();
      target2Rid = target2.getIdentity();
    }

    // Create batch with 2 entries having the same source_id but different target_ids
    final List<Map<String, Object>> batch = new ArrayList<>();
    final RID finalChunkRid = chunkRid;
    final RID finalTarget1Rid = target1Rid;
    final RID finalTarget2Rid = target2Rid;
    batch.add(Map.of("source_id", finalChunkRid.toString(), "target_id", finalTarget1Rid.toString()));
    batch.add(Map.of("source_id", finalChunkRid.toString(), "target_id", finalTarget2Rid.toString()));

    // Execute the query WITHOUT type constraint - THIS WORKS
    final List<Map<String, Object>> resultsWithoutType = new ArrayList<>();
    database.transaction(() -> {
      final String queryWithoutType =
          "UNWIND $batch as row " +
          "MATCH (a) WHERE ID(a) = row.source_id " +
          "MATCH (b) WHERE ID(b) = row.target_id " +
          "MERGE (a)-[r:TEST_EDGE]->(b) " +
          "RETURN a, b, r";

      try (ResultSet rs = database.command("opencypher", queryWithoutType, Map.of("batch", batch))) {
        while (rs.hasNext()) {
          var row = rs.next();
          resultsWithoutType.add(Map.of(
              "a", row.getProperty("a"),
              "b", row.getProperty("b"),
              "r", row.getProperty("r")));
        }
      }
    });

    // Without type constraint, correctly returns 2 results
    assertEquals(2, resultsWithoutType.size(),
        "Without type constraint, should return 2 results for 2 batch entries with same source_id");
  }

  /**
   * Simpler test: UNWIND + MATCH with type constraint, verifying the step chain works correctly.
   */
  @Test
  void testUnwindMatchWithTypeConstraintSimple() {
    // Create test vertices
    database.transaction(() -> {
      database.command("opencypher", "CREATE (c:CHUNK {name: 'simple_chunk'})");
    });

    final RID chunkRid;
    try (ResultSet rs = database.query("opencypher", "MATCH (c:CHUNK {name: 'simple_chunk'}) RETURN c")) {
      assertTrue(rs.hasNext());
      chunkRid = ((Vertex) rs.next().toElement()).getIdentity();
    }

    // Create batch with 3 entries all referencing the same CHUNK
    final List<Map<String, Object>> batch = new ArrayList<>();
    batch.add(Map.of("id", chunkRid.toString()));
    batch.add(Map.of("id", chunkRid.toString()));
    batch.add(Map.of("id", chunkRid.toString()));

    // Simple query: UNWIND + MATCH with type constraint + RETURN
    final String query =
        "UNWIND $batch as row " +
        "MATCH (a:CHUNK) WHERE ID(a) = row.id " +
        "RETURN a, row";

    final List<Object> results = new ArrayList<>();
    try (ResultSet rs = database.query("opencypher", query, Map.of("batch", batch))) {
      while (rs.hasNext()) {
        var row = rs.next();
        results.add(row.getProperty("a"));
      }
    }

    // Should return 3 results (one for each batch entry)
    assertEquals(3, results.size(),
        "UNWIND + MATCH with type constraint should return one result per batch entry");
  }

  /**
   * Test with different source IDs to verify basic UNWIND + MATCH + type constraint works.
   */
  @Test
  void testUnwindMatchWithTypeConstraintDifferentSources() {
    // Create multiple CHUNK vertices
    database.transaction(() -> {
      database.command("opencypher", "CREATE (c1:CHUNK {name: 'diff_chunk1'})");
      database.command("opencypher", "CREATE (c2:CHUNK {name: 'diff_chunk2'})");
    });

    final RID chunk1Rid;
    final RID chunk2Rid;

    try (ResultSet rs = database.query("opencypher", "MATCH (c:CHUNK {name: 'diff_chunk1'}) RETURN c")) {
      assertTrue(rs.hasNext());
      chunk1Rid = ((Vertex) rs.next().toElement()).getIdentity();
    }

    try (ResultSet rs = database.query("opencypher", "MATCH (c:CHUNK {name: 'diff_chunk2'}) RETURN c")) {
      assertTrue(rs.hasNext());
      chunk2Rid = ((Vertex) rs.next().toElement()).getIdentity();
    }

    // Create batch with 2 entries with DIFFERENT source_ids
    final List<Map<String, Object>> batch = new ArrayList<>();
    batch.add(Map.of("id", chunk1Rid.toString()));
    batch.add(Map.of("id", chunk2Rid.toString()));

    final String query =
        "UNWIND $batch as row " +
        "MATCH (a:CHUNK) WHERE ID(a) = row.id " +
        "RETURN a";

    final List<Object> results = new ArrayList<>();
    try (ResultSet rs = database.query("opencypher", query, Map.of("batch", batch))) {
      while (rs.hasNext()) {
        var row = rs.next();
        results.add(row.getProperty("a"));
      }
    }

    // Should return 2 results (one for each batch entry)
    assertEquals(2, results.size(),
        "UNWIND + MATCH with type constraint should work with different source IDs");
  }
}
