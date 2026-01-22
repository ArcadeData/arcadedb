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
 * Test case for GitHub issue #3138.
 * Tests UNWIND + MATCH + MERGE pattern for batch relationship creation.
 * <p>
 * The query pattern is:
 * UNWIND $batch as row
 * MATCH (a),(b) WHERE ID(a) = row.source_id and ID(b) = row.target_id
 * MERGE (a)-[r:in]->(b) RETURN a, b, r
 */
class Issue3138Test {
  private Database     database;
  private List<String> sourceIds;
  private String       targetId;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-3138").create();
    database.getSchema().createVertexType("Source");
    database.getSchema().createVertexType("Target");
    database.getSchema().createEdgeType("in");

    // Create test vertices
    sourceIds = new ArrayList<>();
    database.transaction(() -> {
      // Create source vertices
      for (int i = 0; i < 3; i++) {
        MutableVertex source = database.newVertex("Source");
        source.set("name", "source" + i);
        source.save();
        sourceIds.add(source.getIdentity().toString());
      }

      // Create target vertex
      MutableVertex target = database.newVertex("Target");
      target.set("name", "target");
      target.save();
      targetId = target.getIdentity().toString();
    });

    ////System.out.println("Created source vertices: " + sourceIds);
    ////System.out.println("Created target vertex: " + targetId);
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testUnwindMatchMergeCreateRelations() {
    // Build batch parameter with source and target IDs
    final List<Map<String, Object>> batch = new ArrayList<>();
    for (String sourceId : sourceIds) {
      Map<String, Object> row = new HashMap<>();
      row.put("source_id", sourceId);
      row.put("target_id", targetId);
      batch.add(row);
    }

    //System.out.println("Batch parameter: " + batch);

    // Execute the UNWIND + MATCH + MERGE query
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "UNWIND $batch as row " +
              "MATCH (a),(b) WHERE ID(a) = row.source_id AND ID(b) = row.target_id " +
              "MERGE (a)-[r:in]->(b) " +
              "RETURN a, b, r",
          Map.of("batch", batch));

      int count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        //System.out.println("Result " + count + ": " + result.toJSON());
        count++;
      }

      // Should return 3 results (one for each source vertex)
      assertThat(count)
          .as("Should return 3 results, one for each relationship created")
          .isEqualTo(3);
    });

    // Verify relationships were created
    final ResultSet verifyResult = database.query("opencypher",
        "MATCH (a:Source)-[r:in]->(b:Target) RETURN count(r) AS count");
    final long edgeCount = ((Number) verifyResult.next().getProperty("count")).longValue();

    assertThat(edgeCount)
        .as("Should have created 3 relationships")
        .isEqualTo(3);
  }

  @Test
  void testUnwindMatchMergeWithCreate() {
    // Alternative query using CREATE instead of MERGE
    final List<Map<String, Object>> batch = new ArrayList<>();
    for (String sourceId : sourceIds) {
      Map<String, Object> row = new HashMap<>();
      row.put("source_id", sourceId);
      row.put("target_id", targetId);
      batch.add(row);
    }

    //System.out.println("Testing CREATE instead of MERGE");

    // Execute the UNWIND + MATCH + CREATE query
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "UNWIND $batch as row " +
              "MATCH (a),(b) WHERE ID(a) = row.source_id AND ID(b) = row.target_id " +
              "CREATE (a)-[r:in]->(b) " +
              "RETURN a, b, r",
          Map.of("batch", batch));

      int count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        //System.out.println("Result " + count + ": " + result.toJSON());
        count++;
      }

      assertThat(count)
          .as("Should return 3 results, one for each relationship created")
          .isEqualTo(3);
    });

    // Verify relationships were created
    final ResultSet verifyResult = database.query("opencypher",
        "MATCH (a:Source)-[r:in]->(b:Target) RETURN count(r) AS count");
    final long edgeCount = ((Number) verifyResult.next().getProperty("count")).longValue();

    assertThat(edgeCount)
        .as("Should have created 3 relationships")
        .isEqualTo(3);
  }

  @Test
  void testSimplifiedUnwindMatchReturnOnly() {
    // First test just UNWIND + MATCH to see if that works
    final List<Map<String, Object>> batch = new ArrayList<>();
    for (String sourceId : sourceIds) {
      Map<String, Object> row = new HashMap<>();
      row.put("source_id", sourceId);
      row.put("target_id", targetId);
      batch.add(row);
    }

    //System.out.println("Testing UNWIND + MATCH only (no MERGE/CREATE)");

    // Execute just UNWIND + MATCH + RETURN
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "UNWIND $batch as row " +
              "MATCH (a),(b) WHERE ID(a) = row.source_id AND ID(b) = row.target_id " +
              "RETURN a, b",
          Map.of("batch", batch));

      int count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        //System.out.println("Result " + count + ": " + result.toJSON());
        count++;
      }

      assertThat(count)
          .as("Should return 3 results from UNWIND + MATCH")
          .isEqualTo(3);
    });
  }

  @Test
  void testUnwindOnly() {
    // Test UNWIND alone to ensure it works
    final List<Map<String, Object>> batch = new ArrayList<>();
    for (String sourceId : sourceIds) {
      Map<String, Object> row = new HashMap<>();
      row.put("source_id", sourceId);
      row.put("target_id", targetId);
      batch.add(row);
    }

    //System.out.println("Testing UNWIND only");

    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "UNWIND $batch as row RETURN row.source_id as sid, row.target_id as tid",
          Map.of("batch", batch));

      int count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        //System.out.println("UNWIND Result " + count + ": " + result.toJSON());
        count++;
      }

      assertThat(count)
          .as("UNWIND should produce 3 rows")
          .isEqualTo(3);
    });
  }

  @Test
  void testUnwindMatchSingleNode() {
    // Test UNWIND + MATCH with single node (using label)
    final List<Map<String, Object>> batch = new ArrayList<>();
    for (String sourceId : sourceIds) {
      Map<String, Object> row = new HashMap<>();
      row.put("source_id", sourceId);
      batch.add(row);
    }

    //System.out.println("Testing UNWIND + MATCH single node with label");

    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "UNWIND $batch as row MATCH (a:Source) WHERE ID(a) = row.source_id RETURN a",
          Map.of("batch", batch));

      int count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        //System.out.println("MATCH Result " + count + ": " + result.toJSON());
        count++;
      }

      assertThat(count)
          .as("Should return 3 matched nodes")
          .isEqualTo(3);
    });
  }

  @Test
  void testUnwindMatchNoWhere() {
    // Test UNWIND + MATCH without WHERE to see if they produce rows
    final List<Map<String, Object>> batch = new ArrayList<>();
    for (String sourceId : sourceIds) {
      Map<String, Object> row = new HashMap<>();
      row.put("source_id", sourceId);
      batch.add(row);
    }

    //System.out.println("Testing UNWIND + MATCH (no WHERE)");
    //System.out.println("Batch: " + batch);

    database.transaction(() -> {
      // First check how many Source vertices exist
      final ResultSet countRs = database.query("opencypher",
          "MATCH (a:Source) RETURN count(a) as c");
      //System.out.println("Source vertex count: " + countRs.next().getProperty("c"));

      // Now test UNWIND + MATCH
      final ResultSet rs = database.query("opencypher",
          "UNWIND $batch as row MATCH (a:Source) RETURN row.source_id as sid, ID(a) as aid",
          Map.of("batch", batch));

      int count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        //System.out.println("Result " + count + ": sid=" + result.getProperty("sid") + ", aid=" + result.getProperty("aid"));
        count++;
      }

      // Should return 3 rows x 3 vertices = 9 results (Cartesian product)
      assertThat(count)
          .as("Should return 9 results (3 batch rows x 3 vertices)")
          .isEqualTo(9);
    });
  }

  @Test
  void testUnwindMatchWithSimpleWhere() {
    // Test UNWIND + MATCH with a simple WHERE comparison
    final List<Map<String, Object>> batch = new ArrayList<>();
    for (String sourceId : sourceIds) {
      Map<String, Object> row = new HashMap<>();
      row.put("source_id", sourceId);
      batch.add(row);
    }

    //System.out.println("Testing UNWIND + MATCH with WHERE row.source_id = row.source_id");

    database.transaction(() -> {
      // This should always be true
      final ResultSet rs = database.query("opencypher",
          "UNWIND $batch as row MATCH (a:Source) WHERE row.source_id = row.source_id RETURN a",
          Map.of("batch", batch));

      int count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        //System.out.println("Result " + count + ": " + result.toJSON());
        count++;
      }

      assertThat(count)
          .as("WHERE row.source_id = row.source_id should return 9 results (always true)")
          .isEqualTo(9);
    });
  }

  @Test
  void testUnwindMatchWithIdEqual() {
    // Test UNWIND + MATCH with WHERE ID(a) = ID(a)
    final List<Map<String, Object>> batch = new ArrayList<>();
    for (String sourceId : sourceIds) {
      Map<String, Object> row = new HashMap<>();
      row.put("source_id", sourceId);
      batch.add(row);
    }

    //System.out.println("Testing UNWIND + MATCH with WHERE ID(a) = ID(a)");

    database.transaction(() -> {
      // ID(a) = ID(a) should always be true
      final ResultSet rs = database.query("opencypher",
          "UNWIND $batch as row MATCH (a:Source) WHERE ID(a) = ID(a) RETURN a",
          Map.of("batch", batch));

      int count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        //System.out.println("Result " + count + ": " + result.toJSON());
        count++;
      }

      assertThat(count)
          .as("WHERE ID(a) = ID(a) should return 9 results (always true)")
          .isEqualTo(9);
    });
  }

  @Test
  void testUnwindMatchWithIdEqualsString() {
    // Test UNWIND + MATCH with WHERE ID(a) = literal string
    //System.out.println("Testing UNWIND + MATCH with WHERE ID(a) = '#1:0'");
    //System.out.println("First source ID: " + sourceIds.get(0));

    database.transaction(() -> {
      // ID(a) = '#1:0' should match one Source vertex
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:Source) WHERE ID(a) = '#1:0' RETURN a");

      int count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        //System.out.println("Result " + count + ": " + result.toJSON());
        count++;
      }

      assertThat(count)
          .as("WHERE ID(a) = '#1:0' should return 1 result")
          .isEqualTo(1);
    });
  }

  @Test
  void testUnwindMatchWithIdEqualsRowProperty() {
    // Test UNWIND + MATCH with WHERE ID(a) = row.source_id
    final List<Map<String, Object>> batch = new ArrayList<>();
    for (String sourceId : sourceIds) {
      Map<String, Object> row = new HashMap<>();
      row.put("source_id", sourceId);
      batch.add(row);
    }

    //System.out.println("Testing UNWIND + MATCH with WHERE ID(a) = row.source_id");
    //System.out.println("Batch: " + batch);

    database.transaction(() -> {
      // First check what values are compared
      final ResultSet debugRs = database.query("opencypher",
          "UNWIND $batch as row MATCH (a:Source) " +
              "RETURN ID(a) as aid, row.source_id as sid, ID(a) = row.source_id as isEqual",
          Map.of("batch", batch));

      int debugCount = 0;
      while (debugRs.hasNext()) {
        final Result result = debugRs.next();
        //System.out.println("Debug " + debugCount + ": aid=" + result.getProperty("aid")
        //    + ", sid=" + result.getProperty("sid")
        //    + ", isEqual=" + result.getProperty("isEqual"));
        debugCount++;
      }
    });

    database.transaction(() -> {
      // Now the actual test - first let's verify the simpler case works
      //System.out.println("\n--- Testing WHERE with literal ID ---");
      final ResultSet literalRs = database.query("opencypher",
          "UNWIND $batch as row MATCH (a:Source) WHERE ID(a) = '#1:0' RETURN a, row.source_id as sid",
          Map.of("batch", batch));
      int literalCount = 0;
      while (literalRs.hasNext()) {
        final Result r = literalRs.next();
        //System.out.println("Literal result " + literalCount + ": a=" + r.getProperty("a") + ", sid=" + r.getProperty("sid"));
        literalCount++;
      }
      //System.out.println("Literal WHERE returned " + literalCount + " results");

      //System.out.println("\n--- Testing WHERE with row.source_id ---");
      final ResultSet rs = database.query("opencypher",
          "UNWIND $batch as row MATCH (a:Source) WHERE ID(a) = row.source_id RETURN a",
          Map.of("batch", batch));

      int count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        //System.out.println("Result " + count + ": " + result.toJSON());
        count++;
      }

      assertThat(count)
          .as("WHERE ID(a) = row.source_id should return 3 results (one per batch row)")
          .isEqualTo(3);
    });
  }

  /**
   * Test for GitHub issue #1948: UNWIND + MATCH with type constraint and same source_id.
   * When batch entries have the SAME source_id but different target_ids,
   * the query should return results for ALL batch entries, not just the first one.
   */
  @Test
  void testIssue1948UnwindMatchWithSameSourceId() {
    // Create additional Target vertices
    final List<String> targetIds = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 3; i++) {
        MutableVertex target = database.newVertex("Target");
        target.set("name", "multiTarget" + i);
        target.save();
        targetIds.add(target.getIdentity().toString());
      }
    });

    // Use the FIRST source ID for ALL batch entries (key scenario from issue #1948)
    final String sameSourceId = sourceIds.get(0);
    final List<Map<String, Object>> batch = new ArrayList<>();
    for (String targetIdItem : targetIds) {
      Map<String, Object> row = new HashMap<>();
      row.put("source_id", sameSourceId); // Same source_id for all entries
      row.put("target_id", targetIdItem);  // Different target_ids
      batch.add(row);
    }

    // Execute UNWIND + MATCH with type constraint + MERGE (exact pattern from issue #1948)
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "UNWIND $batch as row " +
              "MATCH (a:Source) WHERE ID(a) = row.source_id " +
              "MATCH (b) WHERE ID(b) = row.target_id " +
              "MERGE (a)-[r:in]->(b) " +
              "RETURN a, b, r",
          Map.of("batch", batch));

      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }

      // Bug in issue #1948: Only 1 result returned instead of 3
      // Fix: Should return 3 results (one for each batch entry with different target_id)
      assertThat(count)
          .as("Issue #1948: With same source_id but different target_ids, should return 3 results")
          .isEqualTo(3);
    });

    // Verify all 3 relationships were created
    final ResultSet verifyResult = database.query("opencypher",
        "MATCH (a:Source {name: 'source0'})-[r:in]->(b:Target) RETURN count(r) AS count");
    final long edgeCount = ((Number) verifyResult.next().getProperty("count")).longValue();

    assertThat(edgeCount)
        .as("Should have created 3 relationships from the same source")
        .isEqualTo(3);
  }

  /**
   * Simpler test for issue #1948: UNWIND + MATCH with type constraint and same ID.
   * Tests just MATCH without MERGE to isolate the iteration issue.
   */
  @Test
  void testIssue1948SimpleMatchWithSameSourceId() {
    // Use the FIRST source ID for ALL batch entries
    final String sameSourceId = sourceIds.get(0);
    final List<Map<String, Object>> batch = new ArrayList<>();
    // Add the same source_id multiple times
    batch.add(Map.of("source_id", sameSourceId));
    batch.add(Map.of("source_id", sameSourceId));
    batch.add(Map.of("source_id", sameSourceId));

    // Execute UNWIND + MATCH with type constraint only (no MERGE)
    final ResultSet rs = database.query("opencypher",
        "UNWIND $batch as row " +
            "MATCH (a:Source) WHERE ID(a) = row.source_id " +
            "RETURN a, row",
        Map.of("batch", batch));

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }

    // Should return 3 results (one for each batch entry, even though they all match the same vertex)
    assertThat(count)
        .as("Issue #1948: UNWIND + MATCH with same source_id should return one result per batch entry")
        .isEqualTo(3);
  }
}
