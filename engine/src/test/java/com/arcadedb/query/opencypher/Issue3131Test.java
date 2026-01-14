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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case for GitHub issue #3131.
 * Tests MATCH with ID() in WHERE clause followed by MERGE with relationship pattern.
 */
class Issue3131Test {
  private Database database;
  private RID sourceRid;
  private RID targetRid;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-3131").create();

    // Create schema
    database.getSchema().createVertexType("Node");

    // Create test nodes
    database.transaction(() -> {
      final MutableVertex source = database.newVertex("Node");
      source.set("name", "Source");
      source.save();
      sourceRid = source.getIdentity();

      final MutableVertex target = database.newVertex("Node");
      target.set("name", "Target");
      target.save();
      targetRid = target.getIdentity();
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testMatchWithIdThenMergeRelationship() {
    // This query should:
    // 1. MATCH two nodes by ID
    // 2. MERGE a relationship between them
    // 3. RETURN all three elements

    final ResultSet rs = database.command("opencypher",
        "MATCH (a), (b) WHERE ID(a) = $source_id AND ID(b) = $target_id " +
        "MERGE (a)-[r:in]->(b) " +
        "RETURN a, b, r",
        Map.of("source_id", sourceRid.toString(), "target_id", targetRid.toString()));

    assertThat(rs.hasNext()).as("Should return one result").isTrue();

    final Result result = rs.next();

    // Verify all three elements are returned
    assertThat(result.getPropertyNames()).as("Should return a, b, and r")
        .containsExactlyInAnyOrder("a", "b", "r");

    // Verify the nodes
    assertThat((Object) result.getProperty("a")).as("a should be a vertex").isNotNull();
    assertThat((Object) result.getProperty("b")).as("b should be a vertex").isNotNull();
    assertThat((Object) result.getProperty("r")).as("r should be an edge").isNotNull();

    System.out.println("Result: " + result.toJSON());

    // No more results
    assertThat(rs.hasNext()).as("Should only return one result").isFalse();
  }

  @Test
  void testMergeRelationshipIdempotency() {
    // First execution - creates the relationship
    database.command("opencypher",
        "MATCH (a), (b) WHERE ID(a) = $source_id AND ID(b) = $target_id " +
        "MERGE (a)-[r:in]->(b) " +
        "RETURN a, b, r",
        Map.of("source_id", sourceRid.toString(), "target_id", targetRid.toString()));

    // Second execution - should find the existing relationship
    final ResultSet rs = database.command("opencypher",
        "MATCH (a), (b) WHERE ID(a) = $source_id AND ID(b) = $target_id " +
        "MERGE (a)-[r:in]->(b) " +
        "RETURN a, b, r",
        Map.of("source_id", sourceRid.toString(), "target_id", targetRid.toString()));

    assertThat(rs.hasNext()).as("Should return one result").isTrue();

    final Result result = rs.next();
    assertThat(result.getPropertyNames()).containsExactlyInAnyOrder("a", "b", "r");

    // Verify only one relationship exists
    final ResultSet countRs = database.query("opencypher",
        "MATCH (a)-[r:in]->(b) WHERE ID(a) = $source_id AND ID(b) = $target_id RETURN count(r) AS count",
        Map.of("source_id", sourceRid.toString(), "target_id", targetRid.toString()));

    final long count = ((Number) countRs.next().getProperty("count")).longValue();
    assertThat(count).as("Should have exactly one relationship").isEqualTo(1);
  }
}
