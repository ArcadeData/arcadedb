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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3403: Adding parentheses around NOT operator changes the result.
 * Tests that `WHERE NOT (pattern)` and `WHERE (NOT (pattern))` return the same results.
 */
class Issue3403Test {
  private Database database;
  private static final String DB_PATH = "./target/databases/test-issue3403";

  @BeforeEach
  void setUp() {
    // Delete any existing database
    new File(DB_PATH).mkdirs();
    database = new DatabaseFactory(DB_PATH).create();

    // Create schema
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("IMAGE");
    database.getSchema().createEdgeType("in");

    // Create test data that matches the issue scenario
    database.transaction(() -> {
      // Create 5 DOCUMENT nodes
      database.command("opencypher", "CREATE (d:DOCUMENT {id: 1})");
      database.command("opencypher", "CREATE (d:DOCUMENT {id: 2})");
      database.command("opencypher", "CREATE (d:DOCUMENT {id: 3})");
      database.command("opencypher", "CREATE (d:DOCUMENT {id: 4})");
      database.command("opencypher", "CREATE (d:DOCUMENT {id: 5})");

      // Create 25 CHUNK nodes
      for (int i = 1; i <= 25; i++) {
        database.command("opencypher", "CREATE (c:CHUNK {id: " + i + "})");
      }

      // Create 3 IMAGE nodes
      database.command("opencypher", "CREATE (i:IMAGE {id: 1})");
      database.command("opencypher", "CREATE (i:IMAGE {id: 2})");
      database.command("opencypher", "CREATE (i:IMAGE {id: 3})");

      // Connect CHUNKs to DOCUMENTs with "in" edges
      // Each DOCUMENT has 5 CHUNKs connected to it
      for (int docId = 1; docId <= 5; docId++) {
        for (int chunkOffset = 0; chunkOffset < 5; chunkOffset++) {
          int chunkId = (docId - 1) * 5 + chunkOffset + 1;
          database.command("opencypher",
              "MATCH (d:DOCUMENT {id: " + docId + "}), (c:CHUNK {id: " + chunkId + "}) " +
              "CREATE (c)-[:in]->(d)");
        }
      }

      // Connect some CHUNKs to IMAGE nodes (to create the scenario where some chunks are connected to images)
      // Let's say chunks 1-8 are connected to images, leaving 17 chunks not connected to images
      // But we want some DOCUMENTs to have all their chunks connected to images, and some not
      for (int chunkId = 1; chunkId <= 17; chunkId++) {
        int imageId = ((chunkId - 1) % 3) + 1;
        database.command("opencypher",
            "MATCH (c:CHUNK {id: " + chunkId + "}), (i:IMAGE {id: " + imageId + "}) " +
            "CREATE (c)-[:connected_to]->(i)");
      }
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
  void testNotOperatorWithoutParentheses() {
    // Query without extra parentheses around NOT
    final String query1 = "MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) " +
                          "WHERE NOT (chunk:CHUNK)--(:IMAGE) " +
                          "RETURN nodeDOc, chunk";

    long count1;
    try (final ResultSet rs = database.query("opencypher", query1)) {
      count1 = rs.stream().count();
    }

    System.out.println("Query without extra parentheses returned: " + count1 + " records");

    // Verify we get the expected results (chunks not connected to any IMAGE)
    assertThat(count1).isGreaterThan(0);
  }

  @Test
  void testNotOperatorWithParentheses() {
    // Query with extra parentheses around NOT expression
    final String query2 = "MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) " +
                          "WHERE (NOT (chunk:CHUNK)--(:IMAGE)) " +
                          "RETURN nodeDOc, chunk";

    long count2;
    try (final ResultSet rs = database.query("opencypher", query2)) {
      count2 = rs.stream().count();
    }

    System.out.println("Query with extra parentheses returned: " + count2 + " records");

    // Verify we get the expected results (chunks not connected to any IMAGE)
    assertThat(count2).isGreaterThan(0);
  }

  @Test
  void testBothQueriesReturnSameResults() {
    // Query without extra parentheses around NOT
    final String query1 = "MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) " +
                          "WHERE NOT (chunk:CHUNK)--(:IMAGE) " +
                          "RETURN nodeDOc, chunk";

    // Query with extra parentheses around NOT expression
    final String query2 = "MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) " +
                          "WHERE (NOT (chunk:CHUNK)--(:IMAGE)) " +
                          "RETURN nodeDOc, chunk";

    long count1;
    try (final ResultSet rs = database.query("opencypher", query1)) {
      count1 = rs.stream().count();
    }

    long count2;
    try (final ResultSet rs = database.query("opencypher", query2)) {
      count2 = rs.stream().count();
    }

    System.out.println("Query 1 (without extra parentheses): " + count1 + " records");
    System.out.println("Query 2 (with extra parentheses): " + count2 + " records");

    // Both queries should return the same number of results
    assertThat(count1).isEqualTo(count2)
        .withFailMessage("Expected both queries to return the same number of results, but got " +
                        count1 + " vs " + count2);
  }

  @Test
  void testSimpleNotPattern() {
    // Simplified test to verify NOT pattern predicate works correctly
    final String query1 = "MATCH (c:CHUNK) WHERE NOT (c)--(:IMAGE) RETURN c";
    final String query2 = "MATCH (c:CHUNK) WHERE (NOT (c)--(:IMAGE)) RETURN c";

    long count1;
    try (final ResultSet rs = database.query("opencypher", query1)) {
      count1 = rs.stream().count();
    }

    long count2;
    try (final ResultSet rs = database.query("opencypher", query2)) {
      count2 = rs.stream().count();
    }

    System.out.println("Simple NOT pattern - Query 1: " + count1 + " records");
    System.out.println("Simple NOT pattern - Query 2: " + count2 + " records");

    // Both should return 8 chunks (those not connected to any IMAGE)
    assertThat(count1).isEqualTo(count2)
        .withFailMessage("Simple NOT pattern: Expected both queries to return the same number of results");
  }

  @Test
  void testTripleParentheses() {
    // Test with triple parentheses to verify fix works with multiple nesting levels
    final String query1 = "MATCH (c:CHUNK) WHERE NOT (c)--(:IMAGE) RETURN c";
    final String query2 = "MATCH (c:CHUNK) WHERE (((NOT (c)--(:IMAGE)))) RETURN c";

    long count1;
    try (final ResultSet rs = database.query("opencypher", query1)) {
      count1 = rs.stream().count();
    }

    long count2;
    try (final ResultSet rs = database.query("opencypher", query2)) {
      count2 = rs.stream().count();
    }

    System.out.println("Triple parentheses - Query 1: " + count1 + " records");
    System.out.println("Triple parentheses - Query 2: " + count2 + " records");

    assertThat(count1).isEqualTo(count2)
        .withFailMessage("Triple parentheses: Expected both queries to return the same number of results");
  }

  @Test
  void testMultipleParenthesesAroundPattern() {
    // Test with multiple parentheses around the pattern itself
    final String query1 = "MATCH (c:CHUNK) WHERE NOT (c)--(:IMAGE) RETURN c";
    final String query2 = "MATCH (c:CHUNK) WHERE NOT (((c)--(:IMAGE))) RETURN c";

    long count1;
    try (final ResultSet rs = database.query("opencypher", query1)) {
      count1 = rs.stream().count();
    }

    long count2;
    try (final ResultSet rs = database.query("opencypher", query2)) {
      count2 = rs.stream().count();
    }

    System.out.println("Multiple parentheses around pattern - Query 1: " + count1 + " records");
    System.out.println("Multiple parentheses around pattern - Query 2: " + count2 + " records");

    assertThat(count1).isEqualTo(count2)
        .withFailMessage("Multiple parentheses around pattern: Expected both queries to return the same number of results");
  }

  @Test
  void testComplexOriginalQueryWithTripleParentheses() {
    // Test the original complex query from the issue with triple parentheses
    final String query1 = "MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) " +
                          "WHERE NOT (chunk:CHUNK)--(:IMAGE) " +
                          "RETURN nodeDOc, chunk";

    final String query2 = "MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) " +
                          "WHERE (((NOT (chunk:CHUNK)--(:IMAGE)))) " +
                          "RETURN nodeDOc, chunk";

    long count1;
    try (final ResultSet rs = database.query("opencypher", query1)) {
      count1 = rs.stream().count();
    }

    long count2;
    try (final ResultSet rs = database.query("opencypher", query2)) {
      count2 = rs.stream().count();
    }

    System.out.println("Complex query with triple parentheses - Query 1: " + count1 + " records");
    System.out.println("Complex query with triple parentheses - Query 2: " + count2 + " records");

    assertThat(count1).isEqualTo(count2)
        .withFailMessage("Complex query with triple parentheses: Expected both queries to return the same number of results");
  }
}
