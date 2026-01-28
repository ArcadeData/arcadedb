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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test case for GitHub issue #3271.
 * Tests multi-hop MATCH patterns where the query engine incorrectly uses the first node's
 * variable as the source for all relationship hops, instead of properly chaining through
 * intermediate nodes.
 * <p>
 * The problem: In a pattern like {@code (n)--(chunk)--(document)}, the engine was incorrectly
 * trying to match both relationships from {@code n}, rather than matching {@code n--chunk} and
 * then {@code chunk--document}.
 * <p>
 * Example failing query from issue:
 * <pre>
 * MATCH (n {subtype:"DATE"})
 * MATCH (n)-[r]-(m:NER)
 * MATCH (n)--(chunk:CHUNK)--(document:DOCUMENT)
 * RETURN n.name AS name
 * </pre>
 */
class Issue3271Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-3271").create();

    // Create schema matching the bug report scenario
    database.getSchema().createVertexType("NER");
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createEdgeType("CONTAINS");
    database.getSchema().createEdgeType("RELATED_TO");

    database.transaction(() -> {
      // Create structure: DOCUMENT -> CHUNK -> NER (DATE)
      //                                     -> NER (PERSON)
      //                   NER (DATE) <--RELATED_TO--> NER (PERSON)
      database.command("opencypher", "CREATE (d:DOCUMENT {name: 'Document1'})");
      database.command("opencypher", "CREATE (c:CHUNK {name: 'Chunk1', text: 'Sample text'})");
      database.command("opencypher", "CREATE (n1:NER {name: 'DateEntity', subtype: 'DATE'})");
      database.command("opencypher", "CREATE (n2:NER {name: 'PersonEntity', subtype: 'PERSON'})");

      // Create the graph: DOCUMENT -[CONTAINS]-> CHUNK -[CONTAINS]-> NERs
      database.command("opencypher",
          "MATCH (d:DOCUMENT {name: 'Document1'}), (c:CHUNK {name: 'Chunk1'}) " +
              "CREATE (d)-[:CONTAINS]->(c)");
      database.command("opencypher",
          "MATCH (c:CHUNK {name: 'Chunk1'}), (n:NER {name: 'DateEntity'}) " +
              "CREATE (c)-[:CONTAINS]->(n)");
      database.command("opencypher",
          "MATCH (c:CHUNK {name: 'Chunk1'}), (n:NER {name: 'PersonEntity'}) " +
              "CREATE (c)-[:CONTAINS]->(n)");

      // Create NER-to-NER relationship (mimics co-occurrence in text)
      database.command("opencypher",
          "MATCH (n1:NER {name: 'DateEntity'}), (n2:NER {name: 'PersonEntity'}) " +
              "CREATE (n1)-[:RELATED_TO]->(n2)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Test the simplified query from the bug report.
   * The multi-hop pattern (n)--(chunk:CHUNK)--(document:DOCUMENT) should correctly
   * traverse from n -> chunk -> document, not n -> chunk AND n -> document.
   */
  @Test
  void testMultiHopPatternInSingleMatch() {
    // This is the core issue: multi-hop pattern in single MATCH
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:NER {subtype: 'DATE'})--(chunk:CHUNK)--(doc:DOCUMENT) " +
            "RETURN n.name AS name, chunk.name AS chunkName, doc.name AS docName");

    List<Result> results = new ArrayList<>();
    while (rs.hasNext()) {
      results.add(rs.next());
    }
    rs.close();

    assertThat(results)
        .as("Multi-hop pattern should traverse NER -> CHUNK -> DOCUMENT")
        .isNotEmpty();

    Result row = results.get(0);
    assertEquals("DateEntity", row.getProperty("name"));
    assertEquals("Chunk1", row.getProperty("chunkName"));
    assertEquals("Document1", row.getProperty("docName"));
  }

  /**
   * Test the exact query structure from the bug report with multiple MATCH clauses
   * where the third MATCH has a multi-hop pattern.
   */
  @Test
  void testBugReportQueryWithMultipleMATCH() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n {subtype: 'DATE'}) " +
            "MATCH (n)-[r]-(m:NER) " +
            "MATCH (n)--(chunk:CHUNK)--(document:DOCUMENT) " +
            "RETURN n.name AS name");

    List<Result> results = new ArrayList<>();
    while (rs.hasNext()) {
      results.add(rs.next());
    }
    rs.close();

    assertThat(results)
        .as("Query with multiple MATCH and multi-hop pattern should return results")
        .isNotEmpty();

    // The DATE entity is connected to PERSON entity via CHUNK
    // So we should get at least one result
    assertEquals("DateEntity", results.get(0).getProperty("name"));
  }

  /**
   * Test three-hop pattern: A -> B -> C -> D.
   * Ensures the chaining is correct for longer patterns.
   */
  @Test
  void testThreeHopPattern() {
    // Create additional types for this test
    database.getSchema().createVertexType("NodeA");
    database.getSchema().createVertexType("NodeB");
    database.getSchema().createVertexType("NodeC");
    database.getSchema().createVertexType("NodeD");
    database.getSchema().createEdgeType("CONNECTS");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:NodeA {name: 'A', start: true})");
      database.command("opencypher", "CREATE (b:NodeB {name: 'B'})");
      database.command("opencypher", "CREATE (c:NodeC {name: 'C'})");
      database.command("opencypher", "CREATE (d:NodeD {name: 'D', end: true})");

      database.command("opencypher",
          "MATCH (a:NodeA {name: 'A'}), (b:NodeB {name: 'B'}) CREATE (a)-[:CONNECTS]->(b)");
      database.command("opencypher",
          "MATCH (b:NodeB {name: 'B'}), (c:NodeC {name: 'C'}) CREATE (b)-[:CONNECTS]->(c)");
      database.command("opencypher",
          "MATCH (c:NodeC {name: 'C'}), (d:NodeD {name: 'D'}) CREATE (c)-[:CONNECTS]->(d)");
    });

    // Three-hop pattern in single MATCH
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:NodeA {start: true})--(b)--(c)--(d:NodeD {end: true}) " +
            "RETURN a.name AS a, b.name AS b, c.name AS c, d.name AS d");

    List<Result> results = new ArrayList<>();
    while (rs.hasNext()) {
      results.add(rs.next());
    }
    rs.close();

    assertThat(results)
        .as("Three-hop pattern should find path A->B->C->D")
        .isNotEmpty();

    Result row = results.get(0);
    assertEquals("A", row.getProperty("a"));
    assertEquals("B", row.getProperty("b"));
    assertEquals("C", row.getProperty("c"));
    assertEquals("D", row.getProperty("d"));
  }

  /**
   * Test the complex query with COLLECT and HEAD from the bug report.
   */
  @Test
  void testComplexQueryWithCollectAndHead() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n {subtype: 'DATE'}) " +
            "MATCH (n)-[r]-(m:NER) " +
            "MATCH (n)--(chunk:CHUNK)--(document:DOCUMENT) " +
            "RETURN n.name AS name, " +
            "COLLECT(DISTINCT {typeO: n.subtype, nameO: n.name, typeR: type(r), typeT: m.subtype, nameT: m.name}) AS relations, " +
            "HEAD(COLLECT(DISTINCT {text: chunk.text, doc_name: document.name})) AS context");

    List<Result> results = new ArrayList<>();
    while (rs.hasNext()) {
      results.add(rs.next());
    }
    rs.close();

    assertThat(results)
        .as("Complex query with COLLECT and HEAD should return results")
        .isNotEmpty();

    Result row = results.get(0);
    assertEquals("DateEntity", row.getProperty("name"));

    // Verify relations is a list
    Object relations = row.getProperty("relations");
    assertTrue(relations instanceof List, "relations should be a List");

    // Verify context is a map (from HEAD(COLLECT(...)))
    Object context = row.getProperty("context");
    assertNotNull(context, "HEAD(COLLECT(...)) should return a map, not null");
    assertTrue(context instanceof Map, "context should be a Map");
  }

  /**
   * Verify that single-hop patterns still work correctly (regression test).
   */
  @Test
  void testSingleHopPatternStillWorks() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:NER {subtype: 'DATE'})--(chunk:CHUNK) " +
            "RETURN n.name AS name, chunk.name AS chunkName");

    List<Result> results = new ArrayList<>();
    while (rs.hasNext()) {
      results.add(rs.next());
    }
    rs.close();

    assertThat(results).isNotEmpty();
    assertEquals("DateEntity", results.get(0).getProperty("name"));
    assertEquals("Chunk1", results.get(0).getProperty("chunkName"));
  }

  /**
   * Test bidirectional traversal in multi-hop pattern.
   * Query from document backwards through chunk to NER.
   */
  @Test
  void testMultiHopReverseDirection() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (doc:DOCUMENT)--(chunk:CHUNK)--(n:NER {subtype: 'DATE'}) " +
            "RETURN doc.name AS docName, chunk.name AS chunkName, n.name AS name");

    List<Result> results = new ArrayList<>();
    while (rs.hasNext()) {
      results.add(rs.next());
    }
    rs.close();

    assertThat(results)
        .as("Reverse multi-hop pattern should also work")
        .isNotEmpty();

    // Find the result with DateEntity (there should be exactly one with subtype DATE filter)
    boolean foundDateEntity = false;
    for (Result row : results) {
      if ("DateEntity".equals(row.getProperty("name"))) {
        foundDateEntity = true;
        assertEquals("Document1", row.getProperty("docName"));
        assertEquals("Chunk1", row.getProperty("chunkName"));
        break;
      }
    }
    assertTrue(foundDateEntity, "Should find DateEntity in results with {subtype: 'DATE'} filter");
  }
}
