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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for multi-hop MATCH patterns in Cypher queries.
 *
 * This test addresses the bug where multi-hop patterns in a single MATCH clause
 * like (n)--(chunk)--(document) would incorrectly use the first node's variable
 * as the source for all hops, instead of chaining properly.
 */
public class CypherMultiHopMatchTest {
  private Database database;

  @BeforeEach
  public void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-multihop").create();

    // Create schema that matches the bug report scenario:
    // NER nodes with subtype property
    // CHUNK nodes
    // DOCUMENT nodes
    database.getSchema().createVertexType("NER");
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createEdgeType("CONTAINS");
    database.getSchema().createEdgeType("FROM");

    database.transaction(() -> {
      // Create a document
      database.command("opencypher", "CREATE (d:DOCUMENT {name: 'Document1'})");

      // Create a chunk that belongs to the document
      database.command("opencypher", "CREATE (c:CHUNK {name: 'Chunk1'})");

      // Create NER nodes with different subtypes
      database.command("opencypher", "CREATE (n:NER {name: 'DateEntity', subtype: 'DATE'})");
      database.command("opencypher", "CREATE (n:NER {name: 'PersonEntity', subtype: 'PERSON'})");

      // Link document -> chunk
      database.command("opencypher",
          "MATCH (d:DOCUMENT {name: 'Document1'}), (c:CHUNK {name: 'Chunk1'}) " +
              "CREATE (d)-[:CONTAINS]->(c)");

      // Link chunk -> NER entities
      database.command("opencypher",
          "MATCH (c:CHUNK {name: 'Chunk1'}), (n:NER {name: 'DateEntity'}) " +
              "CREATE (c)-[:CONTAINS]->(n)");

      database.command("opencypher",
          "MATCH (c:CHUNK {name: 'Chunk1'}), (n:NER {name: 'PersonEntity'}) " +
              "CREATE (c)-[:CONTAINS]->(n)");
    });
  }

  @AfterEach
  public void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Test the exact query from the bug report:
   * MATCH (n {subtype:"DATE"})
   * MATCH (n)-[r]-(m:NER)
   * MATCH (n)--(chunk:CHUNK)--(document:DOCUMENT)
   * RETURN n.name AS name
   */
  @Test
  public void testMultipleMATCHWithMultiHopPattern() {
    // First verify the simpler queries work
    ResultSet result = database.query("opencypher",
        "MATCH (n {subtype: 'DATE'}) RETURN n.name AS name");
    assertTrue(result.hasNext(), "Should find the DATE entity");
    assertEquals("DateEntity", result.next().getProperty("name"));
    assertFalse(result.hasNext());
    result.close();

    // Now test the full query from the bug report
    result = database.query("opencypher",
        "MATCH (n {subtype: 'DATE'}) " +
            "MATCH (n)-[r]-(m:NER) " +
            "MATCH (n)--(chunk:CHUNK)--(document:DOCUMENT) " +
            "RETURN n.name AS name, chunk.name AS chunkName, document.name AS docName");

    List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Should find results - the DateEntity is connected to a CHUNK which is connected to a DOCUMENT
    assertFalse(results.isEmpty(),
        "Query should return results - the bug causes this to return nothing");

    // Verify the correct path was traversed
    Result row = results.get(0);
    assertEquals("DateEntity", row.getProperty("name"));
    assertEquals("Chunk1", row.getProperty("chunkName"));
    assertEquals("Document1", row.getProperty("docName"));
  }

  /**
   * Test multi-hop pattern in a single MATCH clause
   * MATCH (a)--(b)--(c) should traverse a->b then b->c, not a->b and a->c
   */
  @Test
  public void testMultiHopPatternInSingleMatch() {
    // Query: Find NER entities connected to chunks connected to documents
    // Path: NER <-- CHUNK <-- DOCUMENT
    ResultSet result = database.query("opencypher",
        "MATCH (n:NER {subtype: 'DATE'})--(chunk:CHUNK)--(doc:DOCUMENT) " +
            "RETURN n.name AS ner, chunk.name AS chunk, doc.name AS doc");

    List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    assertFalse(results.isEmpty(),
        "Multi-hop pattern should find the path NER<--CHUNK<--DOCUMENT");

    Result row = results.get(0);
    assertEquals("DateEntity", row.getProperty("ner"));
    assertEquals("Chunk1", row.getProperty("chunk"));
    assertEquals("Document1", row.getProperty("doc"));
  }

  /**
   * Test three-hop pattern
   * Create: A -> B -> C -> D
   * Query: MATCH (a)--(b)--(c)--(d) should traverse properly
   */
  @Test
  public void testThreeHopPattern() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("CONNECTS");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Node {name: 'A', start: true})");
      database.command("opencypher", "CREATE (b:Node {name: 'B'})");
      database.command("opencypher", "CREATE (c:Node {name: 'C'})");
      database.command("opencypher", "CREATE (d:Node {name: 'D', end: true})");

      database.command("opencypher",
          "MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:CONNECTS]->(b)");
      database.command("opencypher",
          "MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'}) CREATE (b)-[:CONNECTS]->(c)");
      database.command("opencypher",
          "MATCH (c:Node {name: 'C'}), (d:Node {name: 'D'}) CREATE (c)-[:CONNECTS]->(d)");
    });

    // Three-hop pattern: A -> B -> C -> D
    ResultSet result = database.query("opencypher",
        "MATCH (a:Node {start: true})--(b)--(c)--(d:Node {end: true}) " +
            "RETURN a.name AS a, b.name AS b, c.name AS c, d.name AS d");

    List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    assertFalse(results.isEmpty(), "Three-hop pattern should find the path A->B->C->D");

    Result row = results.get(0);
    assertEquals("A", row.getProperty("a"));
    assertEquals("B", row.getProperty("b"));
    assertEquals("C", row.getProperty("c"));
    assertEquals("D", row.getProperty("d"));
  }

  /**
   * Test multi-hop with property filtering at start node
   */
  @Test
  public void testMultiHopWithPropertyFilter() {
    // The bug scenario: matching by property, then multi-hop traversal
    ResultSet result = database.query("opencypher",
        "MATCH (n {subtype: 'DATE'})--(chunk:CHUNK)--(doc:DOCUMENT) " +
            "RETURN n.name AS name");

    List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    assertFalse(results.isEmpty(),
        "Should find DateEntity through multi-hop with property filter");
    assertEquals("DateEntity", results.get(0).getProperty("name"));
  }

  /**
   * Test COLLECT with map literal containing function calls.
   * This tests the bug where {typeR: type(r)} would be incorrectly parsed as just type(r).
   */
  @Test
  public void testCollectWithMapLiteralContainingFunction() {
    ResultSet result = database.query("opencypher",
        "MATCH (n:NER {subtype: 'DATE'})-[r]-(m:NER) " +
            "RETURN COLLECT(DISTINCT {typeO: n.subtype, nameO: n.name, typeR: type(r)}) AS relations");

    assertTrue(result.hasNext(), "Should return results");
    Result row = result.next();
    result.close();

    Object relationsObj = row.getProperty("relations");
    assertNotNull(relationsObj, "relations should not be null");
    assertTrue(relationsObj instanceof List, "relations should be a list");

    @SuppressWarnings("unchecked")
    List<Object> relations = (List<Object>) relationsObj;
    assertFalse(relations.isEmpty(), "relations list should not be empty");

    // Each item should be a map, not just a string
    Object firstRelation = relations.get(0);
    assertTrue(firstRelation instanceof Map,
        "Each relation should be a map, but got: " + firstRelation.getClass().getName());

    @SuppressWarnings("unchecked")
    Map<String, Object> relMap = (Map<String, Object>) firstRelation;
    assertTrue(relMap.containsKey("typeO"), "Map should contain typeO");
    assertTrue(relMap.containsKey("nameO"), "Map should contain nameO");
    assertTrue(relMap.containsKey("typeR"), "Map should contain typeR");
  }

  /**
   * Test HEAD(COLLECT(...)) - wrapper function around aggregation.
   * This tests the bug where HEAD(COLLECT(...)) returned null because HEAD was not
   * recognized as containing an aggregation.
   */
  @Test
  public void testHeadCollectWrappedAggregation() {
    ResultSet result = database.query("opencypher",
        "MATCH (n:NER {subtype: 'DATE'})--(chunk:CHUNK)--(doc:DOCUMENT) " +
            "RETURN n.name AS name, " +
            "HEAD(COLLECT(DISTINCT {text: chunk.name, doc_name: doc.name})) AS context");

    assertTrue(result.hasNext(), "Should return results");
    Result row = result.next();
    result.close();

    assertEquals("DateEntity", row.getProperty("name"));

    Object contextObj = row.getProperty("context");
    assertNotNull(contextObj, "context should not be null - HEAD(COLLECT(...)) should work");
    assertTrue(contextObj instanceof Map, "context should be a map");

    @SuppressWarnings("unchecked")
    Map<String, Object> contextMap = (Map<String, Object>) contextObj;
    assertTrue(contextMap.containsKey("text"), "Map should contain text");
    assertTrue(contextMap.containsKey("doc_name"), "Map should contain doc_name");
  }

  /**
   * Test the exact query from the bug report with all features combined:
   * - Multiple MATCH clauses
   * - Multi-hop patterns
   * - COLLECT with map literals containing type()
   * - HEAD(COLLECT(...))
   */
  @Test
  public void testFullQueryFromBugReport() {
    ResultSet result = database.query("opencypher",
        "MATCH (n {subtype: 'DATE'}) " +
            "MATCH (n)-[r]-(m:NER) " +
            "MATCH (n)--(chunk:CHUNK)--(document:DOCUMENT) " +
            "RETURN n.name AS name, " +
            "COLLECT(DISTINCT {typeO: n.subtype, nameO: n.name, typeR: type(r), typeT: m.subtype, nameT: m.name}) AS relations, " +
            "HEAD(COLLECT(DISTINCT {text: chunk.name, doc_name: document.name})) AS context");

    List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    assertFalse(results.isEmpty(), "Query should return results");

    Result row = results.get(0);
    assertEquals("DateEntity", row.getProperty("name"));

    // Check relations
    Object relationsObj = row.getProperty("relations");
    assertNotNull(relationsObj, "relations should not be null");
    assertTrue(relationsObj instanceof List, "relations should be a list");

    @SuppressWarnings("unchecked")
    List<Object> relations = (List<Object>) relationsObj;
    if (!relations.isEmpty()) {
      Object firstRelation = relations.get(0);
      assertTrue(firstRelation instanceof Map,
          "Each relation should be a map with typeO, nameO, typeR, typeT, nameT");
    }

    // Check context
    Object contextObj = row.getProperty("context");
    assertNotNull(contextObj, "context should not be null");
    assertTrue(contextObj instanceof Map, "context should be a map");
  }
}
