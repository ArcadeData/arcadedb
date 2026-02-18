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

/**
 * Regression test for GitHub issue #3271.
 * Tests multi-hop MATCH patterns with COLLECT(DISTINCT {...}) map literals,
 * HEAD() function, and type() function inside map literals.
 * <p>
 * The issue is that the new Cypher engine fails to return correct results
 * for complex queries with map literals in COLLECT(DISTINCT {...}) and HEAD().
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/3271">Issue 3271</a>
 */
class Issue3271Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue3271-test").create();

    // Create types matching the issue's data model:
    // NER nodes with subtype (DATE, PERSON, ORG, etc.)
    // CHUNK nodes containing text
    // DOCUMENT nodes
    // Edges between NER nodes and from NER to CHUNK to DOCUMENT
    database.getSchema().createVertexType("NER");
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createEdgeType("RELATED_TO");
    database.getSchema().createEdgeType("IN_CHUNK");
    database.getSchema().createEdgeType("FROM_DOC");

    // Create test data
    database.transaction(() -> {
      database.command("opencypher",
          // Create documents
          "CREATE (doc1:DOCUMENT {name: 'Document1'}), " +
              "(doc2:DOCUMENT {name: 'Document2'}), " +
              // Create chunks linked to documents
              "(chunk1:CHUNK {text: 'This is chunk 1 text'}), " +
              "(chunk2:CHUNK {text: 'This is chunk 2 text'}), " +
              "(chunk1)-[:FROM_DOC]->(doc1), " +
              "(chunk2)-[:FROM_DOC]->(doc2), " +
              // Create NER nodes with subtype DATE
              "(date1:NER {subtype: 'DATE', name: '2024-01-15'}), " +
              "(date2:NER {subtype: 'DATE', name: '2024-02-20'}), " +
              // Create NER nodes with other subtypes
              "(person1:NER {subtype: 'PERSON', name: 'John Doe'}), " +
              "(person2:NER {subtype: 'PERSON', name: 'Jane Smith'}), " +
              "(org1:NER {subtype: 'ORG', name: 'ACME Corp'}), " +
              // Create relationships between NER nodes
              "(date1)-[:RELATED_TO]->(person1), " +
              "(date1)-[:RELATED_TO]->(org1), " +
              "(date2)-[:RELATED_TO]->(person2), " +
              // Link NER nodes to chunks
              "(date1)-[:IN_CHUNK]->(chunk1), " +
              "(date2)-[:IN_CHUNK]->(chunk2), " +
              "(person1)-[:IN_CHUNK]->(chunk1), " +
              "(person2)-[:IN_CHUNK]->(chunk2), " +
              "(org1)-[:IN_CHUNK]->(chunk1)");
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
   * Test the simplified query that was reported as working in the issue.
   * This serves as a baseline to confirm the basic multi-hop MATCH works.
   */
  @Test
  void simplifiedMultiMatchQuery() {
    // Simplified query from issue - this should work
    ResultSet rs = database.query("opencypher",
        """
        MATCH (n:NER {subtype:'DATE'}) \
        MATCH (n)-[r:RELATED_TO]-(m:NER) \
        MATCH (n)-[:IN_CHUNK]-(chunk:CHUNK)-[:FROM_DOC]->(document:DOCUMENT) \
        RETURN n.name AS name""");

    List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      Result result = rs.next();
      names.add((String) result.getProperty("name"));
    }

    // date1 has 2 relations (person1, org1), date2 has 1 relation (person2)
    // So we should get date1 twice and date2 once = 3 results
    assertThat(names).hasSize(3);
    assertThat(names).contains("2024-01-15", "2024-02-20");
  }

  /**
   * Test COLLECT with map literals containing property accesses.
   * This is a building block for the full issue query.
   */
  @Test
  void collectWithMapLiteral() {
    ResultSet rs = database.query("opencypher",
        """
        MATCH (n:NER {subtype:'DATE'}) \
        MATCH (n)-[r:RELATED_TO]-(m:NER) \
        RETURN n.name AS name, \
        COLLECT({nameT: m.name, typeT: m.subtype}) AS relations""");

    List<Result> results = new ArrayList<>();
    while (rs.hasNext()) {
      results.add(rs.next());
    }

    // Should have 2 rows: one for each DATE node
    assertThat(results).hasSize(2);

    for (Result row : results) {
      String name = (String) row.getProperty("name");
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> relations = (List<Map<String, Object>>) row.getProperty("relations");
      assertThat(relations).isNotNull();

      if ("2024-01-15".equals(name)) {
        // date1 has relations to person1 and org1
        assertThat(relations).hasSize(2);
      } else if ("2024-02-20".equals(name)) {
        // date2 has relation to person2
        assertThat(relations).hasSize(1);
      }
    }
  }

  /**
   * Test COLLECT(DISTINCT {...}) with map literals.
   * The DISTINCT modifier should work with map literals.
   */
  @Test
  void collectDistinctWithMapLiteral() {
    ResultSet rs = database.query("opencypher",
        """
        MATCH (n:NER {subtype:'DATE'}) \
        MATCH (n)-[r:RELATED_TO]-(m:NER) \
        RETURN n.name AS name, \
        COLLECT(DISTINCT {nameT: m.name, typeT: m.subtype}) AS relations""");

    List<Result> results = new ArrayList<>();
    while (rs.hasNext()) {
      results.add(rs.next());
    }

    assertThat(results).hasSize(2);

    for (Result row : results) {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> relations = (List<Map<String, Object>>) row.getProperty("relations");
      assertThat(relations).isNotNull();

      // Verify the maps have the expected structure
      for (Map<String, Object> relation : relations) {
        assertThat(relation).containsKeys("nameT", "typeT");
      }
    }
  }

  /**
   * Test type() function inside map literals.
   * This tests the ability to use functions like type(r) in map expressions.
   */
  @Test
  void typeInMapLiteral() {
    ResultSet rs = database.query("opencypher",
        """
        MATCH (n:NER {subtype:'DATE'}) \
        MATCH (n)-[r:RELATED_TO]-(m:NER) \
        RETURN n.name AS name, \
        COLLECT({typeR: type(r), nameT: m.name}) AS relations""");

    List<Result> results = new ArrayList<>();
    while (rs.hasNext()) {
      results.add(rs.next());
    }

    assertThat(results).hasSize(2);

    for (Result row : results) {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> relations = (List<Map<String, Object>>) row.getProperty("relations");
      assertThat(relations).isNotNull();

      // Verify type(r) is correctly resolved
      for (Map<String, Object> relation : relations) {
        assertThat(relation.get("typeR")).isEqualTo("RELATED_TO");
        assertThat(relation.get("nameT")).isNotNull();
      }
    }
  }

  /**
   * Test HEAD(COLLECT(...)) combination.
   * HEAD should return the first element from a collected list.
   */
  @Test
  void headCollect() {
    ResultSet rs = database.query("opencypher",
        """
        MATCH (n:NER {subtype:'DATE'}) \
        MATCH (n)-[:IN_CHUNK]-(chunk:CHUNK)-[:FROM_DOC]->(document:DOCUMENT) \
        RETURN n.name AS name, \
        HEAD(COLLECT({text: chunk.text, doc_name: document.name})) AS context""");

    List<Result> results = new ArrayList<>();
    while (rs.hasNext()) {
      results.add(rs.next());
    }

    assertThat(results).hasSize(2);

    for (Result row : results) {
      @SuppressWarnings("unchecked")
      Map<String, Object> context = (Map<String, Object>) row.getProperty("context");
      assertThat(context).isNotNull();
      assertThat(context).containsKeys("text", "doc_name");
    }
  }

  /**
   * Test HEAD(COLLECT(DISTINCT {...})) - combining HEAD and COLLECT DISTINCT with maps.
   */
  @Test
  void headCollectDistinctWithMapLiteral() {
    ResultSet rs = database.query("opencypher",
        """
        MATCH (n:NER {subtype:'DATE'}) \
        MATCH (n)-[:IN_CHUNK]-(chunk:CHUNK)-[:FROM_DOC]->(document:DOCUMENT) \
        RETURN n.name AS name, \
        HEAD(COLLECT(DISTINCT {text: chunk.text, doc_name: document.name})) AS context""");

    List<Result> results = new ArrayList<>();
    while (rs.hasNext()) {
      results.add(rs.next());
    }

    assertThat(results).hasSize(2);

    for (Result row : results) {
      @SuppressWarnings("unchecked")
      Map<String, Object> context = (Map<String, Object>) row.getProperty("context");
      assertThat(context).isNotNull();
      assertThat(context).containsKeys("text", "doc_name");
    }
  }

  /**
   * Test ID() function inside map literals.
   */
  @Test
  void idInMapLiteral() {
    ResultSet rs = database.query("opencypher",
        """
        MATCH (n:NER {subtype:'DATE'}) \
        MATCH (n)-[:IN_CHUNK]-(chunk:CHUNK)-[:FROM_DOC]->(document:DOCUMENT) \
        RETURN n.name AS name, \
        COLLECT({doc_name: document.name, doc_id: ID(document)}) AS contexts""");

    List<Result> results = new ArrayList<>();
    while (rs.hasNext()) {
      results.add(rs.next());
    }

    assertThat(results).hasSize(2);

    for (Result row : results) {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> contexts = (List<Map<String, Object>>) row.getProperty("contexts");
      assertThat(contexts).isNotNull();

      for (Map<String, Object> context : contexts) {
        assertThat(context.get("doc_name")).isNotNull();
        assertThat(context.get("doc_id")).isNotNull();
      }
    }
  }

  /**
   * The full query from issue #3271.
   * This combines multiple MATCH patterns, COLLECT(DISTINCT {...}) with type(),
   * and HEAD(COLLECT(DISTINCT {...})) with ID().
   */
  @Test
  void fullIssue3271Query() {
    ResultSet rs = database.query("opencypher",
        """
        MATCH (n:NER {subtype:'DATE'}) \
        MATCH (n)-[r:RELATED_TO]-(m:NER) \
        MATCH (n)-[:IN_CHUNK]-(chunk:CHUNK)-[:FROM_DOC]->(document:DOCUMENT) \
        RETURN n.name AS name, \
        COLLECT(DISTINCT {typeO: n.subtype, nameO: n.name, typeR: type(r), typeT: m.subtype, nameT: m.name}) AS relations, \
        HEAD(COLLECT(DISTINCT {text: chunk.text, doc_name: document.name, doc_id: ID(document)})) AS context""");

    List<Result> results = new ArrayList<>();
    while (rs.hasNext()) {
      results.add(rs.next());
    }

    // We expect results - the new engine was returning 0 results
    assertThat(results).isNotEmpty();
    assertThat(results).hasSize(2);

    for (Result row : results) {
      String name = (String) row.getProperty("name");
      assertThat(name).isNotNull();
      assertThat(name).isIn("2024-01-15", "2024-02-20");

      // Check relations
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> relations = (List<Map<String, Object>>) row.getProperty("relations");
      assertThat(relations).isNotNull();
      assertThat(relations).isNotEmpty();

      for (Map<String, Object> relation : relations) {
        assertThat(relation).containsKeys("typeO", "nameO", "typeR", "typeT", "nameT");
        assertThat(relation.get("typeO")).isEqualTo("DATE");
        assertThat(relation.get("typeR")).isEqualTo("RELATED_TO");
      }

      // Check context
      @SuppressWarnings("unchecked")
      Map<String, Object> context = (Map<String, Object>) row.getProperty("context");
      assertThat(context).isNotNull();
      assertThat(context).containsKeys("text", "doc_name", "doc_id");
    }
  }

  /**
   * Test with undirected edge pattern as in the original issue query.
   * The original query used (n)-[]-(chunk:CHUNK)-->(document:DOCUMENT)
   */
  @Test
  void undirectedEdgeInMultiMatch() {
    ResultSet rs = database.query("opencypher",
        """
        MATCH (n:NER {subtype:'DATE'}) \
        MATCH (n)-[r]-(m:NER) \
        MATCH (n)-[]-(chunk:CHUNK)-->(document:DOCUMENT) \
        RETURN n.name AS name""");

    List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      Result result = rs.next();
      names.add((String) result.getProperty("name"));
    }

    // Should return results for the DATE nodes
    assertThat(names).isNotEmpty();
  }
}
