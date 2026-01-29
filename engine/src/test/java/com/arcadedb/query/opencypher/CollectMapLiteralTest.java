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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case for COLLECT with map literals containing function calls.
 * Verifies that expressions like COLLECT(DISTINCT {typeR: type(r), nameT: m.name})
 * correctly return map objects, not just the function call result.
 *
 * This test covers the scenario from the bug report where:
 * - Old engine (cypher-to-gremlin): returns [{typeO: "DATE", typeR: "event_date", ...}]
 * - New engine (before fix): returns ["event_date"]
 */
public class CollectMapLiteralTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/collect-map-literal-test").create();
    database.getSchema().createVertexType("NER");
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createEdgeType("RELATED");
    database.getSchema().createEdgeType("CONTAINS");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testCollectDistinctMapWithFunctionCalls() {
    // Create test data similar to the bug report scenario
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (date1:NER {name: '2020-06-04', subtype: 'DATE'}), " +
              "(event1:NER {name: 'visit', subtype: 'EVENT'}), " +
              "(chunk1:CHUNK {text: 'Sample text'}), " +
              "(doc1:DOCUMENT {name: 'test-doc.pdf'}), " +
              "(date1)-[:RELATED {type: 'event_date'}]->(event1), " +
              "(date1)-[:CONTAINS]->(chunk1), " +
              "(chunk1)-[:CONTAINS]->(doc1)");
    });

    // Query similar to the bug report: COLLECT(DISTINCT {...}) with function call type(r)
    final ResultSet result = database.query("opencypher",
        "MATCH (n:NER {subtype: 'DATE'})-[r:RELATED]-(m:NER) " +
            "RETURN n.name AS name, " +
            "COLLECT(DISTINCT {typeO: n.subtype, nameO: n.name, typeR: type(r), typeT: m.subtype, nameT: m.name}) AS relations");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    // Verify name
    assertThat((String) row.getProperty("name")).isEqualTo("2020-06-04");

    // Verify relations is a list of maps, not just a list of strings
    @SuppressWarnings("unchecked")
    final List<Object> relations = (List<Object>) row.getProperty("relations");
    assertThat(relations).isNotNull();
    assertThat(relations).hasSize(1);

    // The first element should be a map with all the keys
    final Object firstRelation = relations.get(0);
    assertThat(firstRelation).isInstanceOf(Map.class);

    @SuppressWarnings("unchecked")
    final Map<String, Object> relationMap = (Map<String, Object>) firstRelation;
    assertThat(relationMap).containsKey("typeO");
    assertThat(relationMap).containsKey("nameO");
    assertThat(relationMap).containsKey("typeR");
    assertThat(relationMap).containsKey("typeT");
    assertThat(relationMap).containsKey("nameT");

    assertThat(relationMap.get("typeO")).isEqualTo("DATE");
    assertThat(relationMap.get("nameO")).isEqualTo("2020-06-04");
    assertThat(relationMap.get("typeR")).isEqualTo("RELATED");
    assertThat(relationMap.get("typeT")).isEqualTo("EVENT");
    assertThat(relationMap.get("nameT")).isEqualTo("visit");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testHeadCollectDistinctMapWithIdFunction() {
    // Create test data
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (date1:NER {name: '2020-06-04', subtype: 'DATE'}), " +
              "(chunk1:CHUNK {text: 'Sample text about the date'}), " +
              "(doc1:DOCUMENT {name: 'test-doc.pdf'}), " +
              "(date1)-[:CONTAINS]->(chunk1), " +
              "(chunk1)-[:CONTAINS]->(doc1)");
    });

    // Query similar to bug report: HEAD(COLLECT(DISTINCT {...})) with ID(document)
    final ResultSet result = database.query("opencypher",
        "MATCH (n:NER {subtype: 'DATE'})-[:CONTAINS]->(chunk:CHUNK)-[:CONTAINS]->(document:DOCUMENT) " +
            "RETURN n.name AS name, " +
            "HEAD(COLLECT(DISTINCT {text: chunk.text, doc_name: document.name, doc_id: ID(document)})) AS context");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    // Verify name
    assertThat((String) row.getProperty("name")).isEqualTo("2020-06-04");

    // Verify context is a map, not just a string (the RID)
    final Object context = row.getProperty("context");
    assertThat(context).isNotNull();
    assertThat(context).isInstanceOf(Map.class);

    @SuppressWarnings("unchecked")
    final Map<String, Object> contextMap = (Map<String, Object>) context;
    assertThat(contextMap).containsKey("text");
    assertThat(contextMap).containsKey("doc_name");
    assertThat(contextMap).containsKey("doc_id");

    assertThat(contextMap.get("text")).isEqualTo("Sample text about the date");
    assertThat(contextMap.get("doc_name")).isEqualTo("test-doc.pdf");
    // doc_id should be a string representation of the RID
    assertThat(contextMap.get("doc_id")).isNotNull();
    assertThat(contextMap.get("doc_id").toString()).startsWith("#");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testCollectMapWithMultipleFunctionCalls() {
    // Test with multiple function calls in the same map
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (p1:NER {name: 'Person1', subtype: 'PERSON'}), " +
              "(p2:NER {name: 'Person2', subtype: 'PERSON'}), " +
              "(p1)-[:KNOWS]->(p2)");
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (a:NER)-[r]-(b:NER) " +
            "RETURN COLLECT({fromId: ID(a), toId: ID(b), relType: type(r), fromLabels: labels(a)}) AS connections");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    @SuppressWarnings("unchecked")
    final List<Object> connections = (List<Object>) row.getProperty("connections");
    assertThat(connections).isNotNull();
    assertThat(connections).isNotEmpty();

    // Each connection should be a map with all the keys
    for (final Object conn : connections) {
      assertThat(conn).isInstanceOf(Map.class);
      @SuppressWarnings("unchecked")
      final Map<String, Object> connMap = (Map<String, Object>) conn;
      assertThat(connMap).containsKey("fromId");
      assertThat(connMap).containsKey("toId");
      assertThat(connMap).containsKey("relType");
      assertThat(connMap).containsKey("fromLabels");
    }
  }

  @Test
  void testSimpleMapLiteralInReturn() {
    // Test that a simple map literal is parsed correctly
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (p:NER {name: 'Test', subtype: 'TYPE'})");
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (n:NER) RETURN {name: n.name, type: n.subtype} AS info");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    final Object info = row.getProperty("info");
    assertThat(info).isInstanceOf(Map.class);

    @SuppressWarnings("unchecked")
    final Map<String, Object> infoMap = (Map<String, Object>) info;
    assertThat(infoMap.get("name")).isEqualTo("Test");
    assertThat(infoMap.get("type")).isEqualTo("TYPE");
  }

  @Test
  void testMapLiteralWithNestedFunctionCall() {
    // Test map literal with a function call as a value
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (p:NER {name: 'Test'})");
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (n:NER) RETURN {id: ID(n), name: n.name} AS info");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    final Object info = row.getProperty("info");
    assertThat(info).isInstanceOf(Map.class);

    @SuppressWarnings("unchecked")
    final Map<String, Object> infoMap = (Map<String, Object>) info;
    assertThat(infoMap).containsKey("id");
    assertThat(infoMap).containsKey("name");
    assertThat(infoMap.get("name")).isEqualTo("Test");
    // ID should be a string starting with #
    assertThat(infoMap.get("id").toString()).startsWith("#");
  }
}
